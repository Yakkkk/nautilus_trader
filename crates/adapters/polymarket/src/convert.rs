// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Conversion event mapping from loaded Polymarket CTF definitions.

use ahash::{AHashMap, AHashSet};
use alloy_primitives::{Address, B256, U256, address, keccak256};
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{
        Instrument, InstrumentAny,
        neg_risk::{CtfNegRiskEvent, NegRiskCondition, NegRiskOutcome},
    },
};
use ustr::Ustr;

use crate::{
    common::{
        consts::{MARKET_VERSION_KEY, POLYMARKET, POLYMARKET_VENUE, PUSD},
        enums::PolymarketOutcome,
        parse::validate_market_version,
    },
    http::{
        models::{GammaEvent, GammaMarket},
        parse::parse_market_outcomes,
    },
    providers::extract_token_id,
};

/// Verified Polygon `NegRiskAdapter` that derives CTF IDs and executes conversions.
const NEG_RISK_ADAPTER: Address = address!("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296");

/// Explicitly loaded metadata from the Polygon CTF Neg Risk Adapter at one block.
///
/// Use the adapter behind `NegRiskCtfCollateralAdapter` for these reads. Gamma's market
/// array is not an authoritative count: a truncated array can have consecutive indices.
#[derive(Clone, Copy, Debug)]
pub struct CtfNegRiskMarketMetadata<'a> {
    /// Chain market ID supplied to the adapter read methods.
    pub market_id: B256,
    /// Result of `getQuestionCount(market_id)`.
    pub question_count: u8,
    /// Result of `getFeeBips(market_id)`.
    pub fee_bps: u16,
    /// Results of `getPositionId(question_id, true/false)` as `[YES, NO]`, in chain index order.
    pub token_ids: &'a [[U256; 2]],
}

/// Builds a canonical conversion event for a loaded Polymarket outcome.
///
/// The anchor must retain the full Gamma parent event from event discovery. Every YES and NO
/// definition must be present in `instruments`; the parent response does not load any missing
/// instruments. This function makes no network calls and does not inspect positions.
///
/// # Errors
///
/// Returns an error if the anchor, parent, chain metadata, or any required CTF instrument
/// definition is missing, inconsistent, or unsupported.
pub fn build_neg_risk_event<'a>(
    anchor: InstrumentId,
    instruments: impl IntoIterator<Item = &'a InstrumentAny>,
    metadata: &CtfNegRiskMarketMetadata<'_>,
) -> anyhow::Result<CtfNegRiskEvent> {
    validate_metadata(metadata)?;

    let question_count = usize::from(metadata.question_count);
    let required_tokens = metadata
        .token_ids
        .iter()
        .flat_map(|[yes, no]| [*yes, *no])
        .collect::<AHashSet<_>>();
    let mut loaded_by_token = AHashMap::with_capacity(required_tokens.len());
    let mut loaded_by_id = AHashMap::with_capacity(required_tokens.len());

    for instrument in instruments {
        if instrument.id().venue != *POLYMARKET_VENUE {
            continue;
        }
        let id_token_raw = extract_token_id(&instrument.id()).ok();
        let id_token = id_token_raw
            .as_deref()
            .and_then(|token| U256::from_str_radix(token, 10).ok());
        if let Some(token_id) = id_token
            && required_tokens.contains(&token_id)
            && !matches!(instrument, InstrumentAny::BinaryOption(_))
        {
            anyhow::bail!(
                "Polymarket instrument {} for required conversion token {token_id} is not a binary \
                 option",
                instrument.id(),
            );
        }

        let InstrumentAny::BinaryOption(binary) = instrument else {
            continue;
        };
        let raw_token = U256::from_str_radix(binary.raw_symbol.as_str(), 10).ok();
        let id_relevant = id_token.is_some_and(|token| required_tokens.contains(&token));
        let raw_relevant = raw_token.is_some_and(|token| required_tokens.contains(&token));

        if !id_relevant && !raw_relevant {
            continue;
        }

        let Some(token_id) = id_token else {
            anyhow::bail!(
                "Polymarket instrument {} for a required conversion token has a malformed ID token \
                 and raw symbol {}",
                instrument.id(),
                binary.raw_symbol,
            );
        };
        anyhow::ensure!(
            raw_token == Some(token_id),
            "Polymarket instrument {} for a required conversion token has ID token {} and raw \
             symbol {} that do not match",
            instrument.id(),
            id_token_raw.as_deref().unwrap_or("missing"),
            binary.raw_symbol,
        );

        if loaded_by_id.insert(instrument.id(), instrument).is_some() {
            anyhow::bail!(
                "duplicate loaded Polymarket instrument {} for required token {token_id}",
                instrument.id(),
            );
        }

        if let Some(existing) = loaded_by_token.insert(token_id, instrument) {
            anyhow::bail!(
                "duplicate loaded instrument for token {token_id}: {} and {}",
                existing.id(),
                instrument.id(),
            );
        }
    }

    let anchor_instrument = loaded_by_id
        .get(&anchor)
        .ok_or_else(|| anyhow::anyhow!("anchor instrument {anchor} is not loaded"))?;
    let anchor_info = anchor_instrument
        .info()
        .ok_or_else(|| anyhow::anyhow!("anchor instrument {anchor} has no metadata"))?;
    let parent_raw = anchor_info.get_str("gamma_event").ok_or_else(|| {
        anyhow::anyhow!(
            "anchor instrument {anchor} is missing the loaded parent Gamma event metadata; \
             load the full event explicitly"
        )
    })?;
    let parent: GammaEvent = serde_json::from_str(parent_raw)
        .map_err(|e| anyhow::anyhow!("failed to parse parent Gamma event for {anchor}: {e}"))?;

    validate_parent_event(&parent, metadata)?;

    let mut parsed = Vec::with_capacity(question_count);

    for market in &parent.markets {
        parsed.push(parse_condition(market, metadata)?);
    }

    let mut conditions = Vec::with_capacity(question_count);

    for condition in parsed {
        let yes_instrument_id =
            instrument_id(&condition.condition_id_raw, &condition.yes_token_raw);
        let no_instrument_id = instrument_id(&condition.condition_id_raw, &condition.no_token_raw);
        let yes_instrument = loaded_by_id.get(&yes_instrument_id).ok_or_else(|| {
            anyhow::anyhow!(
                "missing loaded YES instrument {yes_instrument_id} for condition {}",
                condition.condition_id_raw,
            )
        })?;
        let no_instrument = loaded_by_id.get(&no_instrument_id).ok_or_else(|| {
            anyhow::anyhow!(
                "missing loaded NO instrument {no_instrument_id} for condition {}",
                condition.condition_id_raw,
            )
        })?;

        let yes = validate_outcome(
            yes_instrument,
            yes_instrument_id,
            OutcomeSide::Yes,
            &condition,
            &parent.id,
        )?;
        let no = validate_outcome(
            no_instrument,
            no_instrument_id,
            OutcomeSide::No,
            &condition,
            &parent.id,
        )?;

        conditions.push(NegRiskCondition {
            index: condition.index,
            condition_id: condition.condition_id.0,
            yes,
            no,
        });
    }

    let event = CtfNegRiskEvent::new_checked(metadata.market_id.0, metadata.fee_bps, conditions)
        .map_err(|e| anyhow::anyhow!("invalid chain market {}: {e}", metadata.market_id))?;
    anyhow::ensure!(
        event.conditions().iter().any(|condition| {
            condition.yes.instrument_id == anchor || condition.no.instrument_id == anchor
        }),
        "anchor instrument {anchor} is not part of parent event {}",
        parent.id,
    );

    Ok(event)
}

fn validate_metadata(metadata: &CtfNegRiskMarketMetadata<'_>) -> anyhow::Result<()> {
    let question_count = usize::from(metadata.question_count);

    anyhow::ensure!(
        metadata.token_ids.len() == question_count,
        "chain market {} reports {question_count} questions but has {} outcome token pairs",
        metadata.market_id,
        metadata.token_ids.len(),
    );

    Ok(())
}

fn validate_parent_event(
    parent: &GammaEvent,
    metadata: &CtfNegRiskMarketMetadata<'_>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !parent.id.is_empty(),
        "parent Gamma event for chain market {} has an empty ID",
        metadata.market_id,
    );
    anyhow::ensure!(
        parent.neg_risk == Some(true),
        "parent Gamma event {} for chain market {} is not marked negRisk",
        parent.id,
        metadata.market_id,
    );
    anyhow::ensure!(
        parent.neg_risk_augmented == Some(false),
        "parent Gamma event {} for chain market {} has unsupported augmented status {:?}; \
         only non-augmented CTF events are supported",
        parent.id,
        metadata.market_id,
        parent.neg_risk_augmented,
    );

    let parent_market_id = parent.neg_risk_market_id.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "parent Gamma event {} for chain market {} is missing negRiskMarketID",
            parent.id,
            metadata.market_id,
        )
    })?;
    let parent_market_id: B256 = parent_market_id.parse().map_err(|e| {
        anyhow::anyhow!(
            "parent Gamma event {} has invalid negRiskMarketID '{parent_market_id}': {e}",
            parent.id,
        )
    })?;
    anyhow::ensure!(
        parent_market_id == metadata.market_id,
        "parent Gamma event {} negRiskMarketID {} does not match chain market {}",
        parent.id,
        parent_market_id,
        metadata.market_id,
    );
    anyhow::ensure!(
        parent.markets.len() == usize::from(metadata.question_count),
        "parent Gamma event {} for chain market {} is incomplete: expected {} conditions, \
         found {} markets",
        parent.id,
        metadata.market_id,
        metadata.question_count,
        parent.markets.len(),
    );

    Ok(())
}

fn parse_condition(
    market: &GammaMarket,
    metadata: &CtfNegRiskMarketMetadata<'_>,
) -> anyhow::Result<ParsedCondition> {
    let condition_id_raw = market.condition_id.clone();
    let condition_id: B256 = market.condition_id.parse().map_err(|e| {
        anyhow::anyhow!("condition {condition_id_raw} has an invalid condition ID: {e}")
    })?;
    let question_id_raw = market
        .question_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("condition {condition_id_raw} is missing questionID"))?;
    let question_id: B256 = question_id_raw.parse().map_err(|e| {
        anyhow::anyhow!(
            "condition {condition_id_raw} has invalid questionID '{question_id_raw}': {e}"
        )
    })?;

    let derived_condition_id = ctf_condition_id(question_id);
    anyhow::ensure!(
        condition_id == derived_condition_id,
        "condition {condition_id_raw} does not match the condition ID {derived_condition_id} \
         derived from questionID {question_id_raw}",
    );

    let question_market_id = market_id_from_question_id(question_id);
    anyhow::ensure!(
        question_market_id == metadata.market_id,
        "condition {condition_id_raw} with questionID {question_id_raw} belongs to chain market \
         {question_market_id}, expected {}",
        metadata.market_id,
    );

    if let Some(market_id_raw) = market.neg_risk_market_id.as_deref() {
        let market_id: B256 = market_id_raw.parse().map_err(|e| {
            anyhow::anyhow!(
                "condition {condition_id_raw} has invalid negRiskMarketID '{market_id_raw}': {e}"
            )
        })?;
        anyhow::ensure!(
            market_id == metadata.market_id,
            "condition {condition_id_raw} negRiskMarketID {market_id} does not match chain market {}",
            metadata.market_id,
        );
    }

    anyhow::ensure!(
        market.neg_risk == Some(true),
        "condition {condition_id_raw} market {} is not marked negRisk",
        market.id,
    );
    validate_market_version(market.version.as_deref())
        .map_err(|e| anyhow::anyhow!("condition {condition_id_raw} market {}: {e}", market.id))?;

    let (token_ids, outcomes) = parse_market_outcomes(market).map_err(|e| {
        anyhow::anyhow!(
            "condition {condition_id_raw} market {} has invalid outcomes: {e}",
            market.id,
        )
    })?;
    let mut yes_token_raw = None;
    let mut no_token_raw = None;

    for (token_id, outcome) in token_ids.iter().zip(outcomes.iter()) {
        match outcome.as_str() {
            "Yes" => yes_token_raw = Some(token_id.as_str()),
            "No" => no_token_raw = Some(token_id.as_str()),
            _ => {
                anyhow::bail!(
                    "condition {condition_id_raw} market {} outcomes must contain Yes and No, found \
                     {outcomes:?}",
                    market.id,
                );
            }
        }
    }

    let Some(yes_token_raw) = yes_token_raw else {
        anyhow::bail!(
            "condition {condition_id_raw} market {} is missing the Yes outcome",
            market.id,
        );
    };
    let Some(no_token_raw) = no_token_raw else {
        anyhow::bail!(
            "condition {condition_id_raw} market {} is missing the No outcome",
            market.id,
        );
    };

    let yes_token = U256::from_str_radix(yes_token_raw, 10).map_err(|e| {
        anyhow::anyhow!(
            "condition {condition_id_raw} market {} has invalid YES token ID '{yes_token_raw}': {e}",
            market.id,
        )
    })?;
    let no_token = U256::from_str_radix(no_token_raw, 10).map_err(|e| {
        anyhow::anyhow!(
            "condition {condition_id_raw} market {} has invalid NO token ID '{no_token_raw}': {e}",
            market.id,
        )
    })?;

    let index = question_index(question_id);
    let Some(&[expected_yes, expected_no]) = metadata.token_ids.get(usize::from(index)) else {
        anyhow::bail!(
            "condition {condition_id_raw} questionID {question_id_raw} has chain index {index} outside \
         question count {}",
            metadata.question_count,
        );
    };
    anyhow::ensure!(
        yes_token == expected_yes,
        "condition {condition_id_raw} YES token ID {yes_token_raw} does not match authoritative \
         chain token {expected_yes}",
    );
    anyhow::ensure!(
        no_token == expected_no,
        "condition {condition_id_raw} NO token ID {no_token_raw} does not match authoritative chain \
         token {expected_no}",
    );

    Ok(ParsedCondition {
        index,
        condition_id,
        condition_id_raw,
        question_id_raw: question_id_raw.to_string(),
        gamma_market_id: market.id.clone(),
        yes_token,
        yes_token_raw: yes_token_raw.to_string(),
        no_token,
        no_token_raw: no_token_raw.to_string(),
    })
}

fn validate_outcome(
    instrument: &InstrumentAny,
    expected_id: InstrumentId,
    side: OutcomeSide,
    condition: &ParsedCondition,
    parent_event_id: &str,
) -> anyhow::Result<NegRiskOutcome> {
    let InstrumentAny::BinaryOption(binary) = instrument else {
        anyhow::bail!(
            "loaded {} instrument {expected_id} for condition {} is not a Polymarket binary option",
            side.name(),
            condition.condition_id_raw,
        );
    };

    anyhow::ensure!(
        binary.outcome == Some(side.label()),
        "loaded {} instrument {expected_id} for condition {} has outcome {:?}, expected {}",
        side.name(),
        condition.condition_id_raw,
        binary.outcome,
        side.label(),
    );
    anyhow::ensure!(
        binary.raw_symbol.as_str() == side.token_raw(condition),
        "loaded {} instrument {expected_id} for condition {} has raw symbol {}, expected {}",
        side.name(),
        condition.condition_id_raw,
        binary.raw_symbol,
        side.token_raw(condition),
    );
    anyhow::ensure!(
        binary
            .event_id
            .is_some_and(|event_id| event_id.as_str() == parent_event_id),
        "loaded {} instrument {expected_id} for condition {} belongs to event {:?}, expected \
         {parent_event_id}",
        side.name(),
        condition.condition_id_raw,
        binary.event_id,
    );
    anyhow::ensure!(
        binary.currency.code.as_str() == PUSD,
        "loaded {} instrument {expected_id} for condition {} has currency {}, expected {PUSD}",
        side.name(),
        condition.condition_id_raw,
        binary.currency.code,
    );

    let info = binary.info.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "loaded {} instrument {expected_id} for condition {} has no metadata",
            side.name(),
            condition.condition_id_raw,
        )
    })?;
    validate_market_version(info.get_str(MARKET_VERSION_KEY)).map_err(|e| {
        anyhow::anyhow!(
            "loaded {} instrument {expected_id} for condition {} has invalid market version: {e}",
            side.name(),
            condition.condition_id_raw,
        )
    })?;
    anyhow::ensure!(
        info.get_str("condition_id") == Some(condition.condition_id_raw.as_str()),
        "loaded {} instrument {expected_id} for condition {} has conflicting condition metadata",
        side.name(),
        condition.condition_id_raw,
    );
    anyhow::ensure!(
        info.get_str("token_id") == Some(side.token_raw(condition)),
        "loaded {} instrument {expected_id} for condition {} has conflicting token metadata",
        side.name(),
        condition.condition_id_raw,
    );
    anyhow::ensure!(
        info.get_str("market_id") == Some(condition.gamma_market_id.as_str()),
        "loaded {} instrument {expected_id} for condition {} has conflicting market metadata",
        side.name(),
        condition.condition_id_raw,
    );
    anyhow::ensure!(
        info.get_str("question_id") == Some(condition.question_id_raw.as_str()),
        "loaded {} instrument {expected_id} for condition {} has conflicting question metadata",
        side.name(),
        condition.condition_id_raw,
    );
    anyhow::ensure!(
        info.get_bool("neg_risk") == Some(true),
        "loaded {} instrument {expected_id} for condition {} is not marked negRisk",
        side.name(),
        condition.condition_id_raw,
    );

    Ok(NegRiskOutcome {
        token_id: side.token_id(condition).to_be_bytes::<32>(),
        instrument_id: expected_id,
    })
}

fn instrument_id(condition_id: &str, token_id: &str) -> InstrumentId {
    InstrumentId::from(format!("{condition_id}-{token_id}.{POLYMARKET}"))
}

fn ctf_condition_id(question_id: B256) -> B256 {
    let mut encoded = [0u8; 84];
    encoded[..20].copy_from_slice(NEG_RISK_ADAPTER.as_slice());
    encoded[20..52].copy_from_slice(question_id.as_slice());
    encoded[52..].copy_from_slice(&U256::from(2).to_be_bytes::<32>());
    keccak256(encoded)
}

fn market_id_from_question_id(question_id: B256) -> B256 {
    let mut bytes = question_id.0;
    bytes[31] = 0;
    B256::from(bytes)
}

fn question_index(question_id: B256) -> u8 {
    question_id.as_slice()[31]
}

#[derive(Clone, Copy)]
enum OutcomeSide {
    Yes,
    No,
}

impl OutcomeSide {
    const fn name(self) -> &'static str {
        match self {
            Self::Yes => "YES",
            Self::No => "NO",
        }
    }

    fn label(self) -> Ustr {
        match self {
            Self::Yes => PolymarketOutcome::yes().inner(),
            Self::No => PolymarketOutcome::no().inner(),
        }
    }

    fn token_raw(self, condition: &ParsedCondition) -> &str {
        match self {
            Self::Yes => &condition.yes_token_raw,
            Self::No => &condition.no_token_raw,
        }
    }

    fn token_id(self, condition: &ParsedCondition) -> U256 {
        match self {
            Self::Yes => condition.yes_token,
            Self::No => condition.no_token,
        }
    }
}

struct ParsedCondition {
    index: u8,
    condition_id: B256,
    condition_id_raw: String,
    question_id_raw: String,
    gamma_market_id: String,
    yes_token: U256,
    yes_token_raw: String,
    no_token: U256,
    no_token_raw: String,
}
