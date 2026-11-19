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

//! Canonical CTF negative-risk event definitions for conversion.

use std::collections::HashSet;

use nautilus_core::{
    correctness::{CorrectnessError, CorrectnessResult},
    hex::encode_prefixed,
};

use crate::identifiers::InstrumentId;

/// A CTF event with a complete set of conversion conditions in chain index order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CtfNegRiskEvent {
    market_id: [u8; 32],
    fee_bps: u16,
    conditions: Vec<NegRiskCondition>,
}

impl CtfNegRiskEvent {
    /// Constructs a mapping from conditions already checked against authoritative chain metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the identifiers, fee, or condition set violates the CTF event contract.
    pub fn new_checked(
        market_id: [u8; 32],
        fee_bps: u16,
        conditions: Vec<NegRiskCondition>,
    ) -> CorrectnessResult<Self> {
        check(fee_bps <= 10_000, || {
            format!("invalid negative-risk fee {fee_bps} basis points; maximum is 10000")
        })?;
        check(conditions.len() >= 2, || {
            "a CTF negative-risk event requires at least two conditions".to_string()
        })?;
        check(conditions.len() <= 255, || {
            "a CTF negative-risk event cannot contain more than 255 conditions".to_string()
        })?;
        check(market_id != [0u8; 32], || {
            "negative-risk market ID must not be zero".to_string()
        })?;
        check(market_id[31] == 0, || {
            format!(
                "negative-risk market ID {} has a nonzero question index byte",
                encode_prefixed(market_id),
            )
        })?;

        let mut conditions = conditions;
        conditions.sort_by_key(|condition| condition.index);

        let mut condition_indices = HashSet::with_capacity(conditions.len());
        let mut condition_ids = HashSet::with_capacity(conditions.len());
        let mut token_ids = HashSet::with_capacity(conditions.len() * 2);
        let mut instrument_ids = HashSet::with_capacity(conditions.len() * 2);

        for (expected_index, condition) in conditions.iter().enumerate() {
            check(condition.condition_id != [0u8; 32], || {
                format!(
                    "negative-risk condition ID at index {} must not be zero",
                    condition.index,
                )
            })?;
            check(condition_indices.insert(condition.index), || {
                format!(
                    "duplicate negative-risk condition index {}",
                    condition.index,
                )
            })?;
            check(usize::from(condition.index) == expected_index, || {
                format!(
                    "negative-risk condition index {} at position {} is not contiguous; expected {}",
                    condition.index, expected_index, expected_index
                )
            })?;
            check(condition_ids.insert(condition.condition_id), || {
                format!(
                    "duplicate negative-risk condition ID {}",
                    encode_prefixed(condition.condition_id),
                )
            })?;

            for (side, outcome) in [("YES", condition.yes), ("NO", condition.no)] {
                check(outcome.token_id != [0u8; 32], || {
                    format!(
                        "negative-risk {side} token ID at condition index {} must not be zero",
                        condition.index,
                    )
                })?;
                check(token_ids.insert(outcome.token_id), || {
                    format!(
                        "duplicate negative-risk token ID {}",
                        encode_prefixed(outcome.token_id),
                    )
                })?;
                check(instrument_ids.insert(outcome.instrument_id), || {
                    format!(
                        "duplicate negative-risk instrument ID {}",
                        outcome.instrument_id
                    )
                })?;
            }
        }

        Ok(Self {
            market_id,
            fee_bps,
            conditions,
        })
    }

    /// Returns the on-chain market ID, not the discovery event ID.
    #[must_use]
    pub const fn market_id(&self) -> [u8; 32] {
        self.market_id
    }

    /// Returns the conversion fee in basis points.
    #[must_use]
    pub const fn fee_bps(&self) -> u16 {
        self.fee_bps
    }

    /// Returns all conditions in on-chain index order.
    #[must_use]
    pub fn conditions(&self) -> &[NegRiskCondition] {
        &self.conditions
    }

    /// Resolves selected NO instruments to inputs and all potential YES outputs.
    ///
    /// Both lists are ordered by on-chain condition index, regardless of input order.
    ///
    /// # Errors
    ///
    /// Returns an error if the selection is empty, repeats an instrument, or contains an
    /// instrument other than a NO outcome of this event.
    pub fn convert_scope(&self, inputs: &[InstrumentId]) -> CorrectnessResult<ConvertScope> {
        check(!inputs.is_empty(), || {
            "a conversion requires at least one NO input".to_string()
        })?;

        let mut positions = Vec::with_capacity(inputs.len());
        let mut selected = HashSet::with_capacity(inputs.len());

        for input in inputs {
            let Some(position) = self
                .conditions
                .iter()
                .position(|condition| condition.no.instrument_id == *input)
            else {
                return Err(CorrectnessError::PredicateViolation {
                    message: format!(
                        "conversion input {input} is not a NO outcome of this negative-risk event"
                    ),
                });
            };

            if !selected.insert(position) {
                return Err(CorrectnessError::PredicateViolation {
                    message: format!("conversion input {input} is duplicated"),
                });
            }

            positions.push(position);
        }

        positions.sort_unstable();

        let inputs = positions
            .into_iter()
            .map(|position| self.conditions[position].no.instrument_id)
            .collect();
        let outputs = self
            .conditions
            .iter()
            .enumerate()
            .filter(|(position, _)| !selected.contains(position))
            .map(|(_, condition)| condition.yes.instrument_id)
            .collect();

        Ok(ConvertScope { inputs, outputs })
    }
}

fn check(condition: bool, message: impl FnOnce() -> String) -> CorrectnessResult<()> {
    if condition {
        Ok(())
    } else {
        Err(CorrectnessError::PredicateViolation { message: message() })
    }
}

/// A convertible binary condition of a negative-risk CTF event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NegRiskCondition {
    /// Chain index of the condition, starting at zero.
    pub index: u8,
    /// CTF condition ID.
    pub condition_id: [u8; 32],
    /// YES outcome of the condition.
    pub yes: NegRiskOutcome,
    /// NO outcome of the condition.
    pub no: NegRiskOutcome,
}

/// The chain token and Nautilus instrument for a binary outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NegRiskOutcome {
    /// Unsigned 256-bit CTF token ID in big-endian byte order.
    pub token_id: [u8; 32],
    /// ID of the loaded instrument for this token.
    pub instrument_id: InstrumentId,
}

/// The complete instrument scope of a conversion, independent of positions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConvertScope {
    /// Selected NO inputs in on-chain index order.
    pub inputs: Vec<InstrumentId>,
    /// Unselected YES outputs in on-chain index order.
    pub outputs: Vec<InstrumentId>,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn identifier(value: u16) -> [u8; 32] {
        let mut bytes = [0; 32];
        bytes[..2].copy_from_slice(&value.to_be_bytes());
        bytes
    }

    #[rstest]
    #[case::largest_event(255, true)]
    #[case::unrepresentable_count(256, false)]
    fn test_ctf_question_count_limit(#[case] count: u16, #[case] accepted: bool) {
        let conditions = (0..count)
            .map(|index| NegRiskCondition {
                index: u8::try_from(index).unwrap(),
                condition_id: identifier(index + 1),
                yes: NegRiskOutcome {
                    token_id: identifier(index * 2 + 1),
                    instrument_id: InstrumentId::from(format!("YES-{index}.POLYMARKET")),
                },
                no: NegRiskOutcome {
                    token_id: identifier(index * 2 + 2),
                    instrument_id: InstrumentId::from(format!("NO-{index}.POLYMARKET")),
                },
            })
            .collect();

        let mapping = CtfNegRiskEvent::new_checked(identifier(33), 10_000, conditions);

        if accepted {
            assert_eq!(mapping.unwrap().conditions().len(), usize::from(count));
        } else {
            let message = mapping.unwrap_err().to_string();
            assert!(message.contains("255"), "{message}");
        }
    }
}
