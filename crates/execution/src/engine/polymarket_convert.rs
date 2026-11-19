// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Core accounting for verified Polymarket Convert operations.

use indexmap::IndexMap;
pub use nautilus_common::messages::execution::{
    ConvertTokenAmount, ConvertTransaction, VerifiedConvert,
};
use nautilus_common::{cache::Cache, messages::execution::has_unique_inputs};
use nautilus_core::UUID4;
use nautilus_model::{
    enums::{InstrumentCloseType, OmsType, PositionSide},
    identifiers::{InstrumentId, PositionId, StrategyId},
    instruments::{Instrument, InstrumentAny, neg_risk::CtfNegRiskEvent},
    polymarket_convert::{
        PolymarketConvertInput, PolymarketConvertOutput, allocate_convert_costs_precise,
    },
    position::{Position, PositionConvert, PositionConvertKind, PositionReplayEvent},
    types::{Currency, Quantity},
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};

use super::ExecutionEngine;

/// Prefix under which all Polymarket Convert records are retained in general cache state.
pub const CONVERT_CACHE_PREFIX: &str = "polymarket_convert:";

/// The number of decimal places used by CTF and USDC chain units.
const CHAIN_UNITS_SCALE: u32 = 6;

/// The simulated asset effect of one negative-risk conversion, in instrument units.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimulatedConvertEffect {
    pub inputs: Vec<ConvertTokenAmount>,
    pub outputs: Vec<ConvertTokenAmount>,
    pub net_cash: Decimal,
}

/// Calculates the chain-unit effects of a negative-risk conversion without applying them.
///
/// Implements the `NegRiskAdapter.convertPositions` arithmetic:
/// `fee_raw = floor(input_raw * fee_bps / 10000)` and `out_raw = input_raw - fee_raw`.
/// Each unselected YES receives `out_raw`, and cash is `(input_count - 1) * out_raw`.
///
/// # Errors
///
/// Returns an error if the inputs are empty, repeated, unequal, nonpositive, not exactly
/// representable in six chain decimal places, outside the event, or the chain-unit arithmetic
/// overflows.
pub fn calculate_convert_effect(
    event: &CtfNegRiskEvent,
    inputs: &[ConvertTokenAmount],
) -> Result<SimulatedConvertEffect, String> {
    if inputs.is_empty() {
        return Err("a conversion requires at least one NO input".to_string());
    }

    if !has_unique_inputs(inputs) {
        return Err("conversion inputs must be unique".to_string());
    }

    let mut quantity = None;
    for input in inputs {
        if input.quantity <= Decimal::ZERO {
            return Err(format!(
                "conversion quantity for {} must be positive",
                input.instrument_id,
            ));
        }

        if input.quantity.normalize().scale() > CHAIN_UNITS_SCALE {
            return Err(format!(
                "conversion quantity for {} is not representable in six chain decimal places",
                input.instrument_id,
            ));
        }

        if let Some(quantity) = quantity {
            if quantity != input.quantity {
                return Err("conversion input quantities must be equal".to_string());
            }
        } else {
            quantity = Some(input.quantity);
        }
    }
    let quantity =
        quantity.ok_or_else(|| "a conversion requires at least one NO input".to_string())?;

    let input_ids: Vec<_> = inputs.iter().map(|input| input.instrument_id).collect();
    let canonical = event
        .convert_scope(&input_ids)
        .map_err(|e| format!("invalid conversion scope: {e}"))?;

    let input_raw = chain_units(quantity, "conversion input")?;
    let fee_raw = input_raw
        .checked_mul(u128::from(event.fee_bps()))
        .ok_or_else(|| "conversion fee arithmetic overflowed".to_string())?
        / 10_000;
    let out_raw = input_raw
        .checked_sub(fee_raw)
        .ok_or_else(|| "conversion fee exceeded the input amount".to_string())?;
    let out_quantity = chain_decimal(out_raw)?;

    let cash_inputs = canonical
        .inputs
        .len()
        .checked_sub(1)
        .ok_or_else(|| "a conversion requires at least one NO input".to_string())?;
    let cash_raw = out_raw
        .checked_mul(cash_inputs as u128)
        .ok_or_else(|| "conversion cash arithmetic overflowed".to_string())?;
    let net_cash = chain_decimal(cash_raw)?;

    let inputs = canonical
        .inputs
        .iter()
        .map(|instrument_id| ConvertTokenAmount {
            instrument_id: *instrument_id,
            quantity,
        })
        .collect();
    let outputs = canonical
        .outputs
        .iter()
        .map(|instrument_id| ConvertTokenAmount {
            instrument_id: *instrument_id,
            quantity: out_quantity,
        })
        .collect();

    Ok(SimulatedConvertEffect {
        inputs,
        outputs,
        net_cash,
    })
}

/// Tracks conversion inventory consumed by a simulated venue but not yet applied by the core.
///
/// Matching engines subtract the reserved quantity before checking available inventory, so a
/// second conversion or ordinary SELL cannot reuse the same NO position while accounting is
/// still deferred.
#[derive(Clone, Debug, Default)]
pub struct ConvertPendingInventory {
    reservations: IndexMap<UUID4, Vec<(PositionId, Decimal)>>,
    consumed: IndexMap<PositionId, Decimal>,
}

impl ConvertPendingInventory {
    /// Reserves the consumed input quantities for one conversion.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation already has a reservation, the legs are empty, or a
    /// position is duplicated or nonpositive.
    pub fn reserve(
        &mut self,
        operation_id: UUID4,
        legs: Vec<(PositionId, Decimal)>,
    ) -> Result<(), String> {
        if self.reservations.contains_key(&operation_id) {
            return Err(format!(
                "conversion {operation_id} already has a pending inventory reservation"
            ));
        }

        if legs.is_empty() {
            return Err(
                "a conversion inventory reservation requires at least one input".to_string(),
            );
        }

        let mut unique = IndexMap::with_capacity(legs.len());
        for (position_id, quantity) in &legs {
            if *quantity <= Decimal::ZERO {
                return Err(format!(
                    "conversion inventory for {position_id} must be positive"
                ));
            }

            if unique.insert(*position_id, *quantity).is_some() {
                return Err(format!(
                    "conversion inventory reserved {position_id} more than once"
                ));
            }
        }

        let mut totals = Vec::with_capacity(unique.len());
        for (position_id, quantity) in &unique {
            let consumed = self.consumed(position_id);
            let total = consumed
                .checked_add(*quantity)
                .ok_or_else(|| format!("conversion inventory overflow for {position_id}"))?;
            totals.push((*position_id, total));
        }

        for (position_id, total) in totals {
            self.consumed.insert(position_id, total);
        }
        self.reservations.insert(operation_id, legs);
        Ok(())
    }

    /// Releases a conversion reservation when its core accounting has applied.
    ///
    /// Returns whether a reservation existed.
    pub fn release(&mut self, operation_id: UUID4) -> bool {
        let Some(legs) = self.reservations.shift_remove(&operation_id) else {
            return false;
        };

        for (position_id, quantity) in legs {
            let Some(consumed) = self.consumed.get_mut(&position_id) else {
                continue;
            };

            *consumed = consumed.saturating_sub(quantity);
            if consumed.is_zero() {
                self.consumed.shift_remove(&position_id);
            }
        }
        true
    }

    /// Returns the pending conversion quantity consumed from the position.
    #[must_use]
    pub fn consumed(&self, position_id: &PositionId) -> Decimal {
        self.consumed.get(position_id).copied().unwrap_or_default()
    }

    /// Clears all pending reservations.
    pub fn clear(&mut self) {
        self.reservations.clear();
        self.consumed.clear();
    }
}

fn chain_units(value: Decimal, field: &str) -> Result<u128, String> {
    let units = value
        .checked_mul(Decimal::from(1_000_000_u64))
        .ok_or_else(|| format!("`{field}` quantity exceeds the supported decimal range"))?;
    if !units.fract().is_zero() {
        return Err(format!(
            "`{field}` quantity is not exactly representable in six chain decimal places"
        ));
    }

    units
        .to_u128()
        .ok_or_else(|| format!("`{field}` quantity exceeds the supported chain-unit range"))
}

fn chain_decimal(raw: u128) -> Result<Decimal, String> {
    let raw = i128::try_from(raw)
        .map_err(|_| "conversion chain-unit amount exceeds the decimal range".to_string())?;
    Decimal::try_from_i128_with_scale(raw, CHAIN_UNITS_SCALE)
        .map_err(|_| "conversion chain-unit amount exceeds the decimal range".to_string())
}

/// The locally retained accounting record for a verified Convert.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvertRecord {
    pub verified: VerifiedConvert,
    /// The original exact plan, including the identity and cycle of each change.
    pub plan: Option<Vec<PositionConvert>>,
    /// Whether an exact plan was fixed, or recovery forbids creating a replacement.
    #[serde(default)]
    pub plan_fixed: bool,
    /// Whether all changes have been applied locally and lifecycle events published.
    pub applied: bool,
    pub unapplied_instruments: Vec<InstrumentId>,
    pub reason: Option<String>,
}

impl ConvertRecord {
    fn new(verified: VerifiedConvert) -> Self {
        Self {
            verified,
            plan: None,
            plan_fixed: false,
            applied: false,
            unapplied_instruments: Vec::new(),
            reason: None,
        }
    }

    /// Returns the key used to retain the record in the general cache.
    #[must_use]
    pub fn cache_key(operation_id: UUID4) -> String {
        format!("{CONVERT_CACHE_PREFIX}{operation_id}")
    }

    fn block(&mut self, reason: String, unapplied_instruments: Vec<InstrumentId>) {
        self.applied = false;
        self.unapplied_instruments = unapplied_instruments;
        self.reason = Some(reason);
    }
}

impl ExecutionEngine {
    /// Applies verified effects or compares a duplicate with the original accounting plan.
    ///
    /// The caller supplies the inputs with known pending fill, void, or core application activity.
    /// Accounting that cannot be applied returns a queryable record with the reason.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation record cannot be read or stored.
    #[allow(
        clippy::needless_pass_by_ref_mut,
        reason = "engine mutation entry point retains a mutable receiver for orchestration consistency"
    )]
    pub fn apply_verified_convert(
        &mut self,
        verified: VerifiedConvert,
        pending_inputs: &[InstrumentId],
    ) -> anyhow::Result<ConvertRecord> {
        let operation_id = verified.operation_id;
        let mut record = match self.load_convert_record(operation_id)? {
            Some(mut record) => {
                match merge_verified_convert(&mut record.verified, &verified) {
                    Ok(true) => self.store_convert_record(&record)?,
                    Ok(false) => {}
                    Err(reason) => {
                        record.reason = Some(format!("conflict: {reason}"));
                        self.store_convert_record(&record)?;
                        return Ok(record);
                    }
                }
                record
            }
            None => ConvertRecord::new(verified),
        };

        if record.plan.is_some() {
            record.plan_fixed = true;
            return self.complete_convert_plan(record, pending_inputs, ConvertApplyMode::Retained);
        }

        if record.applied {
            record.plan_fixed = true;
            record.block(
                "missing original Convert plan; Applied cannot be verified or reconstructed"
                    .to_string(),
                sorted_instruments(&record.verified.affected_instruments),
            );
            self.store_convert_record(&record)?;
            return Ok(record);
        }

        if record.plan_fixed {
            record.block(
                "missing original Convert plan; recovery cannot reconstruct it".to_string(),
                sorted_instruments(&record.verified.affected_instruments),
            );
            self.store_convert_record(&record)?;
            return Ok(record);
        }

        if self.convert_has_replay_evidence(&record.verified) {
            record.plan_fixed = true;
            record.block(
                "Convert plan is missing after local position changes".to_string(),
                sorted_instruments(&record.verified.affected_instruments),
            );
            self.store_convert_record(&record)?;
            return Ok(record);
        }

        match self.build_convert_plan(&record.verified, pending_inputs) {
            Ok((plan, currency)) => {
                record.plan = Some(plan.clone());
                record.plan_fixed = true;
                record.applied = false;
                record.unapplied_instruments = plan_instruments(&plan);
                record.reason = None;

                self.store_convert_record(&record)?;
                self.complete_convert_plan(
                    record,
                    pending_inputs,
                    ConvertApplyMode::Fresh { currency },
                )
            }
            Err(reason) => {
                record.block(
                    reason,
                    sorted_instruments(&record.verified.affected_instruments),
                );
                self.store_convert_record(&record)?;
                Ok(record)
            }
        }
    }

    /// Compares the retained plan with replay evidence and applies only missing changes.
    ///
    /// Unlike first application, recovery never creates a replacement plan.
    /// It can complete a missing decrease or increase only while the target position still
    /// carries its planned cycle, and can reopen only from the recorded prior closed cycle.
    /// Without retained replay for an absent first opening, the change remains pending because
    /// partial persistence does not prove whether it was ever applied.
    ///
    /// # Errors
    ///
    /// Returns an error if no record exists or the record cannot be read or stored.
    #[allow(
        clippy::needless_pass_by_ref_mut,
        reason = "engine mutation entry point retains a mutable receiver for orchestration consistency"
    )]
    pub fn recover_convert(
        &mut self,
        operation_id: UUID4,
        pending_inputs: &[InstrumentId],
    ) -> anyhow::Result<ConvertRecord> {
        let Some(mut record) = self.load_convert_record(operation_id)? else {
            anyhow::bail!("Convert operation {operation_id} has no retained record");
        };

        let Some(plan) = record.plan.clone() else {
            let plan_is_sticky = record.plan_fixed
                || record.applied
                || self.convert_has_replay_evidence(&record.verified);
            record.block(
                "missing original Convert plan; recovery cannot reconstruct it".to_string(),
                sorted_instruments(&record.verified.affected_instruments),
            );
            record.plan_fixed = plan_is_sticky;
            self.store_convert_record(&record)?;
            return Ok(record);
        };

        record.plan_fixed = true;
        record.plan = Some(plan);
        self.complete_convert_plan(record, pending_inputs, ConvertApplyMode::Retained)
    }

    /// Returns the locally retained operation record without triggering accounting.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored record cannot be decoded.
    pub fn convert_record(&self, operation_id: UUID4) -> anyhow::Result<Option<ConvertRecord>> {
        self.load_convert_record(operation_id)
    }

    fn load_convert_record(&self, operation_id: UUID4) -> anyhow::Result<Option<ConvertRecord>> {
        let cache = self.cache.borrow();
        let Some(value) = cache.get(&ConvertRecord::cache_key(operation_id))? else {
            return Ok(None);
        };
        let record = serde_json::from_slice(value)
            .map_err(|e| anyhow::anyhow!("failed to decode Convert record {operation_id}: {e}"))?;
        Ok(Some(record))
    }

    fn store_convert_record(&self, record: &ConvertRecord) -> anyhow::Result<()> {
        let value = serde_json::to_vec(record).map_err(|e| {
            anyhow::anyhow!(
                "failed to encode Convert record {}: {e}",
                record.verified.operation_id
            )
        })?;
        self.cache.borrow_mut().add(
            &ConvertRecord::cache_key(record.verified.operation_id),
            value.into(),
        )
    }

    #[allow(
        clippy::needless_pass_by_ref_mut,
        reason = "engine mutation entry point retains a mutable receiver for orchestration consistency"
    )]
    fn complete_convert_plan(
        &mut self,
        mut record: ConvertRecord,
        pending_inputs: &[InstrumentId],
        mode: ConvertApplyMode,
    ) -> anyhow::Result<ConvertRecord> {
        let allow_missing_open = matches!(mode, ConvertApplyMode::Fresh { .. });
        let record_stored = allow_missing_open;
        let planned_currency = match mode {
            ConvertApplyMode::Fresh { currency } => Some(currency),
            ConvertApplyMode::Retained => None,
        };
        let Some(plan) = record.plan.clone() else {
            record.block(
                "missing original Convert plan".to_string(),
                sorted_instruments(&record.verified.affected_instruments),
            );
            self.store_convert_record(&record)?;
            return Ok(record);
        };

        let validation = match mode {
            ConvertApplyMode::Fresh { .. } => None,
            ConvertApplyMode::Retained => {
                match validate_convert_plan_structure(&record.verified, &plan) {
                    Ok(validation) => Some(validation),
                    Err(reason) => {
                        record.block(reason, plan_instruments(&plan));
                        self.store_convert_record(&record)?;
                        return Ok(record);
                    }
                }
            }
        };

        let (missing, conflicts) = self.missing_convert_changes(&plan);

        if !conflicts.is_empty() {
            let reason = format!("conflict: {}", conflicts.join("; "));
            record.block(reason, plan_instruments(&missing));
            self.store_convert_record(&record)?;
            return Ok(record);
        }

        if missing.is_empty() {
            let was_applied = record.applied && record.unapplied_instruments.is_empty();
            let prior_reason = record.reason.clone();
            let conflict_reason = prior_reason
                .as_deref()
                .is_some_and(|reason| reason.starts_with("conflict:"));
            record.applied = true;
            record.unapplied_instruments.clear();
            if !conflict_reason {
                record.reason = None;
            }

            if !was_applied || (!conflict_reason && prior_reason.is_some()) {
                self.store_convert_record(&record)?;
            }
            return Ok(record);
        }

        if let Some(instrument_id) = self.expired_convert_instrument(&record.verified) {
            record.block(
                format!(
                    "affected instrument {instrument_id} formally expired with ContractExpired"
                ),
                plan_instruments(&missing),
            );
            self.store_convert_record(&record)?;
            return Ok(record);
        }

        if let Some(instrument_id) = input_with_pending_evidence(&record.verified, pending_inputs) {
            record.block(
                format!("input {instrument_id} has pending fill, void, or application activity"),
                plan_instruments(&missing),
            );
            self.store_convert_record(&record)?;
            return Ok(record);
        }

        let missing_open_without_position = (!allow_missing_open).then(|| {
            let cache = self.cache.borrow();
            missing
                .iter()
                .find(|change| {
                    matches!(change.kind, PositionConvertKind::Open { .. })
                        && cache.position_owned(&change.position_id).is_none()
                })
                .map(|change| change.position_id)
        });

        if let Some(Some(position_id)) = missing_open_without_position {
            record.block(
                format!("Convert opening {position_id} has no local application evidence"),
                plan_instruments(&missing),
            );
            self.store_convert_record(&record)?;
            return Ok(record);
        }

        let currency = if let Some(currency) = planned_currency {
            currency
        } else {
            match self.validate_convert_scope(&record.verified) {
                Ok(currency) => currency,
                Err(reason) => {
                    record.block(reason, plan_instruments(&missing));
                    self.store_convert_record(&record)?;
                    return Ok(record);
                }
            }
        };

        if let Some(validation) = validation
            && let Err(reason) =
                validate_convert_plan_allocations(&record.verified, &validation, currency)
        {
            record.block(reason, plan_instruments(&missing));
            self.store_convert_record(&record)?;
            return Ok(record);
        }

        record.applied = false;
        record.unapplied_instruments = plan_instruments(&missing);
        record.reason = None;
        if !record_stored {
            self.store_convert_record(&record)?;
        }

        let events_result = {
            let mut cache = self.cache.borrow_mut();
            cache.apply_position_converts(&missing)
        };
        let events = match events_result {
            Ok(events) => events,
            Err(e) => {
                record.block(
                    format!("Convert changes cannot be applied: {e}"),
                    plan_instruments(&missing),
                );
                self.store_convert_record(&record)?;
                return Ok(record);
            }
        };

        let snapshots = if self.config.snapshot_positions {
            let cache = self.cache.borrow();
            missing
                .iter()
                .filter_map(|change| cache.position_owned(&change.position_id))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        for position in snapshots {
            self.create_position_state_snapshot(&position, false);
        }
        self.publish_position_events(events);

        record.applied = true;
        record.unapplied_instruments.clear();
        record.reason = None;
        self.store_convert_record(&record)?;

        Ok(record)
    }

    fn build_convert_plan(
        &self,
        verified: &VerifiedConvert,
        pending_inputs: &[InstrumentId],
    ) -> Result<(Vec<PositionConvert>, Currency), String> {
        let currency = self.validate_convert_scope(verified)?;

        if let Some(instrument_id) = self.expired_convert_instrument(verified) {
            return Err(format!(
                "affected instrument {instrument_id} formally expired with ContractExpired"
            ));
        }

        if let Some(instrument_id) = verified.inputs.iter().find_map(|input| {
            pending_inputs
                .contains(&input.instrument_id)
                .then_some(input.instrument_id)
        }) {
            return Err(format!(
                "input {instrument_id} has pending fill, void, or application activity"
            ));
        }

        let cache = self.cache.borrow();
        let mut input_legs = Vec::with_capacity(verified.inputs.len());
        let mut input_changes = Vec::with_capacity(verified.inputs.len());

        let mut inputs = verified.inputs.clone();
        inputs.sort_unstable_by_key(|input| input.instrument_id);

        for input in &inputs {
            let instrument = cache
                .instrument(&input.instrument_id)
                .ok_or_else(|| format!("missing instrument {}", input.instrument_id))?;
            let quantity = exact_convert_quantity(
                input.quantity,
                instrument.size_precision(),
                &format!("input {}", input.instrument_id),
            )?;

            if quantity.is_zero() {
                return Err(format!(
                    "input {} quantity was not positive",
                    input.instrument_id
                ));
            }

            let position_id = convert_position_id(input.instrument_id, verified.strategy_id);
            let position = cache.position_owned(&position_id).ok_or_else(|| {
                format!(
                    "input position {position_id} for strategy {} and account {} was not found",
                    verified.strategy_id, verified.account_id
                )
            })?;
            validate_convert_position(&cache, &position, instrument, verified, position_id)?;

            let released_cost = position
                .try_convert_release_cost(quantity)
                .map_err(|e| format!("input {} cannot release cost: {e}", input.instrument_id))?;
            let cycle_id = position.current_cycle_id().ok_or_else(|| {
                format!(
                    "input position {} has no current cycle",
                    input.instrument_id
                )
            })?;

            input_legs.push(PolymarketConvertInput {
                instrument_id: input.instrument_id,
                quantity: input.quantity,
                released_cost,
            });
            input_changes.push(PositionConvert {
                operation_id: verified.operation_id,
                trader_id: verified.trader_id,
                strategy_id: verified.strategy_id,
                instrument_id: input.instrument_id,
                position_id,
                account_id: verified.account_id,
                cycle_id,
                kind: PositionConvertKind::Decrease,
                quantity,
                cost: released_cost,
                realized_pnl: Decimal::ZERO,
                ts_event: verified.ts_event,
                ts_init: verified.ts_event,
            });
        }

        let mut outputs = verified.outputs.clone();
        outputs.sort_unstable_by_key(|output| output.instrument_id);
        let mut positive_outputs = Vec::with_capacity(outputs.len());
        let mut output_targets = Vec::with_capacity(outputs.len());

        for output in &outputs {
            if output.quantity < Decimal::ZERO {
                return Err(format!(
                    "output {} quantity was negative",
                    output.instrument_id
                ));
            }

            let instrument = cache
                .instrument(&output.instrument_id)
                .ok_or_else(|| format!("missing instrument {}", output.instrument_id))?;
            let quantity = exact_convert_quantity(
                output.quantity,
                instrument.size_precision(),
                &format!("output {}", output.instrument_id),
            )?;

            let position_id = convert_position_id(output.instrument_id, verified.strategy_id);
            if let Some(position) = cache.position_owned(&position_id) {
                validate_convert_position(&cache, &position, instrument, verified, position_id)?;

                if position.side == PositionSide::Short || position.signed_qty < Decimal::ZERO {
                    return Err(format!("output position {position_id} is short"));
                }
            }

            if quantity.is_zero() {
                continue;
            }

            let target = match cache.position_owned(&position_id) {
                None => (
                    position_id,
                    PositionConvertKind::Open {
                        previous_cycle_id: None,
                    },
                    verified.operation_id,
                ),
                Some(position) if position.is_open() && position.side == PositionSide::Long => {
                    let cycle_id = position.current_cycle_id().ok_or_else(|| {
                        format!("output position {position_id} has no current cycle")
                    })?;
                    (position_id, PositionConvertKind::Increase, cycle_id)
                }
                Some(position) => {
                    let previous_cycle_id = position.current_cycle_id().ok_or_else(|| {
                        format!("output position {position_id} has no recorded prior cycle")
                    })?;
                    (
                        position_id,
                        PositionConvertKind::Open {
                            previous_cycle_id: Some(previous_cycle_id),
                        },
                        verified.operation_id,
                    )
                }
            };

            positive_outputs.push(PolymarketConvertOutput {
                instrument_id: output.instrument_id,
                quantity: output.quantity,
            });
            output_targets.push((*output, quantity, target));
        }

        drop(cache);

        let allocation = allocate_convert_costs_precise(
            &input_legs,
            &positive_outputs,
            verified.net_cash.as_decimal(),
            currency,
        )
        .map_err(|e| format!("cannot allocate Convert costs: {e}"))?;

        let mut pnl_by_instrument = allocation.input_pnl.into_iter();
        for change in &mut input_changes {
            let Some((instrument_id, realized_pnl)) = pnl_by_instrument.next() else {
                return Err("Convert input PnL allocation was incomplete".to_string());
            };

            if instrument_id != change.instrument_id {
                return Err("Convert input PnL allocation order was inconsistent".to_string());
            }
            change.realized_pnl = realized_pnl;
        }

        if pnl_by_instrument.next().is_some() {
            return Err("Convert input PnL allocation had extra entries".to_string());
        }

        let mut output_costs = allocation.output_costs.into_iter();
        let mut output_changes = Vec::with_capacity(output_targets.len());
        for (output, quantity, (position_id, kind, cycle_id)) in output_targets {
            let Some((instrument_id, cost)) = output_costs.next() else {
                return Err("Convert output cost allocation was incomplete".to_string());
            };

            if instrument_id != output.instrument_id {
                return Err("Convert output cost allocation order was inconsistent".to_string());
            }
            output_changes.push(PositionConvert {
                operation_id: verified.operation_id,
                trader_id: verified.trader_id,
                strategy_id: verified.strategy_id,
                instrument_id: output.instrument_id,
                position_id,
                account_id: verified.account_id,
                cycle_id,
                kind,
                quantity,
                cost,
                realized_pnl: Decimal::ZERO,
                ts_event: verified.ts_event,
                ts_init: verified.ts_event,
            });
        }

        if output_costs.next().is_some() {
            return Err("Convert output cost allocation had extra entries".to_string());
        }

        input_changes.extend(output_changes);
        Ok((input_changes, currency))
    }

    fn validate_convert_scope(&self, verified: &VerifiedConvert) -> Result<Currency, String> {
        if verified.affected_instruments.is_empty() {
            return Err("`affected_instruments` was empty".to_string());
        }

        let mut affected = verified.affected_instruments.clone();
        affected.sort_unstable();
        if affected.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err("`affected_instruments` contained duplicate instrument IDs".to_string());
        }
        let mut inputs = verified
            .inputs
            .iter()
            .map(|input| input.instrument_id)
            .collect::<Vec<_>>();
        inputs.sort_unstable();
        if inputs.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err("`inputs` contained duplicate instrument IDs".to_string());
        }
        let mut outputs = verified
            .outputs
            .iter()
            .map(|output| output.instrument_id)
            .collect::<Vec<_>>();
        outputs.sort_unstable();
        if outputs.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err("`outputs` contained duplicate instrument IDs".to_string());
        }

        if inputs.iter().any(|input| outputs.contains(input)) {
            return Err("Convert inputs and outputs shared an instrument ID".to_string());
        }

        for instrument_id in &inputs {
            if !affected.contains(instrument_id) {
                return Err(format!(
                    "affected instruments did not include input {instrument_id}"
                ));
            }
        }

        for instrument_id in &outputs {
            if !affected.contains(instrument_id) {
                return Err(format!(
                    "affected instruments did not include output {instrument_id}"
                ));
            }
        }

        let cache = self.cache.borrow();
        let mut currency = None;

        for instrument_id in &affected {
            let instrument = cache
                .instrument(instrument_id)
                .ok_or_else(|| format!("instrument {instrument_id} was not loaded"))?;
            if let Some(currency) = currency {
                if instrument.cost_currency() != currency {
                    return Err(format!(
                        "instrument {instrument_id} cost currency differed from {currency}"
                    ));
                }
            } else {
                currency = Some(instrument.cost_currency());
            }
        }
        let currency = currency.expect("affected instruments were not empty");

        if verified.net_cash.currency != currency {
            return Err(format!(
                "net cash currency {} differed from settlement currency {currency}",
                verified.net_cash.currency
            ));
        }

        if verified.net_cash.as_decimal() < Decimal::ZERO {
            return Err("net cash cannot be negative".to_string());
        }

        for input in &verified.inputs {
            let instrument = cache
                .instrument(&input.instrument_id)
                .expect("input instrument was validated");
            if input.quantity <= Decimal::ZERO {
                return Err(format!(
                    "input {} quantity was not positive",
                    input.instrument_id
                ));
            }
            exact_convert_quantity(
                input.quantity,
                instrument.size_precision(),
                &format!("input {}", input.instrument_id),
            )?;
        }

        for output in &verified.outputs {
            let instrument = cache
                .instrument(&output.instrument_id)
                .expect("output instrument was validated");
            if output.quantity < Decimal::ZERO {
                return Err(format!(
                    "output {} quantity was negative",
                    output.instrument_id
                ));
            }
            exact_convert_quantity(
                output.quantity,
                instrument.size_precision(),
                &format!("output {}", output.instrument_id),
            )?;
        }

        Ok(currency)
    }

    fn expired_convert_instrument(&self, verified: &VerifiedConvert) -> Option<InstrumentId> {
        let cache = self.cache.borrow();
        verified.affected_instruments.iter().copied().find(|id| {
            cache
                .instrument_close(id)
                .is_some_and(|close| close.close_type == InstrumentCloseType::ContractExpired)
        })
    }

    fn convert_has_replay_evidence(&self, verified: &VerifiedConvert) -> bool {
        let cache = self.cache.borrow();
        verified.affected_instruments.iter().any(|instrument_id| {
            let position_id = convert_position_id(*instrument_id, verified.strategy_id);
            cache.position_owned(&position_id).is_some_and(|position| {
                position.replay_events.iter().any(|event| {
                    matches!(
                        event,
                        PositionReplayEvent::Converted(change)
                            if change.operation_id == verified.operation_id
                    )
                })
            })
        })
    }

    fn missing_convert_changes(
        &self,
        plan: &[PositionConvert],
    ) -> (Vec<PositionConvert>, Vec<String>) {
        let cache = self.cache.borrow();
        let mut missing = Vec::new();
        let mut conflicts = Vec::new();

        for change in plan {
            let Some(position) = cache.position_owned(&change.position_id) else {
                missing.push(*change);
                continue;
            };

            let identity_matches = position.id == change.position_id
                && position.trader_id == change.trader_id
                && position.strategy_id == change.strategy_id
                && position.account_id == change.account_id
                && position.instrument_id == change.instrument_id;
            if !identity_matches {
                missing.push(*change);
                conflicts.push(format!(
                    "{}: recorded position identity differed",
                    change.instrument_id
                ));
                continue;
            }

            match position.has_convert_change(change) {
                Ok(true) => {}
                Ok(false) => missing.push(*change),
                Err(e) => {
                    missing.push(*change);
                    conflicts.push(format!("{}: {e}", change.instrument_id));
                }
            }
        }

        (missing, conflicts)
    }
}

fn merge_verified_convert(
    original: &mut VerifiedConvert,
    incoming: &VerifiedConvert,
) -> Result<bool, String> {
    if original.operation_id != incoming.operation_id
        || original.trader_id != incoming.trader_id
        || original.strategy_id != incoming.strategy_id
        || original.account_id != incoming.account_id
    {
        return Err("operation identity or ownership differed".to_string());
    }

    if original.transaction.wallet != incoming.transaction.wallet {
        return Err("wallet identity differed".to_string());
    }

    if original
        .inputs
        .iter()
        .any(|input| input.quantity <= Decimal::ZERO)
        || incoming
            .inputs
            .iter()
            .any(|input| input.quantity <= Decimal::ZERO)
        || original
            .outputs
            .iter()
            .any(|output| output.quantity < Decimal::ZERO)
        || incoming
            .outputs
            .iter()
            .any(|output| output.quantity < Decimal::ZERO)
    {
        return Err("input or output token quantities were invalid".to_string());
    }

    let mut original_inputs = sorted_input_amounts(&original.inputs);
    let mut incoming_inputs = sorted_input_amounts(&incoming.inputs);
    original_inputs.sort_unstable_by_key(|amount| amount.0);
    incoming_inputs.sort_unstable_by_key(|amount| amount.0);
    if original_inputs != incoming_inputs {
        return Err("input quantities differed".to_string());
    }

    let mut original_outputs = sorted_nonzero_output_amounts(&original.outputs);
    let mut incoming_outputs = sorted_nonzero_output_amounts(&incoming.outputs);
    original_outputs.sort_unstable_by_key(|amount| amount.0);
    incoming_outputs.sort_unstable_by_key(|amount| amount.0);
    if original_outputs != incoming_outputs {
        return Err("output quantities differed".to_string());
    }

    if sorted_instruments(&original.affected_instruments)
        != sorted_instruments(&incoming.affected_instruments)
    {
        return Err("affected instrument scope differed".to_string());
    }

    if original.net_cash != incoming.net_cash {
        return Err("net cash effect differed".to_string());
    }

    let transaction = merge_convert_transaction(&original.transaction, &incoming.transaction)?;
    let transaction_changed = original.transaction != transaction;
    original.transaction = transaction;

    Ok(transaction_changed)
}

/// Merges compatible original-transaction contexts without combining unrelated locators.
///
/// # Errors
///
/// Returns an error if wallets differ or an existing locator field conflicts.
pub(crate) fn merge_convert_transaction(
    original: &ConvertTransaction,
    incoming: &ConvertTransaction,
) -> Result<ConvertTransaction, String> {
    if original.wallet != incoming.wallet {
        return Err("wallet identity differed".to_string());
    }

    let mut transaction = original.clone();
    merge_optional(
        &mut transaction.relayer_transaction_id,
        incoming.relayer_transaction_id.as_deref(),
        "relayer transaction ID",
    )?;
    merge_optional(
        &mut transaction.transaction_hash,
        incoming.transaction_hash.as_deref(),
        "transaction hash",
    )?;
    merge_optional(
        &mut transaction.nonce,
        incoming.nonce.as_deref(),
        "wallet nonce",
    )?;
    merge_optional(
        &mut transaction.call_summary,
        incoming.call_summary.as_deref(),
        "call summary",
    )?;
    Ok(transaction)
}

fn merge_optional(
    original: &mut Option<String>,
    incoming: Option<&str>,
    field: &str,
) -> Result<(), String> {
    match (original.as_deref(), incoming) {
        (Some(original), Some(incoming)) if original != incoming => {
            Err(format!("{field} differed"))
        }
        (None, Some(incoming)) => {
            *original = Some(incoming.to_string());
            Ok(())
        }
        _ => Ok(()),
    }
}

fn sorted_input_amounts(amounts: &[ConvertTokenAmount]) -> Vec<(InstrumentId, Decimal)> {
    amounts
        .iter()
        .map(|amount| (amount.instrument_id, amount.quantity))
        .collect()
}

fn sorted_nonzero_output_amounts(amounts: &[ConvertTokenAmount]) -> Vec<(InstrumentId, Decimal)> {
    amounts
        .iter()
        .filter(|amount| !amount.quantity.is_zero())
        .map(|amount| (amount.instrument_id, amount.quantity))
        .collect()
}

fn sorted_instruments(instruments: &[InstrumentId]) -> Vec<InstrumentId> {
    let mut instruments = instruments.to_vec();
    instruments.sort_unstable();
    instruments.dedup();
    instruments
}

fn plan_instruments(plan: &[PositionConvert]) -> Vec<InstrumentId> {
    sorted_instruments(
        &plan
            .iter()
            .map(|change| change.instrument_id)
            .collect::<Vec<_>>(),
    )
}

fn input_with_pending_evidence(
    verified: &VerifiedConvert,
    pending_inputs: &[InstrumentId],
) -> Option<InstrumentId> {
    verified
        .inputs
        .iter()
        .find(|input| pending_inputs.contains(&input.instrument_id))
        .map(|input| input.instrument_id)
}

fn validate_convert_position(
    cache: &Cache,
    position: &Position,
    instrument: &InstrumentAny,
    verified: &VerifiedConvert,
    position_id: PositionId,
) -> Result<(), String> {
    if position.id != position_id {
        return Err(format!(
            "position {} did not match canonical ID {position_id}",
            position.id
        ));
    }

    if position.trader_id != verified.trader_id
        || position.strategy_id != verified.strategy_id
        || position.account_id != verified.account_id
        || position.instrument_id != instrument.id()
    {
        return Err(format!(
            "position {position_id} did not belong to trader {}, strategy {}, account {}",
            verified.trader_id, verified.strategy_id, verified.account_id
        ));
    }

    if cache.oms_type(&position_id) != Some(OmsType::Netting) {
        return Err(format!("position {position_id} was not a NETTING position"));
    }

    if position.size_precision != instrument.size_precision()
        || position.settlement_currency != instrument.cost_currency()
    {
        return Err(format!(
            "position {position_id} precision or settlement currency did not match instrument {}",
            instrument.id()
        ));
    }

    Ok(())
}

/// Returns the canonical netting position ID targeted by a conversion.
#[must_use]
pub fn convert_position_id(instrument_id: InstrumentId, strategy_id: StrategyId) -> PositionId {
    PositionId::new(format!("{instrument_id}-{strategy_id}"))
}

/// Converts a decimal quantity to chain-facing units without rounding.
///
/// # Errors
///
/// Returns an error if the quantity is negative or cannot be represented exactly at `precision`.
pub fn exact_convert_quantity(
    value: Decimal,
    precision: u8,
    field: &str,
) -> Result<Quantity, String> {
    if value < Decimal::ZERO {
        return Err(format!("`{field}` quantity cannot be negative"));
    }
    let quantity = Quantity::from_decimal_dp(value, precision).map_err(|e| {
        format!("`{field}` quantity cannot be represented at precision {precision}: {e}")
    })?;

    if quantity.as_decimal() != value {
        return Err(format!(
            "`{field}` quantity cannot be represented exactly at precision {precision}"
        ));
    }
    Ok(quantity)
}

struct ConvertPlanValidation {
    inputs: Vec<PolymarketConvertInput>,
    outputs: Vec<PolymarketConvertOutput>,
    input_pnl: Vec<(InstrumentId, Decimal)>,
    output_costs: Vec<(InstrumentId, Decimal)>,
}

#[derive(Clone, Copy)]
enum ConvertApplyMode {
    Fresh { currency: Currency },
    Retained,
}

fn validate_convert_plan_structure(
    verified: &VerifiedConvert,
    plan: &[PositionConvert],
) -> Result<ConvertPlanValidation, String> {
    if plan.is_empty() {
        return Err("Convert plan was empty".to_string());
    }

    let positive_outputs = verified
        .outputs
        .iter()
        .filter(|output| output.quantity > Decimal::ZERO)
        .count();

    if plan.len() != verified.inputs.len() + positive_outputs {
        return Err(format!(
            "Convert plan had {} changes for {} inputs and {} positive outputs",
            plan.len(),
            verified.inputs.len(),
            positive_outputs
        ));
    }

    let mut plan_instruments = plan
        .iter()
        .map(|change| change.instrument_id)
        .collect::<Vec<_>>();
    plan_instruments.sort_unstable();
    if plan_instruments.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err("Convert plan contained duplicate instrument changes".to_string());
    }

    let mut allocation_inputs = Vec::with_capacity(verified.inputs.len());
    let mut plan_input_pnl = Vec::with_capacity(verified.inputs.len());
    for input in &verified.inputs {
        let change = plan
            .iter()
            .find(|change| change.instrument_id == input.instrument_id)
            .ok_or_else(|| format!("Convert plan did not include input {}", input.instrument_id))?;
        if !matches!(change.kind, PositionConvertKind::Decrease) {
            return Err(format!(
                "Convert plan input {} was not a decrease",
                input.instrument_id
            ));
        }

        if !verified
            .affected_instruments
            .contains(&change.instrument_id)
        {
            return Err(format!(
                "Convert plan input {} was outside the affected scope",
                input.instrument_id
            ));
        }

        if change.quantity.as_decimal() != input.quantity {
            return Err(format!(
                "Convert plan input {} quantity differed",
                input.instrument_id
            ));
        }

        if change.operation_id != verified.operation_id
            || change.trader_id != verified.trader_id
            || change.strategy_id != verified.strategy_id
            || change.account_id != verified.account_id
        {
            return Err(format!(
                "Convert plan input {} identity differed",
                input.instrument_id
            ));
        }

        if change.position_id != convert_position_id(input.instrument_id, verified.strategy_id) {
            return Err(format!(
                "Convert plan input {} position ID differed",
                input.instrument_id
            ));
        }

        allocation_inputs.push(PolymarketConvertInput {
            instrument_id: input.instrument_id,
            quantity: input.quantity,
            released_cost: change.cost,
        });
        plan_input_pnl.push((input.instrument_id, change.realized_pnl));
    }

    let mut allocation_outputs = Vec::new();
    let mut plan_output_costs = Vec::new();

    for output in verified
        .outputs
        .iter()
        .filter(|output| output.quantity > Decimal::ZERO)
    {
        let change = plan
            .iter()
            .find(|change| change.instrument_id == output.instrument_id)
            .ok_or_else(|| {
                format!(
                    "Convert plan did not include output {}",
                    output.instrument_id
                )
            })?;

        if !matches!(
            change.kind,
            PositionConvertKind::Increase | PositionConvertKind::Open { .. }
        ) {
            return Err(format!(
                "Convert plan output {} was not an increase or opening",
                output.instrument_id
            ));
        }

        if change.quantity.as_decimal() != output.quantity {
            return Err(format!(
                "Convert plan output {} quantity differed",
                output.instrument_id
            ));
        }

        if change.operation_id != verified.operation_id
            || change.trader_id != verified.trader_id
            || change.strategy_id != verified.strategy_id
            || change.account_id != verified.account_id
        {
            return Err(format!(
                "Convert plan output {} identity differed",
                output.instrument_id
            ));
        }

        if change.position_id != convert_position_id(output.instrument_id, verified.strategy_id) {
            return Err(format!(
                "Convert plan output {} position ID differed",
                output.instrument_id
            ));
        }

        if !change.realized_pnl.is_zero() {
            return Err(format!(
                "Convert plan output {} carried realized PnL",
                output.instrument_id
            ));
        }

        if !verified
            .affected_instruments
            .contains(&change.instrument_id)
        {
            return Err(format!(
                "Convert plan output {} was outside the affected scope",
                output.instrument_id
            ));
        }

        if matches!(change.kind, PositionConvertKind::Open { .. })
            && change.cycle_id != verified.operation_id
        {
            return Err(format!(
                "Convert plan output {} opening cycle differed",
                output.instrument_id
            ));
        }

        allocation_outputs.push(PolymarketConvertOutput {
            instrument_id: output.instrument_id,
            quantity: output.quantity,
        });
        plan_output_costs.push((output.instrument_id, change.cost));
    }

    Ok(ConvertPlanValidation {
        inputs: allocation_inputs,
        outputs: allocation_outputs,
        input_pnl: plan_input_pnl,
        output_costs: plan_output_costs,
    })
}

fn validate_convert_plan_allocations(
    verified: &VerifiedConvert,
    validation: &ConvertPlanValidation,
    currency: Currency,
) -> Result<(), String> {
    let allocation = allocate_convert_costs_precise(
        &validation.inputs,
        &validation.outputs,
        verified.net_cash.as_decimal(),
        currency,
    )
    .map_err(|e| format!("Convert plan allocations were invalid: {e}"))?;

    let mut expected_input_pnl = validation.input_pnl.clone();
    expected_input_pnl.sort_unstable_by_key(|entry| entry.0);
    if allocation.input_pnl != expected_input_pnl {
        return Err("Convert plan input PnL no longer matches the verified effect".to_string());
    }

    let mut expected_output_costs = validation.output_costs.clone();
    expected_output_costs.sort_unstable_by_key(|entry| entry.0);
    if allocation.output_costs != expected_output_costs {
        return Err("Convert plan output cost no longer matches the verified effect".to_string());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use nautilus_model::instruments::neg_risk::{
        CtfNegRiskEvent, NegRiskCondition, NegRiskOutcome,
    };
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::engine::polymarket_convert::calculate_convert_effect;

    fn event(fee_bps: u16) -> CtfNegRiskEvent {
        let conditions = (0_u8..3)
            .map(|index| NegRiskCondition {
                index,
                condition_id: [index + 1; 32],
                yes: NegRiskOutcome {
                    token_id: [index * 2 + 1; 32],
                    instrument_id: InstrumentId::from(format!("YES-{index}.POLYMARKET")),
                },
                no: NegRiskOutcome {
                    token_id: [index * 2 + 2; 32],
                    instrument_id: InstrumentId::from(format!("NO-{index}.POLYMARKET")),
                },
            })
            .collect::<Vec<_>>();
        let mut market_id = [0; 32];
        market_id[0] = 1;
        CtfNegRiskEvent::new_checked(market_id, fee_bps, conditions).unwrap()
    }

    fn inputs(quantities: &[(u8, Decimal)]) -> Vec<ConvertTokenAmount> {
        quantities
            .iter()
            .map(|(index, quantity)| ConvertTokenAmount {
                instrument_id: InstrumentId::from(format!("NO-{index}.POLYMARKET")),
                quantity: *quantity,
            })
            .collect()
    }

    #[rstest]
    #[case(0, dec!(100), dec!(100), dec!(100))]
    #[case(250, dec!(0.000003), dec!(0.000003), dec!(0.000003))]
    #[case(250, dec!(100), dec!(97.5), dec!(97.5))]
    fn test_convert_effect_uses_floored_chain_fee(
        #[case] fee_bps: u16,
        #[case] quantity: Decimal,
        #[case] expected_output: Decimal,
        #[case] expected_cash: Decimal,
    ) {
        let event = event(fee_bps);
        let inputs = inputs(&[(0, quantity), (1, quantity)]);
        let effect = calculate_convert_effect(&event, &inputs).unwrap();

        assert_eq!(effect.outputs.len(), 1);
        assert_eq!(effect.outputs[0].quantity, expected_output);
        assert_eq!(effect.net_cash, expected_cash);
    }

    #[rstest]
    fn test_convert_effect_with_no_outputs_returns_all_cash() {
        let event = event(0);
        let inputs = inputs(&[(0, dec!(100)), (1, dec!(100)), (2, dec!(100))]);
        let effect = calculate_convert_effect(&event, &inputs).unwrap();

        assert!(effect.outputs.is_empty());
        assert_eq!(effect.net_cash, dec!(200));
    }

    #[rstest]
    fn test_convert_effect_rejects_decimal_overflow_without_panicking() {
        let event = event(0);
        let inputs = inputs(&[(0, Decimal::MAX), (1, Decimal::MAX)]);

        assert!(calculate_convert_effect(&event, &inputs).is_err());
    }

    #[rstest]
    fn test_pending_convert_inventory_prevents_reuse_until_both_released() {
        let position_id = PositionId::from("NO-0.POLYMARKET-CONVERT-001");
        let first = UUID4::new();
        let second = UUID4::new();
        let mut inventory = ConvertPendingInventory::default();

        inventory
            .reserve(first, vec![(position_id, dec!(100))])
            .unwrap();
        inventory
            .reserve(second, vec![(position_id, dec!(100))])
            .unwrap();
        assert_eq!(inventory.consumed(&position_id), dec!(200));

        assert!(inventory.release(first));
        assert_eq!(inventory.consumed(&position_id), dec!(100));
        assert!(inventory.release(second));
        assert_eq!(inventory.consumed(&position_id), Decimal::ZERO);
        assert!(!inventory.release(second));
    }

    #[rstest]
    fn test_pending_convert_inventory_reservation_is_atomic_on_overflow() {
        let position_a = PositionId::from("NO-0.POLYMARKET-CONVERT-001");
        let position_b = PositionId::from("NO-1.POLYMARKET-CONVERT-001");
        let first = UUID4::new();
        let second = UUID4::new();
        let mut inventory = ConvertPendingInventory::default();

        inventory
            .reserve(first, vec![(position_a, Decimal::MAX)])
            .unwrap();
        let error = inventory
            .reserve(second, vec![(position_b, dec!(1)), (position_a, dec!(1))])
            .unwrap_err();

        assert!(error.contains("overflow"), "{error}");
        assert_eq!(inventory.consumed(&position_b), Decimal::ZERO);
        assert_eq!(inventory.consumed(&position_a), Decimal::MAX);
    }
}
