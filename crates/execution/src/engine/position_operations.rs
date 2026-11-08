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

//! Application of confirmed managed position operation effects.

use std::str::FromStr;

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use indexmap::IndexSet;
use nautilus_common::{
    cache::{Cache, settled_cycle_id},
    messages::execution::{
        PositionOperationControlRequest, PositionOperationEffects, PositionOperationRecovery,
        PositionOperationRecoveryLeg, PositionOperationRecoveryOutcome, PositionOperationResult,
        PositionOperationStartupRecovery, PositionOperationStatus, PositionOperationTokenAmount,
        PositionOperationTokenPayout, QueryAccount,
    },
};
use nautilus_core::UUID4;
use nautilus_model::{
    ctf::U256,
    data::InstrumentClose,
    enums::{InstrumentCloseType, OmsType, OrderSide},
    events::{PositionChanged, PositionClosed, PositionEvent, PositionOpened},
    identifiers::{AccountId, ClientId, InstrumentId, PositionId, StrategyId},
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    position::{Position, PositionReplayEvent},
    position_operation::{PositionOperationDirection, PositionOperationLeg, PositionOperationType},
    types::Quantity,
};
use rust_decimal::Decimal;

use super::ExecutionEngine;

/// Local book inputs checked again once the complete operation barrier is installed.
#[derive(Debug)]
struct BarrierInputs {
    strategy_id: StrategyId,
    account_id: AccountId,
    quantity: Option<Quantity>,
    instrument_ids: Vec<InstrumentId>,
}

/// One account-scoped barrier held on an instrument by an unresolved operation.
///
/// A `None` account is a wildcard used by recovery records that do not identify their account.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PositionOperationBarrier {
    operation_id: UUID4,
    account_id: Option<AccountId>,
}

impl PositionOperationBarrier {
    /// Returns whether this barrier blocks orders for `account_id`.
    pub(crate) fn matches_account(&self, account_id: AccountId) -> bool {
        self.account_id
            .is_none_or(|existing| existing == account_id)
    }
}

#[derive(Debug)]
struct OperationLegPlan {
    instrument: InstrumentAny,
    prior: Option<Position>,
    leg: PositionOperationLeg,
    /// Authoritative close to apply to a newly received output in the same application batch.
    settled_close: Option<InstrumentClose>,
}

/// A settled payout marker resolved to the archived cycle it consumed.
#[derive(Debug)]
struct AppliedSettledLeg {
    position_id: PositionId,
    instrument_id: InstrumentId,
    strategy_id: StrategyId,
    quantity: Quantity,
}

impl AppliedSettledLeg {
    fn matches_fields(&self, leg: &ExpectedOperationLeg) -> bool {
        self.instrument_id == leg.instrument_id
            && self.strategy_id == leg.strategy_id
            && self.quantity.as_decimal() == leg.quantity.as_decimal()
    }

    fn matches_exact(&self, leg: &ExpectedOperationLeg) -> bool {
        self.matches_fields(leg) && self.position_id == leg.position_id
    }

    fn matches_archived(&self, leg: &ExpectedOperationLeg) -> bool {
        self.matches_fields(leg) && position_cycle_matches(leg.position_id, self.position_id)
    }
}

/// One owner of redeemable tokens, either an open position or a settled physical holding.
#[derive(Debug)]
struct RedeemSource {
    position_id: PositionId,
    strategy_id: StrategyId,
    account_id: AccountId,
    size_precision: u8,
    quantity: Decimal,
    open_position: Option<Position>,
    settled: bool,
}

impl ExecutionEngine {
    /// Processes an internal adapter-to-core control request.
    pub(crate) fn process_position_operation_control(
        &mut self,
        request: &PositionOperationControlRequest,
    ) {
        match request {
            PositionOperationControlRequest::Barrier {
                operation_id,
                operation,
                instrument_ids,
                strategy_id,
                account_id,
                quantity,
                input_instrument_ids,
                reply,
            } => {
                let inputs = BarrierInputs {
                    strategy_id: *strategy_id,
                    account_id: *account_id,
                    quantity: *quantity,
                    instrument_ids: input_instrument_ids.clone(),
                };
                let result = self.establish_full_barrier(
                    *operation_id,
                    *operation,
                    instrument_ids,
                    Some(&inputs),
                );
                (reply)(result.map_err(|e| e.to_string()));
            }
            PositionOperationControlRequest::Record { key, record, reply } => {
                let result = self.save_position_operation_record(key, record);
                (reply)(result.map_err(|e| e.to_string()));
            }
            PositionOperationControlRequest::Recover { recovery, reply } => {
                let outcome = self
                    .reconcile_recovered_position_operation(recovery)
                    .unwrap_or_else(|e| PositionOperationRecoveryOutcome::Unresolved {
                        message: e.to_string(),
                    });
                (reply)(Ok(outcome));
            }
        }
    }

    /// Restores barriers for unresolved managed operations loaded from the persistent cache.
    ///
    /// Only the account-level barrier scope saved with each record is used. Applied, rejected, or
    /// definitively failed operations no longer block the account.
    pub(crate) fn restore_position_operation_barriers(&self) {
        const PREFIX: &str = "position-operation:";

        let entries = self.cache.borrow().general_entries_with_prefix(PREFIX);
        for (key, bytes) in entries {
            let value: serde_json::Value = match serde_json::from_slice(&bytes) {
                Ok(value) => value,
                Err(e) => {
                    log::error!("Failed to read position operation record `{key}`: {e}");
                    continue;
                }
            };

            let application_status = value
                .get("application_status")
                .and_then(serde_json::Value::as_str);
            let venue_status = value
                .get("venue_status")
                .and_then(serde_json::Value::as_str);
            if matches!(application_status, Some("Rejected"))
                || matches!(venue_status, Some("Failed" | "Invalid"))
            {
                continue;
            }

            let operation_id: UUID4 = match value
                .get("operation_id")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
            {
                Ok(Some(operation_id)) => operation_id,
                Ok(None) => {
                    log::error!("Position operation record `{key}` has no operation identity");
                    continue;
                }
                Err(e) => {
                    log::error!("Invalid position operation identity in `{key}`: {e}");
                    continue;
                }
            };

            let instrument_ids: Vec<InstrumentId> = match value
                .get("barrier_instrument_ids")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
            {
                Ok(Some(instrument_ids)) => instrument_ids,
                Ok(None) => {
                    log::error!("Position operation record `{key}` has no barrier scope");
                    continue;
                }
                Err(e) => {
                    log::error!("Invalid position operation barrier scope in `{key}`: {e}");
                    continue;
                }
            };

            let account_id: Option<AccountId> = match value
                .get("account_id")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
            {
                Ok(account_id) => account_id,
                Err(e) => {
                    log::error!("Invalid position operation account in `{key}`: {e}");
                    None
                }
            };

            if let Some(account_id) = account_id {
                let mut cache = self.cache.borrow_mut();
                for instrument_id in &instrument_ids {
                    if let Err(e) = cache.restore_position_snapshots_for(instrument_id, account_id)
                    {
                        log::warn!(
                            "Cannot restore position snapshots for {instrument_id} during operation recovery: {e}"
                        );
                    }
                }
            }

            let cached_close = {
                let cache = self.cache.borrow();
                instrument_ids
                    .iter()
                    .filter(|instrument_id| cache.instrument_close(instrument_id).is_some())
                    .copied()
                    .collect::<Vec<_>>()
            };
            {
                let barrier = PositionOperationBarrier {
                    operation_id,
                    account_id,
                };
                let mut barriers = self.position_operation_barriers.borrow_mut();
                for instrument_id in &instrument_ids {
                    barriers.entry(*instrument_id).or_default().insert(barrier);
                }
            }
            self.deferred_instrument_closes
                .borrow_mut()
                .extend(cached_close);
            log::warn!(
                "Restored position operation barrier for unresolved operation {operation_id}"
            );
        }
    }

    /// Prepares adapter-held startup recovery evidence before reconciliation.
    ///
    /// This must run outside an active engine borrow so a configured recovery client may query
    /// its venue asynchronously.
    ///
    /// # Errors
    ///
    /// Returns an error if any execution client cannot prepare its recovery evidence.
    pub async fn prepare_startup_position_operation_recovery(&mut self) -> anyhow::Result<()> {
        for adapter in self.clients.values_mut() {
            adapter.client.prepare_startup_recovery().await?;
        }
        Ok(())
    }

    /// Reconciles prepared startup recoveries before ordinary venue reconciliation.
    pub fn apply_startup_position_operation_recovery(&mut self) {
        let mut prepared = Vec::new();
        for adapter in self.clients.values_mut() {
            prepared.push((adapter.client_id, adapter.client.take_startup_recovery()));
        }

        for (client_id, items) in prepared {
            for item in items {
                match item {
                    PositionOperationStartupRecovery::Reconcile(recovery) => {
                        let outcome = self.reconcile_recovered_position_operation(&recovery);
                        let message = match outcome {
                            Ok(PositionOperationRecoveryOutcome::Applied { .. }) => continue,
                            Ok(PositionOperationRecoveryOutcome::Unresolved { message }) => message,
                            Err(e) => e.to_string(),
                        };
                        if let Some(adapter) = self.clients.get(&client_id) {
                            adapter.client.on_position_operation_recovery_unresolved(
                                recovery.operation_id,
                                PositionOperationStatus::Confirmed,
                                recovery.venue_transaction_id.as_deref(),
                                recovery.transaction_hash.as_deref(),
                                &message,
                            );
                        }
                    }
                    PositionOperationStartupRecovery::Unresolved(unresolved) => {
                        if let Some(adapter) = self.clients.get(&client_id) {
                            adapter.client.on_position_operation_recovery_unresolved(
                                unresolved.operation_id,
                                unresolved.venue_status,
                                unresolved.transaction_id.as_deref(),
                                unresolved.transaction_hash.as_deref(),
                                &unresolved.message,
                            );
                        }
                    }
                    PositionOperationStartupRecovery::Failed(failed) => {
                        self.release_position_operation_barrier(
                            failed.operation_id,
                            &failed.instrument_ids,
                        );
                        if let Some(adapter) = self.clients.get(&client_id) {
                            adapter.client.on_position_operation_recovery_failed(
                                failed.operation_id,
                                failed.transaction_id.as_deref(),
                                failed.transaction_hash.as_deref(),
                                &failed.message,
                            );
                        }
                    }
                }
            }
        }
    }

    /// Reconciles a recovered operation record with persisted position replay state.
    ///
    /// Existing legs are compared by stable identity and exact accounting values. Missing legs
    /// are completed only when the original allocation is fully determined. No position event is
    /// published for legs that replay already contains.
    fn reconcile_recovered_position_operation(
        &mut self,
        recovery: &PositionOperationRecovery,
    ) -> anyhow::Result<PositionOperationRecoveryOutcome> {
        anyhow::ensure!(
            !recovery.instrument_ids.is_empty(),
            "position operation recovery has no barrier scope"
        );

        {
            let mut cache = self.cache.borrow_mut();

            for instrument_id in &recovery.instrument_ids {
                if let Err(e) =
                    cache.restore_position_snapshots_for(instrument_id, recovery.account_id)
                {
                    log::warn!(
                        "Cannot restore position snapshots for {instrument_id} during recovery: {e}"
                    );
                }
            }
        }

        let existing = self.operation_replay_legs(recovery)?;

        if existing.is_empty()
            && recovery.applied_legs.is_empty()
            && self.applied_settled_legs(recovery)?.is_empty()
        {
            let Some(effects) = recovery.effects.as_ref() else {
                return Ok(PositionOperationRecoveryOutcome::Unresolved {
                    message: "no verified effects or recorded legs are available".into(),
                });
            };
            let result = self.recovery_result(recovery, Some(effects));

            return match self.apply_position_operation(&result, effects)? {
                Some(legs) => {
                    self.finish_recovered_operation(recovery, &legs, &result);
                    Ok(PositionOperationRecoveryOutcome::Applied {
                        legs: recovery_legs(&legs),
                    })
                }
                None => Ok(PositionOperationRecoveryOutcome::Unresolved {
                    message: "verified effects do not determine complete operation legs".into(),
                }),
            };
        }

        let expected = if recovery.applied_legs.is_empty() {
            let effects = recovery.effects.as_ref().ok_or_else(|| {
                anyhow::anyhow!("partial replay legs require verified effects for recovery")
            })?;
            self.expected_from_effects(recovery, effects, &existing)?
        } else {
            expected_from_record(recovery)?
        };

        let replay_expected = expected
            .iter()
            .filter(|leg| !leg.settled)
            .cloned()
            .collect::<Vec<_>>();
        let settled_expected = expected
            .iter()
            .filter(|leg| leg.settled)
            .cloned()
            .collect::<Vec<_>>();

        let expected_keys: AHashSet<_> = replay_expected
            .iter()
            .map(|leg| {
                (
                    recovery.operation_id,
                    leg.direction,
                    leg.instrument_id,
                    leg.position_id,
                )
            })
            .collect();
        anyhow::ensure!(
            expected_keys.len() == replay_expected.len(),
            "operation recovery requires unique leg identities"
        );
        anyhow::ensure!(
            existing.keys().all(|key| expected_keys.contains(key)),
            "operation replay contains legs outside the recorded allocation"
        );

        let mut missing = Vec::new();
        for leg in &replay_expected {
            let key = (
                recovery.operation_id,
                leg.direction,
                leg.instrument_id,
                leg.position_id,
            );
            match existing.get(&key) {
                Some(replayed) => validate_recovery_leg(recovery, leg, replayed)?,
                None => missing.push(leg.clone()),
            }
        }

        self.validate_expected_quantities(recovery, &expected)?;

        let applied_settled = self.applied_settled_legs(recovery)?;
        let mut matched = vec![false; applied_settled.len()];
        let mut leg_matched = vec![false; settled_expected.len()];

        // Match exact position identities first so the order of recorded legs cannot let a
        // base PositionId consume an archived cycle before its exact leg is matched.
        for (leg_index, leg) in settled_expected.iter().enumerate() {
            if let Some((index, _)) = applied_settled
                .iter()
                .enumerate()
                .find(|(index, applied)| !matched[*index] && applied.matches_exact(leg))
            {
                matched[index] = true;
                leg_matched[leg_index] = true;
            }
        }

        // Only then allow a base record leg to match the archived cycle carrying its UUID suffix.
        for (leg_index, leg) in settled_expected.iter().enumerate() {
            if leg_matched[leg_index] {
                continue;
            }
            let Some((index, _)) = applied_settled
                .iter()
                .enumerate()
                .find(|(index, applied)| !matched[*index] && applied.matches_archived(leg))
            else {
                anyhow::bail!(
                    "settled payout for {} has no per-cycle consumption evidence",
                    leg.instrument_id
                );
            };
            matched[index] = true;
            leg_matched[leg_index] = true;
        }
        anyhow::ensure!(
            matched.iter().all(|matched| *matched) && leg_matched.iter().all(|matched| *matched),
            "settled consumption evidence for {} has no matching recorded leg",
            recovery.operation_id
        );

        let settled_legs = settled_expected
            .iter()
            .map(|leg| expected_settled_leg(recovery, leg, self.clock.borrow().timestamp_ns()))
            .collect::<Vec<_>>();

        if missing.is_empty() {
            let mut legs = Vec::with_capacity(replay_expected.len() + settled_legs.len());
            for leg in &replay_expected {
                let key = (
                    recovery.operation_id,
                    leg.direction,
                    leg.instrument_id,
                    leg.position_id,
                );
                legs.push(
                    existing
                        .get(&key)
                        .cloned()
                        .expect("every expected leg is present"),
                );
            }
            legs.extend(settled_legs);

            self.complete_recovered_settlements(recovery, &expected, &existing)?;
            let result = self.recovery_result(recovery, recovery.effects.as_ref());
            self.finish_recovered_operation(recovery, &legs, &result);
            return Ok(PositionOperationRecoveryOutcome::Applied {
                legs: recovery_legs(&legs),
            });
        }

        for leg in &missing {
            anyhow::ensure!(
                leg.cost.is_some() && leg.income_known,
                "cannot reconstruct missing {} leg for {} without recorded allocation",
                leg.direction,
                leg.instrument_id
            );
        }

        let plans = self.recovery_leg_plans(recovery, &missing)?;
        self.commit_operation_legs(plans)?;

        let refreshed = self.operation_replay_legs(recovery)?;
        anyhow::ensure!(
            refreshed.len() == replay_expected.len(),
            "completed recovery did not produce every expected leg"
        );

        let mut legs = Vec::with_capacity(replay_expected.len() + settled_legs.len());
        for leg in &replay_expected {
            let key = (
                recovery.operation_id,
                leg.direction,
                leg.instrument_id,
                leg.position_id,
            );
            let replayed = refreshed.get(&key).ok_or_else(|| {
                anyhow::anyhow!(
                    "completed recovery is missing {} leg for {}",
                    leg.direction,
                    leg.instrument_id
                )
            })?;
            validate_recovery_leg(recovery, leg, replayed)?;
            legs.push(replayed.clone());
        }
        legs.extend(settled_legs);

        self.complete_recovered_settlements(recovery, &expected, &existing)?;
        let result = self.recovery_result(recovery, recovery.effects.as_ref());
        self.finish_recovered_operation(recovery, &legs, &result);
        Ok(PositionOperationRecoveryOutcome::Applied {
            legs: recovery_legs(&legs),
        })
    }

    /// Completes the economic settlement of already received convert outputs during recovery.
    ///
    /// The received leg, cost allocation, and payout basis are already retained. This method only
    /// applies the missing settlement and records the physical source; it never re-applies the
    /// opening leg. A cycle that is already settled is left unchanged.
    fn complete_recovered_settlements(
        &self,
        recovery: &PositionOperationRecovery,
        expected: &[ExpectedOperationLeg],
        existing: &AHashMap<
            (UUID4, PositionOperationDirection, InstrumentId, PositionId),
            PositionOperationLeg,
        >,
    ) -> anyhow::Result<()> {
        if recovery.operation != PositionOperationType::Convert {
            return Ok(());
        }

        let ts_init = self.clock.borrow().timestamp_ns();
        let mut events = Vec::new();
        {
            let mut cache = self.cache.borrow_mut();
            for leg in expected
                .iter()
                .filter(|leg| !leg.settled && leg.direction == PositionOperationDirection::Open)
            {
                let Some(close) = settled_output_close(&cache, &leg.instrument_id)? else {
                    continue;
                };
                let Some(replayed) = existing.get(&(
                    recovery.operation_id,
                    leg.direction,
                    leg.instrument_id,
                    leg.position_id,
                )) else {
                    // A missing leg is completed through the normal apply path, which also
                    // settles the output and records its source.
                    continue;
                };

                let Some(position) = cache
                    .position(&leg.position_id)
                    .map(|position| position.cloned())
                else {
                    anyhow::bail!(
                        "recovered settle output {} is missing its position",
                        leg.instrument_id
                    );
                };

                match output_cycle_settlement_state(&position, replayed) {
                    OutputSettlementState::Missing => {}
                    OutputSettlementState::Settled => {
                        if current_cycle_settled(&position, replayed)
                            && let Some(physical) = settled_physical_quantity(&position)
                            && let Err(e) =
                                cache.record_settled_position_source(&position, physical)
                        {
                            log::error!(
                                "Failed to backfill settled source for {}: {e}",
                                leg.instrument_id
                            );
                        }
                    }
                    OutputSettlementState::Unsettled => {
                        let Some(pending) = output_cycle_pending_quantity(&position, replayed)
                        else {
                            anyhow::bail!(
                                "recovered settle output {} has indeterminate pending quantity",
                                leg.instrument_id
                            );
                        };

                        if pending.is_zero() {
                            anyhow::ensure!(
                                !position.is_open(),
                                "settle output {} has no pending quantity but remains open",
                                leg.instrument_id
                            );
                            // The received shares were fully sold or consumed before the market
                            // resolved, so there is no physical holding left to settle.
                            continue;
                        }
                        anyhow::ensure!(
                            position.is_open(),
                            "settle output {} replay is not settled but its position is closed",
                            leg.instrument_id
                        );
                        let last_qty = position.quantity;
                        let confirmation = ts_init.max(position.ts_opened).max(close.ts_event);
                        let settled = cache.update_position_from_instrument_close_at(
                            leg.position_id,
                            close,
                            confirmation,
                        )?;
                        let mut event_close = close;
                        event_close.ts_event = confirmation;
                        events.push(PositionEvent::PositionClosed(
                            PositionClosed::create_from_instrument_close(
                                &settled,
                                &event_close,
                                last_qty,
                                UUID4::new(),
                                ts_init,
                            ),
                        ));
                    }
                }
            }
        }
        self.publish_position_events(events);
        Ok(())
    }

    /// Checks that the recovered legs account for every token quantity in the receipt.
    fn validate_expected_quantities(
        &self,
        recovery: &PositionOperationRecovery,
        expected: &[ExpectedOperationLeg],
    ) -> anyhow::Result<()> {
        let Some(effects) = recovery.effects.as_ref() else {
            return Ok(());
        };

        let tokens = self.token_instrument_map(&recovery.instrument_ids);
        let spent = resolve_effects(&effects.spent, &tokens)?;
        let received = resolve_effects(&effects.received, &tokens)?;
        let mut expected_spent = AHashMap::<InstrumentId, Decimal>::new();
        let mut expected_received = AHashMap::<InstrumentId, Decimal>::new();

        for leg in expected {
            let target = match leg.direction {
                PositionOperationDirection::Open => &mut expected_received,
                PositionOperationDirection::Close => &mut expected_spent,
            };
            *target.entry(leg.instrument_id).or_default() += leg.quantity.as_decimal();
        }

        for (instrument, quantity) in spent {
            let expected = expected_spent.remove(&instrument.id()).unwrap_or_default();
            anyhow::ensure!(
                expected == quantity.as_decimal(),
                "recovered legs spend {expected} of {}, receipt spent {}",
                instrument.id(),
                quantity.as_decimal()
            );
        }

        for (instrument, quantity) in received {
            let expected = expected_received
                .remove(&instrument.id())
                .unwrap_or_default();
            anyhow::ensure!(
                expected == quantity.as_decimal(),
                "recovered legs receive {expected} of {}, receipt received {}",
                instrument.id(),
                quantity.as_decimal()
            );
        }
        anyhow::ensure!(
            expected_spent.is_empty() && expected_received.is_empty(),
            "recovered legs contain instruments outside the receipt"
        );
        Ok(())
    }

    fn recovery_result(
        &self,
        recovery: &PositionOperationRecovery,
        effects: Option<&PositionOperationEffects>,
    ) -> PositionOperationResult {
        let ts = self.clock.borrow().timestamp_ns();
        PositionOperationResult::new(
            recovery.trader_id,
            recovery.strategy_id,
            recovery.client_id,
            recovery.account_id,
            recovery.operation_id,
            recovery.operation,
            PositionOperationStatus::Confirmed,
            recovery.instrument_ids.clone(),
            recovery.venue_transaction_id.clone(),
            recovery.transaction_hash.clone(),
            None,
            effects.cloned(),
            UUID4::new(),
            ts,
            ts,
        )
    }

    fn finish_recovered_operation(
        &mut self,
        recovery: &PositionOperationRecovery,
        legs: &[PositionOperationLeg],
        result: &PositionOperationResult,
    ) {
        self.applied_position_operations
            .insert(recovery.operation_id);
        self.release_position_operation_barrier(recovery.operation_id, &recovery.instrument_ids);
        self.confirm_position_operation(result, legs);
    }

    fn operation_replay_legs(
        &self,
        recovery: &PositionOperationRecovery,
    ) -> anyhow::Result<
        AHashMap<
            (UUID4, PositionOperationDirection, InstrumentId, PositionId),
            PositionOperationLeg,
        >,
    > {
        let mut legs = AHashMap::new();
        let cache = self.cache.borrow();

        for position in cache.positions(None, None, None, None, None) {
            for event in &position.replay_events {
                let PositionReplayEvent::Operation(leg) = event else {
                    continue;
                };
                if leg.operation_id != recovery.operation_id {
                    continue;
                }
                anyhow::ensure!(
                    leg.operation == recovery.operation,
                    "operation replay leg type does not match its record"
                );
                anyhow::ensure!(
                    leg.trader_id == recovery.trader_id && leg.account_id == recovery.account_id,
                    "operation replay leg belongs to a different owner"
                );

                let key = leg.identity();
                if let Some(previous) = legs.insert(key, leg.clone()) {
                    anyhow::ensure!(
                        previous == *leg,
                        "operation replay contains conflicting duplicate legs"
                    );
                }
            }
        }
        Ok(legs)
    }

    fn applied_settled_legs(
        &self,
        recovery: &PositionOperationRecovery,
    ) -> anyhow::Result<Vec<AppliedSettledLeg>> {
        let prefix = format!(
            "{SETTLED_CONSUMPTION_PREFIX}:operation:{}:",
            recovery.operation_id
        );
        let legacy_prefix = format!("{SETTLED_CONSUMPTION_PREFIX}:{}:", recovery.operation_id);
        let mut entries = self.cache.borrow().general_entries_with_prefix(&prefix);
        entries.extend(
            self.cache
                .borrow()
                .general_entries_with_prefix(&legacy_prefix),
        );
        let cache = self.cache.borrow();
        let mut legs = Vec::new();
        let mut seen = AHashSet::new();

        for (key, bytes) in entries {
            let position = if let Some(cycle_id) = key.strip_prefix(&prefix) {
                settled_source_by_cycle(&cache, cycle_id, recovery.account_id).ok_or_else(|| {
                    anyhow::anyhow!(
                        "settled payout marker for {} references missing cycle {cycle_id}",
                        recovery.operation_id
                    )
                })?
            } else if let Some(legacy) = key.strip_prefix(&legacy_prefix) {
                let mut parts = legacy.rsplitn(2, ':');
                let cycle = parts
                    .next()
                    .and_then(|cycle| nautilus_core::UnixNanos::from_str(cycle).ok())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "legacy settled payout marker for {} has an invalid cycle",
                            recovery.operation_id
                        )
                    })?;
                let position_id = parts.next().map(PositionId::from).ok_or_else(|| {
                    anyhow::anyhow!(
                        "legacy settled payout marker for {} has no position",
                        recovery.operation_id
                    )
                })?;
                legacy_settled_source(&cache, position_id, cycle, recovery.account_id)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "legacy settled payout marker for {} references missing cycle {position_id}@{cycle}",
                            recovery.operation_id
                        )
                    })?
            } else {
                continue;
            };

            if !seen.insert(settled_cycle_id(&position)) {
                continue;
            }

            let value = std::str::from_utf8(&bytes)
                .ok()
                .and_then(|value| Decimal::from_str(value).ok())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "settled payout marker for {} cycle {} is invalid",
                        recovery.operation_id,
                        position.id
                    )
                })?;
            let quantity = Quantity::from_decimal_dp(value, position.size_precision)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            legs.push(AppliedSettledLeg {
                position_id: position.id,
                instrument_id: position.instrument_id,
                strategy_id: position.strategy_id,
                quantity,
            });
        }
        Ok(legs)
    }

    fn expected_from_effects(
        &self,
        recovery: &PositionOperationRecovery,
        effects: &PositionOperationEffects,
        existing: &AHashMap<
            (UUID4, PositionOperationDirection, InstrumentId, PositionId),
            PositionOperationLeg,
        >,
    ) -> anyhow::Result<Vec<ExpectedOperationLeg>> {
        let tokens = self.token_instrument_map(&recovery.instrument_ids);
        let mut received = resolve_effects(&effects.received, &tokens)?;
        let mut spent = resolve_effects(&effects.spent, &tokens)?;
        let collateral = Decimal::from_str(&effects.collateral_delta)
            .context("invalid collateral delta in recovered operation effects")?;

        match recovery.operation {
            PositionOperationType::Split => {
                anyhow::ensure!(
                    received.len() == 2,
                    "split recovery requires two outcome credits"
                );
                anyhow::ensure!(
                    !collateral.is_sign_positive(),
                    "split recovery must spend pUSD"
                );
                received.sort_by_key(|(instrument, _)| instrument.id());
                let total_cost = -collateral;
                let mut allocated = Decimal::ZERO;
                let mut legs = Vec::with_capacity(2);
                for (index, (instrument, quantity)) in received.into_iter().enumerate() {
                    let cost = if index == 1 {
                        total_cost - allocated
                    } else {
                        let cost = total_cost / Decimal::from(2u8);
                        allocated += cost;
                        cost
                    };
                    legs.push(ExpectedOperationLeg {
                        position_id: netting_position_id(&instrument.id(), recovery.strategy_id),
                        instrument_id: instrument.id(),
                        strategy_id: recovery.strategy_id,
                        direction: PositionOperationDirection::Open,
                        quantity,
                        cost: Some(cost),
                        income_known: true,
                        income: None,
                        settled: false,
                    });
                }
                Ok(legs)
            }
            PositionOperationType::Merge => {
                anyhow::ensure!(
                    spent.len() == 2,
                    "merge recovery requires two outcome debits"
                );
                anyhow::ensure!(
                    !collateral.is_sign_negative(),
                    "merge recovery must receive pUSD"
                );
                spent.sort_by_key(|(instrument, _)| instrument.id());
                let mut allocated = Decimal::ZERO;
                let mut legs = Vec::with_capacity(2);
                for (index, (instrument, quantity)) in spent.into_iter().enumerate() {
                    let income = if index == 1 {
                        collateral - allocated
                    } else {
                        let income = collateral / Decimal::from(2u8);
                        allocated += income;
                        income
                    };
                    legs.push(ExpectedOperationLeg {
                        position_id: self.recovery_close_position(
                            recovery,
                            &instrument.id(),
                            existing,
                        )?,
                        instrument_id: instrument.id(),
                        strategy_id: recovery.strategy_id,
                        direction: PositionOperationDirection::Close,
                        quantity,
                        cost: None,
                        income_known: true,
                        income: Some(income),
                        settled: false,
                    });
                }
                Ok(legs)
            }
            PositionOperationType::Redeem => {
                anyhow::ensure!(
                    !spent.is_empty(),
                    "redeem recovery requires at least one spent token"
                );
                anyhow::ensure!(
                    !collateral.is_sign_negative(),
                    "redeem payout must be non-negative"
                );

                let mut payout_map = AHashMap::new();
                for payout in &effects.payouts {
                    let numerator = U256::from_str_radix(&payout.numerator, 10)
                        .map_err(|_| anyhow::anyhow!("invalid payout numerator"))?;
                    let denominator = U256::from_str_radix(&payout.denominator, 10)
                        .map_err(|_| anyhow::anyhow!("invalid payout denominator"))?;
                    anyhow::ensure!(
                        !denominator.is_zero() && numerator <= denominator,
                        "invalid CTF payout evidence"
                    );
                    payout_map.insert(
                        ustr::Ustr::from(payout.token_id.as_str()),
                        (numerator, denominator),
                    );
                }

                let spent_len = spent.len();

                if spent_len == 1 && payout_map.is_empty() {
                    let (instrument, quantity) = spent.into_iter().next().expect("one spent token");
                    anyhow::ensure!(
                        collateral >= Decimal::ZERO,
                        "redeem payout must be non-negative"
                    );

                    let applied_settled: Vec<_> = self
                        .applied_settled_legs(recovery)?
                        .into_iter()
                        .filter(|leg| leg.instrument_id == instrument.id())
                        .collect();

                    if !applied_settled.is_empty() {
                        let weights = applied_settled
                            .iter()
                            .map(|leg| leg.quantity.as_decimal())
                            .collect::<Vec<_>>();
                        let incomes = allocate(collateral, &weights);
                        return Ok(applied_settled
                            .into_iter()
                            .zip(incomes)
                            .map(|(leg, income)| ExpectedOperationLeg {
                                position_id: leg.position_id,
                                instrument_id: instrument.id(),
                                strategy_id: leg.strategy_id,
                                direction: PositionOperationDirection::Close,
                                quantity: leg.quantity,
                                cost: Some(Decimal::ZERO),
                                income_known: true,
                                income: Some(income),
                                settled: true,
                            })
                            .collect());
                    }

                    let position = self.existing_position(
                        &instrument.id(),
                        recovery.strategy_id,
                        recovery.account_id,
                    )?;
                    let Some(position) = position else {
                        anyhow::bail!(
                            "cannot recover redeem leg for {}: no position exists",
                            instrument.id()
                        );
                    };

                    if position.is_closed() {
                        let Some(physical) = settled_physical_quantity(&position) else {
                            anyhow::bail!(
                                "cannot reconstruct settled physical quantity for {}",
                                position.id
                            );
                        };
                        let consumed = {
                            let cache = self.cache.borrow();
                            settled_consumed(&cache, &position)
                        };
                        let remaining = physical.as_decimal() - consumed;
                        anyhow::ensure!(
                            remaining == quantity.as_decimal(),
                            "settled physical quantity for {} does not match the redeem",
                            instrument.id()
                        );
                        let quantity =
                            Quantity::from_decimal_dp(remaining, position.size_precision)
                                .map_err(|e| anyhow::anyhow!("{e}"))?;
                        return Ok(vec![ExpectedOperationLeg {
                            position_id: position.id,
                            instrument_id: instrument.id(),
                            strategy_id: position.strategy_id,
                            direction: PositionOperationDirection::Close,
                            quantity,
                            cost: Some(Decimal::ZERO),
                            income_known: true,
                            income: Some(collateral),
                            settled: true,
                        }]);
                    }

                    return Ok(vec![ExpectedOperationLeg {
                        position_id: position.id,
                        instrument_id: instrument.id(),
                        strategy_id: position.strategy_id,
                        direction: PositionOperationDirection::Close,
                        quantity,
                        cost: None,
                        income_known: true,
                        income: Some(collateral),
                        settled: false,
                    }]);
                }

                let applied_settled = self.applied_settled_legs(recovery)?;
                let mut total_income = Decimal::ZERO;
                let mut legs = Vec::with_capacity(spent.len());
                for (instrument, quantity) in spent {
                    let token_id = instrument
                        .info()
                        .and_then(|info| info.get_str("token_id"))
                        .map(ustr::Ustr::from)
                        .ok_or_else(|| {
                            anyhow::anyhow!("redeem instrument {} has no token id", instrument.id())
                        })?;
                    let (numerator, denominator) =
                        payout_map.get(&token_id).copied().ok_or_else(|| {
                            anyhow::anyhow!("missing payout evidence for token {}", instrument.id())
                        })?;

                    let replayed: Vec<&PositionOperationLeg> = existing
                        .values()
                        .filter(|leg| {
                            leg.direction == PositionOperationDirection::Close
                                && leg.instrument_id == instrument.id()
                        })
                        .collect();
                    let settled_applied: Vec<_> = applied_settled
                        .iter()
                        .filter(|leg| leg.instrument_id == instrument.id())
                        .collect();

                    if !replayed.is_empty() || !settled_applied.is_empty() {
                        let token_payout = nautilus_model::ctf::ctf_payout_amount(
                            quantity,
                            numerator,
                            denominator,
                        )?;
                        total_income += token_payout;

                        let mut entries = replayed
                            .iter()
                            .map(|leg| {
                                (
                                    leg.position_id,
                                    leg.strategy_id,
                                    leg.quantity,
                                    Some(*leg),
                                    false,
                                )
                            })
                            .chain(settled_applied.iter().map(|leg| {
                                (leg.position_id, leg.strategy_id, leg.quantity, None, true)
                            }))
                            .collect::<Vec<_>>();
                        entries.sort_by_key(|(position_id, _, _, _, _)| position_id.to_string());
                        let weights = entries
                            .iter()
                            .map(|(_, _, quantity, _, _)| quantity.as_decimal())
                            .collect::<Vec<_>>();
                        let incomes = allocate(token_payout, &weights);

                        for ((position_id, strategy_id, quantity, leg, settled), income) in
                            entries.into_iter().zip(incomes)
                        {
                            if let Some(leg) = leg {
                                anyhow::ensure!(
                                    leg.income == Some(income),
                                    "replayed CTF redeem income for {} does not match its payout",
                                    instrument.id()
                                );
                                legs.push(ExpectedOperationLeg {
                                    position_id,
                                    instrument_id: instrument.id(),
                                    strategy_id,
                                    direction: PositionOperationDirection::Close,
                                    quantity,
                                    cost: Some(leg.cost),
                                    income_known: true,
                                    income: Some(income),
                                    settled,
                                });
                            } else {
                                legs.push(ExpectedOperationLeg {
                                    position_id,
                                    instrument_id: instrument.id(),
                                    strategy_id,
                                    direction: PositionOperationDirection::Close,
                                    quantity,
                                    cost: Some(Decimal::ZERO),
                                    income_known: true,
                                    income: Some(income),
                                    settled,
                                });
                            }
                        }
                        continue;
                    }

                    let (mut positions, settled_positions) = {
                        let cache = self.cache.borrow();
                        let positions = cache
                            .positions_open(
                                None,
                                Some(&instrument.id()),
                                None,
                                Some(&recovery.account_id),
                                None,
                            )
                            .into_iter()
                            .filter(|position| position.is_long())
                            .map(|position| position.cloned())
                            .collect::<Vec<_>>();

                        let mut settled_positions = Vec::new();
                        for position in cache.positions(
                            None,
                            Some(&instrument.id()),
                            None,
                            Some(&recovery.account_id),
                            None,
                        ) {
                            if !position.is_closed() {
                                continue;
                            }
                            let Some(physical) = settled_physical_quantity(&position) else {
                                anyhow::ensure!(
                                    !position.replay_events.iter().any(|event| matches!(
                                        event,
                                        PositionReplayEvent::InstrumentClosed(_)
                                    )),
                                    "cannot reconstruct settled physical quantity for {}",
                                    position.id
                                );
                                continue;
                            };
                            let consumed = settled_consumed(&cache, &position);
                            let remaining = physical.as_decimal() - consumed;
                            if remaining > Decimal::ZERO {
                                settled_positions.push((position.cloned(), remaining));
                            }
                        }
                        (positions, settled_positions)
                    };
                    positions.sort_by_key(|position| position.id.to_string());

                    let open_available: Decimal = positions
                        .iter()
                        .map(|position| position.quantity.as_decimal())
                        .sum();
                    let settled_available: Decimal = settled_positions
                        .iter()
                        .map(|(_, quantity)| *quantity)
                        .sum();
                    anyhow::ensure!(
                        !positions.is_empty() || !settled_positions.is_empty(),
                        "no known positions explain the CTF redeem balance for {}",
                        instrument.id()
                    );
                    anyhow::ensure!(
                        open_available + settled_available == quantity.as_decimal(),
                        "known positions do not fully explain the CTF redeem balance for {}",
                        instrument.id()
                    );

                    let token_payout =
                        nautilus_model::ctf::ctf_payout_amount(quantity, numerator, denominator)?;
                    total_income += token_payout;

                    let mut entries = positions
                        .into_iter()
                        .map(|position| (position, false))
                        .chain(
                            settled_positions
                                .into_iter()
                                .map(|(position, _)| (position, true)),
                        )
                        .collect::<Vec<_>>();
                    entries.sort_by_key(|(position, _)| position.id.to_string());
                    let weights = entries
                        .iter()
                        .map(|(position, settled)| {
                            if *settled {
                                let physical =
                                    settled_physical_quantity(position).unwrap_or_default();
                                physical.as_decimal()
                                    - settled_consumed(&self.cache.borrow(), position)
                            } else {
                                position.quantity.as_decimal()
                            }
                        })
                        .collect::<Vec<_>>();
                    let incomes = allocate(token_payout, &weights);

                    for ((position, settled), income) in entries.into_iter().zip(incomes) {
                        let quantity = if settled {
                            let physical =
                                settled_physical_quantity(&position).ok_or_else(|| {
                                    anyhow::anyhow!(
                                        "cannot reconstruct settled physical quantity for {}",
                                        position.id
                                    )
                                })?;
                            let remaining = physical.as_decimal()
                                - settled_consumed(&self.cache.borrow(), &position);
                            Quantity::from_decimal_dp(remaining, position.size_precision)
                                .map_err(|e| anyhow::anyhow!("{e}"))?
                        } else {
                            position.quantity
                        };
                        legs.push(ExpectedOperationLeg {
                            position_id: position.id,
                            instrument_id: instrument.id(),
                            strategy_id: position.strategy_id,
                            direction: PositionOperationDirection::Close,
                            quantity,
                            cost: if settled { Some(Decimal::ZERO) } else { None },
                            income_known: true,
                            income: Some(income),
                            settled,
                        });
                    }
                }
                anyhow::ensure!(
                    total_income == collateral,
                    "recovered CTF payout allocation does not match the collateral return"
                );
                Ok(legs)
            }
            PositionOperationType::Convert => {
                anyhow::ensure!(
                    !spent.is_empty(),
                    "convert recovery requires NO input tokens"
                );
                anyhow::ensure!(
                    !collateral.is_sign_negative(),
                    "convert collateral return must be non-negative"
                );
                spent.sort_by_key(|(instrument, _)| instrument.id());
                received.sort_by_key(|(instrument, _)| instrument.id());

                let mut inputs = Vec::with_capacity(spent.len());
                let mut consumed_cost = Decimal::ZERO;
                let mut all_inputs_present = true;
                for (instrument, quantity) in &spent {
                    let position_id =
                        self.recovery_close_position(recovery, &instrument.id(), existing)?;
                    let key = (
                        recovery.operation_id,
                        PositionOperationDirection::Close,
                        instrument.id(),
                        position_id,
                    );
                    let cost = existing.get(&key).map(|leg| leg.cost);
                    if let Some(cost) = cost {
                        consumed_cost += cost;
                    } else {
                        all_inputs_present = false;
                    }
                    inputs.push((instrument.id(), *quantity, position_id, cost));
                }

                let has_outputs = !received.is_empty();
                let (immediate_pnl, output_cost) = if all_inputs_present {
                    let immediate_pnl = if has_outputs {
                        (collateral - consumed_cost).max(Decimal::ZERO)
                    } else {
                        collateral - consumed_cost
                    };
                    let output_cost = if has_outputs {
                        (consumed_cost - collateral).max(Decimal::ZERO)
                    } else {
                        Decimal::ZERO
                    };
                    (Some(immediate_pnl), Some(output_cost))
                } else {
                    (None, None)
                };

                let pnl_shares = immediate_pnl.map(|pnl| {
                    let weights: Vec<Decimal> = if consumed_cost.is_zero() {
                        inputs
                            .iter()
                            .map(|(_, quantity, _, _)| quantity.as_decimal())
                            .collect()
                    } else {
                        inputs
                            .iter()
                            .map(|(_, _, _, cost)| cost.unwrap_or(Decimal::ZERO))
                            .collect()
                    };
                    allocate(pnl, &weights)
                });

                let mut legs = Vec::with_capacity(inputs.len() + received.len());
                for (index, (instrument_id, quantity, position_id, cost)) in
                    inputs.into_iter().enumerate()
                {
                    let income = match (cost, &pnl_shares) {
                        (Some(cost), Some(shares)) => Some(cost + shares[index]),
                        _ => None,
                    };
                    legs.push(ExpectedOperationLeg {
                        position_id,
                        instrument_id,
                        strategy_id: recovery.strategy_id,
                        direction: PositionOperationDirection::Close,
                        quantity,
                        cost,
                        income_known: income.is_some(),
                        income,
                        settled: false,
                    });
                }

                let output_costs = output_cost.map(|cost| {
                    let weights: Vec<Decimal> = received
                        .iter()
                        .map(|(_, quantity)| quantity.as_decimal())
                        .collect();
                    allocate(cost, &weights)
                });

                for (index, (instrument, quantity)) in received.into_iter().enumerate() {
                    legs.push(ExpectedOperationLeg {
                        position_id: netting_position_id(&instrument.id(), recovery.strategy_id),
                        instrument_id: instrument.id(),
                        strategy_id: recovery.strategy_id,
                        direction: PositionOperationDirection::Open,
                        quantity,
                        cost: output_costs.as_ref().map(|costs| costs[index]),
                        income_known: true,
                        income: None,
                        settled: false,
                    });
                }
                Ok(legs)
            }
        }
    }

    fn recovery_close_position(
        &self,
        recovery: &PositionOperationRecovery,
        instrument_id: &InstrumentId,
        existing: &AHashMap<
            (UUID4, PositionOperationDirection, InstrumentId, PositionId),
            PositionOperationLeg,
        >,
    ) -> anyhow::Result<PositionId> {
        if let Some(leg) = existing.values().find(|leg| {
            leg.direction == PositionOperationDirection::Close
                && leg.instrument_id == *instrument_id
        }) {
            return Ok(leg.position_id);
        }

        self.existing_position(instrument_id, recovery.strategy_id, recovery.account_id)?
            .map(|position| position.id)
            .ok_or_else(|| {
                anyhow::anyhow!("cannot recover close leg for {instrument_id}: no position exists")
            })
    }

    fn recovery_leg_plans(
        &self,
        recovery: &PositionOperationRecovery,
        missing: &[ExpectedOperationLeg],
    ) -> anyhow::Result<Vec<OperationLegPlan>> {
        let ts = self.clock.borrow().timestamp_ns();
        let mut plans = Vec::with_capacity(missing.len());
        let cache = self.cache.borrow();

        for leg in missing {
            let instrument = cache
                .instrument(&leg.instrument_id)
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "cannot recover operation leg: instrument {} is not in the cache",
                        leg.instrument_id
                    )
                })?;
            let prior = cache
                .position(&leg.position_id)
                .map(|position| position.clone());
            anyhow::ensure!(
                leg.direction == PositionOperationDirection::Open || prior.is_some(),
                "cannot recover close leg for {}: position {} is missing",
                leg.instrument_id,
                leg.position_id
            );

            let (trader_id, strategy_id, account_id) = prior.as_ref().map_or(
                (recovery.trader_id, leg.strategy_id, recovery.account_id),
                |position| {
                    (
                        position.trader_id,
                        position.strategy_id,
                        position.account_id,
                    )
                },
            );
            let position_leg = PositionOperationLeg {
                operation_id: recovery.operation_id,
                operation: recovery.operation,
                direction: leg.direction,
                trader_id,
                strategy_id,
                account_id,
                instrument_id: leg.instrument_id,
                position_id: leg.position_id,
                quantity: leg.quantity,
                cost: leg.cost.expect("missing leg cost was validated"),
                income: leg.income,
                settled: false,
                event_id: UUID4::new(),
                ts_event: ts,
                ts_init: ts,
            };

            let settled_close = if recovery.operation == PositionOperationType::Convert
                && leg.direction == PositionOperationDirection::Open
            {
                settled_output_close(&cache, &leg.instrument_id)?
            } else {
                None
            };

            plans.push(OperationLegPlan {
                instrument,
                prior,
                leg: position_leg,
                settled_close,
            });
        }
        Ok(plans)
    }

    fn establish_full_barrier(
        &self,
        operation_id: UUID4,
        operation: PositionOperationType,
        instrument_ids: &[InstrumentId],
        inputs: Option<&BarrierInputs>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !instrument_ids.is_empty(),
            "position operation barrier requires at least one instrument"
        );

        if let Some(inputs) = inputs {
            anyhow::ensure!(
                inputs
                    .instrument_ids
                    .iter()
                    .all(|instrument_id| instrument_ids.contains(instrument_id)),
                "position operation inputs must be inside the barrier scope"
            );
        }

        if operation != PositionOperationType::Redeem {
            let cache = self.cache.borrow();
            let now = self.clock.borrow().timestamp_ns();

            // Convert inputs must remain unsettled, while expected YES outputs may already be
            // settled. Split and merge require every instrument in scope to remain unsettled.
            let input_ids: &[InstrumentId] = match (operation, inputs) {
                (PositionOperationType::Convert, Some(inputs)) => &inputs.instrument_ids,
                _ => instrument_ids,
            };

            for instrument_id in input_ids {
                if cache
                    .instrument_close(instrument_id)
                    .is_some_and(|close| close.close_type == InstrumentCloseType::ContractExpired)
                {
                    anyhow::bail!("instrument {instrument_id} is already economically settled");
                }
            }

            if operation == PositionOperationType::Convert
                && let Some(inputs) = inputs
            {
                for instrument_id in instrument_ids {
                    if inputs.instrument_ids.contains(instrument_id) {
                        continue;
                    }
                    ensure_output_settlement_evidence(&cache, instrument_id, now)?;
                }
            }
        }

        {
            let account_id = inputs.map(|inputs| inputs.account_id);
            let barriers = self.position_operation_barriers.borrow();
            for instrument_id in instrument_ids {
                if let Some(existing) = barriers.get(instrument_id).and_then(|set| {
                    set.iter().find(|barrier| {
                        barrier.account_id.is_none_or(|existing| {
                            account_id.is_none_or(|account_id| existing == account_id)
                        })
                    })
                }) {
                    anyhow::bail!(
                        "instrument {instrument_id} is barred by position operation {}",
                        existing.operation_id
                    );
                }
            }
        }

        {
            let account_id = inputs.map(|inputs| inputs.account_id);
            let cache = self.cache.borrow();
            for instrument_id in instrument_ids {
                // An order that has no account yet could still be routed to the target account,
                // so it counts as active for this account's operation.
                let has_active = cache
                    .orders_open(None, Some(instrument_id), None, None, None)
                    .iter()
                    .any(|order| {
                        account_id.is_none_or(|account_id| {
                            order
                                .account_id()
                                .is_none_or(|existing| existing == account_id)
                        })
                    });
                anyhow::ensure!(
                    !has_active,
                    "instrument {instrument_id} has active CLOB orders"
                );
            }
        }

        {
            let barrier = PositionOperationBarrier {
                operation_id,
                account_id: inputs.map(|inputs| inputs.account_id),
            };
            let mut barriers = self.position_operation_barriers.borrow_mut();
            for instrument_id in instrument_ids {
                barriers.entry(*instrument_id).or_default().insert(barrier);
            }
        }

        if let Some(inputs) = inputs {
            if let Err(e) = self.ensure_barrier_inputs(operation, inputs) {
                self.release_position_operation_barrier(operation_id, instrument_ids);
                return Err(e);
            }

            if let Err(e) = self.ensure_barrier_output_receipts(operation, instrument_ids, inputs) {
                self.release_position_operation_barrier(operation_id, instrument_ids);
                return Err(e);
            }
        }

        Ok(())
    }

    /// Rechecks the receiving strategy has no open short position in an operation output target.
    ///
    /// A normal CLOB sell may use token inventory held by another strategy in the same account,
    /// producing a short book in the originating strategy. Receiving new output shares into that
    /// target would leave the operation effect unappliable, so the full operation is rejected
    /// before signing. Existing open longs and closed historical cycles are valid targets.
    fn ensure_barrier_output_receipts(
        &self,
        operation: PositionOperationType,
        instrument_ids: &[InstrumentId],
        inputs: &BarrierInputs,
    ) -> anyhow::Result<()> {
        let targets: Vec<InstrumentId> = match operation {
            PositionOperationType::Split => instrument_ids.to_vec(),
            PositionOperationType::Convert => instrument_ids
                .iter()
                .filter(|instrument_id| !inputs.instrument_ids.contains(instrument_id))
                .copied()
                .collect(),
            PositionOperationType::Merge | PositionOperationType::Redeem => return Ok(()),
        };

        if targets.is_empty() {
            return Ok(());
        }

        let cache = self.cache.borrow();

        for instrument_id in targets {
            let has_open_short = cache
                .positions_open(
                    None,
                    Some(&instrument_id),
                    Some(&inputs.strategy_id),
                    Some(&inputs.account_id),
                    None,
                )
                .iter()
                .any(|position| position.signed_decimal_qty() < Decimal::ZERO);

            anyhow::ensure!(
                !has_open_short,
                "position operation output {instrument_id} has an open short position for strategy {}",
                inputs.strategy_id
            );
        }

        Ok(())
    }

    /// Rechecks the operation's local book ownership and quantity after the barrier is installed.
    ///
    /// The adapter's earlier check happens before asynchronous metadata and settlement work, so a
    /// CLOB fill can reduce the input position in between. Only a V2 redeem may use the physical
    /// remainder of an economically settled position, and only up to the quantity not yet paid out.
    fn ensure_barrier_inputs(
        &self,
        operation: PositionOperationType,
        inputs: &BarrierInputs,
    ) -> anyhow::Result<()> {
        if operation == PositionOperationType::Split {
            return Ok(());
        }
        if operation == PositionOperationType::Redeem && inputs.instrument_ids.len() == 2 {
            // A CTF redeem is wallet-scoped; ownership is resolved from confirmed effects.
            return Ok(());
        }

        let amount = inputs.quantity.ok_or_else(|| {
            anyhow::anyhow!("position operation barrier requires an explicit quantity")
        })?;
        anyhow::ensure!(
            amount.as_decimal() > Decimal::ZERO,
            "position operation quantity must be positive"
        );

        let cache = self.cache.borrow();
        for instrument_id in &inputs.instrument_ids {
            let open = cache.positions_open(
                None,
                Some(instrument_id),
                Some(&inputs.strategy_id),
                Some(&inputs.account_id),
                None,
            );
            anyhow::ensure!(
                open.len() <= 1,
                "multiple open positions for {instrument_id} in NETTING mode"
            );
            let available = open.first().map_or(Decimal::ZERO, |position| {
                position.signed_decimal_qty().max(Decimal::ZERO)
            });
            if available >= amount.as_decimal() {
                continue;
            }

            anyhow::ensure!(
                operation == PositionOperationType::Redeem,
                "position operation requires {} available {instrument_id}, found {available}",
                amount.as_decimal()
            );
            let position_id = netting_position_id(instrument_id, inputs.strategy_id);
            let position = cache.position(&position_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "position operation requires {} available {instrument_id}, found {available}",
                    amount.as_decimal()
                )
            })?;
            anyhow::ensure!(
                position.account_id == inputs.account_id && position.is_closed(),
                "position operation requires {} available {instrument_id}, found {available}",
                amount.as_decimal()
            );
            let physical = settled_physical_quantity(&position).ok_or_else(|| {
                anyhow::anyhow!(
                    "cannot verify redeemable quantity for settled position {}",
                    position.id
                )
            })?;
            let remaining = physical.as_decimal() - settled_consumed(&cache, &position);
            anyhow::ensure!(
                remaining >= amount.as_decimal(),
                "position operation requires {} redeemable {instrument_id}, found {remaining}",
                amount.as_decimal()
            );
        }

        Ok(())
    }

    fn save_position_operation_record(
        &self,
        key: &str,
        record: &serde_json::Value,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            key.starts_with("position-operation:"),
            "invalid position operation record key"
        );
        let payload = serde_json::to_vec(record)
            .map_err(|e| anyhow::anyhow!("failed to serialize position operation: {e}"))?;
        self.cache
            .borrow_mut()
            .add(key, bytes::Bytes::from(payload))?;
        Ok(())
    }

    /// Releases any barrier held for `operation_id` on the given instruments.
    ///
    /// Deferred contract settlements resume only when no account retains a barrier on the
    /// instrument, since the close itself applies to the whole instrument.
    pub(crate) fn release_position_operation_barrier(
        &self,
        operation_id: UUID4,
        instrument_ids: &[InstrumentId],
    ) {
        let mut released = Vec::new();
        {
            let mut barriers = self.position_operation_barriers.borrow_mut();
            for instrument_id in instrument_ids {
                let Some(set) = barriers.get_mut(instrument_id) else {
                    continue;
                };
                let prior_len = set.len();
                set.retain(|barrier| barrier.operation_id != operation_id);
                if set.len() == prior_len {
                    continue;
                }
                if set.is_empty() {
                    barriers.remove(instrument_id);
                    released.push(*instrument_id);
                }
            }
        }

        for instrument_id in released {
            self.settle_deferred_close_if_unblocked(instrument_id);
        }
    }

    /// Records the instrument scope of a submitted operation whose settlement must wait for the
    /// operation result.
    ///
    /// A contract close for any instrument in the scope is retained and applied only after the
    /// operation is applied or rejected, so a confirmed conversion consumes its inputs before
    /// the remaining inventory settles.
    pub(crate) fn note_position_operation_settlement_scope(
        &self,
        operation_id: UUID4,
        client_id: ClientId,
        instrument_ids: &[InstrumentId],
    ) {
        if instrument_ids.is_empty() {
            return;
        }

        self.position_operation_settlement_scope
            .borrow_mut()
            .insert(operation_id, (client_id, instrument_ids.to_vec()));
    }

    /// Releases the settlement scope recorded for `operation_id` and resumes any deferred close
    /// no longer blocked by an operation or barrier.
    ///
    /// Simulated venues settle their own expirations, including the simulated cash effects, so
    /// their remainder is left to the venue path instead of being closed by the core.
    pub(crate) fn release_position_operation_settlement_scope(&self, operation_id: UUID4) {
        let scope = {
            let mut scope = self.position_operation_settlement_scope.borrow_mut();
            scope.remove(&operation_id)
        };
        let Some((client_id, instrument_ids)) = scope else {
            return;
        };
        let venue_settles = self
            .clients
            .get(&client_id)
            .is_some_and(|adapter| adapter.client.settles_contract_expirations());

        for instrument_id in instrument_ids {
            if venue_settles {
                self.deferred_instrument_closes
                    .borrow_mut()
                    .remove(&instrument_id);
                continue;
            }
            self.settle_deferred_close_if_unblocked(instrument_id);
        }
    }

    /// Returns whether a submitted operation in the scope of `instrument_id` awaits its result.
    pub(crate) fn position_operation_defers_settlement(
        &self,
        instrument_id: &InstrumentId,
    ) -> bool {
        self.position_operation_settlement_scope
            .borrow()
            .values()
            .any(|(_, instrument_ids)| instrument_ids.contains(instrument_id))
    }

    /// Settles a deferred close once no barrier or pending operation covers the instrument.
    fn settle_deferred_close_if_unblocked(&self, instrument_id: InstrumentId) {
        if self.position_operation_barrier(&instrument_id).is_some()
            || self.position_operation_defers_settlement(&instrument_id)
        {
            return;
        }

        if !self
            .deferred_instrument_closes
            .borrow_mut()
            .remove(&instrument_id)
        {
            return;
        }

        let Some(close) = self
            .cache
            .borrow()
            .instrument_close(&instrument_id)
            .copied()
        else {
            return;
        };
        self.settle_instrument_close(&close);
    }

    /// Returns whether any managed position operation currently bars `instrument_id`.
    #[must_use]
    pub fn is_position_operation_barred(&self, instrument_id: &InstrumentId) -> bool {
        self.position_operation_barrier(instrument_id).is_some()
    }

    /// Returns whether a managed position operation currently bars `instrument_id` for
    /// `account_id`.
    #[must_use]
    pub fn is_position_operation_barred_for(
        &self,
        account_id: AccountId,
        instrument_id: &InstrumentId,
    ) -> bool {
        self.position_operation_barrier_for(account_id, instrument_id)
            .is_some()
    }

    /// Returns every instrument currently barred by an unresolved managed position operation.
    #[must_use]
    pub fn position_operation_barred_instruments(&self) -> IndexSet<InstrumentId> {
        self.position_operation_barriers
            .borrow()
            .keys()
            .copied()
            .collect()
    }

    /// Returns the operation currently barring `instrument_id` for any account.
    pub(crate) fn position_operation_barrier(&self, instrument_id: &InstrumentId) -> Option<UUID4> {
        self.position_operation_barriers
            .borrow()
            .get(instrument_id)
            .and_then(|set| set.iter().next().map(|barrier| barrier.operation_id))
    }

    /// Returns the operation barring `instrument_id` for `account_id`.
    ///
    /// Barriers from another account do not block, while a recovery record without an account
    /// remains a wildcard.
    pub(crate) fn position_operation_barrier_for(
        &self,
        account_id: AccountId,
        instrument_id: &InstrumentId,
    ) -> Option<UUID4> {
        self.position_operation_barriers
            .borrow()
            .get(instrument_id)
            .and_then(|set| {
                set.iter()
                    .find(|barrier| barrier.matches_account(account_id))
                    .map(|barrier| barrier.operation_id)
            })
    }

    /// Applies confirmed managed position operation effects exactly once.
    pub(crate) fn process_position_operation(&mut self, result: &PositionOperationResult) {
        if matches!(
            result.status,
            PositionOperationStatus::Rejected
                | PositionOperationStatus::Failed
                | PositionOperationStatus::Applied
        ) {
            self.release_position_operation_barrier(result.operation_id, &result.instrument_ids);
            self.release_position_operation_settlement_scope(result.operation_id);
        }

        if result.status != PositionOperationStatus::Confirmed {
            return;
        }

        if self
            .applied_position_operations
            .contains(&result.operation_id)
        {
            log::debug!(
                "Ignoring duplicate position operation application {}",
                result.operation_id
            );
            return;
        }

        let Some(effects) = &result.effects else {
            log::warn!(
                "Position operation {} is confirmed without parsed effects; local state remains unresolved",
                result.operation_id
            );
            return;
        };

        match self.apply_position_operation(result, effects) {
            Ok(Some(legs)) => {
                self.applied_position_operations.insert(result.operation_id);
                self.release_position_operation_barrier(
                    result.operation_id,
                    &result.instrument_ids,
                );
                self.release_position_operation_settlement_scope(result.operation_id);
                self.confirm_position_operation(result, &legs);
            }
            Ok(None) => {
                log::warn!(
                    "Position operation {} lacks complete ownership or payout evidence; local state remains unresolved",
                    result.operation_id
                );
            }
            Err(e) => {
                log::warn!(
                    "Failed to apply position operation {}: {e}; local state remains unresolved",
                    result.operation_id
                );
            }
        }
    }

    fn confirm_position_operation(
        &self,
        result: &PositionOperationResult,
        legs: &[PositionOperationLeg],
    ) {
        let Some(adapter) = self.clients.get(&result.client_id) else {
            log::warn!(
                "Cannot confirm position operation {}: execution client {} is not registered",
                result.operation_id,
                result.client_id
            );
            return;
        };

        adapter.client.on_position_operation_applied(
            result.operation_id,
            legs,
            result.transaction_hash.as_deref(),
        );

        let ts = self.clock.borrow().timestamp_ns();
        let applied = PositionOperationResult::new(
            result.trader_id,
            result.strategy_id,
            result.client_id,
            result.account_id,
            result.operation_id,
            result.operation,
            PositionOperationStatus::Applied,
            result.instrument_ids.clone(),
            result.venue_transaction_id.clone(),
            result.transaction_hash.clone(),
            None,
            None,
            UUID4::new(),
            ts,
            ts,
        );
        adapter
            .client
            .on_position_operation_applied_result(&applied, legs);

        // Refresh venue balances through the normal account-state path so the applied token and
        // collateral effects reach the account without adding a second cash ledger.
        let query = QueryAccount::new(
            result.trader_id,
            Some(result.client_id),
            result.account_id,
            UUID4::new(),
            self.clock.borrow().timestamp_ns(),
            None,
            Some(result.operation_id),
        );
        self.handle_query_account(adapter.client.as_ref(), query);
    }

    fn apply_position_operation(
        &self,
        result: &PositionOperationResult,
        effects: &PositionOperationEffects,
    ) -> anyhow::Result<Option<Vec<PositionOperationLeg>>> {
        let tokens = self.token_instrument_map(&result.instrument_ids);
        let received = resolve_effects(&effects.received, &tokens)?;
        let spent = resolve_effects(&effects.spent, &tokens)?;

        if result.operation != PositionOperationType::Redeem {
            // A convert may receive settled YES outputs, but its NO inputs must remain
            // unsettled. Split and merge continue to require the whole scope unsettled.
            let settled_scope: Vec<InstrumentId> = match result.operation {
                PositionOperationType::Convert => spent
                    .iter()
                    .map(|(instrument, _)| instrument.id())
                    .collect(),
                _ => result.instrument_ids.clone(),
            };

            let mut defer_now = Vec::new();
            {
                let cache = self.cache.borrow();

                for instrument_id in &settled_scope {
                    let settled = cache.instrument_close(instrument_id).is_some_and(|close| {
                        close.close_type == InstrumentCloseType::ContractExpired
                    });
                    if !settled {
                        continue;
                    }
                    if self
                        .deferred_instrument_closes
                        .borrow()
                        .contains(instrument_id)
                    {
                        continue;
                    }

                    // A close that arrived after the operation was confirmed leaves its input
                    // open for application. Retain it so it settles the inventory the operation
                    // leaves behind instead of blocking the confirmed effects.
                    let input_open = !cache
                        .positions_open(
                            None,
                            Some(instrument_id),
                            Some(&result.strategy_id),
                            Some(&result.account_id),
                            None,
                        )
                        .is_empty();
                    if input_open && self.position_operation_defers_settlement(instrument_id) {
                        defer_now.push(*instrument_id);
                        continue;
                    }

                    log::warn!(
                        "Position operation {} cannot apply to settled instrument {instrument_id}",
                        result.operation_id
                    );
                    return Ok(None);
                }
            }
            self.deferred_instrument_closes
                .borrow_mut()
                .extend(defer_now);
        }

        let collateral = Decimal::from_str(&effects.collateral_delta)
            .context("invalid collateral delta in position operation effects")?;
        let ts_event = result.ts_event;
        let ts_init = self.clock.borrow().timestamp_ns();

        let (plans, settled) = match result.operation {
            PositionOperationType::Split => (
                self.plan_split(result, received, collateral, ts_event, ts_init)?,
                Vec::new(),
            ),
            PositionOperationType::Merge => (
                self.plan_merge(result, spent, collateral, ts_event, ts_init)?,
                Vec::new(),
            ),
            PositionOperationType::Redeem => {
                match self.plan_redeem(
                    result,
                    spent,
                    &effects.payouts,
                    collateral,
                    ts_event,
                    ts_init,
                )? {
                    Some(redeem) => redeem,
                    None => return Ok(None),
                }
            }
            PositionOperationType::Convert => (
                self.plan_convert(result, received, spent, collateral, ts_event, ts_init)?,
                Vec::new(),
            ),
        };

        let mut legs = self.commit_operation_legs(plans)?;
        if !settled.is_empty() {
            self.record_settled_consumption(&settled)?;
            legs.extend(settled);
        }
        Ok(Some(legs))
    }

    fn record_settled_consumption(&self, legs: &[PositionOperationLeg]) -> anyhow::Result<()> {
        let mut errors = Vec::new();

        for leg in legs.iter().filter(|leg| leg.settled) {
            let (key, cycle_id) = {
                let cache = self.cache.borrow();
                let Some(position) =
                    settled_source_position(&cache, leg.position_id, leg.account_id)
                else {
                    log::error!(
                        "Cannot record settled consumption for unknown position {}",
                        leg.position_id
                    );
                    continue;
                };
                (
                    settled_consumption_key(&position),
                    settled_cycle_id(&position),
                )
            };
            let current = {
                let cache = self.cache.borrow();
                cache
                    .get(&key)?
                    .and_then(|bytes| std::str::from_utf8(bytes).ok())
                    .and_then(|value| Decimal::from_str(value).ok())
                    .unwrap_or_default()
            };
            let operation_key = settled_operation_key(&leg.operation_id, &cycle_id);
            let operation_current = {
                let cache = self.cache.borrow();
                cache
                    .get(&operation_key)?
                    .and_then(|bytes| std::str::from_utf8(bytes).ok())
                    .and_then(|value| Decimal::from_str(value).ok())
                    .unwrap_or_default()
            };
            let leg_quantity = leg.quantity.as_decimal();
            let remaining = leg_quantity - operation_current;
            if !remaining.is_sign_positive() {
                // This operation already recorded its payout for this position. Cache persists
                // memory before reporting a database error, so a retry must not add it again.
                continue;
            }

            let next = current + remaining;
            let operation_next = operation_current + remaining;
            {
                let mut cache = self.cache.borrow_mut();
                if let Err(e) = cache.add(&key, bytes::Bytes::from(next.to_string())) {
                    errors.push(e.to_string());
                }
                if let Err(e) = cache.add(
                    &operation_key,
                    bytes::Bytes::from(operation_next.to_string()),
                ) {
                    errors.push(e.to_string());
                }
            }
        }

        if !errors.is_empty() {
            log::error!(
                "Settled position operation consumption was applied in memory with {} persistence error(s): {}",
                errors.len(),
                errors.join("; ")
            );
        }
        Ok(())
    }

    fn token_instrument_map(
        &self,
        instrument_ids: &[InstrumentId],
    ) -> AHashMap<ustr::Ustr, InstrumentAny> {
        let mut map = AHashMap::new();
        let Some(venue) = instrument_ids.first().map(|id| id.venue) else {
            return map;
        };

        let cache = self.cache.borrow();
        for instrument in cache.instruments(&venue, None) {
            if !instrument_ids.contains(&instrument.id()) {
                continue;
            }
            if let Some(info) = instrument.info()
                && let Some(token_id) = info.get_str("token_id")
            {
                map.insert(ustr::Ustr::from(token_id), (*instrument).clone());
            }
        }
        map
    }

    fn existing_position(
        &self,
        instrument_id: &InstrumentId,
        strategy_id: StrategyId,
        account_id: AccountId,
    ) -> anyhow::Result<Option<Position>> {
        let cache = self.cache.borrow();
        let open = cache.positions_open(
            None,
            Some(instrument_id),
            Some(&strategy_id),
            Some(&account_id),
            None,
        );

        match open.as_slice() {
            [position] => Ok(Some(position.cloned())),
            [] => {
                let position_id = netting_position_id(instrument_id, strategy_id);
                Ok(cache
                    .position(&position_id)
                    .map(|position| position.cloned()))
            }
            _ => anyhow::bail!("multiple open positions for {instrument_id} in NETTING mode"),
        }
    }

    fn plan_split(
        &self,
        result: &PositionOperationResult,
        mut received: Vec<(InstrumentAny, Quantity)>,
        collateral: Decimal,
        ts_event: nautilus_core::UnixNanos,
        ts_init: nautilus_core::UnixNanos,
    ) -> anyhow::Result<Vec<OperationLegPlan>> {
        anyhow::ensure!(
            received.len() == 2,
            "split requires two outcome token credits"
        );
        anyhow::ensure!(
            !collateral.is_sign_positive(),
            "split must spend pUSD collateral"
        );
        let total_cost = -collateral;
        received.sort_by_key(|(instrument, _)| instrument.id());

        let mut plans = Vec::with_capacity(2);
        let mut allocated = Decimal::ZERO;
        for (index, (instrument, quantity)) in received.into_iter().enumerate() {
            let cost = if index == 1 {
                total_cost - allocated
            } else {
                let cost = total_cost / Decimal::from(2u8);
                allocated += cost;
                cost
            };
            let position_id = netting_position_id(&instrument.id(), result.strategy_id);
            let prior =
                self.existing_position(&instrument.id(), result.strategy_id, result.account_id)?;
            let leg = build_leg(
                result,
                PositionOperationDirection::Open,
                &instrument,
                position_id,
                quantity,
                cost,
                None,
                ts_event,
                ts_init,
            );
            plans.push(OperationLegPlan {
                instrument,
                prior,
                leg,
                settled_close: None,
            });
        }
        Ok(plans)
    }

    fn plan_merge(
        &self,
        result: &PositionOperationResult,
        mut spent: Vec<(InstrumentAny, Quantity)>,
        collateral: Decimal,
        ts_event: nautilus_core::UnixNanos,
        ts_init: nautilus_core::UnixNanos,
    ) -> anyhow::Result<Vec<OperationLegPlan>> {
        anyhow::ensure!(spent.len() == 2, "merge requires two outcome token debits");
        anyhow::ensure!(!collateral.is_sign_negative(), "merge must receive pUSD");
        spent.sort_by_key(|(instrument, _)| instrument.id());

        let mut plans = Vec::with_capacity(2);
        let mut allocated = Decimal::ZERO;
        for (index, (instrument, quantity)) in spent.into_iter().enumerate() {
            let income = if index == 1 {
                collateral - allocated
            } else {
                let income = collateral / Decimal::from(2u8);
                allocated += income;
                income
            };
            let prior = self
                .existing_position(&instrument.id(), result.strategy_id, result.account_id)?
                .ok_or_else(|| {
                    anyhow::anyhow!("merge input position is not open for {}", instrument.id())
                })?;
            let cost = release_cost(&prior, quantity)?;
            let position_id = prior.id;
            let leg = build_leg(
                result,
                PositionOperationDirection::Close,
                &instrument,
                position_id,
                quantity,
                cost,
                Some(income),
                ts_event,
                ts_init,
            );
            plans.push(OperationLegPlan {
                instrument,
                prior: Some(prior),
                leg,
                settled_close: None,
            });
        }
        Ok(plans)
    }

    fn plan_redeem(
        &self,
        result: &PositionOperationResult,
        spent: Vec<(InstrumentAny, Quantity)>,
        payouts: &[PositionOperationTokenPayout],
        collateral: Decimal,
        ts_event: nautilus_core::UnixNanos,
        ts_init: nautilus_core::UnixNanos,
    ) -> anyhow::Result<Option<(Vec<OperationLegPlan>, Vec<PositionOperationLeg>)>> {
        anyhow::ensure!(
            !collateral.is_sign_negative(),
            "redeem payout must be non-negative"
        );

        let mut payout_map = AHashMap::new();
        for payout in payouts {
            let numerator = U256::from_str_radix(&payout.numerator, 10)
                .map_err(|_| anyhow::anyhow!("invalid CTF payout numerator"))?;
            let denominator = U256::from_str_radix(&payout.denominator, 10)
                .map_err(|_| anyhow::anyhow!("invalid CTF payout denominator"))?;
            if denominator.is_zero() || numerator > denominator {
                return Ok(None);
            }
            payout_map.insert(
                ustr::Ustr::from(payout.token_id.as_str()),
                (numerator, denominator),
            );
        }

        if spent.is_empty() {
            return Ok(None);
        }

        // A CTF redeem burns both outcomes. Allocating the aggregate payout requires verified
        // per-outcome payout evidence for every spent token.
        if spent.len() > 1 || !payout_map.is_empty() {
            let mut plans = Vec::new();
            let mut settled_legs = Vec::new();
            let mut total_income = Decimal::ZERO;

            for (instrument, quantity) in spent {
                let token_id = instrument
                    .info()
                    .and_then(|info| info.get_str("token_id"))
                    .map(ustr::Ustr::from)
                    .ok_or_else(|| {
                        anyhow::anyhow!("redeem instrument {} has no token id", instrument.id())
                    })?;
                let Some((numerator, denominator)) = payout_map.get(&token_id).copied() else {
                    return Ok(None);
                };

                let (open_positions, settled_sources) = {
                    let mut cache = self.cache.borrow_mut();
                    let mut open_positions = cache
                        .positions_open(
                            None,
                            Some(&instrument.id()),
                            None,
                            Some(&result.account_id),
                            None,
                        )
                        .into_iter()
                        .filter(|position| position.is_long())
                        .map(|position| position.cloned())
                        .collect::<Vec<_>>();
                    open_positions.sort_by_key(|position| position.id.to_string());

                    let Some(settled_sources) = settled_redeem_sources(
                        &mut cache,
                        &instrument.id(),
                        result.account_id,
                        None,
                        instrument.size_precision(),
                    ) else {
                        return Ok(None);
                    };
                    (open_positions, settled_sources)
                };

                let open_available: Decimal = open_positions
                    .iter()
                    .map(|position| position.quantity.as_decimal())
                    .sum();
                let settled_available: Decimal =
                    settled_sources.iter().map(|source| source.quantity).sum();
                if open_positions.is_empty() && settled_sources.is_empty() {
                    return Ok(None);
                }
                if open_available + settled_available != quantity.as_decimal() {
                    // The wallet balance is not fully explained by known positions.
                    return Ok(None);
                }

                let token_payout =
                    nautilus_model::ctf::ctf_payout_amount(quantity, numerator, denominator)?;
                total_income += token_payout;

                let mut sources = open_positions
                    .into_iter()
                    .map(|position| RedeemSource {
                        position_id: position.id,
                        strategy_id: position.strategy_id,
                        account_id: position.account_id,
                        size_precision: position.size_precision,
                        quantity: position.quantity.as_decimal(),
                        open_position: Some(position),
                        settled: false,
                    })
                    .chain(settled_sources)
                    .collect::<Vec<_>>();
                sources.sort_by_key(|source| source.position_id.to_string());
                let weights: Vec<Decimal> = sources.iter().map(|source| source.quantity).collect();
                let incomes = allocate(token_payout, &weights);

                for (source, income) in sources.into_iter().zip(incomes) {
                    if source.settled {
                        let quantity =
                            Quantity::from_decimal_dp(source.quantity, source.size_precision)
                                .map_err(|e| anyhow::anyhow!("{e}"))?;
                        settled_legs.push(PositionOperationLeg {
                            operation_id: result.operation_id,
                            operation: result.operation,
                            direction: PositionOperationDirection::Close,
                            trader_id: result.trader_id,
                            strategy_id: source.strategy_id,
                            account_id: source.account_id,
                            instrument_id: instrument.id(),
                            position_id: source.position_id,
                            quantity,
                            cost: Decimal::ZERO,
                            income: Some(income),
                            settled: true,
                            event_id: UUID4::new(),
                            ts_event,
                            ts_init,
                        });
                    } else {
                        let position = source
                            .open_position
                            .expect("an open redeem source carries its position");
                        let position_quantity = position.quantity;
                        let cost = release_cost(&position, position_quantity)?;
                        let leg = build_position_leg(
                            result,
                            PositionOperationDirection::Close,
                            &instrument,
                            &position,
                            position_quantity,
                            cost,
                            Some(income),
                            ts_event,
                            ts_init,
                        );
                        plans.push(OperationLegPlan {
                            instrument: instrument.clone(),
                            prior: Some(position),
                            leg,
                            settled_close: None,
                        });
                    }
                }
            }

            anyhow::ensure!(
                total_income == collateral,
                "CTF payout allocation does not match the actual collateral return"
            );
            return Ok(Some((plans, settled_legs)));
        }

        let (instrument, quantity) = spent.into_iter().next().expect("one spent token");
        let requested = quantity.as_decimal();
        let Some(prior) =
            self.existing_position(&instrument.id(), result.strategy_id, result.account_id)?
        else {
            return Ok(None);
        };

        let mut sources = Vec::new();
        if prior.is_open() {
            sources.push(RedeemSource {
                position_id: prior.id,
                strategy_id: prior.strategy_id,
                account_id: prior.account_id,
                size_precision: prior.size_precision,
                quantity: prior.quantity.as_decimal(),
                open_position: Some(prior),
                settled: false,
            });
        }
        let settled_sources = {
            let mut cache = self.cache.borrow_mut();
            settled_redeem_sources(
                &mut cache,
                &instrument.id(),
                result.account_id,
                Some(result.strategy_id),
                instrument.size_precision(),
            )
        };
        let Some(mut settled_sources) = settled_sources else {
            return Ok(None);
        };
        sources.append(&mut settled_sources);
        sources.sort_by_key(|source| source.position_id.to_string());

        let available: Decimal = sources.iter().map(|source| source.quantity).sum();
        if requested <= Decimal::ZERO || requested > available {
            return Ok(None);
        }

        let mut remaining = requested;
        let mut consumed_sources = Vec::new();

        for source in sources {
            if remaining.is_zero() {
                break;
            }
            let take = source.quantity.min(remaining);
            if take > Decimal::ZERO {
                remaining -= take;
                consumed_sources.push((source, take));
            }
        }

        if remaining > Decimal::ZERO {
            return Ok(None);
        }

        let weights: Vec<Decimal> = consumed_sources.iter().map(|(_, take)| *take).collect();
        let incomes = allocate(collateral, &weights);
        let mut plans = Vec::new();
        let mut settled_legs = Vec::new();

        for ((source, take), income) in consumed_sources.into_iter().zip(incomes) {
            let leg_quantity = Quantity::from_decimal(take).map_err(|e| anyhow::anyhow!("{e}"))?;
            anyhow::ensure!(
                leg_quantity.precision <= source.size_precision,
                "redeem quantity {} exceeds instrument precision {}",
                leg_quantity,
                source.size_precision
            );

            if source.settled {
                settled_legs.push(PositionOperationLeg {
                    operation_id: result.operation_id,
                    operation: result.operation,
                    direction: PositionOperationDirection::Close,
                    trader_id: result.trader_id,
                    strategy_id: source.strategy_id,
                    account_id: source.account_id,
                    instrument_id: instrument.id(),
                    position_id: source.position_id,
                    quantity: leg_quantity,
                    cost: Decimal::ZERO,
                    income: Some(income),
                    settled: true,
                    event_id: UUID4::new(),
                    ts_event,
                    ts_init,
                });
            } else {
                let position = source
                    .open_position
                    .expect("an open redeem source carries its position");
                let cost = release_cost(&position, leg_quantity)?;
                let leg = build_position_leg(
                    result,
                    PositionOperationDirection::Close,
                    &instrument,
                    &position,
                    leg_quantity,
                    cost,
                    Some(income),
                    ts_event,
                    ts_init,
                );
                plans.push(OperationLegPlan {
                    instrument: instrument.clone(),
                    prior: Some(position),
                    leg,
                    settled_close: None,
                });
            }
        }
        Ok(Some((plans, settled_legs)))
    }

    fn plan_convert(
        &self,
        result: &PositionOperationResult,
        mut received: Vec<(InstrumentAny, Quantity)>,
        mut spent: Vec<(InstrumentAny, Quantity)>,
        collateral: Decimal,
        ts_event: nautilus_core::UnixNanos,
        ts_init: nautilus_core::UnixNanos,
    ) -> anyhow::Result<Vec<OperationLegPlan>> {
        anyhow::ensure!(!spent.is_empty(), "convert requires NO input tokens");
        anyhow::ensure!(
            !collateral.is_sign_negative(),
            "convert collateral return must be non-negative"
        );
        spent.sort_by_key(|(instrument, _)| instrument.id());
        received.sort_by_key(|(instrument, _)| instrument.id());

        let mut inputs = Vec::with_capacity(spent.len());
        let mut consumed_cost = Decimal::ZERO;
        for (instrument, quantity) in spent {
            let prior = self
                .existing_position(&instrument.id(), result.strategy_id, result.account_id)?
                .ok_or_else(|| {
                    anyhow::anyhow!("convert input position is not open for {}", instrument.id())
                })?;
            let cost = release_cost(&prior, quantity)?;
            consumed_cost += cost;
            inputs.push((instrument, prior, quantity, cost));
        }

        let has_outputs = !received.is_empty();
        let immediate_pnl = if has_outputs {
            (collateral - consumed_cost).max(Decimal::ZERO)
        } else {
            collateral - consumed_cost
        };
        let output_cost = if has_outputs {
            (consumed_cost - collateral).max(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };

        let pnl_weights: Vec<Decimal> = if consumed_cost.is_zero() {
            inputs
                .iter()
                .map(|(_, _, quantity, _)| quantity.as_decimal())
                .collect()
        } else {
            inputs.iter().map(|(_, _, _, cost)| *cost).collect()
        };
        let pnl_shares = allocate(immediate_pnl, &pnl_weights);

        let mut plans = Vec::with_capacity(inputs.len() + received.len());
        for ((instrument, prior, quantity, cost), pnl_share) in inputs.into_iter().zip(pnl_shares) {
            let position_id = prior.id;
            let leg = build_leg(
                result,
                PositionOperationDirection::Close,
                &instrument,
                position_id,
                quantity,
                cost,
                Some(cost + pnl_share),
                ts_event,
                ts_init,
            );
            plans.push(OperationLegPlan {
                instrument,
                prior: Some(prior),
                leg,
                settled_close: None,
            });
        }

        if has_outputs {
            let output_weights: Vec<Decimal> = received
                .iter()
                .map(|(_, quantity)| quantity.as_decimal())
                .collect();
            let output_costs = allocate(output_cost, &output_weights);
            for ((instrument, quantity), cost) in received.into_iter().zip(output_costs) {
                let position_id = netting_position_id(&instrument.id(), result.strategy_id);
                let prior = self.existing_position(
                    &instrument.id(),
                    result.strategy_id,
                    result.account_id,
                )?;
                let leg = build_leg(
                    result,
                    PositionOperationDirection::Open,
                    &instrument,
                    position_id,
                    quantity,
                    cost,
                    None,
                    ts_event,
                    ts_init,
                );
                let settled_close = {
                    let cache = self.cache.borrow();
                    settled_output_close(&cache, &instrument.id())?
                };
                plans.push(OperationLegPlan {
                    instrument,
                    prior,
                    leg,
                    settled_close,
                });
            }
        }

        Ok(plans)
    }

    fn commit_operation_legs(
        &self,
        plans: Vec<OperationLegPlan>,
    ) -> anyhow::Result<Vec<PositionOperationLeg>> {
        let ts_init = self.clock.borrow().timestamp_ns();
        let mut updates = Vec::with_capacity(plans.len());
        let mut events = Vec::with_capacity(plans.len());
        let mut legs = Vec::with_capacity(plans.len());

        for plan in plans {
            let before_flat = plan.prior.as_ref().is_none_or(Position::is_closed);
            let mut position = match &plan.prior {
                Some(prior) => {
                    let mut position = prior.clone();
                    position
                        .apply_operation_leg(plan.leg.clone())
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                    position
                }
                None => {
                    if plan.leg.direction == PositionOperationDirection::Close {
                        anyhow::bail!(
                            "close operation leg has no prior position for {}",
                            plan.instrument.id()
                        );
                    }
                    Position::new_from_operation_checked(&plan.instrument, plan.leg.clone())
                        .map_err(|e| anyhow::anyhow!("{e}"))?
                }
            };

            let settled_output = plan.settled_close.is_some();
            let settled_quantity = settled_output.then_some(position.quantity);
            let mut event_leg = plan.leg.clone();
            if let Some(close) = plan.settled_close {
                let confirmation = plan.leg.ts_event.max(close.ts_event);
                event_leg.ts_event = confirmation;
                position
                    .apply_instrument_close_at(close, confirmation)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                anyhow::ensure!(
                    position.is_closed(),
                    "settled conversion output {} was not fully closed",
                    plan.instrument.id()
                );
            }

            // A settled output is received and economically settled in one batch. Only the final
            // closed state is published; no intermediate tradable position is exposed.
            let event = if settled_output {
                PositionEvent::PositionClosed(PositionClosed::create_from_operation(
                    &position,
                    &event_leg,
                    UUID4::new(),
                    ts_init,
                ))
            } else if before_flat && position.is_open() {
                PositionEvent::PositionOpened(PositionOpened::create_from_operation(
                    &position,
                    &plan.leg,
                    UUID4::new(),
                    ts_init,
                ))
            } else if !before_flat && position.is_closed() {
                PositionEvent::PositionClosed(PositionClosed::create_from_operation(
                    &position,
                    &plan.leg,
                    UUID4::new(),
                    ts_init,
                ))
            } else {
                PositionEvent::PositionChanged(PositionChanged::create_from_operation(
                    &position,
                    &plan.leg,
                    UUID4::new(),
                    ts_init,
                ))
            };

            updates.push((position, plan.prior, settled_quantity));
            events.push(event);
            legs.push(plan.leg);
        }

        {
            let cache = self.cache.borrow();
            for (position, _, _) in &updates {
                cache.validate_position_update(position, OmsType::Netting)?;
            }
        }

        // Archive any prior closed cycles before their NETTING position ID is reused.
        for (_, prior, _) in &updates {
            if let Some(prior) = prior
                && prior.is_closed()
            {
                self.snapshot_position(prior)?;
            }
        }

        let mut persistence_errors = Vec::new();
        {
            let mut cache = self.cache.borrow_mut();

            for (position, prior, settled_quantity) in &updates {
                let result = if prior.is_some() {
                    cache.replace_position(position, OmsType::Netting, false, false)
                } else {
                    cache.add_position_without_order(position, OmsType::Netting)
                };
                if let Err(e) = result {
                    log::error!(
                        "Position operation leg for {} was applied in memory but not persisted: {e}",
                        position.id
                    );
                    persistence_errors.push(e.to_string());
                } else if let Some(quantity) = settled_quantity
                    && let Err(e) = cache.record_settled_position_source(position, *quantity)
                {
                    log::error!(
                        "Settled output source for {} was applied in memory but not persisted: {e}",
                        position.id
                    );
                }
            }
        }

        {
            let cache = self.cache.borrow();

            for (position, _, _) in &updates {
                anyhow::ensure!(
                    cache
                        .position(&position.id)
                        .is_some_and(|cached| cached.as_ref() == position),
                    "position operation leg for {} was not fully applied",
                    position.id
                );
            }
        }

        if !persistence_errors.is_empty() {
            log::error!(
                "Position operation applied in memory with {} persistence error(s): {}",
                persistence_errors.len(),
                persistence_errors.join("; ")
            );
        }

        self.publish_position_events(events);
        Ok(legs)
    }
}

#[derive(Clone, Debug)]
struct ExpectedOperationLeg {
    position_id: PositionId,
    instrument_id: InstrumentId,
    strategy_id: StrategyId,
    direction: PositionOperationDirection,
    quantity: Quantity,
    cost: Option<Decimal>,
    income_known: bool,
    income: Option<Decimal>,
    settled: bool,
}

fn expected_from_record(
    recovery: &PositionOperationRecovery,
) -> anyhow::Result<Vec<ExpectedOperationLeg>> {
    let mut seen = AHashSet::new();
    let mut legs = Vec::with_capacity(recovery.applied_legs.len());

    for leg in &recovery.applied_legs {
        anyhow::ensure!(
            recovery.instrument_ids.contains(&leg.instrument_id),
            "recorded operation leg instrument {} is outside the barrier scope",
            leg.instrument_id
        );
        anyhow::ensure!(
            leg.quantity.as_decimal() > Decimal::ZERO,
            "recorded operation leg quantity must be positive"
        );
        anyhow::ensure!(
            !leg.cost.is_sign_negative(),
            "recorded operation leg cost must not be negative"
        );
        anyhow::ensure!(
            seen.insert((leg.instrument_id, leg.position_id, leg.direction)),
            "recorded operation legs contain a duplicate identity"
        );
        match leg.direction {
            PositionOperationDirection::Open => anyhow::ensure!(
                leg.income.is_none(),
                "recorded opening operation leg must not carry income"
            ),
            PositionOperationDirection::Close => anyhow::ensure!(
                leg.income.is_some_and(|income| !income.is_sign_negative()),
                "recorded closing operation leg must carry non-negative income"
            ),
        }

        legs.push(ExpectedOperationLeg {
            position_id: leg.position_id,
            instrument_id: leg.instrument_id,
            strategy_id: leg.strategy_id.unwrap_or(recovery.strategy_id),
            direction: leg.direction,
            quantity: leg.quantity,
            cost: Some(leg.cost),
            income_known: true,
            income: leg.income,
            settled: leg.settled,
        });
    }

    match recovery.operation {
        PositionOperationType::Split => {
            anyhow::ensure!(
                legs.len() == 2
                    && legs
                        .iter()
                        .all(|leg| leg.direction == PositionOperationDirection::Open),
                "recorded split requires two opening legs"
            );
        }
        PositionOperationType::Merge => {
            anyhow::ensure!(
                legs.len() == 2
                    && legs
                        .iter()
                        .all(|leg| leg.direction == PositionOperationDirection::Close),
                "recorded merge requires two closing legs"
            );
        }
        PositionOperationType::Redeem => {
            anyhow::ensure!(
                !legs.is_empty()
                    && legs
                        .iter()
                        .all(|leg| leg.direction == PositionOperationDirection::Close),
                "recorded redeem requires closing legs"
            );
        }
        PositionOperationType::Convert => {
            anyhow::ensure!(
                legs.iter()
                    .any(|leg| leg.direction == PositionOperationDirection::Close),
                "recorded convert requires at least one closing leg"
            );
        }
    }

    Ok(legs)
}

fn validate_recovery_leg(
    recovery: &PositionOperationRecovery,
    expected: &ExpectedOperationLeg,
    replayed: &PositionOperationLeg,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        replayed.operation == recovery.operation,
        "operation replay leg type does not match its record"
    );
    anyhow::ensure!(
        replayed.direction == expected.direction
            && replayed.instrument_id == expected.instrument_id
            && replayed.position_id == expected.position_id
            && replayed.strategy_id == expected.strategy_id,
        "operation replay leg identity does not match its record"
    );
    anyhow::ensure!(
        replayed.quantity == expected.quantity,
        "operation replay quantity for {} does not match its record",
        expected.instrument_id
    );
    if let Some(cost) = expected.cost {
        anyhow::ensure!(
            replayed.cost == cost,
            "operation replay cost for {} does not match its record",
            expected.instrument_id
        );
    }
    if expected.income_known {
        anyhow::ensure!(
            replayed.income == expected.income,
            "operation replay income for {} does not match its record",
            expected.instrument_id
        );
    }
    Ok(())
}

fn expected_settled_leg(
    recovery: &PositionOperationRecovery,
    leg: &ExpectedOperationLeg,
    ts: nautilus_core::UnixNanos,
) -> PositionOperationLeg {
    PositionOperationLeg {
        operation_id: recovery.operation_id,
        operation: recovery.operation,
        direction: PositionOperationDirection::Close,
        trader_id: recovery.trader_id,
        strategy_id: leg.strategy_id,
        account_id: recovery.account_id,
        instrument_id: leg.instrument_id,
        position_id: leg.position_id,
        quantity: leg.quantity,
        cost: leg.cost.unwrap_or_default(),
        income: leg.income,
        settled: true,
        event_id: UUID4::new(),
        ts_event: ts,
        ts_init: ts,
    }
}

fn recovery_legs(legs: &[PositionOperationLeg]) -> Vec<PositionOperationRecoveryLeg> {
    legs.iter()
        .map(|leg| PositionOperationRecoveryLeg {
            position_id: leg.position_id,
            instrument_id: leg.instrument_id,
            strategy_id: Some(leg.strategy_id),
            direction: leg.direction,
            quantity: leg.quantity,
            cost: leg.cost,
            income: leg.income,
            settled: leg.settled,
        })
        .collect()
}

const SETTLED_CONSUMPTION_PREFIX: &str = "position-operation-settled";

fn settled_consumption_key(position: &Position) -> String {
    // Archived NETTING cycles receive a new PositionId, so consumption follows the cycle's
    // stable identity across snapshots instead of the mutable position ID.
    format!(
        "{SETTLED_CONSUMPTION_PREFIX}:cycle:{}",
        settled_cycle_id(position)
    )
}

fn position_cycle_matches(recorded: PositionId, source: PositionId) -> bool {
    if recorded == source {
        return true;
    }

    // A record stores the NETTING base ID while the archived cycle carries a UUID suffix.
    let prefix = format!("{recorded}-");
    source
        .as_str()
        .strip_prefix(&prefix)
        .is_some_and(|suffix| UUID4::from_str(suffix).is_ok())
}

fn settled_source_position(
    cache: &Cache,
    position_id: PositionId,
    account_id: AccountId,
) -> Option<Position> {
    cache
        .position(&position_id)
        .map(|position| position.cloned())
        .or_else(|| {
            cache
                .position_snapshots(None, Some(&account_id))
                .into_iter()
                .find(|position| position.id == position_id)
        })
}

fn settled_source_by_cycle(
    cache: &Cache,
    cycle_id: &str,
    account_id: AccountId,
) -> Option<Position> {
    cache
        .positions(None, None, None, Some(&account_id), None)
        .into_iter()
        .map(|position| position.cloned())
        .chain(cache.position_snapshots(None, Some(&account_id)))
        .find(|position| position.is_closed() && settled_cycle_id(position) == cycle_id)
}

fn legacy_settled_source(
    cache: &Cache,
    position_id: PositionId,
    cycle_opened: nautilus_core::UnixNanos,
    account_id: AccountId,
) -> Option<Position> {
    let snapshot_prefix = format!("{position_id}-");
    cache
        .positions(None, None, None, Some(&account_id), None)
        .into_iter()
        .map(|position| position.cloned())
        .chain(cache.position_snapshots(None, Some(&account_id)))
        .find(|position| {
            position.is_closed()
                && position.ts_opened == cycle_opened
                && (position.id == position_id
                    || position
                        .id
                        .as_str()
                        .strip_prefix(&snapshot_prefix)
                        .is_some_and(|suffix| UUID4::from_str(suffix).is_ok()))
        })
}

fn settled_operation_key(operation_id: &UUID4, cycle_id: &str) -> String {
    format!("{SETTLED_CONSUMPTION_PREFIX}:operation:{operation_id}:{cycle_id}")
}

fn settled_consumed(cache: &Cache, position: &Position) -> Decimal {
    cache
        .get(&settled_consumption_key(position))
        .ok()
        .flatten()
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|value| Decimal::from_str(value).ok())
        .unwrap_or_default()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputSettlementState {
    Missing,
    Unsettled,
    Settled,
}

/// Returns whether the recovered output leg's cycle has an authoritative economic settlement.
fn output_cycle_settlement_state(
    position: &Position,
    replayed: &PositionOperationLeg,
) -> OutputSettlementState {
    let identity = replayed.identity();
    let mut found = false;

    for event in &position.replay_events {
        match event {
            PositionReplayEvent::Operation(leg) if leg.identity() == identity => found = true,
            PositionReplayEvent::InstrumentClosed(_) if found => {
                return OutputSettlementState::Settled;
            }
            _ => {}
        }
    }

    if found {
        OutputSettlementState::Unsettled
    } else {
        OutputSettlementState::Missing
    }
}

/// Returns the physical quantity still held by the cycle containing the recovered output leg.
///
/// Sales and operation closes after the received leg reduce the holding. A zero result means the
/// output was fully disposed before the market resolved, so no further settlement is required.
/// `None` means adjustments or negative history prevent a reliable calculation. Price
/// restatements and applied fill voids follow the position's effective replay.
fn output_cycle_pending_quantity(
    position: &Position,
    replayed: &PositionOperationLeg,
) -> Option<Decimal> {
    if !position.adjustments.is_empty() {
        return None;
    }

    let events = position.effective_replay_events().ok()?;
    let identity = replayed.identity();
    let mut found = false;
    let mut quantity = Decimal::ZERO;

    for event in &events {
        match event {
            PositionReplayEvent::Operation(leg) => {
                if leg.identity() == identity {
                    found = true;
                }

                match leg.direction {
                    PositionOperationDirection::Open => quantity += leg.quantity.as_decimal(),
                    PositionOperationDirection::Close => quantity -= leg.quantity.as_decimal(),
                }
            }
            PositionReplayEvent::Filled(fill) => match fill.order_side {
                OrderSide::Buy => quantity += fill.last_qty.as_decimal(),
                OrderSide::Sell => quantity -= fill.last_qty.as_decimal(),
            },
            PositionReplayEvent::InstrumentClosed(_) => {
                if found {
                    return Some(Decimal::ZERO);
                }
                quantity = Decimal::ZERO;
            }
            PositionReplayEvent::Adjusted(_) => return None,
        }

        if quantity < Decimal::ZERO {
            return None;
        }
    }

    found.then_some(quantity)
}

/// Returns whether the recovered leg belongs to the position's last settled cycle.
fn current_cycle_settled(position: &Position, replayed: &PositionOperationLeg) -> bool {
    let identity = replayed.identity();
    let mut found = false;
    let mut closed = false;

    for event in &position.replay_events {
        match event {
            PositionReplayEvent::Operation(leg) if leg.identity() == identity => {
                if closed {
                    return false;
                }
                found = true;
            }
            PositionReplayEvent::InstrumentClosed(_) if found => closed = true,
            PositionReplayEvent::Operation(_) | PositionReplayEvent::Filled(_) if closed => {
                return false;
            }
            _ => {}
        }
    }

    closed
}

/// Returns redeemable physical holdings from settled cycles for an instrument and account.
///
/// Cycles are deduplicated by their stable identity so a source record and a retained current or
/// snapshot position never count the same physical tokens twice. `None` means the available
/// history is inconsistent and the redeem must remain unresolved.
fn settled_redeem_sources(
    cache: &mut Cache,
    instrument_id: &InstrumentId,
    account_id: AccountId,
    strategy_id: Option<StrategyId>,
    size_precision: u8,
) -> Option<Vec<RedeemSource>> {
    if let Err(e) = cache.restore_position_snapshots_for(instrument_id, account_id) {
        log::warn!("Cannot restore position snapshots for {instrument_id}: {e}");
        return None;
    }

    let mut cycles = AHashMap::new();

    for position in closed_position_cycles(cache, instrument_id, account_id) {
        if strategy_id.is_some_and(|strategy| position.strategy_id != strategy) {
            continue;
        }
        cycles
            .entry(settled_cycle_id(&position))
            .or_insert(position);
    }

    let records: AHashMap<_, _> = cache
        .settled_position_source_records(instrument_id, account_id)
        .into_iter()
        .filter(|source| strategy_id.is_none_or(|strategy| source.strategy_id == strategy))
        .map(|source| (source.cycle_id.clone(), source))
        .collect();

    let mut seen = AHashSet::new();
    let mut sources = Vec::new();

    for (key, position) in &cycles {
        // A settlement source records the model's physical quantity at settlement. It remains
        // authoritative when replay cannot determine that quantity, such as for an adjustment
        // that changed the cycle without a replayable transaction.
        let physical = settled_physical_quantity(position)
            .map(|quantity| quantity.as_decimal())
            .or_else(|| records.get(key).map(|source| source.quantity.as_decimal()));
        let Some(physical) = physical else {
            if position
                .replay_events
                .iter()
                .any(|event| matches!(event, PositionReplayEvent::InstrumentClosed(_)))
            {
                return None;
            }
            continue;
        };
        let consumed = settled_consumed(cache, position);
        let remaining = physical - consumed;
        if remaining > Decimal::ZERO && seen.insert(key.clone()) {
            sources.push(RedeemSource {
                position_id: position.id,
                strategy_id: position.strategy_id,
                account_id: position.account_id,
                size_precision: position.size_precision,
                quantity: remaining,
                open_position: None,
                settled: true,
            });
        }
    }

    for (key, source) in &records {
        if !seen.insert(key.clone()) {
            continue;
        }
        let position = cycles.get(key)?;
        let position_id = position.id;
        let consumed = settled_consumed(cache, position);
        let remaining = source.quantity.as_decimal() - consumed;
        if remaining > Decimal::ZERO {
            sources.push(RedeemSource {
                position_id,
                strategy_id: source.strategy_id,
                account_id: source.account_id,
                size_precision,
                quantity: remaining,
                open_position: None,
                settled: true,
            });
        }
    }
    sources.sort_by_key(|source| source.position_id.to_string());
    Some(sources)
}

/// Returns every closed cycle that can hold physical tokens for an instrument and account.
///
/// NETTING cycles archived before a reopen live as snapshot frames with their own `PositionId`, so
/// a wallet-scoped redeem must inspect both the current position and those frames.
fn closed_position_cycles(
    cache: &Cache,
    instrument_id: &InstrumentId,
    account_id: AccountId,
) -> Vec<Position> {
    let mut sources = cache
        .positions(None, Some(instrument_id), None, Some(&account_id), None)
        .into_iter()
        .filter(|position| position.is_closed())
        .map(|position| position.cloned())
        .collect::<Vec<_>>();
    sources.extend(
        cache
            .position_snapshots(None, Some(&account_id))
            .into_iter()
            .filter(|position| position.instrument_id == *instrument_id && position.is_closed()),
    );
    sources.sort_by_key(|position| position.id.to_string());
    sources.dedup_by_key(|position| position.id);
    sources
}

/// Returns the physical quantity held when the position was economically settled.
///
/// This replays the retained cycle using the position's effective fills, so price restatements do
/// not add quantity again and applied fill voids reduce the fills they correct. Adjustments that
/// can change quantity without a replayable transaction remain indeterminate.
fn settled_physical_quantity(position: &Position) -> Option<Quantity> {
    if !position.is_closed() || !position.adjustments.is_empty() {
        return None;
    }

    let events = position.effective_replay_events().ok()?;
    let mut quantity = Decimal::ZERO;
    let mut settled = None;
    let mut reopened_after_settlement = false;

    for event in &events {
        match event {
            PositionReplayEvent::Filled(fill) => {
                reopened_after_settlement = settled.is_some();
                match fill.order_side {
                    OrderSide::Buy => quantity += fill.last_qty.as_decimal(),
                    OrderSide::Sell => quantity -= fill.last_qty.as_decimal(),
                }
            }
            PositionReplayEvent::Operation(leg) => {
                reopened_after_settlement = settled.is_some();
                match leg.direction {
                    PositionOperationDirection::Open => {
                        quantity += leg.quantity.as_decimal();
                    }
                    PositionOperationDirection::Close => {
                        quantity -= leg.quantity.as_decimal();
                    }
                }
            }
            PositionReplayEvent::InstrumentClosed(_) => {
                if quantity <= Decimal::ZERO {
                    return None;
                }
                settled = Some(quantity);
                quantity = Decimal::ZERO;
                reopened_after_settlement = false;
            }
            PositionReplayEvent::Adjusted(_) => return None,
        }

        if quantity < Decimal::ZERO {
            return None;
        }
    }

    if reopened_after_settlement {
        return None;
    }

    Quantity::from_decimal_dp(settled?, position.size_precision).ok()
}

/// Returns the authoritative contract-expiration close for a settled conversion output.
///
/// A cached close of another type is not a settlement and cannot be used to compute the output's
/// economic result.
fn settled_output_close(
    cache: &Cache,
    instrument_id: &InstrumentId,
) -> anyhow::Result<Option<InstrumentClose>> {
    let Some(close) = cache.instrument_close(instrument_id) else {
        return Ok(None);
    };
    anyhow::ensure!(
        close.close_type == InstrumentCloseType::ContractExpired,
        "instrument {instrument_id} has a non-settlement close and cannot be a settled conversion output"
    );
    Ok(Some(*close))
}

/// Validates the settlement basis of an expected conversion output before signing.
///
/// An expired output without an authoritative contract-expiration close cannot be classified as
/// settled or unsettled, so the operation must be rejected before submission.
fn ensure_output_settlement_evidence(
    cache: &Cache,
    instrument_id: &InstrumentId,
    now: nautilus_core::UnixNanos,
) -> anyhow::Result<()> {
    if cache.instrument_close(instrument_id).is_some() {
        settled_output_close(cache, instrument_id)?;
        return Ok(());
    }

    let instrument = cache
        .instrument(instrument_id)
        .ok_or_else(|| anyhow::anyhow!("instrument {instrument_id} is not in the cache"))?;
    if instrument
        .expiration_ns()
        .is_some_and(|expiration| !expiration.is_zero() && now >= expiration)
    {
        anyhow::bail!(
            "conversion output {instrument_id} has expired without authoritative settlement evidence"
        );
    }
    Ok(())
}

fn resolve_effects(
    amounts: &[PositionOperationTokenAmount],
    tokens: &AHashMap<ustr::Ustr, InstrumentAny>,
) -> anyhow::Result<Vec<(InstrumentAny, Quantity)>> {
    let mut resolved = Vec::with_capacity(amounts.len());
    for amount in amounts {
        let instrument = tokens
            .get(&ustr::Ustr::from(amount.token_id.as_str()))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no cached instrument maps token {} from position operation effects",
                    amount.token_id
                )
            })?
            .clone();
        let quantity = Quantity::from(amount.quantity.as_str());
        resolved.push((instrument, quantity));
    }
    Ok(resolved)
}

fn netting_position_id(instrument_id: &InstrumentId, strategy_id: StrategyId) -> PositionId {
    PositionId::new(format!("{instrument_id}-{strategy_id}"))
}

#[allow(
    clippy::too_many_arguments,
    reason = "leg fields mirror the operation identity and exact allocation"
)]
fn build_leg(
    result: &PositionOperationResult,
    direction: PositionOperationDirection,
    instrument: &InstrumentAny,
    position_id: PositionId,
    quantity: Quantity,
    cost: Decimal,
    income: Option<Decimal>,
    ts_event: nautilus_core::UnixNanos,
    ts_init: nautilus_core::UnixNanos,
) -> PositionOperationLeg {
    PositionOperationLeg {
        operation_id: result.operation_id,
        operation: result.operation,
        direction,
        trader_id: result.trader_id,
        strategy_id: result.strategy_id,
        account_id: result.account_id,
        instrument_id: instrument.id(),
        position_id,
        quantity,
        cost,
        income,
        settled: false,
        event_id: UUID4::new(),
        ts_event,
        ts_init,
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "leg fields mirror the operation identity and exact allocation"
)]
fn build_position_leg(
    result: &PositionOperationResult,
    direction: PositionOperationDirection,
    instrument: &InstrumentAny,
    position: &Position,
    quantity: Quantity,
    cost: Decimal,
    income: Option<Decimal>,
    ts_event: nautilus_core::UnixNanos,
    ts_init: nautilus_core::UnixNanos,
) -> PositionOperationLeg {
    PositionOperationLeg {
        operation_id: result.operation_id,
        operation: result.operation,
        direction,
        trader_id: position.trader_id,
        strategy_id: position.strategy_id,
        account_id: position.account_id,
        instrument_id: instrument.id(),
        position_id: position.id,
        quantity,
        cost,
        income,
        settled: false,
        event_id: UUID4::new(),
        ts_event,
        ts_init,
    }
}

fn release_cost(position: &Position, quantity: Quantity) -> anyhow::Result<Decimal> {
    anyhow::ensure!(
        position.is_long(),
        "operation input position is not long for {}",
        position.id
    );
    let quantity = quantity.as_decimal();
    let prior = position.signed_decimal_qty();
    anyhow::ensure!(
        !quantity.is_zero() && quantity <= prior,
        "operation quantity exceeds open position quantity for {}",
        position.id
    );

    if quantity == prior {
        Ok(position.cost_open)
    } else {
        let cost = (position.cost_open * quantity)
            .checked_div(prior)
            .ok_or_else(|| anyhow::anyhow!("position cost allocation division failed"))?;
        anyhow::ensure!(
            cost <= position.cost_open,
            "position cost allocation exceeds open cost"
        );
        Ok(cost)
    }
}

fn allocate(total: Decimal, weights: &[Decimal]) -> Vec<Decimal> {
    debug_assert!(!weights.is_empty());
    let sum: Decimal = weights.iter().copied().sum();
    let count = Decimal::from(weights.len());
    let mut allocated = Decimal::ZERO;
    let mut shares = Vec::with_capacity(weights.len());

    for (index, weight) in weights.iter().enumerate() {
        if index + 1 == weights.len() {
            shares.push(total - allocated);
            break;
        }

        let share = if sum.is_zero() {
            total / count
        } else {
            total * *weight / sum
        };
        allocated += share;
        shares.push(share);
    }

    shares
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clock::VirtualClock,
        messages::execution::{ModifyOrder, PositionOperationEffects, SubmitOrder},
        msgbus::{self, switchboard, typed_handler::TypedHandler},
    };
    use nautilus_core::{Params, UnixNanos};
    use nautilus_model::{
        data::InstrumentClose,
        enums::{
            AssetClass, InstrumentCloseType, LiquiditySide, OrderSide, OrderType,
            PositionAdjustmentType,
        },
        events::{OrderEventAny, OrderFilled, PositionAdjusted, order::spec::OrderFillVoidedSpec},
        identifiers::{
            AccountId, ClientId, InstrumentId, StrategyId, Symbol, TradeId, TraderId, VenueOrderId,
        },
        instruments::{BinaryOption, InstrumentAny},
        orders::{Order, OrderAny, OrderTestBuilder, stubs::TestOrderEventStubs},
        position_operation::{PositionOperationDirection, PositionOperationLeg},
        types::{Currency, Money, Price, Quantity},
    };
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::engine::stubs::StubExecutionClient;

    fn option(token_id: &str, outcome: &str) -> InstrumentAny {
        let info = Params::new(
            [
                (
                    "token_id".to_string(),
                    serde_json::Value::String(token_id.to_string()),
                ),
                (
                    "condition_id".to_string(),
                    serde_json::Value::String("0xcondition".to_string()),
                ),
            ]
            .into(),
        );
        InstrumentAny::BinaryOption(
            BinaryOption::builder()
                .instrument_id(InstrumentId::from(format!("{token_id}.POLYMARKET")))
                .raw_symbol(Symbol::new(token_id))
                .asset_class(AssetClass::Alternative)
                .currency(Currency::pUSD())
                .activation_ns(UnixNanos::default())
                .expiration_ns(UnixNanos::from(1))
                .price_precision(2)
                .size_precision(6)
                .price_increment(Price::from("0.01"))
                .size_increment(Quantity::from("0.000001"))
                .outcome(outcome.into())
                .info(info)
                .ts_event(UnixNanos::default())
                .ts_init(UnixNanos::default())
                .build()
                .unwrap(),
        )
    }

    fn engine_with(instruments: &[InstrumentAny]) -> (ExecutionEngine, Rc<RefCell<Cache>>) {
        let cache = Rc::new(RefCell::new(Cache::default()));
        for instrument in instruments {
            cache
                .borrow_mut()
                .add_instrument(instrument.clone())
                .unwrap();
        }
        let engine = ExecutionEngine::new(
            Rc::new(RefCell::new(VirtualClock::new())),
            cache.clone(),
            None,
        );
        (engine, cache)
    }

    fn result(
        operation: PositionOperationType,
        instrument_ids: Vec<InstrumentId>,
        effects: PositionOperationEffects,
    ) -> PositionOperationResult {
        PositionOperationResult::new(
            TraderId::from("TRADER-001"),
            StrategyId::from("STRATEGY-001"),
            ClientId::from("POLYMARKET"),
            AccountId::from("ACCOUNT-001"),
            UUID4::new(),
            operation,
            PositionOperationStatus::Confirmed,
            instrument_ids,
            None,
            None,
            None,
            Some(effects),
            UUID4::new(),
            UnixNanos::from(1_000_000_000),
            UnixNanos::from(1_000_000_001),
        )
    }

    fn amount(token_id: &str, quantity: &str) -> PositionOperationTokenAmount {
        PositionOperationTokenAmount {
            token_id: token_id.into(),
            quantity: quantity.into(),
        }
    }

    fn short_position(
        instrument: &InstrumentAny,
        strategy_id: StrategyId,
        account_id: AccountId,
        quantity: Quantity,
    ) -> Position {
        let position_id = netting_position_id(&instrument.id(), strategy_id);
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument.id())
            .strategy_id(strategy_id)
            .side(OrderSide::Sell)
            .quantity(quantity)
            .build();
        let event = TestOrderEventStubs::filled(
            &order,
            instrument,
            Some(TradeId::new("SHORT-1")),
            Some(position_id),
            Some(Price::from("0.50")),
            Some(quantity),
            Some(LiquiditySide::Taker),
            None,
            None,
            Some(account_id),
        );
        let OrderEventAny::Filled(fill) = event else {
            panic!("expected a filled event");
        };
        Position::new(instrument, fill)
    }

    fn input_position(
        instrument: &InstrumentAny,
        strategy_id: StrategyId,
        account_id: AccountId,
        quantity: Quantity,
        cost: Decimal,
    ) -> Position {
        let position_id = netting_position_id(&instrument.id(), strategy_id);
        let leg = PositionOperationLeg {
            operation_id: UUID4::new(),
            operation: PositionOperationType::Convert,
            direction: PositionOperationDirection::Open,
            trader_id: TraderId::from("TRADER-001"),
            strategy_id,
            account_id,
            instrument_id: instrument.id(),
            position_id,
            quantity,
            cost,
            income: None,
            settled: false,
            event_id: UUID4::new(),
            ts_event: UnixNanos::from(1),
            ts_init: UnixNanos::from(2),
        };
        Position::new_from_operation_checked(instrument, leg).unwrap()
    }

    fn market_buy_order(
        instrument: &InstrumentAny,
        strategy_id: StrategyId,
        quantity: &str,
    ) -> OrderAny {
        OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument.id())
            .strategy_id(strategy_id)
            .side(OrderSide::Buy)
            .quantity(Quantity::from(quantity))
            .build()
    }

    fn buy_fill(
        order: &OrderAny,
        instrument: &InstrumentAny,
        account_id: AccountId,
        trade_id: &str,
        quantity: &str,
        price: &str,
    ) -> OrderFilled {
        let event = TestOrderEventStubs::filled(
            order,
            instrument,
            Some(TradeId::from(trade_id)),
            Some(netting_position_id(&instrument.id(), order.strategy_id())),
            Some(Price::from(price)),
            Some(Quantity::from(quantity)),
            Some(LiquiditySide::Taker),
            None,
            None,
            Some(account_id),
        );
        let OrderEventAny::Filled(fill) = event else {
            panic!("expected a filled event");
        };
        fill
    }

    fn apply_expiry(position: &mut Position, instrument: &InstrumentAny) {
        position
            .apply_instrument_close(InstrumentClose::new(
                instrument.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(10),
                UnixNanos::from(10),
            ))
            .unwrap();
    }

    #[rstest]
    fn test_split_application_allocates_cost_and_is_idempotent() {
        let yes = option("100", "Yes");
        let no = option("101", "No");
        let (mut engine, cache) = engine_with(&[yes.clone(), no.clone()]);
        let result = result(
            PositionOperationType::Split,
            vec![yes.id(), no.id()],
            PositionOperationEffects {
                received: vec![amount("100", "100"), amount("101", "100")],
                spent: Vec::new(),
                collateral_delta: "-50".into(),
                payouts: Vec::new(),
            },
        );

        engine.process_position_operation(&result);

        let strategy = StrategyId::from("STRATEGY-001");
        let yes_position = cache
            .borrow()
            .position(&netting_position_id(&yes.id(), strategy))
            .unwrap()
            .cloned();
        let no_position = cache
            .borrow()
            .position(&netting_position_id(&no.id(), strategy))
            .unwrap()
            .cloned();
        assert_eq!(yes_position.quantity, Quantity::from("100"));
        assert_eq!(yes_position.cost_open, dec!(25));
        assert_eq!(yes_position.avg_px_open, dec!(0.25));
        assert_eq!(no_position.cost_open, dec!(25));
        assert_eq!(no_position.opening_operation_id, Some(result.operation_id));

        engine.process_position_operation(&result);

        let yes_position = cache
            .borrow()
            .position(&netting_position_id(&yes.id(), strategy))
            .unwrap()
            .cloned();
        assert_eq!(yes_position.quantity, Quantity::from("100"));
        assert_eq!(yes_position.cost_open, dec!(25));
    }

    #[rstest]
    fn test_convert_application_rejects_short_input() {
        let no = option("775", "No");
        let yes = option("776", "Yes");
        let (mut engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = short_position(&no, strategy, account, Quantity::from("100"));
        let position_id = position.id;
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let result = result(
            PositionOperationType::Convert,
            vec![no.id(), yes.id()],
            PositionOperationEffects {
                received: vec![amount("776", "100")],
                spent: vec![amount("775", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );
        engine.process_position_operation(&result);

        let cache = cache.borrow();
        let position = cache
            .position(&position_id)
            .expect("short position must remain");
        assert!(position.is_short());
        assert_eq!(position.quantity, Quantity::from("100"));
        assert!(
            cache
                .position(&netting_position_id(&yes.id(), strategy))
                .is_none()
        );
    }

    #[rstest]
    fn test_convert_application_transfers_cost_without_immediate_pnl() {
        let no_a = option("200", "No");
        let no_b = option("201", "No");
        let yes_c = option("300", "Yes");
        let (mut engine, cache) = engine_with(&[no_a.clone(), no_b.clone(), yes_c.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for (instrument, cost) in [(&no_a, dec!(80)), (&no_b, dec!(70))] {
            let position =
                input_position(instrument, strategy, account, Quantity::from("100"), cost);
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }

        let result = result(
            PositionOperationType::Convert,
            vec![no_a.id(), no_b.id(), yes_c.id()],
            PositionOperationEffects {
                received: vec![amount("300", "100")],
                spent: vec![amount("200", "100"), amount("201", "100")],
                collateral_delta: "100".into(),
                payouts: Vec::new(),
            },
        );

        engine.process_position_operation(&result);

        let no_a_position = cache
            .borrow()
            .position(&netting_position_id(&no_a.id(), strategy))
            .unwrap()
            .cloned();
        let no_b_position = cache
            .borrow()
            .position(&netting_position_id(&no_b.id(), strategy))
            .unwrap()
            .cloned();
        let yes_position = cache
            .borrow()
            .position(&netting_position_id(&yes_c.id(), strategy))
            .unwrap()
            .cloned();

        assert!(no_a_position.is_closed());
        assert!(no_b_position.is_closed());
        assert_eq!(
            no_a_position.realized_pnl,
            Some(nautilus_model::types::Money::zero(Currency::pUSD()))
        );
        assert_eq!(
            no_b_position.realized_pnl,
            Some(nautilus_model::types::Money::zero(Currency::pUSD()))
        );
        assert_eq!(yes_position.quantity, Quantity::from("100"));
        assert_eq!(yes_position.cost_open, dec!(50));
    }

    #[rstest]
    fn test_recovery_repairs_already_applied_convert_without_events() {
        let no_a = option("210", "No");
        let no_b = option("211", "No");
        let yes_c = option("310", "Yes");
        let (mut engine, cache) = engine_with(&[no_a.clone(), no_b.clone(), yes_c.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for (instrument, cost) in [(&no_a, dec!(80)), (&no_b, dec!(70))] {
            let position =
                input_position(instrument, strategy, account, Quantity::from("100"), cost);
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }

        let effects = PositionOperationEffects {
            received: vec![amount("310", "100")],
            spent: vec![amount("210", "100"), amount("211", "100")],
            collateral_delta: "100".into(),
            payouts: Vec::new(),
        };
        let published = Rc::new(RefCell::new(Vec::<PositionEvent>::new()));
        let handler = TypedHandler::from({
            let published = published.clone();
            move |event: &PositionEvent| published.borrow_mut().push(event.clone())
        });
        msgbus::subscribe_position_events(
            switchboard::get_event_position_topic(strategy).into(),
            handler,
            None,
        );

        let result = result(
            PositionOperationType::Convert,
            vec![no_a.id(), no_b.id(), yes_c.id()],
            effects.clone(),
        );
        engine.process_position_operation(&result);

        let published_count = published.borrow().len();
        assert!(
            published_count > 0,
            "initial application must publish position events"
        );

        let before = cache
            .borrow()
            .positions(None, None, None, None, None)
            .into_iter()
            .map(|position| position.cloned())
            .collect::<Vec<_>>();
        let replay_count = replay_legs(&cache, result.operation_id).len();
        assert_eq!(replay_count, 3);

        let recovery = recovery_request(&result, effects, Vec::new());
        let outcome = engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();

        let PositionOperationRecoveryOutcome::Applied { legs } = outcome else {
            panic!("expected applied recovery");
        };
        assert_eq!(legs.len(), 3);

        let after = cache
            .borrow()
            .positions(None, None, None, None, None)
            .into_iter()
            .map(|position| position.cloned())
            .collect::<Vec<_>>();
        assert_eq!(before, after);
        assert_eq!(replay_legs(&cache, result.operation_id).len(), replay_count);
        assert_eq!(
            published.borrow().len(),
            published_count,
            "recovery must not republish position events"
        );

        let repeated = engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        assert!(matches!(
            repeated,
            PositionOperationRecoveryOutcome::Applied { .. }
        ));
        assert_eq!(
            published.borrow().len(),
            published_count,
            "repeated recovery must not republish position events"
        );
    }

    #[rstest]
    fn test_recovery_completes_partial_convert_from_recorded_legs() {
        let no_a = option("220", "No");
        let no_b = option("221", "No");
        let yes_c = option("320", "Yes");
        let (mut engine, cache) = engine_with(&[no_a.clone(), no_b.clone(), yes_c.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for (instrument, cost) in [(&no_a, dec!(80)), (&no_b, dec!(70))] {
            let position =
                input_position(instrument, strategy, account, Quantity::from("100"), cost);
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }

        let effects = PositionOperationEffects {
            received: vec![amount("320", "100")],
            spent: vec![amount("220", "100"), amount("221", "100")],
            collateral_delta: "100".into(),
            payouts: Vec::new(),
        };
        let result = result(
            PositionOperationType::Convert,
            vec![no_a.id(), no_b.id(), yes_c.id()],
            effects.clone(),
        );

        // Simulate a crash after the input legs were persisted but before the output leg existed.
        let mut recorded = Vec::new();
        for (instrument, cost) in [(&no_a, dec!(80)), (&no_b, dec!(70))] {
            let position_id = netting_position_id(&instrument.id(), strategy);
            let leg = PositionOperationLeg {
                operation_id: result.operation_id,
                operation: PositionOperationType::Convert,
                direction: PositionOperationDirection::Close,
                trader_id: result.trader_id,
                strategy_id: strategy,
                account_id: account,
                instrument_id: instrument.id(),
                position_id,
                quantity: Quantity::from("100"),
                cost,
                income: Some(cost),
                settled: false,
                event_id: UUID4::new(),
                ts_event: UnixNanos::from(1_000_000_000),
                ts_init: UnixNanos::from(1_000_000_001),
            };
            let mut position = cache.borrow().position(&position_id).unwrap().cloned();
            position.apply_operation_leg(leg.clone()).unwrap();
            cache
                .borrow_mut()
                .replace_position(&position, OmsType::Netting, false, false)
                .unwrap();
            recorded.push(PositionOperationRecoveryLeg {
                position_id,
                instrument_id: instrument.id(),
                strategy_id: Some(strategy),
                direction: leg.direction,
                quantity: leg.quantity,
                cost: leg.cost,
                income: leg.income,
                settled: leg.settled,
            });
        }
        let yes_position_id = netting_position_id(&yes_c.id(), strategy);
        recorded.push(PositionOperationRecoveryLeg {
            position_id: yes_position_id,
            instrument_id: yes_c.id(),
            strategy_id: Some(strategy),
            direction: PositionOperationDirection::Open,
            quantity: Quantity::from("100"),
            cost: dec!(50),
            income: None,
            settled: false,
        });

        assert_eq!(replay_legs(&cache, result.operation_id).len(), 2);
        assert!(cache.borrow().position(&yes_position_id).is_none());

        let recovery = recovery_request(&result, effects, recorded);
        let outcome = engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();

        let PositionOperationRecoveryOutcome::Applied { legs } = outcome else {
            panic!("expected completed recovery");
        };
        assert_eq!(legs.len(), 3);
        assert_eq!(replay_legs(&cache, result.operation_id).len(), 3);

        let restored_yes = cache.borrow().position(&yes_position_id).unwrap().cloned();
        assert_eq!(restored_yes.quantity, Quantity::from("100"));
        assert_eq!(restored_yes.cost_open, dec!(50));
    }

    #[rstest]
    fn test_recovery_rejects_conflicting_recorded_cost() {
        let no_a = option("230", "No");
        let no_b = option("231", "No");
        let yes_c = option("330", "Yes");
        let (mut engine, cache) = engine_with(&[no_a.clone(), no_b.clone(), yes_c.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for (instrument, cost) in [(&no_a, dec!(80)), (&no_b, dec!(70))] {
            let position =
                input_position(instrument, strategy, account, Quantity::from("100"), cost);
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }

        let effects = PositionOperationEffects {
            received: vec![amount("330", "100")],
            spent: vec![amount("230", "100"), amount("231", "100")],
            collateral_delta: "100".into(),
            payouts: Vec::new(),
        };
        let result = result(
            PositionOperationType::Convert,
            vec![no_a.id(), no_b.id(), yes_c.id()],
            effects.clone(),
        );
        engine.process_position_operation(&result);

        let mut recorded = replay_legs(&cache, result.operation_id)
            .into_iter()
            .map(|leg| PositionOperationRecoveryLeg {
                position_id: leg.position_id,
                instrument_id: leg.instrument_id,
                strategy_id: Some(leg.strategy_id),
                direction: leg.direction,
                quantity: leg.quantity,
                cost: leg.cost,
                income: leg.income,
                settled: leg.settled,
            })
            .collect::<Vec<_>>();
        recorded[0].cost += dec!(1);

        let recovery = recovery_request(&result, effects, recorded);
        assert!(
            engine
                .reconcile_recovered_position_operation(&recovery)
                .is_err()
        );
        assert_eq!(replay_legs(&cache, result.operation_id).len(), 3);
    }

    #[rstest]
    fn test_recovery_applies_unapplied_convert_once() {
        let no_a = option("240", "No");
        let no_b = option("241", "No");
        let yes_c = option("340", "Yes");
        let (mut engine, cache) = engine_with(&[no_a.clone(), no_b.clone(), yes_c.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for (instrument, cost) in [(&no_a, dec!(80)), (&no_b, dec!(70))] {
            let position =
                input_position(instrument, strategy, account, Quantity::from("100"), cost);
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }

        let effects = PositionOperationEffects {
            received: vec![amount("340", "100")],
            spent: vec![amount("240", "100"), amount("241", "100")],
            collateral_delta: "100".into(),
            payouts: Vec::new(),
        };
        let result = result(
            PositionOperationType::Convert,
            vec![no_a.id(), no_b.id(), yes_c.id()],
            effects.clone(),
        );
        let recovery = recovery_request(&result, effects, Vec::new());

        let outcome = engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        let PositionOperationRecoveryOutcome::Applied { legs } = outcome else {
            panic!("expected applied recovery");
        };
        assert_eq!(legs.len(), 3);
        assert_eq!(replay_legs(&cache, result.operation_id).len(), 3);

        let yes_position = cache
            .borrow()
            .position(&netting_position_id(&yes_c.id(), strategy))
            .unwrap()
            .cloned();
        assert_eq!(yes_position.quantity, Quantity::from("100"));
        assert_eq!(yes_position.cost_open, dec!(50));

        let repeated = engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        assert!(matches!(
            repeated,
            PositionOperationRecoveryOutcome::Applied { .. }
        ));
        assert_eq!(replay_legs(&cache, result.operation_id).len(), 3);
        let repeated_yes = cache
            .borrow()
            .position(&netting_position_id(&yes_c.id(), strategy))
            .unwrap()
            .cloned();
        assert_eq!(repeated_yes, yes_position);
    }

    fn recovery_request(
        result: &PositionOperationResult,
        effects: PositionOperationEffects,
        applied_legs: Vec<PositionOperationRecoveryLeg>,
    ) -> PositionOperationRecovery {
        PositionOperationRecovery {
            trader_id: result.trader_id,
            strategy_id: result.strategy_id,
            client_id: result.client_id,
            account_id: result.account_id,
            operation_id: result.operation_id,
            operation: result.operation,
            instrument_ids: result.instrument_ids.clone(),
            quantity: None,
            effects: Some(effects),
            venue_transaction_id: None,
            transaction_hash: None,
            applied_legs,
        }
    }

    fn replay_legs(cache: &Rc<RefCell<Cache>>, operation_id: UUID4) -> Vec<PositionOperationLeg> {
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .into_iter()
            .flat_map(|position| position.replay_events.clone())
            .filter_map(|event| match event {
                PositionReplayEvent::Operation(leg) if leg.operation_id == operation_id => {
                    Some(leg)
                }
                _ => None,
            })
            .collect()
    }

    #[rstest]
    fn test_barrier_does_not_install_partially_on_conflict() {
        let yes = option("700", "Yes");
        let no = option("701", "No");
        let (engine, _cache) = engine_with(&[yes.clone(), no.clone()]);
        let existing = UUID4::new();

        engine
            .establish_full_barrier(existing, PositionOperationType::Split, &[yes.id()], None)
            .unwrap();
        assert!(
            engine
                .establish_full_barrier(
                    UUID4::new(),
                    PositionOperationType::Split,
                    &[yes.id(), no.id()],
                    None,
                )
                .is_err()
        );
        assert_eq!(engine.position_operation_barrier(&yes.id()), Some(existing));
        assert_eq!(engine.position_operation_barrier(&no.id()), None);
    }

    #[rstest]
    fn test_barrier_rejects_active_clob_orders() {
        let yes = option("710", "Yes");
        let no = option("711", "No");
        let (engine, cache) = engine_with(&[yes.clone(), no.clone()]);
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(yes.id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from("1"))
            .price(Price::from("0.50"))
            .build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, true)
            .unwrap();
        let account_id = AccountId::from("ACCOUNT-001");
        let submitted = TestOrderEventStubs::submitted(&order, account_id);
        cache.borrow_mut().update_order(&submitted).unwrap();
        let accepted =
            TestOrderEventStubs::accepted(&order, account_id, VenueOrderId::from("V-BARRIER"));
        cache.borrow_mut().update_order(&accepted).unwrap();

        assert!(
            engine
                .establish_full_barrier(
                    UUID4::new(),
                    PositionOperationType::Split,
                    &[yes.id(), no.id()],
                    None,
                )
                .is_err()
        );
        assert_eq!(engine.position_operation_barrier(&yes.id()), None);
        assert_eq!(engine.position_operation_barrier(&no.id()), None);
    }

    #[rstest]
    fn test_barrier_scope_is_account_and_instrument() {
        let yes = option("712", "Yes");
        let (mut engine, cache) = engine_with(std::slice::from_ref(&yes));
        let strategy = StrategyId::from("STRATEGY-001");
        let account_a = AccountId::from("ACCOUNT-A");
        let account_b = AccountId::from("ACCOUNT-B");
        let client_id_a = ClientId::from("STUB-A");
        let client_id_b = ClientId::from("STUB-B");
        let client_a = StubExecutionClient::new(
            client_id_a,
            account_a,
            yes.id().venue,
            OmsType::Netting,
            None,
        )
        .with_handles_all_order_venues();
        let client_b = StubExecutionClient::new(
            client_id_b,
            account_b,
            yes.id().venue,
            OmsType::Netting,
            None,
        )
        .with_handles_all_order_venues();
        let submitted_a = client_a.submitted_order_ids();
        let submitted_b = client_b.submitted_order_ids();
        let modified_a = client_a.modified_order_ids();
        let modified_b = client_b.modified_order_ids();
        engine.register_client(Box::new(client_a.clone())).unwrap();
        engine.register_client(Box::new(client_b.clone())).unwrap();

        let operation_a = UUID4::new();
        let inputs_a = BarrierInputs {
            strategy_id: strategy,
            account_id: account_a,
            quantity: None,
            instrument_ids: vec![yes.id()],
        };
        engine
            .establish_full_barrier(
                operation_a,
                PositionOperationType::Split,
                &[yes.id()],
                Some(&inputs_a),
            )
            .unwrap();

        // A different account may bar the same instrument independently.
        let operation_b = UUID4::new();
        let inputs_b = BarrierInputs {
            strategy_id: strategy,
            account_id: account_b,
            quantity: None,
            instrument_ids: vec![yes.id()],
        };
        engine
            .establish_full_barrier(
                operation_b,
                PositionOperationType::Split,
                &[yes.id()],
                Some(&inputs_b),
            )
            .unwrap();
        assert_eq!(
            engine.position_operation_barrier_for(account_a, &yes.id()),
            Some(operation_a)
        );
        assert_eq!(
            engine.position_operation_barrier_for(account_b, &yes.id()),
            Some(operation_b)
        );

        // The same account still conflicts across strategies.
        let other_strategy = BarrierInputs {
            strategy_id: StrategyId::from("STRATEGY-002"),
            account_id: account_a,
            quantity: None,
            instrument_ids: vec![yes.id()],
        };
        assert!(
            engine
                .establish_full_barrier(
                    UUID4::new(),
                    PositionOperationType::Split,
                    &[yes.id()],
                    Some(&other_strategy),
                )
                .is_err()
        );

        engine.release_position_operation_barrier(operation_a, &[yes.id()]);
        assert_eq!(
            engine.position_operation_barrier_for(account_a, &yes.id()),
            None
        );
        assert_eq!(
            engine.position_operation_barrier_for(account_b, &yes.id()),
            Some(operation_b)
        );

        // Normal submits and modifies route for the unbarred account only.
        let order_a = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(yes.id())
            .strategy_id(strategy)
            .side(OrderSide::Buy)
            .quantity(Quantity::from("1"))
            .price(Price::from("0.50"))
            .build();
        cache
            .borrow_mut()
            .add_order(order_a.clone(), None, Some(client_id_a), true)
            .unwrap();
        engine.handle_submit_order(
            &client_a,
            SubmitOrder {
                trader_id: TraderId::from("TRADER-001"),
                strategy_id: strategy,
                instrument_id: yes.id(),
                client_order_id: order_a.client_order_id(),
                order_init: order_a.init_event().clone(),
                position_id: None,
                params: None,
                client_id: Some(client_id_a),
                exec_algorithm_id: None,
                command_id: UUID4::new(),
                ts_init: UnixNanos::default(),
                correlation_id: None,
                causation_id: None,
            },
        );
        assert_eq!(*submitted_a.borrow(), vec![order_a.client_order_id()]);

        let order_b = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(yes.id())
            .strategy_id(strategy)
            .side(OrderSide::Buy)
            .quantity(Quantity::from("1"))
            .price(Price::from("0.50"))
            .build();
        cache
            .borrow_mut()
            .add_order(order_b.clone(), None, Some(client_id_b), true)
            .unwrap();
        engine.handle_submit_order(
            &client_b,
            SubmitOrder {
                trader_id: TraderId::from("TRADER-001"),
                strategy_id: strategy,
                instrument_id: yes.id(),
                client_order_id: order_b.client_order_id(),
                order_init: order_b.init_event().clone(),
                position_id: None,
                params: None,
                client_id: Some(client_id_b),
                exec_algorithm_id: None,
                command_id: UUID4::new(),
                ts_init: UnixNanos::default(),
                correlation_id: None,
                causation_id: None,
            },
        );
        assert!(submitted_b.borrow().is_empty());

        engine.handle_modify_order(
            &client_a,
            ModifyOrder {
                trader_id: TraderId::from("TRADER-001"),
                client_id: Some(client_id_a),
                strategy_id: strategy,
                instrument_id: yes.id(),
                client_order_id: order_a.client_order_id(),
                venue_order_id: None,
                quantity: Some(Quantity::from("2")),
                price: Some(Price::from("0.51")),
                trigger_price: None,
                command_id: UUID4::new(),
                ts_init: UnixNanos::default(),
                params: None,
                correlation_id: None,
                causation_id: None,
            },
        );
        assert_eq!(*modified_a.borrow(), vec![order_a.client_order_id()]);

        engine.handle_modify_order(
            &client_b,
            ModifyOrder {
                trader_id: TraderId::from("TRADER-001"),
                client_id: Some(client_id_b),
                strategy_id: strategy,
                instrument_id: yes.id(),
                client_order_id: order_b.client_order_id(),
                venue_order_id: None,
                quantity: Some(Quantity::from("2")),
                price: Some(Price::from("0.51")),
                trigger_price: None,
                command_id: UUID4::new(),
                ts_init: UnixNanos::default(),
                params: None,
                correlation_id: None,
                causation_id: None,
            },
        );
        assert!(modified_b.borrow().is_empty());
    }

    #[rstest]
    fn test_barrier_active_order_check_is_account_scoped() {
        let yes = option("713", "Yes");
        let (engine, cache) = engine_with(std::slice::from_ref(&yes));
        let strategy = StrategyId::from("STRATEGY-001");
        let account_a = AccountId::from("ACCOUNT-A");
        let account_b = AccountId::from("ACCOUNT-B");
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(yes.id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from("1"))
            .price(Price::from("0.50"))
            .build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, true)
            .unwrap();
        cache
            .borrow_mut()
            .update_order(&TestOrderEventStubs::submitted(&order, account_b))
            .unwrap();
        cache
            .borrow_mut()
            .update_order(&TestOrderEventStubs::accepted(
                &order,
                account_b,
                VenueOrderId::from("V-ACTIVE"),
            ))
            .unwrap();

        let inputs_a = BarrierInputs {
            strategy_id: strategy,
            account_id: account_a,
            quantity: None,
            instrument_ids: vec![yes.id()],
        };
        engine
            .establish_full_barrier(
                UUID4::new(),
                PositionOperationType::Split,
                &[yes.id()],
                Some(&inputs_a),
            )
            .unwrap();

        let inputs_b = BarrierInputs {
            strategy_id: strategy,
            account_id: account_b,
            quantity: None,
            instrument_ids: vec![yes.id()],
        };
        assert!(
            engine
                .establish_full_barrier(
                    UUID4::new(),
                    PositionOperationType::Split,
                    &[yes.id()],
                    Some(&inputs_b),
                )
                .is_err(),
            "the target account's own active order must still block"
        );
    }

    #[rstest]
    fn test_merge_application_releases_cost_and_allocates_income() {
        let yes = option("600", "Yes");
        let no = option("601", "No");
        let (mut engine, cache) = engine_with(&[yes.clone(), no.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for (instrument, cost) in [(&yes, dec!(50)), (&no, dec!(60))] {
            let position =
                input_position(instrument, strategy, account, Quantity::from("100"), cost);
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }

        let result = result(
            PositionOperationType::Merge,
            vec![yes.id(), no.id()],
            PositionOperationEffects {
                received: Vec::new(),
                spent: vec![amount("600", "100"), amount("601", "100")],
                collateral_delta: "120".into(),
                payouts: Vec::new(),
            },
        );
        engine.process_position_operation(&result);

        let yes_position = cache
            .borrow()
            .position(&netting_position_id(&yes.id(), strategy))
            .unwrap()
            .cloned();
        let no_position = cache
            .borrow()
            .position(&netting_position_id(&no.id(), strategy))
            .unwrap()
            .cloned();

        assert!(yes_position.is_closed());
        assert!(no_position.is_closed());
        assert_eq!(
            yes_position.realized_pnl,
            Some(nautilus_model::types::Money::new(10.0, Currency::pUSD()))
        );
        assert_eq!(
            no_position.realized_pnl,
            Some(nautilus_model::types::Money::zero(Currency::pUSD()))
        );
        assert_eq!(replay_legs(&cache, result.operation_id).len(), 2);
    }

    #[rstest]
    fn test_convert_application_carries_loss_when_no_outputs() {
        let no_a = option("400", "No");
        let (mut engine, cache) = engine_with(std::slice::from_ref(&no_a));
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&no_a, strategy, account, Quantity::from("100"), dec!(150));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let result = result(
            PositionOperationType::Convert,
            vec![no_a.id()],
            PositionOperationEffects {
                received: Vec::new(),
                spent: vec![amount("400", "100")],
                collateral_delta: "100".into(),
                payouts: Vec::new(),
            },
        );

        engine.process_position_operation(&result);

        let position = cache
            .borrow()
            .position(&netting_position_id(&no_a.id(), strategy))
            .unwrap()
            .cloned();
        assert!(position.is_closed());
        assert_eq!(
            position.realized_pnl,
            Some(nautilus_model::types::Money::new(-50.0, Currency::pUSD()))
        );
    }

    #[rstest]
    fn test_ctf_redeem_allocates_across_strategy_owners() {
        let yes = option("520", "Yes");
        let no = option("521", "No");
        let (mut engine, cache) = engine_with(&[yes.clone(), no.clone()]);
        let account = AccountId::from("ACCOUNT-001");
        let requester = StrategyId::from("STRATEGY-A");
        let second = StrategyId::from("STRATEGY-B");
        let third = StrategyId::from("STRATEGY-C");

        for (instrument, strategy, quantity, cost) in [
            (&yes, requester, "40", dec!(20)),
            (&yes, second, "60", dec!(30)),
            (&no, third, "100", dec!(60)),
        ] {
            let position = input_position(
                instrument,
                strategy,
                account,
                Quantity::from(quantity),
                cost,
            );
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }

        let effects = PositionOperationEffects {
            received: Vec::new(),
            spent: vec![amount("520", "100"), amount("521", "100")],
            collateral_delta: "100".into(),
            payouts: vec![
                PositionOperationTokenPayout {
                    token_id: "520".into(),
                    numerator: "1".into(),
                    denominator: "1".into(),
                },
                PositionOperationTokenPayout {
                    token_id: "521".into(),
                    numerator: "0".into(),
                    denominator: "1".into(),
                },
            ],
        };
        let result = result(
            PositionOperationType::Redeem,
            vec![yes.id(), no.id()],
            effects.clone(),
        );
        engine.process_position_operation(&result);

        let realized = |strategy: StrategyId, instrument: &InstrumentAny| {
            cache
                .borrow()
                .position(&netting_position_id(&instrument.id(), strategy))
                .unwrap()
                .cloned()
        };

        let first_position = realized(requester, &yes);
        let second_position = realized(second, &yes);
        let third_position = realized(third, &no);
        assert!(first_position.is_closed());
        assert!(second_position.is_closed());
        assert!(third_position.is_closed());
        assert_eq!(
            first_position.realized_pnl,
            Some(nautilus_model::types::Money::new(20.0, Currency::pUSD()))
        );
        assert_eq!(
            second_position.realized_pnl,
            Some(nautilus_model::types::Money::new(30.0, Currency::pUSD()))
        );
        assert_eq!(
            third_position.realized_pnl,
            Some(nautilus_model::types::Money::new(-60.0, Currency::pUSD()))
        );

        let legs = replay_legs(&cache, result.operation_id);
        assert_eq!(legs.len(), 3);
        assert_eq!(
            legs.iter().filter(|leg| leg.strategy_id == second).count(),
            1
        );
        assert_eq!(
            legs.iter().filter(|leg| leg.strategy_id == third).count(),
            1
        );

        let recovery = recovery_request(&result, effects, Vec::new());
        let outcome = engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        assert!(matches!(
            outcome,
            PositionOperationRecoveryOutcome::Applied { .. }
        ));
        assert_eq!(replay_legs(&cache, result.operation_id).len(), 3);
    }

    #[rstest]
    fn test_instrument_close_after_redeem_does_not_settle_again() {
        let yes = option("540", "Yes");
        let no = option("541", "No");
        let (mut engine, cache) = engine_with(&[yes.clone(), no.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for (instrument, cost) in [(&yes, dec!(40)), (&no, dec!(60))] {
            let position =
                input_position(instrument, strategy, account, Quantity::from("100"), cost);
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }

        let result = result(
            PositionOperationType::Redeem,
            vec![yes.id(), no.id()],
            PositionOperationEffects {
                received: Vec::new(),
                spent: vec![amount("540", "100"), amount("541", "100")],
                collateral_delta: "100".into(),
                payouts: vec![
                    PositionOperationTokenPayout {
                        token_id: "540".into(),
                        numerator: "1".into(),
                        denominator: "1".into(),
                    },
                    PositionOperationTokenPayout {
                        token_id: "541".into(),
                        numerator: "0".into(),
                        denominator: "1".into(),
                    },
                ],
            },
        );
        engine.process_position_operation(&result);

        let yes_position_id = netting_position_id(&yes.id(), strategy);
        let before = cache.borrow().position(&yes_position_id).unwrap().cloned();

        engine.settle_instrument_close(&InstrumentClose::new(
            yes.id(),
            Price::from("1"),
            InstrumentCloseType::ContractExpired,
            UnixNanos::from(10),
            UnixNanos::from(10),
        ));

        let after = cache.borrow().position(&yes_position_id).unwrap().cloned();
        assert_eq!(before, after);
    }

    #[rstest]
    fn test_ctf_redeem_records_settled_physical_payout_without_new_pnl() {
        let yes = option("530", "Yes");
        let no = option("531", "No");
        let (mut engine, cache) = engine_with(&[yes.clone(), no.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for (instrument, cost, price) in [(&yes, dec!(40), "1"), (&no, dec!(60), "0")] {
            let mut position =
                input_position(instrument, strategy, account, Quantity::from("100"), cost);
            let close = InstrumentClose::new(
                instrument.id(),
                Price::from(price),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(3),
                UnixNanos::from(3),
            );
            position.apply_instrument_close(close).unwrap();
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }

        let yes_position_id = netting_position_id(&yes.id(), strategy);
        let no_position_id = netting_position_id(&no.id(), strategy);
        let before_yes = cache.borrow().position(&yes_position_id).unwrap().cloned();
        let before_no = cache.borrow().position(&no_position_id).unwrap().cloned();

        let effects = PositionOperationEffects {
            received: Vec::new(),
            spent: vec![amount("530", "100"), amount("531", "100")],
            collateral_delta: "100".into(),
            payouts: vec![
                PositionOperationTokenPayout {
                    token_id: "530".into(),
                    numerator: "1".into(),
                    denominator: "1".into(),
                },
                PositionOperationTokenPayout {
                    token_id: "531".into(),
                    numerator: "0".into(),
                    denominator: "1".into(),
                },
            ],
        };
        let result = result(
            PositionOperationType::Redeem,
            vec![yes.id(), no.id()],
            effects.clone(),
        );
        engine.process_position_operation(&result);

        let after_yes = cache.borrow().position(&yes_position_id).unwrap().cloned();
        let after_no = cache.borrow().position(&no_position_id).unwrap().cloned();
        assert_eq!(before_yes, after_yes);
        assert_eq!(before_no, after_no);
        assert!(replay_legs(&cache, result.operation_id).is_empty());

        let consumed = {
            let cache = cache.borrow();
            settled_consumed(&cache, &cache.position(&yes_position_id).unwrap())
        };
        assert_eq!(consumed, dec!(100));

        let settled_entries = cache.borrow().general_entries_with_prefix(&format!(
            "{SETTLED_CONSUMPTION_PREFIX}:operation:{}:",
            result.operation_id
        ));
        assert!(
            !settled_entries.is_empty(),
            "missing settled consumption entries"
        );

        let recovery = recovery_request(&result, effects.clone(), Vec::new());
        let outcome = engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        let PositionOperationRecoveryOutcome::Applied { legs } = outcome else {
            panic!("expected applied settled recovery: {outcome:?}");
        };
        assert_eq!(legs.len(), 2);
        assert!(legs.iter().all(|leg| leg.settled));
        assert!(replay_legs(&cache, result.operation_id).is_empty());
        let recovered_yes = cache.borrow().position(&yes_position_id).unwrap().cloned();
        assert_eq!(recovered_yes, before_yes);

        let record_legs = vec![
            PositionOperationRecoveryLeg {
                position_id: yes_position_id,
                instrument_id: yes.id(),
                strategy_id: Some(strategy),
                direction: PositionOperationDirection::Close,
                quantity: Quantity::from("100"),
                cost: Decimal::ZERO,
                income: Some(dec!(100)),
                settled: true,
            },
            PositionOperationRecoveryLeg {
                position_id: no_position_id,
                instrument_id: no.id(),
                strategy_id: Some(strategy),
                direction: PositionOperationDirection::Close,
                quantity: Quantity::from("100"),
                cost: Decimal::ZERO,
                income: Some(dec!(0)),
                settled: true,
            },
        ];
        let recovery = recovery_request(&result, effects, record_legs);
        let outcome = engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        assert!(matches!(
            outcome,
            PositionOperationRecoveryOutcome::Applied { legs } if legs.iter().all(|leg| leg.settled)
        ));
    }

    #[rstest]
    fn test_settled_consumption_is_idempotent_per_operation_leg() {
        let yes = option("532", "Yes");
        let (engine, cache) = engine_with(std::slice::from_ref(&yes));
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let mut position = input_position(&yes, strategy, account, Quantity::from("100"), dec!(40));
        position
            .apply_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(3),
                UnixNanos::from(3),
            ))
            .unwrap();
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();
        let position_id = position.id;
        let cycle_id = settled_cycle_id(&position);
        let operation_id = UUID4::new();
        let leg = PositionOperationLeg {
            operation_id,
            operation: PositionOperationType::Redeem,
            direction: PositionOperationDirection::Close,
            trader_id: TraderId::from("TRADER-001"),
            strategy_id: strategy,
            account_id: account,
            instrument_id: yes.id(),
            position_id,
            quantity: Quantity::from("40"),
            cost: Decimal::ZERO,
            income: Some(dec!(40)),
            settled: true,
            event_id: UUID4::new(),
            ts_event: UnixNanos::from(4),
            ts_init: UnixNanos::from(4),
        };

        engine
            .record_settled_consumption(std::slice::from_ref(&leg))
            .unwrap();
        engine.record_settled_consumption(&[leg]).unwrap();

        let cache = cache.borrow();
        let position = cache.position(&position_id).unwrap();
        assert_eq!(settled_consumed(&cache, &position), dec!(40));
        let operation_consumed = cache
            .get(&settled_operation_key(&operation_id, &cycle_id))
            .unwrap()
            .and_then(|bytes| std::str::from_utf8(bytes).ok().map(str::to_string))
            .and_then(|value| Decimal::from_str(&value).ok())
            .unwrap();
        assert_eq!(
            operation_consumed,
            dec!(40),
            "retry must not add the same operation payout twice"
        );
    }

    #[rstest]
    fn test_ctf_redeem_fractional_payout_allocates_exact_remainder() {
        let yes = option("550", "Yes");
        let no = option("551", "No");
        let (mut engine, cache) = engine_with(&[yes.clone(), no.clone()]);
        let account = AccountId::from("ACCOUNT-001");
        let strategies = [
            StrategyId::from("STRATEGY-A"),
            StrategyId::from("STRATEGY-B"),
            StrategyId::from("STRATEGY-C"),
        ];

        for (strategy, quantity) in strategies.iter().zip(["10", "20", "30"]) {
            let position =
                input_position(&yes, *strategy, account, Quantity::from(quantity), dec!(0));
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }
        let no_position =
            input_position(&no, strategies[0], account, Quantity::from("60"), dec!(0));
        cache
            .borrow_mut()
            .add_position_without_order(&no_position, OmsType::Netting)
            .unwrap();

        let result = result(
            PositionOperationType::Redeem,
            vec![yes.id(), no.id()],
            PositionOperationEffects {
                received: Vec::new(),
                spent: vec![amount("550", "60"), amount("551", "60")],
                collateral_delta: "60".into(),
                payouts: vec![
                    PositionOperationTokenPayout {
                        token_id: "550".into(),
                        numerator: "1".into(),
                        denominator: "3".into(),
                    },
                    PositionOperationTokenPayout {
                        token_id: "551".into(),
                        numerator: "2".into(),
                        denominator: "3".into(),
                    },
                ],
            },
        );
        engine.process_position_operation(&result);

        let legs = replay_legs(&cache, result.operation_id);
        let yes_incomes: Vec<Decimal> = legs
            .iter()
            .filter(|leg| leg.instrument_id == yes.id())
            .map(|leg| leg.income.unwrap())
            .collect();
        assert_eq!(yes_incomes.len(), 3);
        assert_eq!(yes_incomes.iter().sum::<Decimal>(), dec!(20));
        assert_eq!(yes_incomes[0], dec!(20) * dec!(10) / dec!(60));
    }

    #[rstest]
    fn test_ctf_redeem_single_unit_payout_floors_to_base_units() {
        let yes = option("560", "Yes");
        let (mut engine, cache) = engine_with(std::slice::from_ref(&yes));
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&yes, strategy, account, Quantity::from("1"), dec!(0));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let result = result(
            PositionOperationType::Redeem,
            vec![yes.id()],
            PositionOperationEffects {
                received: Vec::new(),
                spent: vec![amount("560", "1")],
                collateral_delta: "0.333333".into(),
                payouts: vec![PositionOperationTokenPayout {
                    token_id: "560".into(),
                    numerator: "1".into(),
                    denominator: "3".into(),
                }],
            },
        );
        engine.process_position_operation(&result);

        let redeemed = cache
            .borrow()
            .position(&netting_position_id(&yes.id(), strategy))
            .unwrap()
            .cloned();
        assert_eq!(
            redeemed.realized_pnl,
            Some(
                nautilus_model::types::Money::from_decimal(dec!(0.333333), Currency::pUSD())
                    .unwrap()
            )
        );
    }

    #[rstest]
    fn test_v2_redeem_applies_single_outcome_payout() {
        let yes = option("510", "Yes");
        let (mut engine, cache) = engine_with(std::slice::from_ref(&yes));
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&yes, strategy, account, Quantity::from("100"), dec!(60));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let result = result(
            PositionOperationType::Redeem,
            vec![yes.id()],
            PositionOperationEffects {
                received: Vec::new(),
                spent: vec![amount("510", "100")],
                collateral_delta: "100".into(),
                payouts: Vec::new(),
            },
        );
        engine.process_position_operation(&result);

        let redeemed = cache
            .borrow()
            .position(&netting_position_id(&yes.id(), strategy))
            .unwrap()
            .cloned();
        assert!(redeemed.is_closed());
        assert_eq!(
            redeemed.realized_pnl,
            Some(nautilus_model::types::Money::new(40.0, Currency::pUSD()))
        );
    }

    #[rstest]
    fn test_ctf_redeem_allocates_payout_by_outcome() {
        let yes = option("500", "Yes");
        let no = option("501", "No");
        let (mut engine, cache) = engine_with(&[yes.clone(), no.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for (instrument, cost) in [(&yes, dec!(40)), (&no, dec!(60))] {
            let position =
                input_position(instrument, strategy, account, Quantity::from("100"), cost);
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }

        let result = result(
            PositionOperationType::Redeem,
            vec![yes.id(), no.id()],
            PositionOperationEffects {
                received: Vec::new(),
                spent: vec![amount("500", "100"), amount("501", "100")],
                collateral_delta: "100".into(),
                payouts: vec![
                    PositionOperationTokenPayout {
                        token_id: "500".into(),
                        numerator: "1".into(),
                        denominator: "1".into(),
                    },
                    PositionOperationTokenPayout {
                        token_id: "501".into(),
                        numerator: "0".into(),
                        denominator: "1".into(),
                    },
                ],
            },
        );

        engine.process_position_operation(&result);

        let yes_position = cache
            .borrow()
            .position(&netting_position_id(&yes.id(), strategy))
            .unwrap()
            .cloned();
        let no_position = cache
            .borrow()
            .position(&netting_position_id(&no.id(), strategy))
            .unwrap()
            .cloned();
        assert!(yes_position.is_closed());
        assert!(no_position.is_closed());
        assert_eq!(
            yes_position.realized_pnl,
            Some(nautilus_model::types::Money::new(60.0, Currency::pUSD()))
        );
        assert_eq!(
            no_position.realized_pnl,
            Some(nautilus_model::types::Money::new(-60.0, Currency::pUSD()))
        );
    }

    #[rstest]
    fn test_restore_position_operation_barriers_from_cache() {
        let yes = option("100", "Yes");
        let (engine, cache) = engine_with(std::slice::from_ref(&yes));
        let pending_id = UUID4::new();
        let applied_id = UUID4::new();

        let record = |operation_id: UUID4, application_status: &str, venue_status: &str| {
            serde_json::json!({
                "operation_id": operation_id,
                "application_status": application_status,
                "venue_status": venue_status,
                "barrier_instrument_ids": [yes.id()],
            })
        };

        for (operation_id, application_status, venue_status) in [
            (pending_id, "Pending", "Submitted"),
            (applied_id, "Rejected", "Failed"),
        ] {
            let bytes = serde_json::to_vec(&record(operation_id, application_status, venue_status))
                .unwrap();
            cache
                .borrow_mut()
                .add(&format!("position-operation:{operation_id}"), bytes.into())
                .unwrap();
        }

        engine.restore_position_operation_barriers();

        assert_eq!(
            engine.position_operation_barrier(&yes.id()),
            Some(pending_id)
        );
    }

    #[rstest]
    fn test_restore_position_operation_barriers_includes_convert_outputs() {
        let no_a = option("720", "No");
        let no_b = option("721", "No");
        let yes_c = option("730", "Yes");
        let (engine, cache) = engine_with(&[no_a.clone(), no_b.clone(), yes_c.clone()]);
        let operation_id = UUID4::new();
        let record = serde_json::json!({
            "operation_id": operation_id,
            "application_status": "Pending",
            "venue_status": "Submitted",
            "barrier_instrument_ids": [no_a.id(), no_b.id(), yes_c.id()],
        });
        cache
            .borrow_mut()
            .add(
                &format!("position-operation:{operation_id}"),
                serde_json::to_vec(&record).unwrap().into(),
            )
            .unwrap();

        engine.restore_position_operation_barriers();

        assert_eq!(
            engine.position_operation_barrier(&no_a.id()),
            Some(operation_id)
        );
        assert_eq!(
            engine.position_operation_barrier(&no_b.id()),
            Some(operation_id)
        );
        assert_eq!(
            engine.position_operation_barrier(&yes_c.id()),
            Some(operation_id)
        );
    }

    #[rstest]
    fn test_instrument_close_is_deferred_until_operation_barrier_releases() {
        let yes = option("740", "Yes");
        let no = option("741", "No");
        let (engine, cache) = engine_with(&[yes.clone(), no]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&yes, strategy, account, Quantity::from("100"), dec!(40));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let operation_id = UUID4::new();
        engine
            .establish_full_barrier(
                operation_id,
                PositionOperationType::Split,
                &[yes.id()],
                None,
            )
            .unwrap();
        engine.settle_instrument_close(&InstrumentClose::new(
            yes.id(),
            Price::from("1"),
            InstrumentCloseType::ContractExpired,
            UnixNanos::from(10),
            UnixNanos::from(10),
        ));
        engine.settle_instrument_close(&InstrumentClose::new(
            yes.id(),
            Price::from("0"),
            InstrumentCloseType::ContractExpired,
            UnixNanos::from(11),
            UnixNanos::from(11),
        ));

        let position_id = netting_position_id(&yes.id(), strategy);
        assert!(!cache.borrow().position(&position_id).unwrap().is_closed());

        engine.release_position_operation_barrier(operation_id, &[yes.id()]);
        let settled = cache.borrow().position(&position_id).unwrap().cloned();
        assert!(settled.is_closed());
        assert_eq!(
            settled.realized_pnl,
            Some(nautilus_model::types::Money::new(60.0, Currency::pUSD()))
        );
    }

    #[rstest]
    fn test_barrier_rechecks_input_quantity_after_metadata_resolution() {
        let no = option("770", "No");
        let yes = option("771", "Yes");
        let (engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&no, strategy, account, Quantity::from("50"), dec!(20));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let operation_id = UUID4::new();
        let error = engine
            .establish_full_barrier(
                operation_id,
                PositionOperationType::Convert,
                &[no.id(), yes.id()],
                Some(&BarrierInputs {
                    strategy_id: strategy,
                    account_id: account,
                    quantity: Some(Quantity::from("100")),
                    instrument_ids: vec![no.id()],
                }),
            )
            .unwrap_err();

        assert!(error.to_string().contains("requires 100 available"));
        assert_eq!(engine.position_operation_barrier(&no.id()), None);
        assert_eq!(engine.position_operation_barrier(&yes.id()), None);
    }

    #[rstest]
    fn test_barrier_does_not_count_short_quantity_as_convert_input() {
        let no = option("773", "No");
        let yes = option("774", "Yes");
        let (engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = short_position(&no, strategy, account, Quantity::from("100"));
        assert!(position.is_short());
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let error = engine
            .establish_full_barrier(
                UUID4::new(),
                PositionOperationType::Convert,
                &[no.id(), yes.id()],
                Some(&BarrierInputs {
                    strategy_id: strategy,
                    account_id: account,
                    quantity: Some(Quantity::from("100")),
                    instrument_ids: vec![no.id()],
                }),
            )
            .unwrap_err();

        assert!(error.to_string().contains("requires 100 available"));
        assert_eq!(engine.position_operation_barrier(&no.id()), None);
    }

    #[rstest]
    fn test_barrier_convert_rejects_open_short_output_target() {
        let no = option("780", "No");
        let yes = option("781", "Yes");
        let (engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let input = input_position(&no, strategy, account, Quantity::from("100"), dec!(50));
        cache
            .borrow_mut()
            .add_position_without_order(&input, OmsType::Netting)
            .unwrap();
        let short = short_position(&yes, strategy, account, Quantity::from("100"));
        cache
            .borrow_mut()
            .add_position_without_order(&short, OmsType::Netting)
            .unwrap();

        let error = engine
            .establish_full_barrier(
                UUID4::new(),
                PositionOperationType::Convert,
                &[no.id(), yes.id()],
                Some(&BarrierInputs {
                    strategy_id: strategy,
                    account_id: account,
                    quantity: Some(Quantity::from("100")),
                    instrument_ids: vec![no.id()],
                }),
            )
            .unwrap_err();

        assert!(error.to_string().contains("open short position"));
        assert_eq!(engine.position_operation_barrier(&no.id()), None);
        assert_eq!(engine.position_operation_barrier(&yes.id()), None);
    }

    #[rstest]
    #[case::yes_side(true)]
    #[case::no_side(false)]
    fn test_barrier_split_rejects_open_short_on_either_side(#[case] short_yes: bool) {
        let yes = option("782", "Yes");
        let no = option("783", "No");
        let (engine, cache) = engine_with(&[yes.clone(), no.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let short_instrument = if short_yes { &yes } else { &no };
        let short = short_position(short_instrument, strategy, account, Quantity::from("100"));
        cache
            .borrow_mut()
            .add_position_without_order(&short, OmsType::Netting)
            .unwrap();

        let error = engine
            .establish_full_barrier(
                UUID4::new(),
                PositionOperationType::Split,
                &[yes.id(), no.id()],
                Some(&BarrierInputs {
                    strategy_id: strategy,
                    account_id: account,
                    quantity: Some(Quantity::from("100")),
                    instrument_ids: vec![yes.id(), no.id()],
                }),
            )
            .unwrap_err();

        assert!(error.to_string().contains("open short position"));
        assert_eq!(engine.position_operation_barrier(&yes.id()), None);
        assert_eq!(engine.position_operation_barrier(&no.id()), None);
    }

    #[rstest]
    fn test_barrier_convert_accepts_open_long_output_target() {
        let no = option("784", "No");
        let yes = option("785", "Yes");
        let (engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for instrument in [&no, &yes] {
            let position = input_position(
                instrument,
                strategy,
                account,
                Quantity::from("100"),
                dec!(50),
            );
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }

        engine
            .establish_full_barrier(
                UUID4::new(),
                PositionOperationType::Convert,
                &[no.id(), yes.id()],
                Some(&BarrierInputs {
                    strategy_id: strategy,
                    account_id: account,
                    quantity: Some(Quantity::from("100")),
                    instrument_ids: vec![no.id()],
                }),
            )
            .unwrap();

        assert!(engine.position_operation_barrier(&no.id()).is_some());
        assert!(engine.position_operation_barrier(&yes.id()).is_some());
    }

    #[rstest]
    fn test_barrier_convert_accepts_closed_short_output_cycle() {
        let no = option("786", "No");
        let yes = option("787", "Yes");
        let (engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let input = input_position(&no, strategy, account, Quantity::from("100"), dec!(50));
        cache
            .borrow_mut()
            .add_position_without_order(&input, OmsType::Netting)
            .unwrap();
        let mut short = short_position(&yes, strategy, account, Quantity::from("100"));
        short
            .apply_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("0"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();
        assert!(short.is_closed());
        cache
            .borrow_mut()
            .add_position_without_order(&short, OmsType::Netting)
            .unwrap();
        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("0"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();

        engine
            .establish_full_barrier(
                UUID4::new(),
                PositionOperationType::Convert,
                &[no.id(), yes.id()],
                Some(&BarrierInputs {
                    strategy_id: strategy,
                    account_id: account,
                    quantity: Some(Quantity::from("100")),
                    instrument_ids: vec![no.id()],
                }),
            )
            .unwrap();

        assert!(engine.position_operation_barrier(&no.id()).is_some());
        assert!(engine.position_operation_barrier(&yes.id()).is_some());
    }

    #[rstest]
    fn test_barrier_redeem_checks_unredeemed_settled_quantity() {
        let yes = option("772", "Yes");
        let (engine, cache) = engine_with(std::slice::from_ref(&yes));
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let mut position = input_position(&yes, strategy, account, Quantity::from("100"), dec!(40));
        position
            .apply_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();
        cache
            .borrow_mut()
            .add(
                &settled_consumption_key(&position),
                bytes::Bytes::from_static(b"40"),
            )
            .unwrap();

        let input = BarrierInputs {
            strategy_id: strategy,
            account_id: account,
            quantity: Some(Quantity::from("50")),
            instrument_ids: vec![yes.id()],
        };
        assert!(
            engine
                .establish_full_barrier(
                    UUID4::new(),
                    PositionOperationType::Convert,
                    &[yes.id()],
                    Some(&input),
                )
                .is_err(),
            "convert must not draw on a settled position"
        );

        let mut excessive = BarrierInputs {
            strategy_id: strategy,
            account_id: account,
            quantity: Some(Quantity::from("61")),
            instrument_ids: vec![yes.id()],
        };
        assert!(
            engine
                .establish_full_barrier(
                    UUID4::new(),
                    PositionOperationType::Redeem,
                    &[yes.id()],
                    Some(&excessive),
                )
                .is_err(),
            "redeem must not exceed the unredeemed quantity"
        );

        excessive.quantity = Some(Quantity::from("60"));
        let operation_id = UUID4::new();
        engine
            .establish_full_barrier(
                operation_id,
                PositionOperationType::Redeem,
                &[yes.id()],
                Some(&excessive),
            )
            .unwrap();
        assert_eq!(
            engine.position_operation_barrier(&yes.id()),
            Some(operation_id)
        );
    }

    #[rstest]
    #[case("0", dec!(-40))]
    #[case("0.5", dec!(10))]
    #[case("1", dec!(60))]
    fn test_convert_receives_and_settles_output_with_known_payout(
        #[case] close_price: &str,
        #[case] expected_pnl: Decimal,
    ) {
        let no = option("750", "No");
        let yes = option("751", "Yes");
        let (mut engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&no, strategy, account, Quantity::from("100"), dec!(40));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();
        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from(close_price),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();

        let result = result(
            PositionOperationType::Convert,
            vec![no.id(), yes.id()],
            PositionOperationEffects {
                received: vec![amount("751", "100")],
                spent: vec![amount("750", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );
        engine.process_position_operation(&result);

        let cache = cache.borrow();
        let no_position = cache
            .position(&netting_position_id(&no.id(), strategy))
            .expect("input position must remain");
        assert!(no_position.is_closed());
        assert_eq!(
            no_position.realized_pnl,
            Some(Money::zero(Currency::pUSD()))
        );

        let yes_position = cache
            .position(&netting_position_id(&yes.id(), strategy))
            .expect("settled output must be received");
        assert!(yes_position.is_closed());
        assert!(yes_position.is_settled());
        assert_eq!(yes_position.quantity, Quantity::from("0"));
        assert_eq!(yes_position.cost_open, Decimal::ZERO);
        assert_eq!(
            yes_position.realized_pnl,
            Some(Money::from_decimal(expected_pnl, Currency::pUSD()).unwrap())
        );
        assert_eq!(
            settled_physical_quantity(&yes_position),
            Some(Quantity::from("100"))
        );
        assert!(matches!(
            yes_position.replay_events.as_slice(),
            [
                PositionReplayEvent::Operation(_),
                PositionReplayEvent::InstrumentClosed(_)
            ]
        ));
    }

    #[rstest]
    fn test_restore_barrier_for_applied_record_requires_verification() {
        let yes = option("760", "Yes");
        let (engine, cache) = engine_with(std::slice::from_ref(&yes));
        let operation_id = UUID4::new();
        let record = serde_json::json!({
            "operation_id": operation_id,
            "application_status": "Applied",
            "venue_status": "Confirmed",
            "barrier_instrument_ids": [yes.id()],
        });
        cache
            .borrow_mut()
            .add(
                &format!("position-operation:{operation_id}"),
                serde_json::to_vec(&record).unwrap().into(),
            )
            .unwrap();

        engine.restore_position_operation_barriers();

        assert_eq!(
            engine.position_operation_barrier(&yes.id()),
            Some(operation_id)
        );
    }

    #[rstest]
    fn test_v2_redeem_supports_partial_settled_physical_quantity() {
        let yes = option("570", "Yes");
        let (mut engine, cache) = engine_with(std::slice::from_ref(&yes));
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let mut position = input_position(&yes, strategy, account, Quantity::from("100"), dec!(40));
        position
            .apply_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();
        let position_id = netting_position_id(&yes.id(), strategy);

        let result = result(
            PositionOperationType::Redeem,
            vec![yes.id()],
            PositionOperationEffects {
                received: Vec::new(),
                spent: vec![amount("570", "40")],
                collateral_delta: "40".into(),
                payouts: Vec::new(),
            },
        );
        engine.process_position_operation(&result);

        let after = cache.borrow().position(&position_id).unwrap().cloned();
        assert_eq!(after, position);
        let consumed = {
            let cache = cache.borrow();
            settled_consumed(&cache, &cache.position(&position_id).unwrap())
        };
        assert_eq!(consumed, dec!(40));
        assert!(replay_legs(&cache, result.operation_id).is_empty());
    }

    #[rstest]
    fn test_settled_redeem_sources_ignore_price_restatements() {
        let yes = option("820", "Yes");
        let (mut engine, cache) = engine_with(std::slice::from_ref(&yes));
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let order = market_buy_order(&yes, strategy, "10");
        let opening = buy_fill(&order, &yes, account, "TRADE-RESTATE", "10", "0.50");
        let mut position = Position::new(&yes, opening);
        let restatement = buy_fill(&order, &yes, account, "TRADE-RESTATE", "10", "0.60");
        position.apply(&restatement);
        assert_eq!(position.quantity, Quantity::from("10"));
        apply_expiry(&mut position, &yes);

        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let sources = settled_redeem_sources(&mut cache.borrow_mut(), &yes.id(), account, None, 6)
            .expect("a restated cycle remains redeemable");
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].quantity, dec!(10));

        let redeem = result(
            PositionOperationType::Redeem,
            vec![yes.id()],
            PositionOperationEffects {
                received: Vec::new(),
                spent: vec![amount("820", "10")],
                collateral_delta: "10".into(),
                payouts: vec![PositionOperationTokenPayout {
                    token_id: "820".into(),
                    numerator: "1".into(),
                    denominator: "1".into(),
                }],
            },
        );
        engine.process_position_operation(&redeem);

        let position_id = netting_position_id(&yes.id(), strategy);
        let cache = cache.borrow();
        let position = cache.position(&position_id).unwrap();
        assert_eq!(settled_consumed(&cache, &position), dec!(10));
        drop(position);
        drop(cache);
        assert!(
            engine
                .applied_position_operations
                .contains(&redeem.operation_id)
        );
    }

    #[rstest]
    fn test_settled_redeem_sources_apply_fully_voided_fills_before_rebuy() {
        let yes = option("821", "Yes");
        let (mut engine, cache) = engine_with(std::slice::from_ref(&yes));
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let first_order = market_buy_order(&yes, strategy, "10");
        let first_fill = buy_fill(&first_order, &yes, account, "TRADE-VOIDED", "10", "0.50");
        let mut position = Position::new(&yes, first_fill.clone());

        let voided = OrderFillVoidedSpec::builder()
            .trader_id(first_fill.trader_id)
            .strategy_id(first_fill.strategy_id)
            .instrument_id(first_fill.instrument_id)
            .client_order_id(first_fill.client_order_id)
            .venue_order_id(first_fill.venue_order_id)
            .account_id(first_fill.account_id)
            .trade_id(first_fill.trade_id)
            .voided_qty(first_fill.last_qty)
            .order_side(first_fill.order_side)
            .order_type(first_fill.order_type)
            .last_px(first_fill.last_px)
            .currency(first_fill.currency)
            .liquidity_side(first_fill.liquidity_side)
            .maybe_position_id(first_fill.position_id)
            .build();
        position
            .apply_fill_void(voided, first_fill.last_qty, None)
            .unwrap();
        assert_eq!(position.quantity, Quantity::zero(6));

        let second_order = market_buy_order(&yes, strategy, "8");
        let second_fill = buy_fill(&second_order, &yes, account, "TRADE-REBUY", "8", "0.60");
        position.apply(&second_fill);
        assert_eq!(position.quantity, Quantity::from("8"));
        apply_expiry(&mut position, &yes);

        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let sources = settled_redeem_sources(&mut cache.borrow_mut(), &yes.id(), account, None, 6)
            .expect("a fully voided fill does not discard the later rebuy");
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].quantity, dec!(8));

        let redeem = result(
            PositionOperationType::Redeem,
            vec![yes.id()],
            PositionOperationEffects {
                received: Vec::new(),
                spent: vec![amount("821", "8")],
                collateral_delta: "8".into(),
                payouts: vec![PositionOperationTokenPayout {
                    token_id: "821".into(),
                    numerator: "1".into(),
                    denominator: "1".into(),
                }],
            },
        );
        engine.process_position_operation(&redeem);

        let position_id = netting_position_id(&yes.id(), strategy);
        let cache = cache.borrow();
        let position = cache.position(&position_id).unwrap();
        assert_eq!(settled_consumed(&cache, &position), dec!(8));
        drop(position);
        drop(cache);
        assert!(
            engine
                .applied_position_operations
                .contains(&redeem.operation_id)
        );
    }

    #[rstest]
    fn test_settled_redeem_sources_fall_back_to_recorded_physical_quantity() {
        let yes = option("822", "Yes");
        let (_, cache) = engine_with(std::slice::from_ref(&yes));
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let order = market_buy_order(&yes, strategy, "10");
        let opening = buy_fill(&order, &yes, account, "TRADE-RECORD", "10", "0.50");
        let mut position = Position::new(&yes, opening);
        apply_expiry(&mut position, &yes);
        position
            .apply_adjustment(PositionAdjusted::new(
                position.trader_id,
                position.strategy_id,
                position.instrument_id,
                position.id,
                position.account_id,
                PositionAdjustmentType::Commission,
                None,
                Some(Money::new(-1.0, Currency::pUSD())),
                None,
                UUID4::new(),
                UnixNanos::from(11),
                UnixNanos::from(11),
            ))
            .unwrap();
        assert!(settled_physical_quantity(&position).is_none());

        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();
        cache
            .borrow_mut()
            .record_settled_position_source(&position, Quantity::from("10"))
            .unwrap();

        let sources = settled_redeem_sources(&mut cache.borrow_mut(), &yes.id(), account, None, 6)
            .expect("the recorded settlement quantity resolves an indeterminate replay");
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].quantity, dec!(10));
    }

    #[rstest]
    fn test_convert_rejects_effects_outside_the_prepared_scope() {
        let no = option("770", "No");
        let yes = option("771", "Yes");
        let extra = option("772", "Yes");
        let (mut engine, cache) = engine_with(&[no.clone(), yes.clone(), extra.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&no, strategy, account, Quantity::from("100"), dec!(40));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let result = result(
            PositionOperationType::Convert,
            vec![no.id(), yes.id()],
            PositionOperationEffects {
                received: vec![amount("771", "100"), amount("772", "100")],
                spent: vec![amount("770", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );
        engine.process_position_operation(&result);

        assert!(
            cache
                .borrow()
                .position(&netting_position_id(&extra.id(), strategy))
                .is_none()
        );
        assert!(
            cache
                .borrow()
                .position(&netting_position_id(&yes.id(), strategy))
                .is_none()
        );
    }

    #[rstest]
    fn test_overlapping_recoveries_keep_each_operations_barrier() {
        let yes = option("780", "Yes");
        let (engine, cache) = engine_with(std::slice::from_ref(&yes));
        let applied = UUID4::new();
        let pending = UUID4::new();
        for (operation_id, status) in [(applied, "Applied"), (pending, "Pending")] {
            let record = serde_json::json!({
                "operation_id": operation_id,
                "application_status": status,
                "venue_status": "Confirmed",
                "barrier_instrument_ids": [yes.id()],
            });
            cache
                .borrow_mut()
                .add(
                    &format!("position-operation:{operation_id}"),
                    serde_json::to_vec(&record).unwrap().into(),
                )
                .unwrap();
        }

        engine.restore_position_operation_barriers();
        assert_eq!(
            engine
                .position_operation_barriers
                .borrow()
                .get(&yes.id())
                .unwrap()
                .len(),
            2
        );

        engine.release_position_operation_barrier(applied, &[yes.id()]);
        assert_eq!(engine.position_operation_barrier(&yes.id()), Some(pending));

        engine.release_position_operation_barrier(pending, &[yes.id()]);
        assert_eq!(engine.position_operation_barrier(&yes.id()), None);
    }

    #[rstest]
    fn test_convert_mixes_settled_and_unsettled_outputs() {
        let no = option("820", "No");
        let settled_yes = option("821", "Yes");
        let open_yes = option("822", "Yes");
        let (mut engine, cache) = engine_with(&[no.clone(), settled_yes.clone(), open_yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&no, strategy, account, Quantity::from("100"), dec!(150));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();
        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                settled_yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();

        let result = result(
            PositionOperationType::Convert,
            vec![no.id(), settled_yes.id(), open_yes.id()],
            PositionOperationEffects {
                received: vec![amount("821", "99"), amount("822", "99")],
                spent: vec![amount("820", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );
        engine.process_position_operation(&result);

        let cache = cache.borrow();
        let settled = cache
            .position(&netting_position_id(&settled_yes.id(), strategy))
            .expect("settled output must be received")
            .clone();
        assert!(settled.is_closed());
        assert!(settled.is_settled());
        assert_eq!(
            settled.realized_pnl,
            Some(Money::new(24.0, Currency::pUSD()))
        );
        assert_eq!(
            settled_physical_quantity(&settled),
            Some(Quantity::from("99"))
        );

        let open = cache
            .position(&netting_position_id(&open_yes.id(), strategy))
            .expect("unsettled output must open")
            .clone();
        assert!(open.is_open());
        assert_eq!(open.quantity, Quantity::from("99"));
        assert_eq!(open.cost_open, dec!(75));
    }

    #[rstest]
    fn test_convert_settles_output_once_when_close_arrives_during_operation() {
        let no = option("840", "No");
        let yes = option("841", "Yes");
        let (mut engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&no, strategy, account, Quantity::from("100"), dec!(50));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let operation_id = UUID4::new();
        let inputs = BarrierInputs {
            strategy_id: strategy,
            account_id: account,
            quantity: Some(Quantity::from("100")),
            instrument_ids: vec![no.id()],
        };
        engine
            .establish_full_barrier(
                operation_id,
                PositionOperationType::Convert,
                &[no.id(), yes.id()],
                Some(&inputs),
            )
            .unwrap();
        engine.settle_instrument_close(&InstrumentClose::new(
            yes.id(),
            Price::from("0"),
            InstrumentCloseType::ContractExpired,
            UnixNanos::from(5),
            UnixNanos::from(5),
        ));

        let mut result = result(
            PositionOperationType::Convert,
            vec![no.id(), yes.id()],
            PositionOperationEffects {
                received: vec![amount("841", "99")],
                spent: vec![amount("840", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );
        result.operation_id = operation_id;
        engine.process_position_operation(&result);

        assert_eq!(engine.position_operation_barrier(&no.id()), None);
        assert_eq!(engine.position_operation_barrier(&yes.id()), None);

        let yes_position_id = netting_position_id(&yes.id(), strategy);
        let yes_position = cache
            .borrow()
            .position(&yes_position_id)
            .expect("settled output must be received")
            .cloned();
        assert!(yes_position.is_closed());
        assert!(yes_position.is_settled());
        assert_eq!(
            yes_position.realized_pnl,
            Some(Money::new(-50.0, Currency::pUSD()))
        );
        assert_eq!(
            settled_physical_quantity(&yes_position),
            Some(Quantity::from("99"))
        );
        assert_eq!(yes_position.ts_opened, UnixNanos::from(1_000_000_000));
        assert_eq!(yes_position.ts_last, yes_position.ts_opened);
        assert_eq!(yes_position.ts_closed, Some(yes_position.ts_opened));
        assert_eq!(
            yes_position
                .replay_events
                .iter()
                .filter(|event| matches!(event, PositionReplayEvent::InstrumentClosed(_)))
                .count(),
            1
        );
        assert_eq!(
            cache
                .borrow()
                .instrument_close(&yes.id())
                .map(|close| close.ts_event),
            Some(UnixNanos::from(5)),
            "the original market result timestamp must be retained"
        );
        let close_event = yes_position
            .replay_events
            .iter()
            .find_map(|event| match event {
                PositionReplayEvent::InstrumentClosed(close) => Some(close),
                _ => None,
            })
            .expect("settlement replay event must exist");
        assert_eq!(close_event.ts_event, yes_position.ts_opened);
        assert_eq!(close_event.ts_init, UnixNanos::from(5));
        assert_eq!(close_event.close_price, Price::from("0"));

        engine.process_position_operation(&result);

        let after = cache
            .borrow()
            .position(&yes_position_id)
            .expect("settled output must remain")
            .cloned();
        assert_eq!(after, yes_position);

        engine.settle_instrument_close(&InstrumentClose::new(
            yes.id(),
            Price::from("0"),
            InstrumentCloseType::ContractExpired,
            UnixNanos::from(5),
            UnixNanos::from(5),
        ));
        assert_eq!(
            cache
                .borrow()
                .position(&yes_position_id)
                .expect("settled output must remain")
                .cloned(),
            yes_position,
            "repeated close evidence must not settle the output twice"
        );
    }

    #[rstest]
    fn test_recovery_applies_unapplied_settled_convert_output() {
        let no = option("812", "No");
        let yes = option("813", "Yes");
        let (mut engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&no, strategy, account, Quantity::from("100"), dec!(40));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();
        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("0"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();

        let effects = PositionOperationEffects {
            received: vec![amount("813", "99")],
            spent: vec![amount("812", "100")],
            collateral_delta: "0".into(),
            payouts: Vec::new(),
        };
        let result = result(
            PositionOperationType::Convert,
            vec![no.id(), yes.id()],
            effects.clone(),
        );
        let recovery = recovery_request(&result, effects, Vec::new());
        let outcome = engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        assert!(matches!(
            outcome,
            PositionOperationRecoveryOutcome::Applied { .. }
        ));

        let yes_position = cache
            .borrow()
            .position(&netting_position_id(&yes.id(), strategy))
            .expect("settled output must be received")
            .cloned();
        assert!(yes_position.is_settled());
        assert_eq!(
            yes_position.realized_pnl,
            Some(Money::new(-40.0, Currency::pUSD()))
        );
        assert_eq!(
            settled_physical_quantity(&yes_position),
            Some(Quantity::from("99"))
        );
    }

    #[rstest]
    fn test_convert_output_adds_to_existing_settled_holding_and_redeems_both() {
        let no = option("830", "No");
        let yes = option("831", "Yes");
        let (mut engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let input = input_position(&no, strategy, account, Quantity::from("100"), dec!(40));
        cache
            .borrow_mut()
            .add_position_without_order(&input, OmsType::Netting)
            .unwrap();

        let mut existing = input_position(&yes, strategy, account, Quantity::from("50"), dec!(20));
        existing
            .apply_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();
        cache
            .borrow_mut()
            .add_position_without_order(&existing, OmsType::Netting)
            .unwrap();
        cache
            .borrow_mut()
            .record_settled_position_source(&existing, Quantity::from("50"))
            .unwrap();
        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();

        let convert = result(
            PositionOperationType::Convert,
            vec![no.id(), yes.id()],
            PositionOperationEffects {
                received: vec![amount("831", "99")],
                spent: vec![amount("830", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );
        engine.process_position_operation(&convert);

        let redeem = result(
            PositionOperationType::Redeem,
            vec![yes.id()],
            PositionOperationEffects {
                received: Vec::new(),
                spent: vec![amount("831", "149")],
                collateral_delta: "149".into(),
                payouts: vec![PositionOperationTokenPayout {
                    token_id: "831".into(),
                    numerator: "1".into(),
                    denominator: "1".into(),
                }],
            },
        );
        engine.process_position_operation(&redeem);

        let cache = cache.borrow();
        let current = cache
            .position(&netting_position_id(&yes.id(), strategy))
            .expect("new settled cycle must exist");
        assert_eq!(settled_consumed(&cache, &current), dec!(99));
        let snapshots = cache.position_snapshots(Some(&current.id), Some(&account));
        assert_eq!(snapshots.len(), 1);
        assert_eq!(settled_consumed(&cache, &snapshots[0]), dec!(50));
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_recovery_completes_saved_receive_missing_settlement(#[case] recorded_legs: bool) {
        let no = option("814", "No");
        let yes = option("815", "Yes");
        let (mut engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&no, strategy, account, Quantity::from("100"), dec!(40));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let effects = PositionOperationEffects {
            received: vec![amount("815", "99")],
            spent: vec![amount("814", "100")],
            collateral_delta: "0".into(),
            payouts: Vec::new(),
        };
        let result = result(
            PositionOperationType::Convert,
            vec![no.id(), yes.id()],
            effects.clone(),
        );
        engine.process_position_operation(&result);

        let yes_position_id = netting_position_id(&yes.id(), strategy);
        {
            let cache = cache.borrow();
            let open = cache.position(&yes_position_id).unwrap();
            assert!(open.is_open(), "receive is saved without its settlement");
        }

        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("0"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();

        let applied_legs = if recorded_legs {
            recovery_legs(&replay_legs(&cache, result.operation_id))
        } else {
            Vec::new()
        };
        let recovery = recovery_request(&result, effects, applied_legs);
        let outcome = engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        assert!(matches!(
            outcome,
            PositionOperationRecoveryOutcome::Applied { .. }
        ));

        let settled = cache
            .borrow()
            .position(&yes_position_id)
            .expect("received output must remain")
            .cloned();
        assert!(settled.is_closed());
        assert!(settled.is_settled());
        assert_eq!(settled.quantity, Quantity::from("0"));
        assert_eq!(
            settled.realized_pnl,
            Some(Money::new(-40.0, Currency::pUSD()))
        );
        assert_eq!(
            settled_physical_quantity(&settled),
            Some(Quantity::from("99"))
        );
        assert_eq!(settled.ts_opened, UnixNanos::from(1_000_000_000));
        assert_eq!(settled.ts_last, settled.ts_opened);
        assert_eq!(settled.ts_closed, Some(settled.ts_opened));
        assert!(
            settled.ts_closed >= Some(settled.ts_opened),
            "settlement confirmation must not precede the receive"
        );
        assert_eq!(
            settled
                .replay_events
                .iter()
                .filter(|event| matches!(event, PositionReplayEvent::Operation(_)))
                .count(),
            1,
            "recovery must not receive the output again"
        );
        assert_eq!(
            settled
                .replay_events
                .iter()
                .filter(|event| matches!(event, PositionReplayEvent::InstrumentClosed(_)))
                .count(),
            1
        );
        assert_eq!(
            cache
                .borrow()
                .instrument_close(&yes.id())
                .map(|close| close.ts_event),
            Some(UnixNanos::from(5))
        );
        let close_event = settled
            .replay_events
            .iter()
            .find_map(|event| match event {
                PositionReplayEvent::InstrumentClosed(close) => Some(close),
                _ => None,
            })
            .expect("settlement replay event must exist");
        assert_eq!(close_event.ts_event, settled.ts_opened);
        assert_eq!(close_event.ts_init, UnixNanos::from(5));
        assert_eq!(close_event.close_price, Price::from("0"));

        let before = settled;
        let outcome = engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        assert!(matches!(
            outcome,
            PositionOperationRecoveryOutcome::Applied { .. }
        ));
        assert_eq!(
            cache.borrow().position(&yes_position_id).unwrap().cloned(),
            before,
            "repeated recovery must not change the settled position"
        );
    }

    #[rstest]
    fn test_restore_operation_barriers_restores_archived_cycle_index() {
        let yes = option("818", "Yes");
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let mut position = input_position(&yes, strategy, account, Quantity::from("100"), dec!(40));
        position
            .apply_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();
        let position_id = position.id;

        let source_cache = Rc::new(RefCell::new(Cache::default()));
        source_cache
            .borrow_mut()
            .add_instrument(yes.clone())
            .unwrap();
        source_cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();
        source_cache
            .borrow_mut()
            .snapshot_position(&position)
            .unwrap();
        let frames = source_cache
            .borrow()
            .position_snapshot_bytes(&position_id)
            .expect("snapshot frames must exist");

        let rebuilt = Rc::new(RefCell::new(Cache::default()));
        rebuilt.borrow_mut().add_instrument(yes.clone()).unwrap();
        rebuilt
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        for (index, bytes) in frames.into_iter().enumerate() {
            let key = format!(
                "{}{}/{}",
                nautilus_common::cache::CacheSnapshotRef::POSITION_PREFIX,
                position_id,
                index
            );
            rebuilt
                .borrow_mut()
                .add(&key, bytes::Bytes::from(bytes))
                .unwrap();
        }
        assert!(
            rebuilt
                .borrow()
                .position_snapshots(Some(&position_id), Some(&account))
                .is_empty()
        );

        let operation_id = UUID4::new();
        let record = serde_json::json!({
            "operation_id": operation_id,
            "account_id": account,
            "application_status": "Pending",
            "venue_status": "Submitted",
            "barrier_instrument_ids": [yes.id()],
        });
        rebuilt
            .borrow_mut()
            .add(
                &format!("position-operation:{operation_id}"),
                serde_json::to_vec(&record).unwrap().into(),
            )
            .unwrap();
        let engine = ExecutionEngine::new(
            Rc::new(RefCell::new(VirtualClock::new())),
            rebuilt.clone(),
            None,
        );

        engine.restore_position_operation_barriers();

        assert_eq!(
            rebuilt
                .borrow()
                .position_snapshots(Some(&position_id), Some(&account))
                .len(),
            1
        );
        assert_eq!(
            engine.position_operation_barrier(&yes.id()),
            Some(operation_id)
        );
    }

    #[rstest]
    fn test_recovery_completes_output_sold_before_market_settlement() {
        let no = option("816", "No");
        let yes = option("817", "Yes");
        let (mut engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&no, strategy, account, Quantity::from("100"), dec!(40));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let effects = PositionOperationEffects {
            received: vec![amount("817", "99")],
            spent: vec![amount("816", "100")],
            collateral_delta: "0".into(),
            payouts: Vec::new(),
        };
        let result = result(
            PositionOperationType::Convert,
            vec![no.id(), yes.id()],
            effects.clone(),
        );
        engine.process_position_operation(&result);

        let yes_position_id = netting_position_id(&yes.id(), strategy);
        let sell_order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(yes.id())
            .side(OrderSide::Sell)
            .quantity(Quantity::from("99"))
            .build();
        let sell_event = TestOrderEventStubs::filled(
            &sell_order,
            &yes,
            Some(TradeId::new("SELL-YES")),
            Some(yes_position_id),
            Some(Price::from("0.50")),
            Some(Quantity::from("99")),
            Some(LiquiditySide::Taker),
            None,
            None,
            Some(account),
        );
        let OrderEventAny::Filled(sell_fill) = sell_event else {
            panic!("expected a filled sell event");
        };
        let mut sold = cache
            .borrow()
            .position(&yes_position_id)
            .expect("received output must exist")
            .cloned();
        sold.try_apply(&sell_fill).unwrap();
        cache
            .borrow_mut()
            .replace_position(&sold, OmsType::Netting, false, false)
            .unwrap();
        assert!(sold.is_closed());
        assert!(!sold.is_settled());

        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("0"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();
        let record = serde_json::json!({
            "operation_id": result.operation_id,
            "application_status": "Pending",
            "venue_status": "Submitted",
            "barrier_instrument_ids": [no.id(), yes.id()],
        });
        cache
            .borrow_mut()
            .add(
                &format!("position-operation:{}", result.operation_id),
                serde_json::to_vec(&record).unwrap().into(),
            )
            .unwrap();
        engine.restore_position_operation_barriers();
        assert_eq!(
            engine.position_operation_barrier(&yes.id()),
            Some(result.operation_id)
        );

        let recovery = recovery_request(&result, effects, Vec::new());
        let outcome = engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        assert!(matches!(
            outcome,
            PositionOperationRecoveryOutcome::Applied { .. }
        ));

        assert_eq!(engine.position_operation_barrier(&no.id()), None);
        assert_eq!(engine.position_operation_barrier(&yes.id()), None);
        let after = cache
            .borrow()
            .position(&yes_position_id)
            .expect("sold output must remain")
            .cloned();
        assert_eq!(after, sold, "recovery must not change the completed sale");
        assert_eq!(after.realized_pnl, Some(Money::new(9.5, Currency::pUSD())));
        assert!(!after.is_settled());
        assert!(
            !after
                .replay_events
                .iter()
                .any(|event| matches!(event, PositionReplayEvent::InstrumentClosed(_)))
        );
    }

    #[rstest]
    fn test_recovery_of_settled_convert_output_does_not_duplicate() {
        let no = option("810", "No");
        let yes = option("811", "Yes");
        let (mut engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&no, strategy, account, Quantity::from("100"), dec!(40));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();
        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();
        let effects = PositionOperationEffects {
            received: vec![amount("811", "100")],
            spent: vec![amount("810", "100")],
            collateral_delta: "0".into(),
            payouts: Vec::new(),
        };
        let result = result(
            PositionOperationType::Convert,
            vec![no.id(), yes.id()],
            effects.clone(),
        );
        engine.process_position_operation(&result);

        let yes_position_id = netting_position_id(&yes.id(), strategy);
        let before = cache.borrow().position(&yes_position_id).unwrap().cloned();
        let recovery = recovery_request(&result, effects, Vec::new());
        let outcome = engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        assert!(matches!(
            outcome,
            PositionOperationRecoveryOutcome::Applied { .. }
        ));
        let after = cache.borrow().position(&yes_position_id).unwrap().cloned();
        assert_eq!(before, after);
        assert_eq!(replay_legs(&cache, result.operation_id).len(), 2);
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_repeated_settled_convert_outputs_redeem_all_cycles(#[case] same_timestamp: bool) {
        let no1 = option("800", "No");
        let no2 = option("802", "No");
        let yes = option("801", "Yes");
        let (mut engine, cache) = engine_with(&[no1.clone(), no2.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for instrument in [&no1, &no2] {
            let position = input_position(
                instrument,
                strategy,
                account,
                Quantity::from("100"),
                dec!(40),
            );
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }
        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();
        let yes_position_id = netting_position_id(&yes.id(), strategy);

        let first = result(
            PositionOperationType::Convert,
            vec![no1.id(), yes.id()],
            PositionOperationEffects {
                received: vec![amount("801", "100")],
                spent: vec![amount("800", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );
        engine.process_position_operation(&first);

        let partial = result(
            PositionOperationType::Redeem,
            vec![yes.id()],
            PositionOperationEffects {
                received: Vec::new(),
                spent: vec![amount("801", "40")],
                collateral_delta: "40".into(),
                payouts: Vec::new(),
            },
        );
        engine.process_position_operation(&partial);
        {
            let cache = cache.borrow();
            let position = cache.position(&yes_position_id).unwrap();
            assert_eq!(settled_consumed(&cache, &position), dec!(40));
        }

        let mut second = result(
            PositionOperationType::Convert,
            vec![no2.id(), yes.id()],
            PositionOperationEffects {
                received: vec![amount("801", "100")],
                spent: vec![amount("802", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );

        if !same_timestamp {
            second.ts_event = UnixNanos::from(2_000_000_000);
            second.ts_init = UnixNanos::from(2_000_000_001);
        }
        engine.process_position_operation(&second);

        let snapshots = cache
            .borrow()
            .position_snapshots(Some(&yes_position_id), Some(&account));
        assert_eq!(snapshots.len(), 1, "the first settled cycle is archived");
        assert_eq!(
            cache
                .borrow()
                .settled_position_source_records(&yes.id(), account)
                .len(),
            2,
            "same-timestamp operations must retain distinct physical sources"
        );

        let redeem_effects = PositionOperationEffects {
            received: Vec::new(),
            spent: vec![amount("801", "160")],
            collateral_delta: "160".into(),
            payouts: vec![PositionOperationTokenPayout {
                token_id: "801".into(),
                numerator: "1".into(),
                denominator: "1".into(),
            }],
        };
        let full = result(
            PositionOperationType::Redeem,
            vec![yes.id()],
            redeem_effects.clone(),
        );

        // Persist the position, its source records, consumption state, and snapshot bytes into a
        // fresh cache. The normal load path restores blobs without their frame index.
        let rebuilt_cache = Rc::new(RefCell::new(Cache::default()));
        for instrument in [&no1, &no2, &yes] {
            rebuilt_cache
                .borrow_mut()
                .add_instrument(instrument.clone())
                .unwrap();
        }
        let persisted_position = cache
            .borrow()
            .position(&yes_position_id)
            .expect("current position must exist")
            .cloned();
        rebuilt_cache
            .borrow_mut()
            .add_position_without_order(&persisted_position, OmsType::Netting)
            .unwrap();
        let mut persisted_entries = cache
            .borrow()
            .general_entries_with_prefix("position-settlement-source:");
        persisted_entries.extend(
            cache
                .borrow()
                .general_entries_with_prefix("position-operation-settled:"),
        );

        for (key, bytes) in persisted_entries {
            rebuilt_cache.borrow_mut().add(&key, bytes).unwrap();
        }

        if let Some(frames) = cache.borrow().position_snapshot_bytes(&yes_position_id) {
            for (index, bytes) in frames.into_iter().enumerate() {
                let key = format!(
                    "{}{}/{}",
                    nautilus_common::cache::CacheSnapshotRef::POSITION_PREFIX,
                    yes_position_id,
                    index
                );
                rebuilt_cache
                    .borrow_mut()
                    .add(&key, bytes::Bytes::from(bytes))
                    .unwrap();
            }
        }
        assert!(
            rebuilt_cache
                .borrow()
                .position_snapshots(Some(&yes_position_id), Some(&account))
                .is_empty(),
            "the rebuilt cache must not have a restored snapshot index"
        );
        let mut rebuilt_engine = ExecutionEngine::new(
            Rc::new(RefCell::new(VirtualClock::new())),
            rebuilt_cache.clone(),
            None,
        );

        engine.process_position_operation(&full);

        {
            let cache = cache.borrow();
            let current = cache.position(&yes_position_id).unwrap();
            assert_eq!(settled_consumed(&cache, &current), dec!(100));
            assert_eq!(settled_consumed(&cache, &snapshots[0]), dec!(100));
        }
        assert!(replay_legs(&cache, full.operation_id).is_empty());

        let rebuilt_full = result(
            PositionOperationType::Redeem,
            vec![yes.id()],
            redeem_effects,
        );
        rebuilt_engine.process_position_operation(&rebuilt_full);

        {
            let cache = rebuilt_cache.borrow();
            let current = cache.position(&yes_position_id).unwrap();
            assert_eq!(
                settled_consumed(&cache, &current),
                dec!(100),
                "rebuilt cache must consume the current cycle"
            );
            let restored = cache.position_snapshots(Some(&yes_position_id), Some(&account));
            assert_eq!(restored.len(), 1, "snapshot frame index must be restored");
            assert_eq!(
                settled_consumed(&cache, &restored[0]),
                dec!(100),
                "rebuilt cache must consume the archived cycle"
            );
        }
    }

    #[rstest]
    #[case(false, false)]
    #[case(false, true)]
    #[case(true, false)]
    #[case(true, true)]
    fn test_recover_old_redeem_uses_archived_cycle(
        #[case] recorded_legs: bool,
        #[case] legacy_marker: bool,
    ) {
        let no1 = option("850", "No");
        let no2 = option("852", "No");
        let yes = option("851", "Yes");
        let (mut engine, cache) = engine_with(&[no1.clone(), no2.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for instrument in [&no1, &no2] {
            let position = input_position(
                instrument,
                strategy,
                account,
                Quantity::from("100"),
                dec!(40),
            );
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }
        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();
        let yes_position_id = netting_position_id(&yes.id(), strategy);

        let first = result(
            PositionOperationType::Convert,
            vec![no1.id(), yes.id()],
            PositionOperationEffects {
                received: vec![amount("851", "100")],
                spent: vec![amount("850", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );
        engine.process_position_operation(&first);

        let redeem_effects = PositionOperationEffects {
            received: Vec::new(),
            spent: vec![amount("851", "100")],
            collateral_delta: "100".into(),
            payouts: Vec::new(),
        };
        let redeem = result(
            PositionOperationType::Redeem,
            vec![yes.id()],
            redeem_effects.clone(),
        );
        engine.process_position_operation(&redeem);

        let mut second = result(
            PositionOperationType::Convert,
            vec![no2.id(), yes.id()],
            PositionOperationEffects {
                received: vec![amount("851", "100")],
                spent: vec![amount("852", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );
        second.ts_event = UnixNanos::from(2_000_000_000);
        second.ts_init = UnixNanos::from(2_000_000_001);
        engine.process_position_operation(&second);

        let current = cache
            .borrow()
            .position(&yes_position_id)
            .expect("new cycle must exist")
            .cloned();
        let mut rebuilt_cache = Cache::default();
        rebuilt_cache.add_instrument(yes.clone()).unwrap();
        rebuilt_cache
            .add_position_without_order(&current, OmsType::Netting)
            .unwrap();
        let mut persisted_entries = cache
            .borrow()
            .general_entries_with_prefix("position-settlement-source:");
        persisted_entries.extend(
            cache
                .borrow()
                .general_entries_with_prefix("position-operation-settled:"),
        );
        let archived_cycle = cache
            .borrow()
            .position_snapshots(Some(&yes_position_id), Some(&account))
            .first()
            .expect("archived cycle must exist")
            .ts_opened;
        let new_marker_prefix = format!(
            "{SETTLED_CONSUMPTION_PREFIX}:operation:{}:",
            redeem.operation_id
        );
        let legacy_marker_key = format!(
            "{SETTLED_CONSUMPTION_PREFIX}:{}:{}:{}",
            redeem.operation_id, yes_position_id, archived_cycle
        );

        for (key, bytes) in persisted_entries {
            if legacy_marker && key.starts_with(&new_marker_prefix) {
                rebuilt_cache.add(&legacy_marker_key, bytes).unwrap();
            } else {
                rebuilt_cache.add(&key, bytes).unwrap();
            }
        }

        if let Some(frames) = cache.borrow().position_snapshot_bytes(&yes_position_id) {
            for (index, bytes) in frames.into_iter().enumerate() {
                let key = format!(
                    "{}{}/{}",
                    nautilus_common::cache::CacheSnapshotRef::POSITION_PREFIX,
                    yes_position_id,
                    index
                );
                rebuilt_cache.add(&key, bytes::Bytes::from(bytes)).unwrap();
            }
        }
        let record = serde_json::json!({
            "operation_id": redeem.operation_id,
            "account_id": account,
            "application_status": "Pending",
            "venue_status": "Submitted",
            "barrier_instrument_ids": [yes.id()],
        });
        rebuilt_cache
            .add(
                &format!("position-operation:{}", redeem.operation_id),
                serde_json::to_vec(&record).unwrap().into(),
            )
            .unwrap();

        let rebuilt = Rc::new(RefCell::new(rebuilt_cache));
        let mut rebuilt_engine = ExecutionEngine::new(
            Rc::new(RefCell::new(VirtualClock::new())),
            rebuilt.clone(),
            None,
        );
        rebuilt_engine.restore_position_operation_barriers();
        assert_eq!(
            rebuilt_engine.position_operation_barrier(&yes.id()),
            Some(redeem.operation_id)
        );

        let applied_legs = if recorded_legs {
            vec![PositionOperationRecoveryLeg {
                position_id: yes_position_id,
                instrument_id: yes.id(),
                strategy_id: Some(strategy),
                direction: PositionOperationDirection::Close,
                quantity: Quantity::from("100"),
                cost: Decimal::ZERO,
                income: Some(dec!(100)),
                settled: true,
            }]
        } else {
            Vec::new()
        };
        let recovery = recovery_request(&redeem, redeem_effects, applied_legs);
        let outcome = rebuilt_engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        assert!(
            matches!(outcome, PositionOperationRecoveryOutcome::Applied { .. }),
            "old redeem recovery must complete from its archived cycle"
        );
        assert_eq!(
            rebuilt_engine.position_operation_barrier(&yes.id()),
            None,
            "recovery must release the barrier"
        );

        {
            let cache = rebuilt.borrow();
            let current = cache.position(&yes_position_id).unwrap();
            assert_eq!(
                settled_consumed(&cache, &current),
                Decimal::ZERO,
                "old redeem must not consume the new cycle"
            );
            let restored = cache.position_snapshots(Some(&yes_position_id), Some(&account));
            assert_eq!(restored.len(), 1);
            assert_eq!(
                settled_consumed(&cache, &restored[0]),
                dec!(100),
                "archived cycle must retain the original consumption"
            );
        }

        let outcome = rebuilt_engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        assert!(matches!(
            outcome,
            PositionOperationRecoveryOutcome::Applied { .. }
        ));
        let cache = rebuilt.borrow();
        let current = cache.position(&yes_position_id).unwrap();
        assert_eq!(settled_consumed(&cache, &current), Decimal::ZERO);
        let restored = cache.position_snapshots(Some(&yes_position_id), Some(&account));
        assert_eq!(settled_consumed(&cache, &restored[0]), dec!(100));
    }

    #[rstest]
    #[case(false, false)]
    #[case(true, false)]
    #[case(true, true)]
    fn test_recover_complete_multicycle_redeem_is_order_independent(
        #[case] recorded_legs: bool,
        #[case] reverse_legs: bool,
    ) {
        let no1 = option("856", "No");
        let no2 = option("858", "No");
        let yes = option("857", "Yes");
        let (mut engine, cache) = engine_with(&[no1.clone(), no2.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for instrument in [&no1, &no2] {
            let position = input_position(
                instrument,
                strategy,
                account,
                Quantity::from("100"),
                dec!(40),
            );
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }
        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();
        let yes_position_id = netting_position_id(&yes.id(), strategy);

        let first = result(
            PositionOperationType::Convert,
            vec![no1.id(), yes.id()],
            PositionOperationEffects {
                received: vec![amount("857", "100")],
                spent: vec![amount("856", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );
        engine.process_position_operation(&first);
        let mut second = result(
            PositionOperationType::Convert,
            vec![no2.id(), yes.id()],
            PositionOperationEffects {
                received: vec![amount("857", "100")],
                spent: vec![amount("858", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );
        second.ts_event = UnixNanos::from(2_000_000_000);
        second.ts_init = UnixNanos::from(2_000_000_001);
        engine.process_position_operation(&second);

        let archived = cache
            .borrow()
            .position_snapshots(Some(&yes_position_id), Some(&account))
            .first()
            .expect("first cycle must be archived")
            .clone();
        let archived_id = archived.id;
        let archived_cycle_id = settled_cycle_id(&archived);

        let redeem_effects = PositionOperationEffects {
            received: Vec::new(),
            spent: vec![amount("857", "200")],
            collateral_delta: "200".into(),
            payouts: Vec::new(),
        };
        let redeem = result(
            PositionOperationType::Redeem,
            vec![yes.id()],
            redeem_effects.clone(),
        );
        engine.process_position_operation(&redeem);

        let current = cache
            .borrow()
            .position(&yes_position_id)
            .expect("second cycle must exist")
            .cloned();
        let mut rebuilt_cache = Cache::default();
        rebuilt_cache.add_instrument(yes.clone()).unwrap();
        rebuilt_cache
            .add_position_without_order(&current, OmsType::Netting)
            .unwrap();
        let mut persisted_entries = cache
            .borrow()
            .general_entries_with_prefix("position-settlement-source:");
        persisted_entries.extend(
            cache
                .borrow()
                .general_entries_with_prefix("position-operation-settled:"),
        );

        for (key, bytes) in persisted_entries {
            rebuilt_cache.add(&key, bytes).unwrap();
        }

        if let Some(frames) = cache.borrow().position_snapshot_bytes(&yes_position_id) {
            for (index, bytes) in frames.into_iter().enumerate() {
                let key = format!(
                    "{}{}/{}",
                    nautilus_common::cache::CacheSnapshotRef::POSITION_PREFIX,
                    yes_position_id,
                    index
                );
                rebuilt_cache.add(&key, bytes::Bytes::from(bytes)).unwrap();
            }
        }
        let marker_prefix = format!(
            "{SETTLED_CONSUMPTION_PREFIX}:operation:{}:",
            redeem.operation_id
        );
        assert!(
            rebuilt_cache
                .general_entries_with_prefix(&marker_prefix)
                .first()
                .is_some_and(|(key, _)| key.ends_with(&archived_cycle_id)),
            "the archived cycle marker must sort before the current cycle marker"
        );
        let record = serde_json::json!({
            "operation_id": redeem.operation_id,
            "account_id": account,
            "application_status": "Pending",
            "venue_status": "Submitted",
            "barrier_instrument_ids": [yes.id()],
        });
        rebuilt_cache
            .add(
                &format!("position-operation:{}", redeem.operation_id),
                serde_json::to_vec(&record).unwrap().into(),
            )
            .unwrap();

        let rebuilt = Rc::new(RefCell::new(rebuilt_cache));
        let mut rebuilt_engine = ExecutionEngine::new(
            Rc::new(RefCell::new(VirtualClock::new())),
            rebuilt.clone(),
            None,
        );
        rebuilt_engine.restore_position_operation_barriers();

        let mut applied_legs = if recorded_legs {
            vec![
                PositionOperationRecoveryLeg {
                    position_id: yes_position_id,
                    instrument_id: yes.id(),
                    strategy_id: Some(strategy),
                    direction: PositionOperationDirection::Close,
                    quantity: Quantity::from("100"),
                    cost: Decimal::ZERO,
                    income: Some(dec!(100)),
                    settled: true,
                },
                PositionOperationRecoveryLeg {
                    position_id: archived_id,
                    instrument_id: yes.id(),
                    strategy_id: Some(strategy),
                    direction: PositionOperationDirection::Close,
                    quantity: Quantity::from("100"),
                    cost: Decimal::ZERO,
                    income: Some(dec!(100)),
                    settled: true,
                },
            ]
        } else {
            Vec::new()
        };

        if reverse_legs {
            applied_legs.reverse();
        }
        let recovery = recovery_request(&redeem, redeem_effects, applied_legs);
        let outcome = rebuilt_engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        assert!(
            matches!(outcome, PositionOperationRecoveryOutcome::Applied { .. }),
            "complete multi-cycle consumption must recover"
        );
        assert_eq!(rebuilt_engine.position_operation_barrier(&yes.id()), None);

        let before = {
            let cache = rebuilt.borrow();
            let current = cache.position(&yes_position_id).unwrap().cloned();
            let restored = cache.position_snapshots(Some(&yes_position_id), Some(&account));
            assert_eq!(settled_consumed(&cache, &current), dec!(100));
            assert_eq!(settled_consumed(&cache, &restored[0]), dec!(100));
            current
        };

        let outcome = rebuilt_engine
            .reconcile_recovered_position_operation(&recovery)
            .unwrap();
        assert!(matches!(
            outcome,
            PositionOperationRecoveryOutcome::Applied { .. }
        ));
        let cache = rebuilt.borrow();
        assert_eq!(
            cache.position(&yes_position_id).unwrap().cloned(),
            before,
            "repeated recovery must not change the position"
        );
        let current = cache.position(&yes_position_id).unwrap();
        assert_eq!(settled_consumed(&cache, &current), dec!(100));
        let restored = cache.position_snapshots(Some(&yes_position_id), Some(&account));
        assert_eq!(settled_consumed(&cache, &restored[0]), dec!(100));
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_recover_redeem_rejects_partial_multicycle_consumption(#[case] recorded_legs: bool) {
        let no1 = option("853", "No");
        let no2 = option("855", "No");
        let yes = option("854", "Yes");
        let (mut engine, cache) = engine_with(&[no1.clone(), no2.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");

        for instrument in [&no1, &no2] {
            let position = input_position(
                instrument,
                strategy,
                account,
                Quantity::from("100"),
                dec!(40),
            );
            cache
                .borrow_mut()
                .add_position_without_order(&position, OmsType::Netting)
                .unwrap();
        }
        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();
        let yes_position_id = netting_position_id(&yes.id(), strategy);

        let first = result(
            PositionOperationType::Convert,
            vec![no1.id(), yes.id()],
            PositionOperationEffects {
                received: vec![amount("854", "100")],
                spent: vec![amount("853", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );
        engine.process_position_operation(&first);
        let mut second = result(
            PositionOperationType::Convert,
            vec![no2.id(), yes.id()],
            PositionOperationEffects {
                received: vec![amount("854", "100")],
                spent: vec![amount("855", "100")],
                collateral_delta: "0".into(),
                payouts: Vec::new(),
            },
        );
        second.ts_event = UnixNanos::from(2_000_000_000);
        second.ts_init = UnixNanos::from(2_000_000_001);
        engine.process_position_operation(&second);

        let archived = cache
            .borrow()
            .position_snapshots(Some(&yes_position_id), Some(&account))
            .first()
            .expect("first cycle must be archived")
            .clone();
        let archived_id = archived.id;
        let archived_cycle_id = settled_cycle_id(&archived);

        let redeem_effects = PositionOperationEffects {
            received: Vec::new(),
            spent: vec![amount("854", "200")],
            collateral_delta: "200".into(),
            payouts: Vec::new(),
        };
        let redeem = result(
            PositionOperationType::Redeem,
            vec![yes.id()],
            redeem_effects.clone(),
        );
        engine.process_position_operation(&redeem);

        let current = cache
            .borrow()
            .position(&yes_position_id)
            .expect("second cycle must exist")
            .cloned();
        let mut rebuilt_cache = Cache::default();
        rebuilt_cache.add_instrument(yes.clone()).unwrap();
        rebuilt_cache
            .add_position_without_order(&current, OmsType::Netting)
            .unwrap();
        let mut persisted_entries = cache
            .borrow()
            .general_entries_with_prefix("position-settlement-source:");
        persisted_entries.extend(
            cache
                .borrow()
                .general_entries_with_prefix("position-operation-settled:"),
        );

        for (key, bytes) in persisted_entries {
            if key.starts_with("position-operation-settled:") && key.ends_with(&archived_cycle_id) {
                continue;
            }
            rebuilt_cache.add(&key, bytes).unwrap();
        }

        if let Some(frames) = cache.borrow().position_snapshot_bytes(&yes_position_id) {
            for (index, bytes) in frames.into_iter().enumerate() {
                let key = format!(
                    "{}{}/{}",
                    nautilus_common::cache::CacheSnapshotRef::POSITION_PREFIX,
                    yes_position_id,
                    index
                );
                rebuilt_cache.add(&key, bytes::Bytes::from(bytes)).unwrap();
            }
        }
        let record = serde_json::json!({
            "operation_id": redeem.operation_id,
            "account_id": account,
            "application_status": "Pending",
            "venue_status": "Submitted",
            "barrier_instrument_ids": [yes.id()],
        });
        rebuilt_cache
            .add(
                &format!("position-operation:{}", redeem.operation_id),
                serde_json::to_vec(&record).unwrap().into(),
            )
            .unwrap();

        let rebuilt = Rc::new(RefCell::new(rebuilt_cache));
        let mut rebuilt_engine = ExecutionEngine::new(
            Rc::new(RefCell::new(VirtualClock::new())),
            rebuilt.clone(),
            None,
        );
        rebuilt_engine.restore_position_operation_barriers();

        let applied_legs = if recorded_legs {
            vec![
                PositionOperationRecoveryLeg {
                    position_id: archived_id,
                    instrument_id: yes.id(),
                    strategy_id: Some(strategy),
                    direction: PositionOperationDirection::Close,
                    quantity: Quantity::from("100"),
                    cost: Decimal::ZERO,
                    income: Some(dec!(100)),
                    settled: true,
                },
                PositionOperationRecoveryLeg {
                    position_id: yes_position_id,
                    instrument_id: yes.id(),
                    strategy_id: Some(strategy),
                    direction: PositionOperationDirection::Close,
                    quantity: Quantity::from("100"),
                    cost: Decimal::ZERO,
                    income: Some(dec!(100)),
                    settled: true,
                },
            ]
        } else {
            Vec::new()
        };
        let recovery = recovery_request(&redeem, redeem_effects, applied_legs);
        let error = rebuilt_engine
            .reconcile_recovered_position_operation(&recovery)
            .expect_err("partial multi-cycle consumption evidence must remain unresolved")
            .to_string();
        assert!(
            error.contains("consumption") || error.contains("receipt"),
            "unexpected partial recovery error: {error}"
        );
        assert_eq!(
            rebuilt_engine.position_operation_barrier(&yes.id()),
            Some(redeem.operation_id),
            "an unresolved recovery must keep the barrier"
        );
        let cache = rebuilt.borrow();
        let current = cache.position(&yes_position_id).unwrap();
        assert_eq!(settled_consumed(&cache, &current), dec!(100));
        let restored = cache.position_snapshots(Some(&yes_position_id), Some(&account));
        assert_eq!(restored.len(), 1);
        assert_eq!(settled_consumed(&cache, &restored[0]), Decimal::ZERO);
    }

    #[rstest]
    fn test_barrier_allows_settled_convert_output_but_rejects_settled_input() {
        let no = option("790", "No");
        let yes = option("791", "Yes");
        let (engine, cache) = engine_with(&[no.clone(), yes.clone()]);
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&no, strategy, account, Quantity::from("100"), dec!(40));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();
        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();

        let inputs = BarrierInputs {
            strategy_id: strategy,
            account_id: account,
            quantity: Some(Quantity::from("100")),
            instrument_ids: vec![no.id()],
        };
        let operation_id = UUID4::new();
        engine
            .establish_full_barrier(
                operation_id,
                PositionOperationType::Convert,
                &[no.id(), yes.id()],
                Some(&inputs),
            )
            .unwrap();
        assert_eq!(
            engine.position_operation_barrier(&yes.id()),
            Some(operation_id)
        );
        engine.release_position_operation_barrier(operation_id, &[no.id(), yes.id()]);

        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                no.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();
        let error = engine
            .establish_full_barrier(
                UUID4::new(),
                PositionOperationType::Convert,
                &[no.id(), yes.id()],
                Some(&inputs),
            )
            .unwrap_err()
            .to_string();
        assert!(error.contains("economically settled"), "{error}");

        assert!(
            engine
                .establish_full_barrier(
                    UUID4::new(),
                    PositionOperationType::Redeem,
                    &[yes.id()],
                    None,
                )
                .is_ok()
        );
    }

    #[rstest]
    fn test_barrier_rejects_expired_output_without_settlement_evidence() {
        let no = option("792", "No");
        let yes = option("793", "Yes");
        let cache = Rc::new(RefCell::new(Cache::default()));
        for instrument in [&no, &yes] {
            cache
                .borrow_mut()
                .add_instrument(instrument.clone())
                .unwrap();
        }
        let strategy = StrategyId::from("STRATEGY-001");
        let account = AccountId::from("ACCOUNT-001");
        let position = input_position(&no, strategy, account, Quantity::from("100"), dec!(40));
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();

        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock.borrow_mut().advance_time(UnixNanos::from(5), true);
        let clock_dyn: Rc<RefCell<dyn nautilus_common::clock::Clock>> = clock;
        let engine = ExecutionEngine::new(clock_dyn, cache.clone(), None);
        let inputs = BarrierInputs {
            strategy_id: strategy,
            account_id: account,
            quantity: Some(Quantity::from("100")),
            instrument_ids: vec![no.id()],
        };

        let error = engine
            .establish_full_barrier(
                UUID4::new(),
                PositionOperationType::Convert,
                &[no.id(), yes.id()],
                Some(&inputs),
            )
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("expired without authoritative settlement evidence"),
            "{error}"
        );

        cache
            .borrow_mut()
            .add_instrument_close(InstrumentClose::new(
                yes.id(),
                Price::from("1"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::from(5),
                UnixNanos::from(5),
            ))
            .unwrap();
        engine
            .establish_full_barrier(
                UUID4::new(),
                PositionOperationType::Convert,
                &[no.id(), yes.id()],
                Some(&inputs),
            )
            .unwrap();
    }

    #[rstest]
    fn test_allocate_preserves_total_with_remainder() {
        let shares = allocate(dec!(100), &[dec!(1), dec!(1), dec!(1)]);
        assert_eq!(shares[0], dec!(33.333333333333333333333333333));
        assert_eq!(shares[0] + shares[1] + shares[2], dec!(100));
    }

    #[rstest]
    fn test_allocate_zero_weights_splits_evenly() {
        let shares = allocate(dec!(10), &[dec!(0), dec!(0)]);
        assert_eq!(shares, vec![dec!(5), dec!(5)]);
    }

    #[rstest]
    fn test_allocate_negative_total_keeps_sign() {
        let shares = allocate(dec!(-50), &[dec!(3), dec!(1)]);
        assert_eq!(shares[0], dec!(-37.5));
        assert_eq!(shares[1], dec!(-12.5));
        assert_eq!(shares[0] + shares[1], dec!(-50));
    }
}
