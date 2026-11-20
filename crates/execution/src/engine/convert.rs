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

//! Dispatch and evidence handling for negative-risk conversions.

use nautilus_common::{
    clients::ConvertQueryDispatchError,
    messages::execution::{
        ConvertExecutionEvent, ConvertExecutionOutcome, ConvertExecutionStatus,
        ConvertOperationRecord, ConvertResult, ConvertTransaction, QueryConvert,
        SimulatedConvertExecutionEvent, SubmitConvert, VerifiedConvert, has_unique_inputs,
        normalized_inputs,
    },
};
use nautilus_model::{
    accounts::{Account, AccountAny},
    events::AccountState,
    instruments::neg_risk::ConvertScope,
};

use super::{
    AccountId, ClientId, ExecutionClient, ExecutionEngine, HashSet, InstrumentId,
    MessagingSwitchboard, UUID4, msgbus, switchboard,
};
use crate::engine::polymarket_convert::merge_convert_transaction;

const CONVERT_QUERY_MAX_ATTEMPTS: usize = 3;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum ConvertLiveRunState {
    #[default]
    Inactive,
    Active(UUID4),
    Stopped,
}

impl ExecutionEngine {
    /// Starts a live run identity that Convert clients echo on evidence.
    pub fn begin_convert_live_run(&mut self) {
        self.convert_live_run = ConvertLiveRunState::Active(UUID4::new());
    }

    /// Ends the live run identity so late Convert evidence is ignored.
    pub fn end_convert_live_run(&mut self) {
        self.convert_live_run = ConvertLiveRunState::Stopped;
    }

    pub(super) fn stop_convert_live_run_if_active(&mut self) {
        if matches!(self.convert_live_run, ConvertLiveRunState::Active(_)) {
            self.convert_live_run = ConvertLiveRunState::Stopped;
        }
    }

    /// Returns the current live run identity, if any.
    #[must_use]
    pub fn convert_live_run_id(&self) -> Option<UUID4> {
        match self.convert_live_run {
            ConvertLiveRunState::Active(run_id) => Some(run_id),
            ConvertLiveRunState::Inactive | ConvertLiveRunState::Stopped => None,
        }
    }

    pub(super) fn handle_submit_convert(&self, command: &SubmitConvert) {
        if self.convert_live_run == ConvertLiveRunState::Stopped {
            publish_rejection(
                command,
                "Convert submission is unavailable after the live run has stopped".to_string(),
            );
            return;
        }

        let mut record = match self.load_convert_operation(command.operation_id) {
            Ok(Some(record)) => record,
            Ok(None) => {
                let record = ConvertOperationRecord::rejected(
                    command,
                    "Convert request was not admitted by the risk engine".to_string(),
                );
                self.store_convert_operation(&record);
                publish_result(&record.result());
                return;
            }
            Err(e) => {
                log::error!(
                    "Cannot read Convert operation {}: {e}",
                    command.operation_id,
                );
                publish_rejection(
                    command,
                    format!("cannot read retained Convert operation: {e}"),
                );
                return;
            }
        };

        if !record.matches_request(command) {
            publish_rejection(
                command,
                format!(
                    "Convert operation {} conflicts with its original request",
                    command.operation_id,
                ),
            );
            return;
        }

        if record.status.is_some() || record.dispatched {
            publish_result(&record.result());
            return;
        }

        let Some(client) = self.explicit_convert_client(command.client_id, command.account_id)
        else {
            record.status = Some(ConvertExecutionStatus::Rejected);
            record.reason =
                Some("no matching execution client is available for conversion".to_string());
            self.store_convert_operation(&record);
            publish_result(&record.result());
            return;
        };

        let Some(scope) = self.record_scope(&record) else {
            record.status = Some(ConvertExecutionStatus::Rejected);
            record.reason =
                Some("conversion scope is not fixed or no longer available".to_string());
            self.store_convert_operation(&record);
            publish_result(&record.result());
            return;
        };

        record.dispatched = true;
        record.reason = Some("conversion dispatch is in progress".to_string());
        if let Err(e) = self.try_store_convert_operation(&record) {
            let reason = format!("cannot register conversion dispatch scope: {e}");
            record.dispatched = false;
            record.status = Some(ConvertExecutionStatus::Rejected);
            record.reason = Some(reason.clone());
            self.store_convert_operation(&record);
            publish_rejection(command, reason);
            return;
        }

        match client.submit_convert_for_run(command.clone(), scope, self.convert_live_run_id()) {
            Ok(()) => {
                record.reason =
                    Some("conversion dispatched; awaiting execution evidence".to_string());
                self.store_convert_operation(&record);
            }
            Err(e) => {
                record.dispatched = false;
                record.status = Some(ConvertExecutionStatus::Rejected);
                record.reason = Some(format!("conversion dispatch failed: {e}"));
                self.store_convert_operation(&record);
                publish_result(&record.result());
            }
        }
    }

    pub(super) fn handle_query_convert(&self, command: &QueryConvert) -> bool {
        if self.convert_live_run == ConvertLiveRunState::Stopped {
            publish_query_rejection(
                command,
                "Convert query is unavailable after the live run has stopped".to_string(),
            );
            return false;
        }

        let record = match self.load_convert_operation(command.operation_id) {
            Ok(record) => record,
            Err(e) => {
                log::error!(
                    "Cannot read Convert operation {}: {e}",
                    command.operation_id,
                );
                publish_query_rejection(
                    command,
                    "cannot read the retained Convert operation".to_string(),
                );
                return false;
            }
        };

        let Some(mut record) = record.filter(|record| record.is_owned_by(command)) else {
            publish_query_rejection(command, "unknown Convert operation".to_string());
            return false;
        };

        if record.applied
            || record.authorization_invalid
            || (record.status == Some(ConvertExecutionStatus::Rejected)
                && record.transaction.is_none())
        {
            publish_result(&record.result());
            return false;
        }

        if record.status == Some(ConvertExecutionStatus::Confirmed)
            && self
                .cache
                .borrow()
                .convert_scope_is_expired(&record.affected_instruments)
        {
            record.mark_historical_gap();
            self.store_convert_operation(&record);
            publish_result(&record.result());
            return false;
        }

        let Some(transaction) = record
            .transaction
            .as_ref()
            .filter(|transaction| transaction.has_original_locator())
        else {
            publish_result(&record.result());
            return false;
        };

        let Some(client) = self.explicit_convert_client(command.client_id, command.account_id)
        else {
            let mut result = record.result();
            result.reason =
                Some("no matching execution client is available to query conversion".to_string());
            publish_result(&result);
            return false;
        };

        for attempt in 1..=CONVERT_QUERY_MAX_ATTEMPTS {
            match client.query_convert_for_run(
                command.clone(),
                transaction,
                self.convert_live_run_id(),
            ) {
                Ok(()) => return true,
                Err(e) => {
                    let retryable = error_has_retryable_convert_query(&e);
                    if !retryable {
                        log::warn!(
                            "Convert query failed without retry for operation {}: {e}",
                            record.operation_id,
                        );
                        publish_result(&record.result());
                        return false;
                    }

                    log::warn!(
                        "Convert query attempt {attempt}/{CONVERT_QUERY_MAX_ATTEMPTS} failed for \
                         operation {}: {e}",
                        record.operation_id,
                    );
                }
            }
        }

        log::warn!(
            "Convert query budget exhausted for operation {} after \
             {CONVERT_QUERY_MAX_ATTEMPTS} attempts; recovery remains pending",
            record.operation_id,
        );
        publish_result(&record.result());
        false
    }

    /// Dispatches a bounded, read-only query for a retained conversion through the normal client
    /// route, retrying only explicitly retryable dispatch errors up to a fixed attempt budget.
    ///
    /// The client must emit any evidence as a [`ConvertExecutionEvent`]; this method does not
    /// create a new submission or apply a parallel result path.
    ///
    /// Clients must cancel outstanding query work on stop or reset, return an error only when the
    /// query was not dispatched, mark transient failures with [`ConvertQueryDispatchError::retryable`],
    /// and must not emit evidence for a stopped run.
    pub fn query_convert(&self, command: &QueryConvert) -> bool {
        self.handle_query_convert(command)
    }

    /// Recovers one retained conversion's accounting against its original fixed plan.
    ///
    /// The operation record is downgraded from Applied only when the retained plan comparison
    /// finds saved position changes missing or conflicting. Historical gaps are left untouched.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation record cannot be read or written, or the retained core
    /// plan cannot be decoded.
    pub fn recover_convert_operation(
        &mut self,
        operation_id: UUID4,
    ) -> anyhow::Result<ConvertOperationRecord> {
        let mut operation = self
            .load_convert_operation(operation_id)?
            .ok_or_else(|| anyhow::anyhow!("Convert operation {operation_id} is not retained"))?;

        if operation.historical_gap {
            return Ok(operation);
        }

        let was_applied = operation.applied;
        let was_confirmed = operation.status == Some(ConvertExecutionStatus::Confirmed);

        if operation.plan_lost || operation.recovery_blocked {
            if !operation.recovery_blocked
                && let Some(core_record) = self.convert_record(operation_id)?
            {
                let mut verified = core_record.verified;
                if self
                    .validate_convert_confirmation(&operation, &mut verified)
                    .is_ok()
                {
                    operation.status = Some(ConvertExecutionStatus::Confirmed);
                    operation.transaction = Some(verified.transaction);
                }
            }

            operation.applied = false;
            operation.unapplied_instruments = sorted_instruments(&operation.affected_instruments);
            if operation.reason.is_none() {
                operation.reason = Some(if operation.recovery_blocked {
                    "conflicting retained Convert evidence disables automatic recovery".to_string()
                } else {
                    "original Convert accounting plan is lost; recovery cannot reconstruct it"
                        .to_string()
                });
            }
            self.cache
                .borrow_mut()
                .restore_convert_operation_recovery(&operation)?;
            return Ok(operation);
        }

        let Some(core_record) = self.convert_record(operation_id)? else {
            let lost_plan = was_applied || was_confirmed || operation.plan_lost;
            if lost_plan {
                operation.plan_lost = true;
            }
            operation.applied = false;
            operation.unapplied_instruments = sorted_instruments(&operation.affected_instruments);
            if lost_plan {
                operation.reason = Some(
                    "missing original Convert verification and accounting plan; recovery cannot \
                     reconstruct it"
                        .to_string(),
                );
            } else if operation.reason.is_none() {
                operation.reason = Some(
                    "retained Convert execution remains pending without a recoverable accounting \
                     plan"
                        .to_string(),
                );
            }

            self.cache
                .borrow_mut()
                .restore_convert_operation_recovery(&operation)?;
            return Ok(operation);
        };

        let mut verified = core_record.verified;
        if let Err(reason) = self.validate_convert_confirmation(&operation, &mut verified) {
            let conflict = format!(
                "conflict: recovered Convert plan did not match the retained operation: {reason}"
            );
            operation.recovery_blocked = true;
            operation.applied = false;
            operation.unapplied_instruments = sorted_instruments(&operation.affected_instruments);

            operation.reason = Some(conflict);

            self.cache
                .borrow_mut()
                .restore_convert_operation_recovery(&operation)?;
            return Ok(operation);
        }

        operation.status = Some(ConvertExecutionStatus::Confirmed);
        operation.transaction = Some(verified.transaction);

        let recovered = self.recover_convert(operation_id, &[])?;

        operation.applied = recovered.applied;
        let lost_plan = recovered.plan.is_none() && (was_applied || recovered.plan_fixed);
        operation.plan_lost |= lost_plan;
        operation.unapplied_instruments = recovered.unapplied_instruments;
        operation.reason = if lost_plan {
            Some(
                "original Convert accounting plan is missing; recovery cannot reconstruct it"
                    .to_string(),
            )
        } else {
            recovered.reason
        };

        self.cache
            .borrow_mut()
            .restore_convert_operation_recovery(&operation)?;
        Ok(operation)
    }

    pub(super) fn handle_convert_event(&mut self, event: ConvertExecutionEvent) {
        match self.convert_live_run {
            ConvertLiveRunState::Inactive => {
                if event.run_id.is_some() {
                    log::debug!("Ignoring conversion evidence emitted outside a live run");
                    return;
                }
            }
            ConvertLiveRunState::Active(run_id) => {
                if event.run_id != Some(run_id) {
                    log::debug!(
                        "Ignoring conversion evidence from stale live run {:?}",
                        event.run_id,
                    );
                    return;
                }
            }
            ConvertLiveRunState::Stopped => {
                log::debug!(
                    "Ignoring conversion evidence after the live run stopped: {:?}",
                    event.run_id,
                );
                return;
            }
        }

        let Some(record) = self.owned_convert_record(&event) else {
            return;
        };
        self.handle_convert_event_with_record(event, record);
    }

    fn handle_convert_event_with_record(
        &mut self,
        event: ConvertExecutionEvent,
        mut record: ConvertOperationRecord,
    ) {
        if convert_evidence_is_terminal(&record) {
            log::debug!(
                "Ignoring contradictory conversion evidence for terminal operation {}",
                record.operation_id,
            );
            publish_result(&record.result());
            return;
        }

        self.apply_convert_evidence(&mut record, event.outcome);

        self.store_convert_operation(&record);
        publish_result(&record.result());
    }

    fn apply_convert_evidence(
        &mut self,
        record: &mut ConvertOperationRecord,
        outcome: ConvertExecutionOutcome,
    ) {
        match outcome {
            ConvertExecutionOutcome::Rejected(reason) => {
                if !record.applied && record.status.is_none() && record.transaction.is_none() {
                    record.status = Some(ConvertExecutionStatus::Rejected);
                    if !record.historical_gap {
                        record.reason = Some(reason);
                    }
                }
            }
            ConvertExecutionOutcome::Submitted(transaction) => {
                self.apply_convert_submitted_evidence(record, &transaction);
            }
            ConvertExecutionOutcome::Unknown {
                transaction,
                reason,
            } => {
                self.apply_convert_unknown_evidence(record, transaction, reason);
            }
            ConvertExecutionOutcome::Failed {
                transaction,
                reason,
            } => {
                self.apply_convert_failed_evidence(record, transaction, reason, false);
            }
            ConvertExecutionOutcome::AuthorizationInvalid {
                transaction,
                reason,
            } => {
                self.apply_convert_failed_evidence(record, transaction, reason, true);
            }
            ConvertExecutionOutcome::Confirmed {
                verified,
                pending_inputs,
            } => {
                self.apply_convert_confirmation(record, verified, &pending_inputs);
            }
        }
    }

    fn apply_convert_submitted_evidence(
        &self,
        record: &mut ConvertOperationRecord,
        transaction: &ConvertTransaction,
    ) {
        if record.applied || record.status == Some(ConvertExecutionStatus::Confirmed) {
            return;
        }

        let eligible = if transaction.relayer_transaction_id.is_none() {
            matches!(record.status, None | Some(ConvertExecutionStatus::Unknown))
        } else {
            matches!(
                record.status,
                None | Some(ConvertExecutionStatus::Unknown | ConvertExecutionStatus::Submitted)
            )
        };

        if !eligible {
            return;
        }

        let Some(merged) = self.merge_event_transaction_or_ignore(record, transaction) else {
            return;
        };

        if transaction.relayer_transaction_id.is_none() {
            record.status = Some(ConvertExecutionStatus::Unknown);
            if !record.historical_gap {
                record.reason =
                    Some("submitted evidence had no Relayer transaction ID".to_string());
            }
        } else {
            record.status = Some(ConvertExecutionStatus::Submitted);
            if !record.historical_gap {
                record.reason = None;
            }
        }

        record.transaction = Some(merged);
    }

    fn apply_convert_unknown_evidence(
        &self,
        record: &mut ConvertOperationRecord,
        transaction: Option<ConvertTransaction>,
        reason: String,
    ) {
        if !self.can_apply_unknown_evidence(record) {
            return;
        }

        let was_submitted = record.status == Some(ConvertExecutionStatus::Submitted);
        if let Some(transaction) = transaction {
            let Some(merged) = self.merge_event_transaction_or_ignore(record, &transaction) else {
                return;
            };
            record.transaction = Some(merged);
        }

        if !was_submitted {
            record.status = Some(ConvertExecutionStatus::Unknown);
        }

        if !record.historical_gap {
            record.reason = Some(reason);
        }
    }

    fn apply_convert_failed_evidence(
        &self,
        record: &mut ConvertOperationRecord,
        transaction: Option<ConvertTransaction>,
        reason: String,
        authorization_invalid: bool,
    ) {
        if record.applied || record.status == Some(ConvertExecutionStatus::Confirmed) {
            return;
        }

        if let Some(transaction) = transaction {
            let Some(merged) = self.merge_event_transaction_or_ignore(record, &transaction) else {
                return;
            };
            record.transaction = Some(merged);
        }

        record.status = Some(ConvertExecutionStatus::Failed);
        record.authorization_invalid |= authorization_invalid;
        if !record.historical_gap {
            record.reason = Some(reason);
        }
    }

    fn owned_convert_record(
        &self,
        event: &ConvertExecutionEvent,
    ) -> Option<ConvertOperationRecord> {
        let record = match self.load_convert_operation(event.operation_id) {
            Ok(Some(record)) => record,
            Ok(None) => {
                log::warn!(
                    "Ignoring conversion evidence for unknown operation {}",
                    event.operation_id,
                );
                return None;
            }
            Err(e) => {
                log::error!("Cannot read Convert operation {}: {e}", event.operation_id);
                return None;
            }
        };

        if record.account_id != event.account_id || record.client_id != event.client_id {
            log::error!(
                "Ignoring conversion evidence for operation {}: account or client does not match",
                event.operation_id,
            );
            return None;
        }

        if !record.dispatched {
            log::error!(
                "Ignoring conversion evidence for operation {}: request was not dispatched",
                event.operation_id,
            );
            return None;
        }

        Some(record)
    }

    pub(super) fn handle_simulated_convert_event(
        &mut self,
        simulation: SimulatedConvertExecutionEvent,
    ) {
        if !simulation.is_current() || self.convert_live_run == ConvertLiveRunState::Stopped {
            log::debug!(
                "Dropping stale simulated Convert outcome {}",
                simulation.event.operation_id,
            );
            return;
        }

        let apply_cash = simulation.apply_cash;
        let event = simulation.event;
        let Some(mut record) = self.owned_convert_record(&event) else {
            return;
        };

        if matches!(
            record.status,
            Some(ConvertExecutionStatus::Rejected | ConvertExecutionStatus::Failed)
        ) {
            publish_result(&record.result());
            return;
        }

        let ConvertExecutionEvent {
            operation_id,
            account_id,
            client_id,
            outcome,
            run_id,
        } = event;

        let ConvertExecutionOutcome::Confirmed {
            verified,
            pending_inputs,
        } = outcome
        else {
            let event = ConvertExecutionEvent {
                operation_id,
                account_id,
                client_id,
                outcome,
                run_id,
            };
            self.handle_convert_event_with_record(event, record);
            return;
        };
        let mut verified = *verified;

        if let Err(reason) = self.validate_convert_confirmation(&record, &mut verified) {
            log::error!(
                "Rejected simulated conversion confirmation for operation {operation_id}: {reason}",
            );
            self.reject_simulated_convert(
                record,
                format!("simulated conversion confirmation failed: {reason}"),
            );
            return;
        }

        if apply_cash && !record.simulated_cash_credited {
            if let Err(reason) = self.credit_simulated_convert_cash(&verified) {
                log::error!(
                    "Cannot apply simulated conversion cash for operation {operation_id}: {reason}",
                );
                self.reject_simulated_convert(
                    record,
                    format!("cannot apply simulated conversion cash: {reason}"),
                );
                return;
            }

            record.simulated_cash_credited = true;
            self.store_convert_operation(&record);
            log::debug!("Applied simulated conversion cash for operation {operation_id}");
        }

        self.apply_validated_convert_confirmation(&mut record, verified, &pending_inputs);
        self.store_convert_operation(&record);
        publish_result(&record.result());
    }

    fn reject_simulated_convert(&self, mut record: ConvertOperationRecord, reason: String) {
        if record.status == Some(ConvertExecutionStatus::Confirmed) {
            publish_result(&record.result());
            return;
        }

        if !record.applied {
            record.status = Some(ConvertExecutionStatus::Rejected);
            record.reason = Some(reason);
        }
        self.store_convert_operation(&record);
        publish_result(&record.result());
    }

    fn credit_simulated_convert_cash(&self, verified: &VerifiedConvert) -> Result<(), String> {
        if verified.net_cash.is_zero() {
            return Ok(());
        }

        let state = self.simulated_convert_cash_state(verified)?;
        let event_id = state.event_id;
        msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state);

        let applied = self
            .cache
            .borrow()
            .account(&verified.account_id)
            .and_then(|account| account.last_event())
            .is_some_and(|event| event.event_id == event_id);
        if !applied {
            return Err("conversion account state was not applied".to_string());
        }

        Ok(())
    }

    fn simulated_convert_cash_state(
        &self,
        verified: &VerifiedConvert,
    ) -> Result<AccountState, String> {
        let net_cash = verified.net_cash;
        let (balances, margins, account_type, base_currency) = {
            let cache = self.cache.borrow();
            let account = cache.account(&verified.account_id).ok_or_else(|| {
                format!("conversion account {} was not found", verified.account_id)
            })?;
            let balance = account
                .balance(Some(net_cash.currency))
                .copied()
                .ok_or_else(|| {
                    format!(
                        "conversion account {} has no {} balance",
                        verified.account_id, net_cash.currency
                    )
                })?;
            let total = balance.total.checked_add(net_cash).ok_or_else(|| {
                format!("conversion cash total overflow for {}", net_cash.currency)
            })?;
            let free = balance.free.checked_add(net_cash).ok_or_else(|| {
                format!(
                    "conversion cash free-balance overflow for {}",
                    net_cash.currency
                )
            })?;
            let mut updated_balance = balance;
            updated_balance.total = total;
            updated_balance.free = free;

            let margins = match &*account {
                AccountAny::Margin(margin_account) => margin_account
                    .margins
                    .values()
                    .chain(margin_account.account_margins.values())
                    .copied()
                    .collect(),
                _ => Vec::new(),
            };

            let mut balances = account.balances();
            balances.insert(net_cash.currency, updated_balance);

            (
                balances.into_values().collect(),
                margins,
                account.account_type(),
                account.base_currency(),
            )
        };

        let ts_now = self.clock.borrow().timestamp_ns();
        Ok(AccountState::new(
            verified.account_id,
            account_type,
            balances,
            margins,
            false,
            UUID4::new(),
            ts_now,
            ts_now,
            base_currency,
        ))
    }

    fn apply_convert_confirmation(
        &mut self,
        record: &mut ConvertOperationRecord,
        verified: Box<VerifiedConvert>,
        pending_inputs: &[InstrumentId],
    ) {
        let mut verified = *verified;

        if let Err(reason) = self.validate_convert_confirmation(record, &mut verified) {
            log::error!(
                "Rejected conversion confirmation for operation {}: {reason}",
                record.operation_id,
            );
            return;
        }

        self.apply_validated_convert_confirmation(record, verified, pending_inputs);
    }

    fn apply_validated_convert_confirmation(
        &mut self,
        record: &mut ConvertOperationRecord,
        verified: VerifiedConvert,
        pending_inputs: &[InstrumentId],
    ) {
        if record.historical_gap {
            record.status = Some(ConvertExecutionStatus::Confirmed);
            record.transaction = Some(verified.transaction);
            return;
        }

        if record.recovery_blocked || record.plan_lost {
            record.status = Some(ConvertExecutionStatus::Confirmed);
            record.transaction = Some(verified.transaction);
            record.applied = false;
            record.unapplied_instruments = record.affected_instruments.clone();
            if record.reason.is_none() {
                record.reason = Some(if record.recovery_blocked {
                    "conflicting retained Convert evidence disables automatic recovery".to_string()
                } else {
                    "original Convert accounting plan is lost; execution evidence retained \
                     without rebuilding accounting from current costs"
                        .to_string()
                });
            }

            if self
                .cache
                .borrow()
                .convert_scope_is_expired(&record.affected_instruments)
            {
                record.mark_historical_gap();
            }
            return;
        }

        if record.applied {
            record.status = Some(ConvertExecutionStatus::Confirmed);
            record.transaction = Some(verified.transaction);
            return;
        }

        let expired = self
            .cache
            .borrow()
            .convert_scope_is_expired(&record.affected_instruments);

        match self.apply_verified_convert(verified.clone(), pending_inputs) {
            Ok(core_record) => {
                record.status = Some(ConvertExecutionStatus::Confirmed);
                record.transaction = Some(core_record.verified.transaction.clone());

                if core_record.applied {
                    record.applied = true;
                    record.unapplied_instruments.clear();
                    record.reason = None;
                } else {
                    record.applied = false;
                    record.unapplied_instruments = core_record.unapplied_instruments;
                    record.reason = core_record.reason;
                    if expired {
                        record.mark_historical_gap();
                    }
                }
            }
            Err(e) => {
                record.status = Some(ConvertExecutionStatus::Confirmed);
                record.transaction = Some(verified.transaction);
                record.applied = false;
                record.unapplied_instruments = record.affected_instruments.clone();
                record.reason = Some(format!("Convert accounting failed: {e}"));
                if expired {
                    record.mark_historical_gap();
                }
            }
        }
    }

    fn validate_convert_confirmation(
        &self,
        record: &ConvertOperationRecord,
        verified: &mut VerifiedConvert,
    ) -> Result<(), String> {
        if verified.operation_id != record.operation_id
            || verified.trader_id != record.trader_id
            || verified.strategy_id != record.strategy_id
            || verified.account_id != record.account_id
        {
            return Err("operation or ownership does not match the original request".to_string());
        }

        if !has_unique_inputs(&verified.inputs)
            || normalized_inputs(&record.inputs) != normalized_inputs(&verified.inputs)
        {
            return Err("verified inputs do not match the original request".to_string());
        }

        let affected = sorted_instruments(&record.affected_instruments);
        let confirmed_affected = sorted_instruments(&verified.affected_instruments);
        if affected.is_empty() || affected != confirmed_affected {
            return Err("verified effects do not cover the complete admitted scope".to_string());
        }

        let input_set: HashSet<_> = verified
            .inputs
            .iter()
            .map(|input| input.instrument_id)
            .collect();
        let output_scope: HashSet<_> = record
            .affected_instruments
            .iter()
            .filter(|instrument_id| !input_set.contains(instrument_id))
            .copied()
            .collect();
        let mut output_legs = HashSet::with_capacity(verified.outputs.len());

        for output in &verified.outputs {
            if input_set.contains(&output.instrument_id) {
                return Err(format!(
                    "verified output {} was an admitted conversion input",
                    output.instrument_id,
                ));
            }

            if !output_scope.contains(&output.instrument_id) {
                return Err(format!(
                    "verified output {} was outside the admitted output scope",
                    output.instrument_id,
                ));
            }

            if output.quantity.is_sign_negative() {
                return Err(format!(
                    "verified output {} had a negative quantity",
                    output.instrument_id,
                ));
            }

            if !output_legs.insert(output.instrument_id) {
                return Err(format!(
                    "verified output {} was duplicated",
                    output.instrument_id,
                ));
            }
        }

        if let Some(original) = record.transaction.as_ref() {
            verified.transaction = merge_convert_transaction(original, &verified.transaction)?;
        }

        if !verified.transaction.has_original_locator() {
            return Err("confirmation has no original transaction locator".to_string());
        }

        Ok(())
    }

    fn record_scope(&self, record: &ConvertOperationRecord) -> Option<ConvertScope> {
        if record.affected_instruments.is_empty() {
            return None;
        }

        let inputs: Vec<_> = record
            .inputs
            .iter()
            .map(|input| input.instrument_id)
            .collect();
        let input_set: HashSet<_> = inputs.iter().copied().collect();

        if input_set.len() != inputs.len() {
            return None;
        }

        let scope_inputs = record
            .affected_instruments
            .iter()
            .filter(|instrument_id| input_set.contains(instrument_id))
            .copied()
            .collect::<Vec<_>>();

        if scope_inputs.len() != inputs.len() {
            return None;
        }

        let scope_outputs = record
            .affected_instruments
            .iter()
            .filter(|instrument_id| !input_set.contains(instrument_id))
            .copied()
            .collect();

        Some(ConvertScope {
            inputs: scope_inputs,
            outputs: scope_outputs,
        })
    }

    fn explicit_convert_client(
        &self,
        client_id: ClientId,
        account_id: AccountId,
    ) -> Option<&dyn ExecutionClient> {
        self.clients
            .get(&client_id)
            .map(|adapter| adapter.client.as_ref())
            .filter(|client| client.client_id() == client_id && client.account_id() == account_id)
    }

    fn can_apply_unknown_evidence(&self, record: &ConvertOperationRecord) -> bool {
        !record.applied
            && matches!(
                record.status,
                None | Some(ConvertExecutionStatus::Unknown | ConvertExecutionStatus::Submitted)
            )
    }

    fn merge_event_transaction(
        record: &ConvertOperationRecord,
        incoming: &ConvertTransaction,
    ) -> Result<ConvertTransaction, String> {
        match record.transaction.as_ref() {
            Some(original) => merge_convert_transaction(original, incoming),
            None => Ok(incoming.clone()),
        }
    }

    fn merge_event_transaction_or_ignore(
        &self,
        record: &ConvertOperationRecord,
        incoming: &ConvertTransaction,
    ) -> Option<ConvertTransaction> {
        match Self::merge_event_transaction(record, incoming) {
            Ok(transaction) => Some(transaction),
            Err(reason) => {
                log::warn!(
                    "Ignoring conflicting conversion evidence for operation {}: {reason}",
                    record.operation_id,
                );
                None
            }
        }
    }

    fn load_convert_operation(
        &self,
        operation_id: UUID4,
    ) -> anyhow::Result<Option<ConvertOperationRecord>> {
        self.cache.borrow().load_convert_operation(operation_id)
    }

    fn store_convert_operation(&self, record: &ConvertOperationRecord) {
        if let Err(e) = self.try_store_convert_operation(record) {
            log::error!(
                "Cannot retain Convert operation {}: {e}",
                record.operation_id,
            );
        }
    }

    fn try_store_convert_operation(&self, record: &ConvertOperationRecord) -> anyhow::Result<()> {
        self.cache.borrow_mut().add_convert_operation(record)
    }
}

fn convert_evidence_is_terminal(record: &ConvertOperationRecord) -> bool {
    record.authorization_invalid
        || (record.status == Some(ConvertExecutionStatus::Rejected) && record.transaction.is_none())
}

fn error_has_retryable_convert_query(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<ConvertQueryDispatchError>()
            .is_some_and(ConvertQueryDispatchError::is_retryable)
    })
}

fn sorted_instruments(instruments: &[InstrumentId]) -> Vec<InstrumentId> {
    let mut instruments = instruments.to_vec();
    instruments.sort_unstable();
    instruments.dedup();
    instruments
}

fn publish_rejection(command: &SubmitConvert, reason: String) {
    publish_result(&ConvertResult::rejected(
        command.operation_id,
        command.trader_id,
        command.strategy_id,
        command.account_id,
        command.client_id,
        reason,
    ));
}

fn publish_query_rejection(command: &QueryConvert, reason: String) {
    publish_result(&ConvertResult::rejected(
        command.operation_id,
        command.trader_id,
        command.strategy_id,
        command.account_id,
        command.client_id,
        reason,
    ));
}

fn publish_result(result: &ConvertResult) {
    let topic = switchboard::get_event_convert_topic(result.strategy_id);
    msgbus::publish_any(topic, result);
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{cache::Cache, clock::VirtualClock};
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        accounts::{AccountAny, MarginAccount},
        enums::AccountType,
        events::AccountState,
        identifiers::{StrategyId, TraderId},
        types::{AccountBalance, Currency, MarginBalance, Money},
    };
    use rust_decimal_macros::dec;

    use super::*;

    #[rstest::rstest]
    fn simulated_cash_state_preserves_other_balances_and_live_margin_maps() {
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let account_id = AccountId::from("POLYMARKET-001");
        let usd = Currency::USD();
        let eur = Currency::EUR();
        let instrument_id = InstrumentId::from("TEST.POLYMARKET");
        let balances = vec![
            AccountBalance::new(
                Money::from("100 USD"),
                Money::zero(usd),
                Money::from("100 USD"),
            ),
            AccountBalance::new(
                Money::from("50 EUR"),
                Money::zero(eur),
                Money::from("50 EUR"),
            ),
        ];
        let margins = vec![
            MarginBalance::new_checked(
                Money::from("5 USD"),
                Money::from("2 USD"),
                Some(instrument_id),
            )
            .unwrap(),
            MarginBalance::new_checked(Money::from("7 USD"), Money::from("3 USD"), None).unwrap(),
        ];
        let state = AccountState::new(
            account_id,
            AccountType::Margin,
            balances,
            margins,
            true,
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
            Some(usd),
        );
        let mut margin_account = MarginAccount::new(state, true);
        let live_instrument_margin = MarginBalance::new_checked(
            Money::from("9 USD"),
            Money::from("4 USD"),
            Some(instrument_id),
        )
        .unwrap();
        let live_account_margin =
            MarginBalance::new_checked(Money::from("11 USD"), Money::from("6 USD"), None).unwrap();
        *margin_account.margins.get_mut(&instrument_id).unwrap() = live_instrument_margin;
        *margin_account.account_margins.get_mut(&usd).unwrap() = live_account_margin;
        cache
            .borrow_mut()
            .add_account(AccountAny::Margin(margin_account))
            .unwrap();

        let engine = ExecutionEngine::new(clock, Rc::clone(&cache), None);
        let verified = VerifiedConvert {
            operation_id: UUID4::new(),
            trader_id: TraderId::from("TRADER-001"),
            strategy_id: StrategyId::from("CONVERT-001"),
            account_id,
            transaction: ConvertTransaction {
                wallet: "test-wallet".to_string(),
                relayer_transaction_id: Some("sim-operation".to_string()),
                transaction_hash: None,
                nonce: None,
                call_summary: None,
            },
            inputs: Vec::new(),
            outputs: Vec::new(),
            affected_instruments: Vec::new(),
            net_cash: Money::from_decimal_exact(dec!(10), usd).unwrap(),
            ts_event: UnixNanos::default(),
        };

        let state = engine.simulated_convert_cash_state(&verified).unwrap();
        assert_eq!(state.account_id, account_id);
        assert_eq!(state.account_type, AccountType::Margin);
        assert_eq!(state.base_currency, Some(usd));
        assert_eq!(
            state
                .balances
                .iter()
                .find(|balance| balance.currency == usd)
                .unwrap()
                .total,
            Money::from("110 USD"),
        );
        assert_eq!(
            state
                .balances
                .iter()
                .find(|balance| balance.currency == eur)
                .unwrap()
                .total,
            Money::from("50 EUR"),
        );
        assert_ne!(
            cache
                .borrow()
                .account(&account_id)
                .and_then(|account| account.last_event())
                .map(|event| event.margins)
                .unwrap(),
            state.margins,
        );
        assert_eq!(
            state.margins,
            vec![live_instrument_margin, live_account_margin],
        );
    }
}
