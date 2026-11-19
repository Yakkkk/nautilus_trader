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

use nautilus_common::messages::execution::{
    ConvertExecutionEvent, ConvertExecutionOutcome, ConvertExecutionStatus, ConvertOperationRecord,
    ConvertResult, ConvertTransaction, QueryConvert, SubmitConvert, VerifiedConvert,
    has_unique_inputs, normalized_inputs,
};
use nautilus_model::{enums::InstrumentCloseType, instruments::neg_risk::ConvertScope};

use super::{
    AccountId, ClientId, ExecutionClient, ExecutionEngine, HashSet, InstrumentId, UUID4, msgbus,
    switchboard,
};
use crate::engine::polymarket_convert::merge_convert_transaction;

impl ExecutionEngine {
    pub(super) fn handle_submit_convert(&self, command: &SubmitConvert) {
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

        match client.submit_convert(command.clone(), scope) {
            Ok(()) => {
                record.dispatched = true;
                record.reason =
                    Some("conversion dispatched; awaiting execution evidence".to_string());
                self.store_convert_operation(&record);
            }
            Err(e) => {
                record.status = Some(ConvertExecutionStatus::Rejected);
                record.reason = Some(format!("conversion dispatch failed: {e}"));
                self.store_convert_operation(&record);
                publish_result(&record.result());
            }
        }
    }

    pub(super) fn handle_query_convert(&self, command: QueryConvert) {
        let record = match self.load_convert_operation(command.operation_id) {
            Ok(record) => record,
            Err(e) => {
                log::error!(
                    "Cannot read Convert operation {}: {e}",
                    command.operation_id,
                );
                publish_query_rejection(
                    &command,
                    "cannot read the retained Convert operation".to_string(),
                );
                return;
            }
        };

        let Some(mut record) = record.filter(|record| record.is_owned_by(&command)) else {
            publish_query_rejection(&command, "unknown Convert operation".to_string());
            return;
        };

        if record.applied
            || record.historical_gap
            || matches!(
                record.status,
                Some(ConvertExecutionStatus::Rejected | ConvertExecutionStatus::Failed)
            )
        {
            publish_result(&record.result());
            return;
        }

        if record.status == Some(ConvertExecutionStatus::Confirmed)
            && self.all_affected_expired(&record.affected_instruments)
        {
            if record.unapplied_instruments.is_empty() {
                record.unapplied_instruments = record.affected_instruments.clone();
            }
            record.historical_gap = true;
            record.reason = Some(
                "all affected instruments formally expired; accounting retained as a historical gap"
                    .to_string(),
            );
            self.store_convert_operation(&record);
            publish_result(&record.result());
            return;
        }

        let Some(transaction) = record
            .transaction
            .as_ref()
            .filter(|transaction| transaction.has_original_locator())
        else {
            publish_result(&record.result());
            return;
        };

        let Some(client) = self.explicit_convert_client(command.client_id, command.account_id)
        else {
            let mut result = record.result();
            result.reason =
                Some("no matching execution client is available to query conversion".to_string());
            publish_result(&result);
            return;
        };

        if let Err(e) = client.query_convert(command, transaction) {
            log::warn!(
                "Convert query failed for operation {}: {e}",
                record.operation_id,
            );
            publish_result(&record.result());
        }
    }

    pub(super) fn handle_convert_event(&mut self, event: ConvertExecutionEvent) {
        let record = match self.load_convert_operation(event.operation_id) {
            Ok(record) => record,
            Err(e) => {
                log::error!("Cannot read Convert operation {}: {e}", event.operation_id);
                return;
            }
        };

        let Some(mut record) = record else {
            log::warn!(
                "Ignoring conversion evidence for unknown operation {}",
                event.operation_id,
            );
            return;
        };

        if record.account_id != event.account_id || record.client_id != event.client_id {
            log::error!(
                "Ignoring conversion evidence for operation {}: account or client does not match",
                event.operation_id,
            );
            return;
        }

        if !record.dispatched {
            log::error!(
                "Ignoring conversion evidence for operation {}: request was not dispatched",
                event.operation_id,
            );
            return;
        }

        if record.historical_gap {
            if let ConvertExecutionOutcome::Confirmed { verified, .. } = event.outcome {
                let mut verified = *verified;
                match self.validate_convert_confirmation(&record, &mut verified) {
                    Ok(()) => record.transaction = Some(verified.transaction),
                    Err(reason) => log::error!(
                        "Rejected conversion confirmation for operation {}: {reason}",
                        record.operation_id,
                    ),
                }
            }
            self.store_convert_operation(&record);
            publish_result(&record.result());
            return;
        }

        match event.outcome {
            ConvertExecutionOutcome::Rejected(reason) => {
                if !record.applied
                    && matches!(record.status, None | Some(ConvertExecutionStatus::Unknown))
                {
                    record.status = Some(ConvertExecutionStatus::Rejected);
                    record.reason = Some(reason);
                }
            }
            ConvertExecutionOutcome::Submitted(transaction) => {
                if transaction.relayer_transaction_id.is_none() {
                    if self.can_set_unknown(&record)
                        && let Ok(merged) = Self::merge_event_transaction(&record, &transaction)
                    {
                        record.transaction = Some(merged);
                        record.status = Some(ConvertExecutionStatus::Unknown);
                        record.reason =
                            Some("submitted evidence had no Relayer transaction ID".to_string());
                    }
                } else if !record.applied
                    && matches!(
                        record.status,
                        None | Some(
                            ConvertExecutionStatus::Unknown | ConvertExecutionStatus::Submitted
                        )
                    )
                    && let Some(merged) =
                        self.merge_event_transaction_or_ignore(&record, &transaction)
                {
                    record.transaction = Some(merged);
                    record.status = Some(ConvertExecutionStatus::Submitted);
                    record.reason = None;
                }
            }
            ConvertExecutionOutcome::Unknown {
                transaction,
                reason,
            } => {
                if self.can_set_unknown(&record) {
                    if let Some(transaction) = transaction {
                        let Some(merged) =
                            self.merge_event_transaction_or_ignore(&record, &transaction)
                        else {
                            self.store_convert_operation(&record);
                            publish_result(&record.result());
                            return;
                        };
                        record.transaction = Some(merged);
                    }

                    record.status = Some(ConvertExecutionStatus::Unknown);
                    record.reason = Some(reason);
                }
            }
            ConvertExecutionOutcome::Failed {
                transaction,
                reason,
            } => {
                if !record.applied && record.status != Some(ConvertExecutionStatus::Confirmed) {
                    if let Some(transaction) = transaction {
                        let Some(merged) =
                            self.merge_event_transaction_or_ignore(&record, &transaction)
                        else {
                            self.store_convert_operation(&record);
                            publish_result(&record.result());
                            return;
                        };
                        record.transaction = Some(merged);
                    }

                    record.status = Some(ConvertExecutionStatus::Failed);
                    record.reason = Some(reason);
                }
            }
            ConvertExecutionOutcome::Confirmed {
                verified,
                pending_inputs,
            } => {
                self.apply_convert_confirmation(&mut record, verified, &pending_inputs);
            }
        }

        self.store_convert_operation(&record);
        publish_result(&record.result());
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

        if record.applied {
            record.status = Some(ConvertExecutionStatus::Confirmed);
            record.transaction = Some(verified.transaction);
            return;
        }

        if self.all_affected_expired(&record.affected_instruments) {
            record.status = Some(ConvertExecutionStatus::Confirmed);
            record.transaction = Some(verified.transaction);
            record.applied = false;
            record.historical_gap = true;
            record.unapplied_instruments = record.affected_instruments.clone();
            record.reason = Some(
                "all affected instruments formally expired; accounting retained as a historical gap"
                    .to_string(),
            );
            return;
        }

        match self.apply_verified_convert(verified.clone(), pending_inputs) {
            Ok(core_record) => {
                record.status = Some(ConvertExecutionStatus::Confirmed);
                record.transaction = Some(core_record.verified.transaction.clone());

                if core_record.applied {
                    record.applied = true;
                    record.unapplied_instruments.clear();
                    record.reason = None;
                } else {
                    record.unapplied_instruments = core_record.unapplied_instruments;
                    record.reason = core_record.reason;
                }
            }
            Err(e) => {
                record.status = Some(ConvertExecutionStatus::Confirmed);
                record.transaction = Some(verified.transaction);
                record.unapplied_instruments = record.affected_instruments.clone();
                record.reason = Some(format!("Convert accounting failed: {e}"));
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

    fn all_affected_expired(&self, affected: &[InstrumentId]) -> bool {
        !affected.is_empty()
            && affected.iter().all(|instrument_id| {
                self.cache
                    .borrow()
                    .instrument_close(instrument_id)
                    .is_some_and(|close| close.close_type == InstrumentCloseType::ContractExpired)
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

    fn can_set_unknown(&self, record: &ConvertOperationRecord) -> bool {
        !record.applied && matches!(record.status, None | Some(ConvertExecutionStatus::Unknown))
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
        if let Err(e) = self.cache.borrow_mut().add_convert_operation(record) {
            log::error!(
                "Cannot retain Convert operation {}: {e}",
                record.operation_id,
            );
        }
    }
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
