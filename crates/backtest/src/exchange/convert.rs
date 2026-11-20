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

//! Polymarket Convert execution for the simulated venue.

use std::rc::Rc;

use nautilus_common::messages::execution::{
    ConvertExecutionEvent, ConvertExecutionOutcome, ConvertTransaction, SubmitConvert,
    TradingCommand, VerifiedConvert,
};
use nautilus_core::UUID4;
use nautilus_execution::engine::polymarket_convert::{
    calculate_convert_effect, convert_position_id, validate_simulated_convert_effect,
};
use nautilus_model::{instruments::neg_risk::ConvertScope, types::Money};
use rust_decimal::Decimal;

use super::{DeferEventsGuard, ExchangeCommand, InflightCommand, SimulatedExchange};

impl SimulatedExchange {
    pub(crate) fn send_convert(
        &mut self,
        command: SubmitConvert,
        scope: ConvertScope,
    ) -> anyhow::Result<()> {
        if self.callback_error.is_some() {
            anyhow::bail!("simulated exchange has halted");
        }

        if self.convert_event_handler.is_none() {
            anyhow::bail!("simulated exchange has no deferred conversion event handler");
        }

        if !self.use_message_queue {
            let _guard = DeferEventsGuard::new(Rc::clone(&self.deferring_events));
            self.execute_convert(&command, &scope);
            return Ok(());
        }

        if self.latency_model.is_none() {
            self.message_queue
                .push_back(ExchangeCommand::Convert(command, scope));
            return Ok(());
        }

        let convert = TradingCommand::SubmitConvert(command.clone());
        let (timestamp, counter) = self.generate_inflight_command(&convert)?;
        self.inflight_queue.push(InflightCommand::new(
            timestamp,
            counter,
            ExchangeCommand::Convert(command, scope),
        ));
        Ok(())
    }

    pub(super) fn execute_convert(&mut self, command: &SubmitConvert, scope: &ConvertScope) {
        match self.apply_simulated_convert(command, scope) {
            Ok(verified) => {
                let event = ConvertExecutionEvent {
                    operation_id: command.operation_id,
                    account_id: command.account_id,
                    client_id: command.client_id,
                    run_id: None,
                    outcome: ConvertExecutionOutcome::Confirmed {
                        verified: Box::new(verified),
                        pending_inputs: Vec::new(),
                    },
                };
                self.dispatch_convert_event(event);
            }
            Err(reason) => {
                let event = ConvertExecutionEvent {
                    operation_id: command.operation_id,
                    account_id: command.account_id,
                    client_id: command.client_id,
                    run_id: None,
                    outcome: ConvertExecutionOutcome::Rejected(reason),
                };
                self.dispatch_convert_event(event);
            }
        }
    }

    fn apply_simulated_convert(
        &mut self,
        command: &SubmitConvert,
        scope: &ConvertScope,
    ) -> Result<VerifiedConvert, String> {
        let client_account_id = self.exec_client.as_ref().map(|client| client.account_id());
        if client_account_id != Some(command.account_id) {
            return Err(format!(
                "simulated exchange has no execution client for account {}",
                command.account_id,
            ));
        }

        let first_input = command
            .inputs
            .first()
            .ok_or_else(|| "a conversion requires at least one NO input".to_string())?;
        let event = {
            let cache = self.cache.borrow();
            cache
                .convert_event_for_instrument(&first_input.instrument_id)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "no complete negative-risk event mapping for input {}",
                        first_input.instrument_id,
                    )
                })?
        };
        let effect = calculate_convert_effect(&event, &command.inputs)?;
        let currency = {
            let cache = self.cache.borrow();
            validate_simulated_convert_effect(&cache, command, scope, &effect)?
        };

        let net_cash = Money::from_decimal_exact(effect.net_cash, currency)
            .map_err(|e| format!("conversion net cash cannot be represented exactly: {e}"))?;

        for output in &effect.outputs {
            let position_id = convert_position_id(output.instrument_id, command.strategy_id);
            let matching_engine = self
                .matching_engines
                .get_mut(&output.instrument_id)
                .ok_or_else(|| {
                    format!(
                        "no matching engine for conversion output {}",
                        output.instrument_id,
                    )
                })?;
            let actual = matching_engine
                .available_position_quantity(position_id)
                .map_err(|e| {
                    format!(
                        "cannot read simulated inventory for conversion output {}: {e}",
                        output.instrument_id,
                    )
                })?;

            if actual < Decimal::ZERO {
                return Err(format!(
                    "conversion output {} would be short after pending fills: signed quantity {actual}",
                    output.instrument_id,
                ));
            }
        }

        for input in &effect.inputs {
            let position_id = convert_position_id(input.instrument_id, command.strategy_id);
            let matching_engine = self
                .matching_engines
                .get_mut(&input.instrument_id)
                .ok_or_else(|| {
                    format!(
                        "no matching engine for conversion input {}",
                        input.instrument_id,
                    )
                })?;

            if matching_engine.has_unapplied_fills_for_position(position_id) {
                return Err(format!(
                    "conversion input {} has fill activity which the core has not applied",
                    input.instrument_id,
                ));
            }

            let available = matching_engine
                .available_position_quantity(position_id)
                .map_err(|e| {
                    format!(
                        "cannot read simulated inventory for conversion input {}: {e}",
                        input.instrument_id,
                    )
                })?;
            let committed = matching_engine
                .arrived_sell_remaining(command.strategy_id, command.account_id)
                .map_err(|e| {
                    format!(
                        "cannot read committed sell inventory for conversion input {}: {e}",
                        input.instrument_id,
                    )
                })?;
            let required = committed.checked_add(input.quantity).ok_or_else(|| {
                format!(
                    "conversion inventory requirement overflow for {}",
                    input.instrument_id,
                )
            })?;

            if available < required {
                return Err(format!(
                    "insufficient uncommitted simulated NO inventory for {}: available {available}, committed sell {committed}, required {}",
                    input.instrument_id, input.quantity,
                ));
            }
        }

        let legs = effect
            .inputs
            .iter()
            .map(|input| {
                (
                    convert_position_id(input.instrument_id, command.strategy_id),
                    input.quantity,
                )
            })
            .collect();
        self.convert_inventory
            .borrow_mut()
            .reserve(command.operation_id, legs)
            .map_err(|e| format!("cannot reserve conversion inventory: {e}"))?;

        if !net_cash.as_decimal().is_zero()
            && let Err(e) = self.try_adjust_account(net_cash)
        {
            self.convert_inventory
                .borrow_mut()
                .release(command.operation_id);
            return Err(format!("cannot apply conversion cash: {e}"));
        }

        let transaction = ConvertTransaction {
            wallet: format!("sim-wallet-{}", command.account_id),
            relayer_transaction_id: Some(format!("sim-{}", command.operation_id)),
            transaction_hash: None,
            nonce: None,
            call_summary: Some("simulated negative-risk conversion".to_string()),
        };
        let affected_instruments = effect
            .inputs
            .iter()
            .chain(effect.outputs.iter())
            .map(|leg| leg.instrument_id)
            .collect();
        let ts_event = self.clock.borrow().timestamp_ns();

        Ok(VerifiedConvert {
            operation_id: command.operation_id,
            trader_id: command.trader_id,
            strategy_id: command.strategy_id,
            account_id: command.account_id,
            transaction,
            inputs: effect.inputs,
            outputs: effect.outputs,
            affected_instruments,
            net_cash,
            ts_event,
        })
    }

    fn dispatch_convert_event(&self, event: ConvertExecutionEvent) {
        let Some(handler) = &self.convert_event_handler else {
            log::error!("Cannot dispatch conversion event without a deferred handler");
            return;
        };
        handler(event);
    }

    pub(crate) fn release_convert_inventory_if_applied(&self, operation_id: UUID4) {
        let applied = match self.cache.borrow().load_convert_operation(operation_id) {
            Ok(Some(record)) => record.applied,
            Ok(None) => false,
            Err(e) => {
                log::error!("Cannot read Convert operation {operation_id}: {e}");
                return;
            }
        };

        if applied {
            self.convert_inventory.borrow_mut().release(operation_id);
        }
    }
}
