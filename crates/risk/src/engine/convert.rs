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

//! Admission checks for negative-risk conversion requests.

use nautilus_common::{
    cache::Cache,
    messages::execution::{
        ConvertOperationRecord, ConvertResult, ConvertTokenAmount, SubmitConvert, TradingCommand,
        has_unique_inputs,
    },
    msgbus,
};
use nautilus_execution::engine::polymarket_convert::{convert_position_id, exact_convert_quantity};
use nautilus_model::{
    enums::{InstrumentCloseType, OmsType, OrderSide, TradingState},
    identifiers::InstrumentId,
    instruments::{Instrument, neg_risk::ConvertScope},
    orders::Order,
};
use rust_decimal::Decimal;

use super::RiskEngine;

impl RiskEngine {
    pub(super) fn handle_submit_convert(&self, command: SubmitConvert) {
        let existing = self
            .cache
            .borrow()
            .load_convert_operation(command.operation_id);

        let existing = match existing {
            Ok(record) => record,
            Err(e) => {
                let reason = format!("cannot read retained Convert operation: {e}");
                publish_rejection(&command, reason);
                return;
            }
        };

        if let Some(record) = existing {
            if !record.matches_request(&command) {
                publish_rejection(
                    &command,
                    format!(
                        "Convert operation {} conflicts with its original request",
                        command.operation_id,
                    ),
                );
                return;
            }

            publish_result(&record.result());
            return;
        }

        match self.validate_convert_admission(&command) {
            Ok(affected_instruments) => {
                let mut record = ConvertOperationRecord::new(&command);
                record.affected_instruments = affected_instruments;
                if let Err(e) = self.store_convert_operation(&record) {
                    publish_rejection(&command, format!("cannot retain Convert operation: {e}"));
                    return;
                }
                Self::send_to_execution(TradingCommand::SubmitConvert(command));
            }
            Err(reason) => {
                let record = ConvertOperationRecord::rejected(&command, reason);
                if let Err(e) = self.store_convert_operation(&record) {
                    log::error!(
                        "Cannot retain rejected Convert operation {}: {e}",
                        command.operation_id,
                    );
                    publish_result(&record.result());
                    return;
                }
                publish_result(&record.result());
            }
        }
    }

    fn validate_convert_admission(
        &self,
        command: &SubmitConvert,
    ) -> Result<Vec<InstrumentId>, String> {
        if self.trading_state == TradingState::Halted {
            return Err("trading is halted".to_string());
        }

        if command.inputs.is_empty() {
            return Err("a conversion requires at least one NO input".to_string());
        }

        if !has_unique_inputs(&command.inputs) {
            return Err("conversion inputs must be unique".to_string());
        }

        let quantity = validate_input_quantities(&command.inputs)?;
        let input_ids: Vec<_> = command
            .inputs
            .iter()
            .map(|input| input.instrument_id)
            .collect();

        let cache = self.cache.borrow();
        let event = command
            .inputs
            .first()
            .and_then(|input| cache.convert_event_for_instrument(&input.instrument_id))
            .ok_or_else(|| {
                format!(
                    "no complete negative-risk event mapping for input {}",
                    command.inputs[0].instrument_id,
                )
            })?;

        let scope = event
            .convert_scope(&input_ids)
            .map_err(|e| format!("invalid conversion scope: {e}"))?;
        let affected = affected_instruments(&scope);

        for instrument_id in &affected {
            if cache.instrument(instrument_id).is_none() {
                return Err(format!(
                    "conversion instrument {instrument_id} is not loaded"
                ));
            }
        }

        for instrument_id in &affected {
            if cache
                .instrument_close(instrument_id)
                .is_some_and(|close| close.close_type == InstrumentCloseType::ContractExpired)
            {
                return Err(format!("conversion instrument {instrument_id} has expired"));
            }
        }

        validate_output_positions(&cache, command, &scope)?;
        validate_input_positions(&cache, command, &scope, quantity)?;

        if self.trading_state == TradingState::Reducing {
            if !scope.outputs.is_empty() {
                return Err("reducing conversions cannot produce any outcome token".to_string());
            }

            if event.fee_bps() >= 10_000 {
                return Err("reducing conversions require a positive cash expectation".to_string());
            }
        }

        Ok(affected)
    }

    fn store_convert_operation(&self, record: &ConvertOperationRecord) -> Result<(), String> {
        self.cache
            .borrow_mut()
            .add_convert_operation(record)
            .map_err(|e| e.to_string())
    }
}

fn validate_output_positions(
    cache: &Cache,
    command: &SubmitConvert,
    scope: &ConvertScope,
) -> Result<(), String> {
    for output in &scope.outputs {
        let position_id = convert_position_id(*output, command.strategy_id);
        if let Some(position) = cache.position_owned(&position_id) {
            let instrument = cache
                .instrument(output)
                .ok_or_else(|| format!("conversion instrument {output} is not loaded"))?;

            if position.trader_id != command.trader_id
                || position.strategy_id != command.strategy_id
                || position.account_id != command.account_id
                || position.instrument_id != *output
                || cache.oms_type(&position_id) != Some(OmsType::Netting)
                || position.size_precision != instrument.size_precision()
            {
                return Err(format!(
                    "conversion output position {position_id} is not a matching netting position"
                ));
            }

            if position.signed_qty.is_sign_negative() {
                return Err(format!(
                    "conversion output {output} would be applied to a short position"
                ));
            }
        }
    }

    Ok(())
}

fn validate_input_positions(
    cache: &Cache,
    command: &SubmitConvert,
    scope: &ConvertScope,
    quantity: Decimal,
) -> Result<(), String> {
    for input in &scope.inputs {
        let instrument = cache
            .instrument(input)
            .ok_or_else(|| format!("conversion instrument {input} is not loaded"))?;
        exact_convert_quantity(quantity, instrument.size_precision(), "conversion input")?;

        let position_id = convert_position_id(*input, command.strategy_id);
        let Some(position) = cache.position_owned(&position_id) else {
            return Err(format!(
                "conversion input {input} has no netting long position"
            ));
        };

        if position.trader_id != command.trader_id
            || position.strategy_id != command.strategy_id
            || position.account_id != command.account_id
            || position.instrument_id != *input
        {
            return Err(format!(
                "conversion input position {position_id} does not belong to the strategy account"
            ));
        }

        if cache.oms_type(&position_id) != Some(OmsType::Netting)
            || position.size_precision != instrument.size_precision()
        {
            return Err(format!(
                "conversion input position {position_id} is not a matching netting position"
            ));
        }

        let held = position.signed_qty;

        if held < quantity {
            return Err(format!(
                "insufficient NO inventory for {input}: held {held}, required {quantity}"
            ));
        }

        let committed = committed_sell_quantity(cache, command, input);
        if held - committed < quantity {
            return Err(format!(
                "insufficient uncommitted NO inventory for {input}: held {held}, \
                 committed sell {committed}, required {quantity}"
            ));
        }
    }

    Ok(())
}

fn validate_input_quantities(inputs: &[ConvertTokenAmount]) -> Result<Decimal, String> {
    let mut quantity = None;

    for input in inputs {
        if !input.quantity.is_sign_positive() || input.quantity.is_zero() {
            return Err(format!(
                "conversion quantity for {} must be positive",
                input.instrument_id,
            ));
        }

        if input.quantity.normalize().scale() > 6 {
            return Err(format!(
                "conversion quantity for {} is not representable in chain units",
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

    quantity.ok_or_else(|| "a conversion requires at least one NO input".to_string())
}

fn committed_sell_quantity(
    cache: &Cache,
    command: &SubmitConvert,
    instrument_id: &InstrumentId,
) -> Decimal {
    let orders = cache.orders_refs(
        None,
        Some(instrument_id),
        Some(&command.strategy_id),
        None,
        Some(OrderSide::Sell),
    );

    orders
        .iter()
        .filter(|order| !order.is_closed())
        .filter(|order| {
            cache.is_order_dispatched(&order.client_order_id())
                || order.is_open()
                || order.is_inflight()
        })
        .filter(|order| match order.account_id() {
            Some(account_id) => account_id == command.account_id,
            None => cache
                .client_id(&order.client_order_id())
                .and_then(|client_id| cache.account_id_for_client(client_id))
                .is_some_and(|account_id| *account_id == command.account_id),
        })
        .map(|order| order.leaves_qty().as_decimal())
        .sum()
}

fn affected_instruments(scope: &ConvertScope) -> Vec<InstrumentId> {
    let mut affected = Vec::with_capacity(scope.inputs.len() + scope.outputs.len());
    affected.extend(scope.inputs.iter().copied());
    affected.extend(scope.outputs.iter().copied());
    affected
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

fn publish_result(result: &ConvertResult) {
    let topic = msgbus::switchboard::get_event_convert_topic(result.strategy_id);
    msgbus::publish_any(topic, result);
}
