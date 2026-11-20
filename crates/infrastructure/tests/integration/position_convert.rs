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

use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::{Cache, database::CacheDatabaseAdapter},
    clock::VirtualClock,
    messages::execution::{ConvertExecutionStatus, ConvertOperationRecord, SubmitConvert},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_execution::engine::{
    ExecutionEngine,
    polymarket_convert::{ConvertRecord, ConvertTokenAmount, ConvertTransaction, VerifiedConvert},
};
use nautilus_live::{
    config::{LiveExecutionEngineConfig, LiveNodeConfig},
    node::LiveNode,
};
use nautilus_model::{
    accounts::{AccountAny, CashAccount},
    data::InstrumentClose,
    enums::{AccountType, InstrumentCloseType, OmsType, OrderSide, OrderStatus, OrderType},
    events::{
        AccountState, OrderEventAny, OrderFillVoided, OrderFilled,
        order::spec::{
            OrderAcceptedSpec, OrderFillVoidedSpec, OrderFilledSpec, OrderSubmittedSpec,
        },
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, PositionId, StrategyId, TradeId,
        TraderId, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny, stubs::binary_option},
    orders::{Order, OrderTestBuilder},
    position::{Position, PositionConvert, PositionConvertKind, PositionReplayEvent},
    types::{AccountBalance, Money, Price, Quantity},
};
use rust_decimal::Decimal;

pub(super) fn convert_position(trader_id: TraderId, instrument: &InstrumentAny) -> Position {
    let operation_id = UUID4::new();
    let strategy_id = StrategyId::from("CONVERT-001");

    Position::new_from_convert(
        instrument,
        PositionConvert {
            operation_id,
            trader_id,
            strategy_id,
            instrument_id: instrument.id(),
            position_id: PositionId::from(format!("{}-{strategy_id}", instrument.id())),
            account_id: AccountId::from("POLYMARKET-001"),
            cycle_id: operation_id,
            kind: PositionConvertKind::Open {
                previous_cycle_id: None,
            },
            quantity: Quantity::from("100.00"),
            cost: Decimal::from(50),
            realized_pnl: Decimal::ZERO,
            ts_event: UnixNanos::from(100),
            ts_init: UnixNanos::from(100),
        },
    )
    .unwrap()
}

pub(super) async fn exercise_convert_restart(
    trader_id: TraderId,
    connect: impl AsyncFn() -> Box<dyn CacheDatabaseAdapter>,
) {
    let instrument = InstrumentAny::BinaryOption(binary_option());
    let mut original = convert_position(trader_id, &instrument);
    let mut writer = Cache::new(None, Some(connect().await));
    writer.add_currency(instrument.quote_currency()).unwrap();
    writer.add_instrument(instrument.clone()).unwrap();
    let funds = Money::from("1000.00 USDC");
    let account = AccountState::new(
        original.account_id,
        AccountType::Cash,
        vec![AccountBalance::new(funds, Money::from("0.00 USDC"), funds)],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        Some(instrument.quote_currency()),
    );
    writer
        .add_account(AccountAny::Cash(CashAccount::new(account, false, false)))
        .unwrap();
    writer
        .add_position_without_order(&original, OmsType::Netting)
        .unwrap();

    let clob_position_id = PositionId::from("P-CLOB-ALONGSIDE-CONVERT");
    let clob_fill = OrderFilledSpec::builder()
        .trader_id(trader_id)
        .strategy_id(original.strategy_id)
        .instrument_id(instrument.id())
        .account_id(original.account_id)
        .last_qty(Quantity::from("2.00"))
        .last_px(Price::from("0.500"))
        .currency(instrument.quote_currency())
        .position_id(clob_position_id)
        .event_id(UUID4::new())
        .build();
    let clob = Position::new(&instrument, clob_fill);
    writer.add_position(&clob, OmsType::Hedging).unwrap();
    writer.dispose();

    let mut reader = connect().await;
    assert_same_position(
        &reader.load_position(&original.id).await.unwrap().unwrap(),
        &original,
    );
    assert_eq!(reader.load_positions().await.unwrap().len(), 2);
    let PositionReplayEvent::Converted(opening) = original.replay_events[0] else {
        unreachable!();
    };
    original
        .apply_convert(PositionConvert {
            operation_id: UUID4::new(),
            kind: PositionConvertKind::Increase,
            quantity: Quantity::from("20.00"),
            cost: Decimal::from(10),
            ts_event: UnixNanos::from(150),
            ts_init: UnixNanos::from(150),
            ..opening
        })
        .unwrap();
    reader.update_position(&original).unwrap();
    reader.close().unwrap();

    let mut recovered = Cache::new(None, Some(connect().await));
    recovered.cache_all().await.unwrap();
    recovered.build_index();
    assert_same_position(&recovered.position(&original.id).unwrap(), &original);
    assert_same_position(&recovered.position(&clob_position_id).unwrap(), &clob);

    let cache = Rc::new(RefCell::new(recovered));
    let sell = sell_through_execution(&cache, &instrument, &original);
    let mut continuous = original.clone();
    continuous.try_apply(&sell).unwrap();
    let sold = cache.borrow().position(&original.id).unwrap().cloned();
    assert_same_position(&sold, &continuous);
    assert_eq!(sold.quantity, Quantity::from("80.00"));
    assert_eq!(sold.cost_open, Decimal::from(40));
    assert_eq!(sold.realized_pnl, Some(Money::from("8.00 USDC")));
    cache.borrow_mut().dispose();

    let mut corrected_cache = Cache::new(None, Some(connect().await));
    corrected_cache.cache_all().await.unwrap();
    corrected_cache.build_index();
    let mut corrected = corrected_cache.position(&original.id).unwrap().cloned();
    assert_same_position(&corrected, &continuous);
    let voided = build_sell_void(&sell);
    let close = InstrumentClose::new(
        instrument.id(),
        Price::from("1.000"),
        InstrumentCloseType::ContractExpired,
        UnixNanos::from(400),
        UnixNanos::from(400),
    );

    for position in [&mut continuous, &mut corrected] {
        position
            .apply_fill_void(voided.clone(), voided.voided_qty, None)
            .unwrap();
        position.apply_instrument_close(close).unwrap();
    }
    assert_same_position(&corrected, &continuous);
    assert!(corrected.is_settled());
    assert!(corrected.is_closed());
    assert_eq!(corrected.cost_open, Decimal::ZERO);
    assert_eq!(corrected.realized_pnl, Some(Money::from("51.00 USDC")));
    corrected_cache.update_position(&corrected).unwrap();
    corrected_cache.dispose();

    let mut cleaned = Cache::new(None, Some(connect().await));
    cleaned.cache_all().await.unwrap();
    cleaned.build_index();
    assert_same_position(&cleaned.position(&original.id).unwrap(), &corrected);
    cleaned
        .purge_closed_positions(UnixNanos::from(500), 0, true)
        .unwrap();
    cleaned.dispose();

    let mut reader = connect().await;
    assert!(reader.load_position(&original.id).await.unwrap().is_none());
    let positions = reader.load_positions().await.unwrap();
    assert_eq!(positions.len(), 1);
    assert_same_position(&positions[&clob_position_id], &clob);
    reader.close().unwrap();
}

pub(super) async fn exercise_convert_cycle_reuse(
    trader_id: TraderId,
    connect: impl AsyncFn() -> Box<dyn CacheDatabaseAdapter>,
) {
    let mut binary = binary_option();
    binary.id = InstrumentId::from("CONVERT:OUTCOME.POLYMARKET");
    let instrument = InstrumentAny::BinaryOption(binary);
    let original = convert_position(trader_id, &instrument);
    let mut closed = original.clone();
    let mut fill = OrderFilledSpec::builder()
        .trader_id(trader_id)
        .strategy_id(original.strategy_id)
        .instrument_id(instrument.id())
        .account_id(original.account_id)
        .position_id(original.id)
        .last_px(Price::from("0.700"))
        .currency(instrument.quote_currency())
        .trade_id(TradeId::from("T-CLOSE-CONVERT"))
        .last_qty(original.quantity)
        .order_side(OrderSide::Sell)
        .event_id(UUID4::new())
        .ts_event(UnixNanos::from(200))
        .ts_init(UnixNanos::from(200))
        .build();
    closed.try_apply(&fill).unwrap();
    assert!(closed.is_closed());

    let mut writer = connect().await;
    writer.add_currency(&instrument.quote_currency()).unwrap();
    writer.add_instrument(&instrument).unwrap();
    writer.add_position(&original).unwrap();
    writer.update_position(&closed).unwrap();
    writer.close().unwrap();

    let mut writer = connect().await;
    assert_same_position(
        &writer.load_position(&original.id).await.unwrap().unwrap(),
        &closed,
    );
    assert_same_position(
        &writer.load_positions().await.unwrap()[&original.id],
        &closed,
    );
    fill.trade_id = TradeId::from("T-REOPEN-CONVERT-ID");
    fill.order_side = OrderSide::Buy;
    fill.last_qty = Quantity::from("3.00");
    fill.event_id = UUID4::new();
    fill.ts_event = UnixNanos::from(300);
    fill.ts_init = fill.ts_event;
    let reopened = Position::new(&instrument, fill);
    writer.add_position(&reopened).unwrap();
    writer.close().unwrap();

    let mut writer = connect().await;
    assert_same_position(
        &writer.load_positions().await.unwrap()[&original.id],
        &reopened,
    );
    writer.close().unwrap();

    exercise_replay_after_removing_fill(&connect, reopened).await;
}

async fn exercise_replay_after_removing_fill(
    connect: &impl AsyncFn() -> Box<dyn CacheDatabaseAdapter>,
    mut position: Position,
) {
    let mut writer = connect().await;
    let mut fill = position.last_event().unwrap();
    fill.client_order_id = ClientOrderId::from("O-POSITION-FILL-TO-PURGE");
    fill.trade_id = TradeId::from("T-POSITION-FILL-TO-PURGE");
    fill.event_id = UUID4::new();
    fill.ts_event = UnixNanos::from(400);
    fill.ts_init = fill.ts_event;
    position.try_apply(&fill).unwrap();
    writer.update_position(&position).unwrap();

    position
        .apply_convert(PositionConvert {
            operation_id: UUID4::new(),
            trader_id: position.trader_id,
            strategy_id: position.strategy_id,
            instrument_id: position.instrument_id,
            position_id: position.id,
            account_id: position.account_id,
            cycle_id: position.current_cycle_id().unwrap(),
            kind: PositionConvertKind::Increase,
            quantity: Quantity::from("2.00"),
            cost: "1.234567890123456789".parse().unwrap(),
            realized_pnl: Decimal::ZERO,
            ts_event: UnixNanos::from(500),
            ts_init: UnixNanos::from(500),
        })
        .unwrap();
    writer.update_position(&position).unwrap();
    position
        .purge_events_for_order(fill.client_order_id)
        .unwrap();
    assert_eq!(position.quantity, Quantity::from("5.00"));
    assert_eq!(
        position.cost_open,
        "3.334567890123456789".parse::<Decimal>().unwrap()
    );
    writer.update_position(&position).unwrap();
    writer.close().unwrap();

    let mut writer = connect().await;
    assert_same_position(
        &writer.load_position(&position.id).await.unwrap().unwrap(),
        &position,
    );
    writer.delete_position(&position.id).unwrap();
    writer.close().unwrap();
    let mut reader = connect().await;
    assert!(reader.load_positions().await.unwrap().is_empty());
    reader.close().unwrap();
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ConvertRecoveryCase {
    Complete,
    Partial,
    StaleEnvelope,
    StaleEnvelopeSettled,
    MissingPlan,
    MissingIndex,
    ConflictingEnvelope,
    ReplacedCycle,
}

pub(super) async fn exercise_convert_batch_recovery(
    trader_id: TraderId,
    connect: impl AsyncFn() -> Box<dyn CacheDatabaseAdapter>,
    case: ConvertRecoveryCase,
) {
    let mut binary_no = binary_option();
    binary_no.id = InstrumentId::from("RECOVERY-NO.POLYMARKET");
    let input = InstrumentAny::BinaryOption(binary_no);
    let mut binary_yes = binary_option();
    binary_yes.id = InstrumentId::from("RECOVERY-YES.POLYMARKET");
    let output = InstrumentAny::BinaryOption(binary_yes);
    let original = convert_position(trader_id, &input);
    let currency = input.cost_currency();
    let verified = VerifiedConvert {
        operation_id: UUID4::new(),
        trader_id,
        strategy_id: original.strategy_id,
        account_id: original.account_id,
        transaction: ConvertTransaction {
            wallet: "0x1234".into(),
            relayer_transaction_id: Some("RELAYER-RECOVERY".into()),
            transaction_hash: Some("0x5678".into()),
            nonce: Some("42".into()),
            call_summary: Some("convert RECOVERY-NO".into()),
        },
        inputs: vec![ConvertTokenAmount {
            instrument_id: input.id(),
            quantity: Decimal::from(40),
        }],
        outputs: vec![ConvertTokenAmount {
            instrument_id: output.id(),
            quantity: Decimal::from(40),
        }],
        affected_instruments: vec![input.id(), output.id()],
        net_cash: Money::from("10 USDC"),
        ts_event: UnixNanos::from(200),
    };
    let mut reference = Cache::default();
    reference.add_instrument(input.clone()).unwrap();
    reference.add_instrument(output.clone()).unwrap();
    reference
        .add_position_without_order(&original, OmsType::Netting)
        .unwrap();
    let mut engine = ExecutionEngine::new(
        Rc::new(RefCell::new(VirtualClock::new())),
        Rc::new(RefCell::new(reference)),
        None,
    );
    let mut record = engine
        .apply_verified_convert(verified.clone(), &[])
        .unwrap();
    assert!(record.applied, "{:?}", record.reason);
    let plan = record.plan.as_ref().unwrap();
    let output_change = *plan
        .iter()
        .find(|change| change.instrument_id == output.id())
        .unwrap();

    let mut writer = Cache::new(None, Some(connect().await));
    writer.add_currency(currency).unwrap();
    writer.add_instrument(input.clone()).unwrap();
    writer.add_instrument(output.clone()).unwrap();
    let funds = Money::from("1000 USDC");
    let account = AccountState::new(
        original.account_id,
        AccountType::Cash,
        vec![AccountBalance::new(funds, Money::zero(currency), funds)],
        Vec::new(),
        true,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        Some(currency),
    );
    writer
        .add_account(AccountAny::Cash(CashAccount::new(account, false, false)))
        .unwrap();
    let persisted_input = if matches!(case, ConvertRecoveryCase::ReplacedCycle) {
        convert_position(trader_id, &input)
    } else {
        original.clone()
    };
    writer
        .add_position_without_order(&persisted_input, OmsType::Netting)
        .unwrap();
    writer
        .apply_position_convert(&output, output_change)
        .unwrap()
        .unwrap();

    if matches!(
        case,
        ConvertRecoveryCase::Complete | ConvertRecoveryCase::StaleEnvelopeSettled
    ) {
        let input_change = *plan
            .iter()
            .find(|change| change.instrument_id == input.id())
            .unwrap();
        writer
            .apply_position_convert(&input, input_change)
            .unwrap()
            .unwrap();
    }

    if matches!(case, ConvertRecoveryCase::MissingPlan) {
        record.plan = None;
    }

    if matches!(case, ConvertRecoveryCase::StaleEnvelopeSettled) {
        for instrument_id in [input.id(), output.id()] {
            writer
                .add_instrument_close(InstrumentClose::new(
                    instrument_id,
                    Price::from("0.00"),
                    InstrumentCloseType::ContractExpired,
                    UnixNanos::from(300),
                    UnixNanos::from(300),
                ))
                .unwrap();
        }
    }

    let command = SubmitConvert {
        operation_id: verified.operation_id,
        trader_id,
        strategy_id: verified.strategy_id,
        account_id: verified.account_id,
        client_id: ClientId::from("POLYMARKET"),
        inputs: verified.inputs.clone(),
        ts_init: verified.ts_event,
    };
    let mut operation = ConvertOperationRecord::new(&command);
    operation.dispatched = true;
    operation.applied = true;
    operation.status = Some(ConvertExecutionStatus::Confirmed);
    operation.transaction = Some(verified.transaction.clone());
    operation.affected_instruments = verified.affected_instruments.clone();
    if matches!(
        case,
        ConvertRecoveryCase::StaleEnvelope | ConvertRecoveryCase::StaleEnvelopeSettled
    ) {
        operation.status = None;
        operation.applied = false;
        operation.transaction = None;
    }

    if matches!(case, ConvertRecoveryCase::ConflictingEnvelope) {
        operation.transaction.as_mut().unwrap().wallet = "0x9876".into();
    }

    if !matches!(case, ConvertRecoveryCase::MissingIndex) {
        writer.add_convert_operation(&operation).unwrap();
    }
    writer
        .add(
            &ConvertRecord::cache_key(verified.operation_id),
            serde_json::to_vec(&record).unwrap().into(),
        )
        .unwrap();
    writer.dispose();

    let config = LiveNodeConfig {
        trader_id,
        exec_engine: LiveExecutionEngineConfig {
            load_cache: true,
            reconciliation: false,
            ..Default::default()
        },
        delay_post_stop: std::time::Duration::ZERO,
        ..Default::default()
    };
    let mut node = LiveNode::build("ConvertRecovery".to_string(), Some(config)).unwrap();
    node.set_cache_database(connect().await).unwrap();
    node.start().await.unwrap();
    let cache = node.kernel().cache();
    let engine = node.kernel().exec_engine();
    let recovered = engine
        .borrow()
        .convert_record(verified.operation_id)
        .unwrap()
        .unwrap();
    let restored = cache
        .borrow()
        .load_convert_operation(verified.operation_id)
        .unwrap();

    match case {
        ConvertRecoveryCase::Complete
        | ConvertRecoveryCase::Partial
        | ConvertRecoveryCase::StaleEnvelope
        | ConvertRecoveryCase::StaleEnvelopeSettled => {
            assert!(recovered.applied, "{:?}", recovered.reason);
            assert!(recovered.unapplied_instruments.is_empty());
            let restored = restored.unwrap();
            assert!(!restored.historical_gap);
            let result = restored.result();
            assert!(result.applied);
            assert_eq!(result.status, ConvertExecutionStatus::Confirmed);
            assert_eq!(result.transaction, Some(verified.transaction.clone()));
            assert_eq!(
                engine
                    .borrow_mut()
                    .recover_convert(verified.operation_id, &[])
                    .unwrap(),
                recovered
            );
            let input_position = cache.borrow().position_owned(&original.id).unwrap();
            assert_eq!(input_position.quantity, Quantity::from("60.00"));
            assert_eq!(input_position.cost_open, Decimal::from(30));
            assert_eq!(input_position.replay_events.len(), 2);
        }
        ConvertRecoveryCase::MissingPlan | ConvertRecoveryCase::ReplacedCycle => {
            assert!(!recovered.applied);
            assert!(recovered.reason.is_some());
            assert!(recovered.unapplied_instruments.contains(&input.id()));

            if matches!(case, ConvertRecoveryCase::ReplacedCycle) {
                assert_eq!(recovered.unapplied_instruments, vec![input.id()]);
            }
            let restored = restored.unwrap();
            let result = restored.result();
            assert_eq!(result.status, ConvertExecutionStatus::Confirmed);
            assert_eq!(result.transaction, Some(verified.transaction.clone()));
            assert_eq!(
                result.unapplied_instruments,
                recovered.unapplied_instruments
            );

            if matches!(case, ConvertRecoveryCase::MissingPlan) {
                assert!(restored.plan_lost);
                let reason = result.reason.as_deref().unwrap();
                assert!(reason.contains("plan"));
                assert!(reason.contains("cannot reconstruct"));
            } else {
                assert!(!restored.plan_lost);
                assert_eq!(result.reason, recovered.reason);
            }
            assert!(!result.applied);

            assert_same_position(
                &cache.borrow().position(&original.id).unwrap(),
                &persisted_input,
            );
        }
        ConvertRecoveryCase::MissingIndex => {
            assert!(restored.is_none());
            assert_eq!(recovered, record);
            assert_same_position(
                &cache.borrow().position(&original.id).unwrap(),
                &persisted_input,
            );
        }
        ConvertRecoveryCase::ConflictingEnvelope => {
            assert_eq!(recovered, record);
            let result = restored.unwrap().result();
            assert!(!result.applied);
            assert_eq!(result.status, ConvertExecutionStatus::Confirmed);
            assert_eq!(result.transaction, operation.transaction);
            assert!(!result.unapplied_instruments.is_empty());
            assert!(result.reason.unwrap().contains("conflict"));
            assert_same_position(
                &cache.borrow().position(&original.id).unwrap(),
                &persisted_input,
            );
        }
    }

    assert_eq!(recovered.verified, verified);
    assert_eq!(recovered.plan, record.plan);
    let out = cache
        .borrow()
        .position_owned(&output_change.position_id)
        .unwrap();
    assert_eq!(out.quantity, Quantity::from("40.00"));
    let [PositionReplayEvent::Converted(replayed)] = out.replay_events.as_slice() else {
        panic!("Expected a single Convert replay change");
    };
    assert_eq!(*replayed, output_change);
    node.stop().await.unwrap();
    node.dispose();
}

fn sell_through_execution(
    cache: &Rc<RefCell<Cache>>,
    instrument: &InstrumentAny,
    position: &Position,
) -> OrderFilled {
    let client_order_id = ClientOrderId::from("O-CONVERT-SELL");
    let venue_order_id = VenueOrderId::from("V-CONVERT-SELL");
    let quantity = Quantity::from("40.00");
    let ts_event = UnixNanos::from(200);
    let order = OrderTestBuilder::new(OrderType::Market)
        .trader_id(position.trader_id)
        .strategy_id(position.strategy_id)
        .instrument_id(instrument.id())
        .client_order_id(client_order_id)
        .side(OrderSide::Sell)
        .quantity(quantity)
        .build();
    cache
        .borrow_mut()
        .add_order(order, None, None, true)
        .unwrap();

    let submitted = OrderSubmittedSpec::builder()
        .trader_id(position.trader_id)
        .strategy_id(position.strategy_id)
        .instrument_id(instrument.id())
        .client_order_id(client_order_id)
        .account_id(position.account_id)
        .event_id(UUID4::new())
        .build();
    let accepted = OrderAcceptedSpec::builder()
        .trader_id(position.trader_id)
        .strategy_id(position.strategy_id)
        .instrument_id(instrument.id())
        .client_order_id(client_order_id)
        .account_id(position.account_id)
        .venue_order_id(venue_order_id)
        .event_id(UUID4::new())
        .build();
    let fill = OrderFilledSpec::builder()
        .trader_id(position.trader_id)
        .strategy_id(position.strategy_id)
        .instrument_id(instrument.id())
        .client_order_id(client_order_id)
        .account_id(position.account_id)
        .venue_order_id(venue_order_id)
        .trade_id(TradeId::from("T-CONVERT-SELL"))
        .order_side(OrderSide::Sell)
        .last_qty(quantity)
        .last_px(Price::from("0.700"))
        .currency(instrument.quote_currency())
        .position_id(position.id)
        .event_id(UUID4::new())
        .ts_event(ts_event)
        .ts_init(ts_event)
        .build();

    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    let mut engine = ExecutionEngine::new(clock, Rc::clone(cache), None);
    engine.process(&OrderEventAny::Submitted(submitted));
    engine.process(&OrderEventAny::Accepted(accepted));
    engine.process(&OrderEventAny::Filled(fill.clone()));
    assert_eq!(
        cache.borrow().order(&client_order_id).unwrap().status(),
        OrderStatus::Filled
    );
    fill
}

fn build_sell_void(fill: &OrderFilled) -> OrderFillVoided {
    let timestamp = UnixNanos::from(300);
    OrderFillVoidedSpec::builder()
        .trader_id(fill.trader_id)
        .strategy_id(fill.strategy_id)
        .instrument_id(fill.instrument_id)
        .client_order_id(fill.client_order_id)
        .venue_order_id(fill.venue_order_id)
        .account_id(fill.account_id)
        .trade_id(fill.trade_id)
        .order_side(fill.order_side)
        .order_type(fill.order_type)
        .liquidity_side(fill.liquidity_side)
        .last_px(fill.last_px)
        .currency(fill.currency)
        .position_id(fill.position_id.unwrap())
        .voided_qty(Quantity::from("10.00"))
        .event_id(UUID4::new())
        .ts_event(timestamp)
        .ts_init(timestamp)
        .build()
}

pub(super) fn assert_same_position(actual: &Position, expected: &Position) {
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
}
