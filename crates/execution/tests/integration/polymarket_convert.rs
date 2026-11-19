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
    cache::Cache,
    clock::VirtualClock,
    msgbus::{self, MessageBus, TypedHandler},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_execution::{
    engine::{
        ExecutionEngine,
        polymarket_convert::{
            ConvertRecord, ConvertTokenAmount, ConvertTransaction, VerifiedConvert,
        },
    },
    matching_engine::OrderMatchingEngine,
    models::{
        fee::{FeeModelAny, FixedFeeModel},
        fill::FillModelHandle,
    },
};
use nautilus_model::{
    data::{InstrumentClose, QuoteTick},
    enums::{
        AccountType, BookType, InstrumentCloseType, OmsType, OrderSide, OrderStatus, OrderType,
    },
    events::{AccountState, PositionEvent, order::spec::OrderFilledSpec},
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, PositionId, StrategyId, TradeId, TraderId,
    },
    instruments::{Instrument, InstrumentAny, stubs::binary_option},
    orders::{Order, builder::OrderTestBuilder, stubs::TestOrderEventStubs},
    position::Position,
    types::{AccountBalance, Money, Price, Quantity},
};
use nautilus_portfolio::Portfolio;
use rstest::rstest;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

struct ConvertFixture {
    engine: ExecutionEngine,
    request: VerifiedConvert,
    no_a: InstrumentAny,
    no_b: InstrumentAny,
    yes_c: InstrumentAny,
}

impl ConvertFixture {
    fn new() -> Self {
        msgbus::set_message_bus(Rc::new(RefCell::new(MessageBus::default())));
        let no_a = outcome("NO-A.POLYMARKET");
        let no_b = outcome("NO-B.POLYMARKET");
        let yes_c = outcome("YES-C.POLYMARKET");
        let cache = Rc::new(RefCell::new(Cache::default()));

        for instrument in [&no_a, &no_b, &yes_c] {
            cache
                .borrow_mut()
                .add_instrument(instrument.clone())
                .unwrap();
        }

        let request = VerifiedConvert {
            operation_id: UUID4::new(),
            trader_id: TraderId::from("TRADER-001"),
            strategy_id: StrategyId::from("CONVERT-001"),
            account_id: AccountId::from("POLYMARKET-001"),
            transaction: ConvertTransaction {
                wallet: "0x1234".into(),
                relayer_transaction_id: Some("RELAYER-1".into()),
                transaction_hash: None,
                nonce: Some("42".into()),
                call_summary: Some("convert A and B".into()),
            },
            inputs: vec![amount(&no_a, dec!(100)), amount(&no_b, dec!(100))],
            outputs: vec![amount(&yes_c, dec!(100))],
            affected_instruments: vec![no_a.id(), no_b.id(), yes_c.id()],
            net_cash: Money::from("100 USDC"),
            ts_event: UnixNanos::from(100),
        };

        let mut fixture = Self {
            engine: ExecutionEngine::new(Rc::new(RefCell::new(VirtualClock::new())), cache, None),
            request,
            no_a,
            no_b,
            yes_c,
        };
        fixture.seed(&fixture.no_a, OrderSide::Buy, "100.00", "0.800");
        fixture.seed(&fixture.no_b, OrderSide::Buy, "100.00", "0.700");
        fixture
            .engine
            .register_oms_type(fixture.request.strategy_id, OmsType::Netting);
        fixture
    }

    fn seed(&self, instrument: &InstrumentAny, side: OrderSide, quantity: &str, price: &str) {
        let event_id = UUID4::new();
        let fill = OrderFilledSpec::builder()
            .trader_id(self.request.trader_id)
            .strategy_id(self.request.strategy_id)
            .account_id(self.request.account_id)
            .instrument_id(instrument.id())
            .client_order_id(ClientOrderId::new(format!("O-{event_id}")))
            .position_id(self.position_id(instrument))
            .order_side(side)
            .last_qty(Quantity::from(quantity))
            .last_px(Price::from(price))
            .currency(instrument.quote_currency())
            .event_id(event_id)
            .build();
        let position = Position::new(instrument, fill);
        self.engine
            .cache()
            .borrow_mut()
            .add_position(&position, OmsType::Netting)
            .unwrap();
    }

    fn position_id(&self, instrument: &InstrumentAny) -> PositionId {
        PositionId::new(format!("{}-{}", instrument.id(), self.request.strategy_id))
    }

    fn position(&self, instrument: &InstrumentAny) -> Option<Position> {
        self.engine
            .cache()
            .borrow()
            .position_owned(&self.position_id(instrument))
    }

    fn add_lot(&self, instrument: &InstrumentAny, quantity: &str, price: &str) {
        let mut position = self.position(instrument).unwrap();
        let mut fill = position.events[0].clone();
        fill.event_id = UUID4::new();
        fill.client_order_id = ClientOrderId::new(format!("O-{}", fill.event_id));
        fill.trade_id = TradeId::new(fill.event_id.to_string());
        fill.last_qty = Quantity::from(quantity);
        fill.last_px = Price::from(price);
        position.try_apply(&fill).unwrap();
        self.engine
            .cache()
            .borrow_mut()
            .update_position(&position)
            .unwrap();
    }

    fn apply(&mut self) -> ConvertRecord {
        self.engine
            .apply_verified_convert(self.request.clone(), &[])
            .unwrap()
    }
}

#[rstest]
fn verified_convert_updates_all_positions_before_the_first_event() {
    let mut fixture = ConvertFixture::new();
    let cache = Rc::clone(fixture.engine.cache());
    let [no_a, no_b, yes_c] = [&fixture.no_a, &fixture.no_b, &fixture.yes_c]
        .map(|instrument| fixture.position_id(instrument));
    let operation_id = fixture.request.operation_id;
    let published = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::clone(&published);
    let handler = TypedHandler::from(move |event: &PositionEvent| {
        let cache = cache.borrow();
        assert!(cache.position(&no_a).unwrap().is_closed());
        assert!(cache.position(&no_b).unwrap().is_closed());
        assert_eq!(cache.position(&yes_c).unwrap().cost_open, dec!(50));
        assert_eq!(cache.positions_open(None, None, None, None, None).len(), 1);

        let record: ConvertRecord = serde_json::from_slice(
            cache
                .get(&ConvertRecord::cache_key(operation_id))
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(record.plan.unwrap().len(), 3);
        assert!(!record.applied);
        events.borrow_mut().push(event.clone());
    });
    msgbus::subscribe_position_events("events.position.*".into(), handler.clone(), None);

    let applied = fixture.apply();
    assert!(applied.applied, "{:?}", applied.reason);
    assert!(applied.unapplied_instruments.is_empty());
    assert_eq!(fixture.apply(), applied);
    assert_eq!(
        fixture.engine.recover_convert(operation_id, &[]).unwrap(),
        applied
    );
    msgbus::unsubscribe_position_events("events.position.*".into(), &handler);

    let published = published.borrow();
    assert_eq!(published.len(), 3);
    assert_eq!(
        published
            .iter()
            .filter(|event| matches!(event, PositionEvent::PositionClosed(_)))
            .count(),
        2
    );
    assert!(published.iter().any(|event| matches!(event, PositionEvent::PositionOpened(opened) if opened.last_px.is_none() && opened.opening_order_id.is_none())));
    assert!(fixture.position(&fixture.yes_c).unwrap().events.is_empty());
}

#[rstest]
#[case::pending_input("pending")]
#[case::short_output("short")]
#[case::expired_potential_output("expired")]
#[case::wrong_account("account")]
fn verified_convert_preflight_leaves_all_positions_unchanged(#[case] failure: &str) {
    let mut fixture = ConvertFixture::new();
    let mut pending = Vec::new();

    match failure {
        "pending" => pending.push(fixture.no_b.id()),
        "short" => fixture.seed(&fixture.yes_c, OrderSide::Sell, "1.00", "0.500"),
        "expired" => {
            let potential = outcome("YES-D.POLYMARKET");
            let mut cache = fixture.engine.cache().borrow_mut();
            cache.add_instrument(potential.clone()).unwrap();
            cache
                .add_instrument_close(InstrumentClose::new(
                    potential.id(),
                    Price::from("0.000"),
                    InstrumentCloseType::ContractExpired,
                    UnixNanos::from(90),
                    UnixNanos::from(90),
                ))
                .unwrap();
            fixture.request.affected_instruments.push(potential.id());
        }
        "account" => fixture.request.account_id = AccountId::from("POLYMARKET-002"),
        _ => unreachable!(),
    }

    let original = fixture
        .position(&fixture.yes_c)
        .map(|position| position.quantity);
    let events = Rc::new(RefCell::new(Vec::new()));
    let captured = Rc::clone(&events);
    let handler =
        TypedHandler::from(move |event: &PositionEvent| captured.borrow_mut().push(event.clone()));
    msgbus::subscribe_position_events("events.position.*".into(), handler.clone(), None);
    let record = fixture
        .engine
        .apply_verified_convert(fixture.request.clone(), &pending)
        .unwrap();
    msgbus::unsubscribe_position_events("events.position.*".into(), &handler);

    assert!(!record.applied);
    assert!(record.reason.is_some());
    assert!(events.borrow().is_empty());
    assert_eq!(fixture.position(&fixture.no_a).unwrap().cost_open, dec!(80));
    assert_eq!(fixture.position(&fixture.no_b).unwrap().cost_open, dec!(70));
    assert_eq!(
        fixture
            .position(&fixture.yes_c)
            .map(|position| position.quantity),
        original
    );
}

#[rstest]
fn verified_convert_releases_fractional_cost_without_a_final_remainder() {
    let mut fixture = ConvertFixture::new();
    fixture.seed(&fixture.no_a, OrderSide::Buy, "2.00", "0.200");
    fixture.add_lot(&fixture.no_a, "1.00", "0.300");
    fixture.request.inputs = vec![amount(&fixture.no_a, dec!(1))];
    fixture.request.outputs.clear();
    fixture.request.affected_instruments = vec![fixture.no_a.id()];
    fixture.request.net_cash = Money::from("0 USDC");

    let mut released = Decimal::ZERO;

    for _ in 0..3 {
        let record = fixture.apply();
        assert!(record.applied, "{:?}", record.reason);
        let plan = record.plan.unwrap();
        assert!(plan[0].cost.scale() > u32::from(fixture.request.net_cash.currency.precision));
        assert_eq!(plan[0].realized_pnl, -plan[0].cost);
        released += plan[0].cost;
        fixture.request.operation_id = UUID4::new();
    }

    let remaining = fixture.position(&fixture.no_a).unwrap();
    assert_eq!(released, dec!(0.700));
    assert!(remaining.is_closed());
    assert_eq!(remaining.cost_open, Decimal::ZERO);
}

#[rstest]
fn verified_convert_does_not_require_a_decimal_intermediate_total() {
    let mut fixture = ConvertFixture::new();
    fixture.seed(&fixture.no_a, OrderSide::Buy, "200.00", "0.002");
    fixture.add_lot(&fixture.no_a, "100.00", "0.003");
    fixture.seed(&fixture.no_b, OrderSide::Buy, "100.00", "0.800");
    fixture.request.net_cash = Money::from("80 USDC");
    let fractional_cost = fixture
        .position(&fixture.no_a)
        .unwrap()
        .try_convert_release_cost(Quantity::from("100.00"))
        .unwrap();
    assert_eq!(fractional_cost, dec!(0.2333333333333333333333333333));

    let record = fixture.apply();
    assert!(record.applied, "{:?}", record.reason);
    assert_eq!(
        fixture.position(&fixture.yes_c).unwrap().cost_open,
        fractional_cost
    );
    assert_eq!(
        fixture
            .engine
            .recover_convert(fixture.request.operation_id, &[])
            .unwrap(),
        record
    );
}

#[rstest]
fn conflicting_delivery_keeps_the_original_verified_effect_and_plan() {
    let mut fixture = ConvertFixture::new();
    let applied = fixture.apply();
    fixture.request.net_cash = Money::from("101 USDC");
    let _ = fixture
        .engine
        .apply_verified_convert(fixture.request.clone(), &[]);
    let record = fixture
        .engine
        .convert_record(fixture.request.operation_id)
        .unwrap()
        .unwrap();

    assert!(record.applied);
    assert!(
        record
            .reason
            .as_deref()
            .unwrap()
            .to_lowercase()
            .contains("conflict")
    );
    assert_eq!(record.verified, applied.verified);
    assert_eq!(record.plan, applied.plan);
    assert_eq!(
        fixture.position(&fixture.yes_c).unwrap().cost_open,
        dec!(50)
    );
}

#[rstest]
fn conflicting_transaction_does_not_attach_any_of_its_locators() {
    let mut fixture = ConvertFixture::new();
    fixture.request.transaction.relayer_transaction_id = None;
    fixture.request.transaction.transaction_hash = Some("0xoriginal".into());
    let original = fixture.apply();
    assert!(original.applied);

    fixture.request.transaction.relayer_transaction_id = Some("contradicting-relayer".into());
    fixture.request.transaction.transaction_hash = Some("0xdifferent".into());
    let _ = fixture
        .engine
        .apply_verified_convert(fixture.request.clone(), &[]);
    let retained = fixture
        .engine
        .convert_record(fixture.request.operation_id)
        .unwrap()
        .unwrap();
    assert!(retained.applied);
    assert_eq!(retained.verified.transaction, original.verified.transaction);
    assert!(retained.reason.is_some());
}

#[rstest]
fn recovery_without_a_previously_fixed_plan_does_not_prevent_a_new_valid_attempt() {
    let mut fixture = ConvertFixture::new();
    let operation_id = fixture.request.operation_id;
    let blocked = fixture
        .engine
        .apply_verified_convert(fixture.request.clone(), &[fixture.no_b.id()])
        .unwrap();
    assert!(!blocked.applied);
    assert!(blocked.plan.is_none());

    let recovered = fixture
        .engine
        .recover_convert(operation_id, &[fixture.no_b.id()])
        .unwrap();
    assert!(!recovered.applied);
    assert!(recovered.plan.is_none());
    let retried = fixture.apply();
    assert!(retried.applied, "{:?}", retried.reason);
}

#[rstest]
fn redelivery_repairs_only_changes_missing_from_an_applied_record() {
    let mut fixture = ConvertFixture::new();
    let original = fixture.position(&fixture.no_a).unwrap();
    let initial = fixture.apply();
    assert!(initial.applied);
    fixture
        .engine
        .cache()
        .borrow_mut()
        .update_position(&original)
        .unwrap();

    let events = Rc::new(RefCell::new(Vec::new()));
    let captured = Rc::clone(&events);
    let handler =
        TypedHandler::from(move |event: &PositionEvent| captured.borrow_mut().push(event.clone()));
    msgbus::subscribe_position_events("events.position.*".into(), handler.clone(), None);
    let redelivered = fixture.apply();
    msgbus::unsubscribe_position_events("events.position.*".into(), &handler);

    assert!(redelivered.applied);
    assert_eq!(redelivered.plan, initial.plan);
    assert!(fixture.position(&fixture.no_a).unwrap().is_closed());
    assert!(
        matches!(events.borrow().as_slice(), [PositionEvent::PositionClosed(closed)] if closed.position_id == original.id)
    );
    assert_eq!(
        fixture.position(&fixture.yes_c).unwrap().quantity,
        Quantity::from("100.00")
    );
}

#[rstest]
fn recovery_keeps_all_unapplied_changes_pending_when_one_cycle_does_not_match() {
    let mut fixture = ConvertFixture::new();
    let original = fixture.position(&fixture.no_a).unwrap();
    let applied = fixture.apply();
    assert!(applied.applied);
    {
        let mut cache = fixture.engine.cache().borrow_mut();
        cache.update_position(&original).unwrap();
        cache
            .purge_position(fixture.position_id(&fixture.no_b), false)
            .unwrap();
    }
    fixture.seed(&fixture.no_b, OrderSide::Buy, "100.00", "0.700");

    let recovered = fixture
        .engine
        .recover_convert(fixture.request.operation_id, &[])
        .unwrap();
    assert!(!recovered.applied);
    assert_eq!(
        recovered.unapplied_instruments,
        vec![fixture.no_a.id(), fixture.no_b.id()]
    );
    assert!(recovered.reason.is_some());
    assert_eq!(recovered.plan, applied.plan);
    assert_eq!(
        fixture.position(&fixture.no_a).unwrap().quantity,
        original.quantity
    );
    assert_eq!(
        fixture.position(&fixture.yes_c).unwrap().cost_open,
        dec!(50)
    );
}

#[rstest]
fn recovery_waits_for_pending_input_evidence_before_applying_any_missing_changes() {
    let mut fixture = ConvertFixture::new();
    let original = fixture.position(&fixture.no_b).unwrap();
    let applied = fixture.apply();
    assert!(applied.applied);
    fixture
        .engine
        .cache()
        .borrow_mut()
        .update_position(&original)
        .unwrap();

    let pending = fixture
        .engine
        .recover_convert(fixture.request.operation_id, &[fixture.no_a.id()])
        .unwrap();
    assert!(!pending.applied);
    assert_eq!(pending.unapplied_instruments, vec![fixture.no_b.id()]);
    assert_eq!(
        fixture.position(&fixture.no_b).unwrap().quantity,
        original.quantity
    );

    let recovered = fixture
        .engine
        .recover_convert(fixture.request.operation_id, &[])
        .unwrap();
    assert!(recovered.applied, "{:?}", recovered.reason);
    assert_eq!(recovered.plan, applied.plan);
}

#[rstest]
#[case::identical(false)]
#[case::conflicting(true)]
fn cache_batch_does_not_prepare_multiple_changes_against_the_same_position(#[case] conflict: bool) {
    let mut fixture = ConvertFixture::new();
    let original = fixture.position(&fixture.no_a).unwrap();
    let mut record = fixture.apply();
    assert!(record.applied);
    let change = record
        .plan
        .take()
        .unwrap()
        .into_iter()
        .find(|change| change.position_id == original.id)
        .unwrap();
    let mut cache = fixture.engine.cache().borrow_mut();
    cache.update_position(&original).unwrap();

    let mut repeated = change;
    if conflict {
        repeated.realized_pnl = dec!(1);
    }

    let result = cache.apply_position_converts(&[change, repeated]);
    let position = cache.position(&original.id).unwrap();

    if conflict {
        assert!(result.is_err());
        assert_eq!(position.quantity, original.quantity);
    } else {
        assert_eq!(result.unwrap().len(), 1);
        assert!(position.is_closed());
    }
}

#[rstest]
fn recovery_does_not_reopen_an_applied_output_after_closed_position_retention() {
    let mut fixture = ConvertFixture::new();
    fixture.seed(&fixture.no_a, OrderSide::Buy, "150.00", "0.800");
    fixture.seed(&fixture.no_b, OrderSide::Buy, "150.00", "0.700");
    let initial = fixture.apply();
    assert!(initial.applied);

    let mut output = fixture.position(&fixture.yes_c).unwrap();
    let fill = OrderFilledSpec::builder()
        .trader_id(output.trader_id)
        .strategy_id(output.strategy_id)
        .account_id(output.account_id)
        .instrument_id(output.instrument_id)
        .position_id(output.id)
        .order_side(OrderSide::Sell)
        .last_qty(output.quantity)
        .last_px(Price::from("0.700"))
        .currency(output.quote_currency)
        .event_id(UUID4::new())
        .ts_event(UnixNanos::from(200))
        .build();
    output.try_apply(&fill).unwrap();
    {
        let mut cache = fixture.engine.cache().borrow_mut();
        cache.update_position(&output).unwrap();
        cache
            .purge_closed_positions(UnixNanos::from(1_000), 0, false)
            .unwrap();
    }
    assert!(fixture.position(&fixture.yes_c).is_none());

    let recovered = fixture
        .engine
        .recover_convert(fixture.request.operation_id, &[])
        .unwrap();
    assert!(!recovered.applied);
    assert_eq!(recovered.unapplied_instruments, vec![fixture.yes_c.id()]);
    assert!(recovered.reason.is_some());
    let _ = fixture.apply();
    assert!(fixture.position(&fixture.yes_c).is_none());
}

#[rstest]
fn replay_with_a_missing_plan_never_recognizes_the_same_assets_twice() {
    let mut fixture = ConvertFixture::new();
    let mut record = fixture.apply();
    assert!(record.applied);
    record.plan = None;
    fixture
        .engine
        .cache()
        .borrow_mut()
        .add(
            &ConvertRecord::cache_key(record.verified.operation_id),
            serde_json::to_vec(&record).unwrap().into(),
        )
        .unwrap();

    let recovered = fixture
        .engine
        .recover_convert(record.verified.operation_id, &[])
        .unwrap();
    assert!(!recovered.applied);
    assert!(recovered.plan.is_none());
    assert!(recovered.reason.is_some());
    let redelivered = fixture.apply();
    assert!(!redelivered.applied);
    assert!(redelivered.plan.is_none());
    assert_eq!(
        fixture.position(&fixture.yes_c).unwrap().cost_open,
        dec!(50)
    );
}

#[rstest]
fn verified_convert_inventory_sells_normally_and_portfolio_accounts_for_each_cycle() {
    let mut fixture = ConvertFixture::new();
    let cache = Rc::clone(fixture.engine.cache());
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    let mut portfolio = Portfolio::new(clock.clone(), Rc::clone(&cache), None);
    let account_id = fixture.request.account_id;
    let currency = fixture.yes_c.cost_currency();
    let funds = Money::from("1000 USDC");
    portfolio.update_account(&AccountState::new(
        account_id,
        AccountType::Cash,
        vec![AccountBalance::new(funds, Money::zero(currency), funds)],
        Vec::new(),
        true,
        UUID4::new(),
        UnixNanos::from(1),
        UnixNanos::from(1),
        Some(currency),
    ));
    let applied = fixture.apply();
    assert!(applied.applied, "{:?}", applied.reason);
    assert_eq!(
        cache
            .borrow()
            .account(&account_id)
            .unwrap()
            .balance(Some(currency))
            .unwrap()
            .total,
        funds
    );

    let quantity = Quantity::from("100.00");
    let mut order = OrderTestBuilder::new(OrderType::Market)
        .trader_id(fixture.request.trader_id)
        .strategy_id(fixture.request.strategy_id)
        .instrument_id(fixture.yes_c.id())
        .side(OrderSide::Sell)
        .quantity(quantity)
        .build();
    order
        .apply(TestOrderEventStubs::submitted(&order, account_id))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    let commission = Money::from("1 USDC");

    let mut matching = OrderMatchingEngine::new(
        fixture.yes_c.clone(),
        1,
        FillModelHandle::default(),
        FeeModelAny::Fixed(FixedFeeModel::new(commission, None).unwrap()).into(),
        BookType::L1_MBP,
        OmsType::Netting,
        AccountType::Cash,
        clock,
        Rc::clone(&cache),
        Default::default(),
    );
    let pending = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::clone(&pending);
    matching.set_event_handler(Rc::new(move |event| events.borrow_mut().push(event)));
    matching.process_quote_tick(&QuoteTick::new(
        fixture.yes_c.id(),
        Price::from("0.700"),
        Price::from("0.710"),
        quantity,
        quantity,
        UnixNanos::from(200),
        UnixNanos::from(200),
    ));
    matching.process_order(&mut order, account_id);

    for event in pending.borrow_mut().drain(..) {
        fixture.engine.process(&event);
    }

    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Filled
    );
    let sold = fixture.position(&fixture.yes_c).unwrap();
    assert!(sold.is_closed());
    assert_eq!(sold.events.len(), 1);
    assert_eq!(sold.events[0].commission, Some(commission));
    assert_eq!(sold.realized_pnl, Some(Money::from("19 USDC")));
    assert_eq!(
        portfolio.realized_pnl(&fixture.yes_c.id()),
        sold.realized_pnl
    );
    let realized = portfolio.recorded_realized_pnls();
    let cycles = &realized[&currency];
    assert_eq!(cycles.len(), 3);
    assert!(
        cycles
            .iter()
            .any(|(id, _, pnl)| *id == sold.id && (*pnl - 19.0).abs() < f64::EPSILON)
    );
}

fn outcome(id: &str) -> InstrumentAny {
    let mut instrument = binary_option();
    instrument.id = InstrumentId::from(id);
    InstrumentAny::BinaryOption(instrument)
}

fn amount(instrument: &InstrumentAny, quantity: Decimal) -> ConvertTokenAmount {
    ConvertTokenAmount {
        instrument_id: instrument.id(),
        quantity,
    }
}
