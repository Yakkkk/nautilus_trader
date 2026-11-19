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

use nautilus_backtest::{
    config::{BacktestEngineConfig, SimulatedVenueConfig},
    engine::BacktestEngine,
};
use nautilus_common::{
    actor::DataActor,
    messages::execution::{
        ConvertExecutionEvent, ConvertExecutionOutcome, ConvertExecutionStatus, ConvertResult,
        ConvertTokenAmount, TradingCommand,
    },
    msgbus::{self, MessagingSwitchboard},
};
use nautilus_core::{DurationNanos, UUID4, UnixNanos};
use nautilus_execution::{
    engine::polymarket_convert::convert_position_id,
    models::{
        fee::{FeeModelAny, PerContractFeeModel},
        latency::{LatencyModel, LatencyModelHandle},
    },
};
use nautilus_model::{
    accounts::Account,
    data::{Data, QuoteTick},
    enums::{AccountType, AssetClass, BookType, OmsType, OrderSide, OrderStatus},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, Symbol, Venue},
    instruments::{
        BinaryOption, Instrument, InstrumentAny,
        neg_risk::{CtfNegRiskEvent, NegRiskCondition, NegRiskOutcome},
    },
    orders::Order,
    types::{Currency, Money, Price, Quantity},
};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use rstest::rstest;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scenario {
    RoundTrip,
    Fee,
    AllCash,
    Competing,
    MissingMapping,
    InexactOutput,
    InterleavedSell,
    RestingSellBeforeArrival,
    PendingSellResize,
    PendingShortOutput,
}

impl Scenario {
    fn fee_bps(self) -> u16 {
        if matches!(self, Self::Fee | Self::InexactOutput) {
            250
        } else {
            0
        }
    }

    fn yes_quantity(self) -> Quantity {
        if self == Self::Fee {
            Quantity::from("97.500000")
        } else {
            Quantity::from("100.000000")
        }
    }
}

#[derive(Debug)]
struct ConvertStrategy {
    core: StrategyCore,
    scenario: Scenario,
    no: [InstrumentId; 3],
    yes: InstrumentId,
    account_id: AccountId,
    client_id: ClientId,
    operation_id: UUID4,
    competing_id: UUID4,
    results: Rc<RefCell<Vec<ConvertResult>>>,
    yes_cost_on_applied: Rc<RefCell<Option<Decimal>>>,
    queried: bool,
    resting_sell: Option<ClientOrderId>,
}

impl ConvertStrategy {
    fn trade(
        &mut self,
        instrument: InstrumentId,
        side: OrderSide,
        quantity: Quantity,
    ) -> anyhow::Result<()> {
        let order = self.order().market(
            instrument, side, quantity, None, None, None, None, None, None, None,
        );
        self.submit_order(order, None, None, None)
    }

    fn submit_resting_sell(&mut self, quantity: Quantity) -> anyhow::Result<()> {
        let order = self.order().limit(
            self.no[0],
            OrderSide::Sell,
            quantity,
            Price::from("0.90"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.resting_sell = Some(order.client_order_id());
        self.submit_order(order, None, None, None)
    }
}

nautilus_strategy!(ConvertStrategy, {
    fn on_convert_result(&mut self, result: ConvertResult) {
        let applied = result.operation_id == self.operation_id && result.applied;
        self.results.borrow_mut().push(result);

        if applied
            && !self.queried
            && matches!(
                self.scenario,
                Scenario::RoundTrip
                    | Scenario::Fee
                    | Scenario::AllCash
                    | Scenario::Competing
                    | Scenario::InterleavedSell
            )
        {
            self.queried = true;

            if self.scenario != Scenario::AllCash {
                let position_id = convert_position_id(self.yes, StrategyId::from("CONVERT-001"));
                let position = self.cache().position(&position_id).unwrap();
                assert_eq!(position.quantity, self.scenario.yes_quantity());
                assert_eq!(position.opening_operation_id, Some(self.operation_id));
                assert_eq!(position.opening_order_id, None);
                assert!(position.events.is_empty());
                *self.yes_cost_on_applied.borrow_mut() = Some(position.cost_open);

                self.trade(self.yes, OrderSide::Sell, self.scenario.yes_quantity())
                    .unwrap();
            }

            self.query_convert(self.operation_id, self.account_id, self.client_id)
                .unwrap();
        }
    }
});

impl DataActor for ConvertStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_quotes(self.no[0], None, None);
        Ok(())
    }

    #[expect(
        clippy::panic_in_result_fn,
        reason = "test assertions verify the scripted scenario"
    )]
    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        let buy_count = if self.scenario == Scenario::AllCash {
            3
        } else {
            2
        };

        match quote.ts_init.as_u64() {
            4_000_000_000 => {
                for instrument_id in self.no.into_iter().take(buy_count) {
                    self.trade(instrument_id, OrderSide::Buy, Quantity::from("100.000000"))?;
                }
            }
            5_000_000_000 if self.scenario == Scenario::PendingSellResize => {
                self.submit_resting_sell(Quantity::from("50.000000"))?;
            }
            6_000_000_000 => {
                if self.scenario == Scenario::PendingSellResize {
                    self.modify_order(
                        self.resting_sell.unwrap(),
                        Some(Quantity::from("100.000000")),
                        None,
                        None,
                        None,
                        None,
                    )?;
                }

                if self.scenario == Scenario::PendingShortOutput {
                    self.trade(self.yes, OrderSide::Sell, Quantity::from("100.000000"))?;
                }

                let quantity = if self.scenario == Scenario::PendingSellResize {
                    dec!(50)
                } else {
                    dec!(100)
                };
                let inputs: Vec<_> = self.no[..buy_count]
                    .iter()
                    .map(|&instrument_id| ConvertTokenAmount {
                        instrument_id,
                        quantity,
                    })
                    .collect();
                let operation_id = self.submit_convert(
                    inputs.clone(),
                    self.account_id,
                    self.client_id,
                    Some(self.operation_id),
                )?;
                assert_eq!(operation_id, self.operation_id);

                if self.scenario == Scenario::Competing {
                    self.submit_convert(
                        inputs.clone(),
                        self.account_id,
                        self.client_id,
                        Some(operation_id),
                    )?;
                    self.submit_convert(
                        inputs,
                        self.account_id,
                        self.client_id,
                        Some(self.competing_id),
                    )?;
                }

                if self.scenario == Scenario::InterleavedSell {
                    self.trade(self.no[0], OrderSide::Sell, Quantity::from("100.000000"))?;
                }
            }
            7_000_000_000 if self.scenario == Scenario::RestingSellBeforeArrival => {
                self.submit_resting_sell(Quantity::from("100.000000"))?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ConvertLatency;

impl LatencyModel for ConvertLatency {
    fn get_insert_latency(
        &self,
        command: &TradingCommand,
        _ts_now: UnixNanos,
    ) -> anyhow::Result<DurationNanos> {
        if matches!(command, TradingCommand::SubmitConvert(_)) {
            Ok(DurationNanos::new(2_000_000_000))
        } else {
            Ok(DurationNanos::default())
        }
    }

    fn get_update_latency(
        &self,
        _command: &TradingCommand,
        _ts_now: UnixNanos,
    ) -> anyhow::Result<DurationNanos> {
        Ok(DurationNanos::default())
    }

    fn get_delete_latency(
        &self,
        _command: &TradingCommand,
        _ts_now: UnixNanos,
    ) -> anyhow::Result<DurationNanos> {
        Ok(DurationNanos::default())
    }

    fn get_base_latency(&self) -> DurationNanos {
        DurationNanos::default()
    }
}

fn outcome_instrument(name: &str, size_increment: Quantity) -> InstrumentAny {
    BinaryOption::builder()
        .instrument_id(InstrumentId::from(format!("{name}.POLYMARKET")))
        .raw_symbol(Symbol::from(name))
        .asset_class(AssetClass::Alternative)
        .currency(Currency::pUSD())
        .activation_ns(UnixNanos::default())
        .expiration_ns(UnixNanos::from(60_000_000_000_u64))
        .price_precision(2)
        .size_precision(size_increment.precision)
        .price_increment(Price::from("0.01"))
        .size_increment(size_increment)
        .ts_event(UnixNanos::default())
        .ts_init(UnixNanos::default())
        .build()
        .unwrap()
        .into()
}

fn quote(
    instrument_id: InstrumentId,
    bid: &str,
    ask: &str,
    seconds: u64,
    size_precision: u8,
) -> Data {
    let timestamp = UnixNanos::from(seconds * 1_000_000_000);
    let size = Quantity::from_decimal_dp(dec!(200), size_precision).unwrap();
    Data::Quote(QuoteTick::new(
        instrument_id,
        Price::from(bid),
        Price::from(ask),
        size,
        size,
        timestamp,
        timestamp,
    ))
}

#[rstest]
#[case::round_trip(Scenario::RoundTrip, true)]
#[case::immediate_round_trip(Scenario::RoundTrip, false)]
#[case::convert_fee(Scenario::Fee, true)]
#[case::all_cash(Scenario::AllCash, true)]
#[case::competing_requests(Scenario::Competing, true)]
#[case::immediate_competing_requests(Scenario::Competing, false)]
#[case::missing_mapping(Scenario::MissingMapping, true)]
#[case::unrepresentable_net_output(Scenario::InexactOutput, true)]
#[case::interleaved_sell(Scenario::InterleavedSell, true)]
#[case::resting_sell_before_convert_arrival(Scenario::RestingSellBeforeArrival, true)]
#[case::immediate_pending_sell_resize(Scenario::PendingSellResize, false)]
#[case::immediate_pending_short_output(Scenario::PendingShortOutput, false)]
fn test_polymarket_convert_in_backtest(#[case] scenario: Scenario, #[case] use_queue: bool) {
    let mut engine = BacktestEngine::new(BacktestEngineConfig {
        bypass_logging: true,
        run_analysis: false,
        ..Default::default()
    })
    .unwrap();
    let venue = Venue::new("POLYMARKET");
    let account_id = AccountId::from("POLYMARKET-001");
    let client_id = ClientId::from("POLYMARKET");
    let currency = Currency::pUSD();
    let fee_model =
        PerContractFeeModel::new(Money::from_decimal(dec!(0.01), currency).unwrap()).unwrap();

    let account_type = if scenario == Scenario::PendingShortOutput {
        AccountType::Margin
    } else {
        AccountType::Cash
    };

    let mut config = SimulatedVenueConfig::builder()
        .venue(venue)
        .oms_type(OmsType::Netting)
        .account_type(account_type)
        .book_type(BookType::L1_MBP)
        .base_currency(currency)
        .starting_balances(vec![Money::from_decimal(dec!(1000), currency).unwrap()])
        .fee_model(FeeModelAny::PerContract(fee_model).into())
        .use_message_queue(use_queue)
        .build()
        .unwrap();

    if scenario == Scenario::RestingSellBeforeArrival {
        config.latency_model = Some(LatencyModelHandle::new(ConvertLatency));
    }
    engine.add_venue(config).unwrap();

    let increment = Quantity::from("0.000001");
    let no = ["NO-A", "NO-B", "NO-C"].map(|name| outcome_instrument(name, increment));
    let yes = ["YES-A", "YES-B", "YES-C"].map(|name| {
        let increment = if scenario == Scenario::InexactOutput && name == "YES-C" {
            Quantity::from(1)
        } else {
            increment
        };
        outcome_instrument(name, increment)
    });

    for instrument in no.iter().chain(&yes) {
        engine.add_instrument(instrument).unwrap();
    }

    let no_size_precision = no[0].size_precision();
    let yes_c_size_precision = yes[2].size_precision();
    let no = no.map(|instrument| instrument.id());
    let yes = yes.map(|instrument| instrument.id());

    if scenario != Scenario::MissingMapping {
        let conditions = (0_u8..3)
            .map(|index| NegRiskCondition {
                index,
                condition_id: [index + 1; 32],
                yes: NegRiskOutcome {
                    token_id: [index * 2 + 1; 32],
                    instrument_id: yes[usize::from(index)],
                },
                no: NegRiskOutcome {
                    token_id: [index * 2 + 2; 32],
                    instrument_id: no[usize::from(index)],
                },
            })
            .collect();
        let mut market_id = [0; 32];
        market_id[0] = 1;
        let event =
            CtfNegRiskEvent::new_checked(market_id, scenario.fee_bps(), conditions).unwrap();
        engine
            .kernel()
            .cache
            .borrow_mut()
            .add_convert_event(event)
            .unwrap();
    }

    let operation_id = UUID4::new();
    let competing_id = UUID4::new();
    let strategy_id = StrategyId::from("CONVERT-001");
    let results = Rc::new(RefCell::new(Vec::new()));
    let yes_cost_on_applied = Rc::new(RefCell::new(None));

    let strategy = ConvertStrategy {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(strategy_id),
            order_id_tag: Some("001".to_owned()),
            ..Default::default()
        }),
        scenario,
        no,
        yes: yes[2],
        account_id,
        client_id,
        operation_id,
        competing_id,
        results: Rc::clone(&results),
        yes_cost_on_applied: Rc::clone(&yes_cost_on_applied),
        queried: false,
        resting_sell: None,
    };
    engine.add_strategy(strategy).unwrap();

    let mut data = vec![
        quote(no[0], "0.79", "0.80", 1, no_size_precision),
        quote(no[1], "0.69", "0.70", 2, no_size_precision),
        quote(no[2], "0.09", "0.10", 3, no_size_precision),
        quote(no[0], "0.79", "0.80", 4, no_size_precision),
        quote(yes[2], "0.70", "0.71", 5, yes_c_size_precision),
        quote(no[0], "0.79", "0.80", 6, no_size_precision),
    ];

    if scenario == Scenario::RestingSellBeforeArrival {
        data.extend([
            quote(no[0], "0.79", "0.80", 7, no_size_precision),
            quote(no[0], "0.79", "0.80", 8, no_size_precision),
            quote(no[0], "0.90", "0.91", 9, no_size_precision),
        ]);
    }

    if scenario == Scenario::PendingSellResize {
        data.extend([
            quote(no[0], "0.79", "0.80", 5, no_size_precision),
            quote(no[0], "0.90", "0.91", 7, no_size_precision),
        ]);
    }
    engine.add_data(data, None, true, true).unwrap();
    engine.run(None, None, None, false).unwrap();

    let record = engine
        .kernel()
        .exec_engine
        .borrow()
        .convert_record(operation_id)
        .unwrap();

    if let Some(record) = &record {
        let replay = ConvertExecutionEvent {
            operation_id,
            account_id,
            client_id,
            outcome: ConvertExecutionOutcome::Confirmed {
                verified: Box::new(record.verified.clone()),
                pending_inputs: Vec::new(),
            },
        };
        msgbus::send_any(MessagingSwitchboard::exec_engine_process_convert(), &replay);
    }

    let cache = engine.kernel().cache.borrow();
    let cash = cache
        .account(&account_id)
        .unwrap()
        .balance_total(Some(currency))
        .unwrap()
        .as_decimal();
    let first = cache
        .position_owned(&convert_position_id(no[0], strategy_id))
        .unwrap();
    let second = cache
        .position_owned(&convert_position_id(no[1], strategy_id))
        .unwrap();
    let orders = cache.orders(Some(&venue), None, None, None, None);
    let results = results.borrow();
    let operation = cache.load_convert_operation(operation_id).unwrap().unwrap();

    if matches!(
        scenario,
        Scenario::RestingSellBeforeArrival | Scenario::PendingSellResize
    ) {
        assert!(operation.dispatched);
        assert_eq!(operation.status, Some(ConvertExecutionStatus::Rejected));
        assert!(!operation.applied);
        assert!(record.is_none());
        assert!(first.is_closed());
        assert_eq!(first.events.len(), 2);
        assert_eq!(first.events[1].order_side, OrderSide::Sell);
        assert_eq!(first.events[1].last_qty.as_decimal(), dec!(100));
        assert_eq!(first.events[1].last_px, Price::from("0.90"));
        assert_eq!(second.quantity.as_decimal(), dec!(100));
        assert!(
            cache
                .position_owned(&convert_position_id(yes[2], strategy_id))
                .is_none()
        );
        assert_eq!(orders.len(), 3);
        assert_eq!(cash, dec!(937));
        return;
    }

    if scenario == Scenario::PendingShortOutput {
        assert!(operation.dispatched);
        assert_eq!(operation.status, Some(ConvertExecutionStatus::Rejected));
        assert!(!operation.applied);
        assert!(record.is_none());
        assert_eq!(first.signed_qty, dec!(100));
        assert_eq!(second.signed_qty, dec!(100));
        let output = cache
            .position_owned(&convert_position_id(yes[2], strategy_id))
            .unwrap();
        assert_eq!(output.signed_qty, dec!(-100));
        assert_eq!(output.events.len(), 1);
        assert_eq!(orders.len(), 3);
        assert_eq!(cash, dec!(917));
        return;
    }

    if matches!(scenario, Scenario::MissingMapping | Scenario::InexactOutput) {
        assert_eq!(operation.status, Some(ConvertExecutionStatus::Rejected));
        assert!(!operation.applied);
        let reason = operation.reason.as_deref().unwrap();
        let expected = if scenario == Scenario::MissingMapping {
            "mapping"
        } else {
            "represent"
        };
        assert!(reason.contains(expected), "{reason}");
        assert_eq!(first.quantity.as_decimal(), dec!(100));
        assert_eq!(second.quantity.as_decimal(), dec!(100));
        assert!(
            cache
                .position_owned(&convert_position_id(yes[2], strategy_id))
                .is_none()
        );
        assert!(record.is_none());
        assert_eq!(orders.len(), 2);
        assert_eq!(cash, dec!(848));
        return;
    }

    assert!(operation.applied, "{operation:?}");
    assert_eq!(operation.status, Some(ConvertExecutionStatus::Confirmed));
    assert_eq!(
        results
            .iter()
            .filter(|result| result.operation_id == operation_id && result.applied)
            .count(),
        2,
    );
    assert!(first.is_closed());
    assert!(second.is_closed());
    assert_eq!(first.events.len(), 1);
    assert_eq!(second.events.len(), 1);
    assert_eq!(first.commissions()[0].as_decimal(), dec!(1));
    assert_eq!(second.commissions()[0].as_decimal(), dec!(1));

    let record = record.unwrap();
    let plan = record.plan.unwrap();
    let cost = |instrument_id| {
        plan.iter()
            .find(|change| change.instrument_id == instrument_id)
            .unwrap()
            .cost
    };
    assert_eq!(cost(no[0]), dec!(80));
    assert_eq!(cost(no[1]), dec!(70));

    if scenario == Scenario::Competing {
        let competing = cache.load_convert_operation(competing_id).unwrap().unwrap();
        assert!(
            competing.dispatched,
            "test must reach the simulated exchange"
        );
        assert_eq!(competing.status, Some(ConvertExecutionStatus::Rejected));
        assert!(!competing.applied);
        let reason = competing.reason.unwrap();
        assert!(reason.contains("inventory"), "{reason}");
    }

    if scenario == Scenario::AllCash {
        let third = cache
            .position_owned(&convert_position_id(no[2], strategy_id))
            .unwrap();
        assert!(third.is_closed());
        assert_eq!(third.events.len(), 1);
        assert_eq!(cost(no[2]), dec!(10));
        assert_eq!(record.verified.net_cash.as_decimal(), dec!(200));
        assert!(record.verified.outputs.is_empty());
        assert_eq!(
            plan.iter()
                .map(|change| change.realized_pnl)
                .sum::<Decimal>(),
            dec!(40)
        );
        assert_eq!(orders.len(), 3);
        assert_eq!(cash, dec!(1037));
    } else {
        let position = cache
            .position_owned(&convert_position_id(yes[2], strategy_id))
            .unwrap();
        let (net_cash, yes_cost, pnl, commission, balance) = if scenario == Scenario::Fee {
            (
                dec!(97.5),
                dec!(52.5),
                dec!(15.75),
                dec!(0.975),
                dec!(1012.775),
            )
        } else {
            (dec!(100), dec!(50), dec!(20), dec!(1), dec!(1017))
        };
        assert_eq!(record.verified.net_cash.as_decimal(), net_cash);
        assert_eq!(cost(yes[2]), yes_cost);
        assert_eq!(*yes_cost_on_applied.borrow(), Some(yes_cost));
        assert!(position.is_closed());
        assert_eq!(position.events.len(), 1);
        let sale = &position.events[0];
        assert_eq!(sale.order_side, OrderSide::Sell);
        assert_eq!(sale.last_qty, scenario.yes_quantity());
        assert_eq!(sale.last_px, Price::from("0.70"));
        assert_eq!(position.commissions()[0].as_decimal(), commission);
        assert_eq!(
            position.realized_pnl.unwrap().as_decimal(),
            pnl - commission
        );
        assert_eq!(cash, balance);
        assert_eq!(
            orders
                .iter()
                .filter(|order| order.status() == OrderStatus::Filled)
                .count(),
            3
        );
        assert_eq!(
            orders.len(),
            if scenario == Scenario::InterleavedSell {
                4
            } else {
                3
            }
        );
    }
}
