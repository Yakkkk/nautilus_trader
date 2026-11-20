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

use nautilus_common::{
    actor::registry::{ActorRef, try_get_actor_unchecked},
    messages::execution::{
        ConvertExecutionEvent, ConvertExecutionOutcome, ConvertExecutionStatus,
        ConvertOperationRecord, ConvertResult, ConvertTokenAmount, ConvertTransaction,
        QueryConvert, SubmitConvert, VerifiedConvert,
    },
};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    accounts::CashAccount,
    data::InstrumentClose,
    enums::{InstrumentCloseType, TradingState},
    events::{PositionOpened, order::spec::OrderFilledSpec},
    instruments::{
        neg_risk::{ConvertScope, CtfNegRiskEvent, NegRiskCondition, NegRiskOutcome},
        stubs::binary_option,
    },
    position::Position,
    reports::ExecutionMassStatus,
};

use super::*;
use crate::{execution::emitter::ExecutionEventEmitter, runner::AsyncRunnerChannels};

#[derive(Debug, Default)]
struct ConvertCallbacks {
    results: Vec<ConvertResult>,
    opened: Vec<PositionOpened>,
}

#[derive(Debug)]
struct ConvertStrategy {
    core: StrategyCore,
    callbacks: Rc<RefCell<ConvertCallbacks>>,
}

impl DataActor for ConvertStrategy {}

nautilus_strategy!(ConvertStrategy, {
    fn on_position_opened(&mut self, event: PositionOpened) {
        self.callbacks.borrow_mut().opened.push(event);
    }

    fn on_convert_result(&mut self, result: ConvertResult) {
        self.callbacks.borrow_mut().results.push(result);
    }
});

#[derive(Default)]
struct ClientCalls {
    submits: Vec<(SubmitConvert, ConvertScope)>,
    queries: Vec<(QueryConvert, ConvertTransaction)>,
    orders: Vec<SubmitOrder>,
}

struct ConvertClient {
    account_id: AccountId,
    client_id: ClientId,
    calls: Rc<RefCell<ClientCalls>>,
    fail_dispatch: Rc<Cell<bool>>,
    reporting_cache: Option<Rc<RefCell<Cache>>>,
    connected: bool,
}

#[async_trait::async_trait(?Send)]
impl ExecutionClient for ConvertClient {
    fn is_connected(&self) -> bool {
        self.connected
    }

    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn account_id(&self) -> AccountId {
        self.account_id
    }

    fn venue(&self) -> Venue {
        Venue::from("POLYMARKET")
    }

    fn oms_type(&self) -> OmsType {
        OmsType::Netting
    }

    fn get_account(&self) -> Option<AccountAny> {
        None
    }

    fn generate_account_state(
        &self,
        _balances: Vec<AccountBalance>,
        _margins: Vec<MarginBalance>,
        _reported: bool,
        _ts_event: UnixNanos,
        _info: Option<Params>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        Ok(())
    }

    fn submit_convert(&self, command: SubmitConvert, scope: ConvertScope) -> anyhow::Result<()> {
        self.calls.borrow_mut().submits.push((command, scope));
        anyhow::ensure!(!self.fail_dispatch.get(), "client could not dispatch");
        Ok(())
    }

    fn query_convert(
        &self,
        command: QueryConvert,
        transaction: &ConvertTransaction,
    ) -> anyhow::Result<()> {
        self.calls
            .borrow_mut()
            .queries
            .push((command, transaction.clone()));
        Ok(())
    }

    async fn generate_mass_status(
        &self,
        _lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        if let Some(cache) = &self.reporting_cache {
            cache
                .borrow()
                .prepare_convert_position_report(self.account_id, None)?;
        }
        Ok(None)
    }

    fn submit_order(&self, command: SubmitOrder) -> anyhow::Result<()> {
        self.calls.borrow_mut().orders.push(command);
        Ok(())
    }
}

struct ConvertFixture {
    node: LiveNode,
    channels: AsyncRunnerChannels,
    emitter: ExecutionEventEmitter,
    callbacks: Rc<RefCell<ConvertCallbacks>>,
    other_callbacks: Rc<RefCell<ConvertCallbacks>>,
    calls: Rc<RefCell<ClientCalls>>,
    fail_dispatch: Rc<Cell<bool>>,
    strategy_id: StrategyId,
    other_strategy_id: StrategyId,
    account_id: AccountId,
    client_id: ClientId,
    no_a: InstrumentId,
    no_b: InstrumentId,
    no_c: InstrumentId,
    yes_c: InstrumentId,
}

impl ConvertFixture {
    fn new(register_mapping: bool) -> Self {
        Self::with_primary_strategy(register_mapping, |node, strategy_id, callbacks| {
            Self::register_strategy(node, strategy_id, callbacks);
        })
    }

    fn with_primary_strategy(
        register_mapping: bool,
        register: impl FnOnce(&mut LiveNode, StrategyId, Rc<RefCell<ConvertCallbacks>>),
    ) -> Self {
        let mut node = LiveNode::build("Convert".to_string(), None).unwrap();
        let account_id = AccountId::from("POLYMARKET-001");
        let client_id = ClientId::from("POLYMARKET");
        let strategy_id = StrategyId::from("CONVERT-001");
        let other_strategy_id = StrategyId::from("OTHER-002");
        let callbacks = Rc::new(RefCell::new(ConvertCallbacks::default()));
        let other_callbacks = Rc::new(RefCell::new(ConvertCallbacks::default()));
        let calls = Rc::new(RefCell::new(ClientCalls::default()));
        let fail_dispatch = Rc::new(Cell::new(false));
        let mut conditions = Vec::new();

        for (index, label) in ["A", "B", "C"].into_iter().enumerate() {
            let index = u8::try_from(index).unwrap();
            let outcome = |side, token| {
                let mut instrument = binary_option();
                instrument.id = InstrumentId::from(format!("{side}-{label}.POLYMARKET"));
                let instrument_id = instrument.id;
                node.kernel
                    .cache
                    .borrow_mut()
                    .add_instrument(InstrumentAny::BinaryOption(instrument))
                    .unwrap();
                NegRiskOutcome {
                    token_id: [token; 32],
                    instrument_id,
                }
            };
            conditions.push(NegRiskCondition {
                index,
                condition_id: [index + 1; 32],
                yes: outcome("YES", index * 2 + 1),
                no: outcome("NO", index * 2 + 2),
            });
        }

        let no_a = conditions[0].no.instrument_id;
        let no_b = conditions[1].no.instrument_id;
        let no_c = conditions[2].no.instrument_id;
        let yes_c = conditions[2].yes.instrument_id;

        if register_mapping {
            let mut market_id = [1; 32];
            market_id[31] = 0;
            node.kernel
                .cache
                .borrow_mut()
                .add_convert_event(CtfNegRiskEvent::new_checked(market_id, 0, conditions).unwrap())
                .unwrap();
        }

        register(&mut node, strategy_id, callbacks.clone());
        Self::register_strategy(&mut node, other_strategy_id, other_callbacks.clone());
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(LiveExecutionClient::new(Box::new(
                ConvertClient {
                    account_id,
                    client_id,
                    calls: calls.clone(),
                    fail_dispatch: fail_dispatch.clone(),
                    reporting_cache: None,
                    connected: true,
                },
            ))))
            .unwrap();
        node.kernel.trader.borrow_mut().initialize().unwrap();
        node.kernel.start_trader().unwrap();

        let runner = node.runner.take().unwrap();
        runner.bind_senders_for_node(node.handle.clone());
        let channels = runner.take_channels();
        let mut emitter = ExecutionEventEmitter::new(
            get_atomic_clock_realtime(),
            node.trader_id(),
            account_id,
            AccountType::Cash,
            Some(Currency::USDC()),
        );
        emitter.set_sender(get_exec_event_sender());
        node.kernel
            .cache
            .borrow_mut()
            .add_account(AccountAny::Cash(CashAccount::new(
                AccountState::new(
                    account_id,
                    AccountType::Cash,
                    vec![AccountBalance::new(
                        Money::from("1000 USDC"),
                        Money::from("0 USDC"),
                        Money::from("1000 USDC"),
                    )],
                    Vec::new(),
                    true,
                    UUID4::new(),
                    UnixNanos::default(),
                    UnixNanos::default(),
                    Some(Currency::USDC()),
                ),
                true,
                false,
            )))
            .unwrap();

        let fixture = Self {
            node,
            channels,
            emitter,
            callbacks,
            other_callbacks,
            calls,
            fail_dispatch,
            strategy_id,
            other_strategy_id,
            account_id,
            client_id,
            no_a,
            no_b,
            no_c,
            yes_c,
        };
        fixture.seed(no_a, OrderSide::Buy, "0.800");
        fixture.seed(no_b, OrderSide::Buy, "0.700");
        fixture
    }

    fn register_strategy(
        node: &mut LiveNode,
        strategy_id: StrategyId,
        callbacks: Rc<RefCell<ConvertCallbacks>>,
    ) {
        node.add_strategy(ConvertStrategy {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(strategy_id),
                oms_type: Some(OmsType::Netting),
                ..Default::default()
            }),
            callbacks,
        })
        .unwrap();
    }

    fn position_id(&self, instrument_id: InstrumentId) -> PositionId {
        PositionId::new(format!("{instrument_id}-{}", self.strategy_id))
    }

    fn seed(&self, instrument_id: InstrumentId, side: OrderSide, price: &str) {
        let mut cache = self.node.kernel.cache.borrow_mut();
        let instrument = cache.instrument(&instrument_id).unwrap();
        let fill = OrderFilledSpec::builder()
            .trader_id(self.node.trader_id())
            .strategy_id(self.strategy_id)
            .account_id(self.account_id)
            .instrument_id(instrument_id)
            .position_id(self.position_id(instrument_id))
            .client_order_id(ClientOrderId::new(format!("O-{}", UUID4::new())))
            .order_side(side)
            .last_qty(Quantity::from("100.00"))
            .last_px(Price::from(price))
            .currency(instrument.quote_currency())
            .build();
        let position = Position::new(instrument, fill);
        cache.add_position(&position, OmsType::Netting).unwrap();
    }

    fn inputs(&self) -> Vec<ConvertTokenAmount> {
        [self.no_a, self.no_b]
            .into_iter()
            .map(|instrument_id| ConvertTokenAmount {
                instrument_id,
                quantity: dec!(100),
            })
            .collect()
    }

    fn strategy(identifier: StrategyId) -> ActorRef<ConvertStrategy> {
        try_get_actor_unchecked(&Ustr::from(identifier.as_str())).unwrap()
    }

    fn submit(&self, inputs: Vec<ConvertTokenAmount>, operation_id: Option<UUID4>) -> UUID4 {
        Self::strategy(self.strategy_id)
            .submit_convert(inputs, self.account_id, self.client_id, operation_id)
            .unwrap()
    }

    fn query(&self, operation_id: UUID4, strategy_id: StrategyId, account_id: AccountId) {
        Self::strategy(strategy_id)
            .query_convert(operation_id, account_id, self.client_id)
            .unwrap();
    }

    fn flush_commands(&mut self) {
        while let Ok(message) = self.channels.exec_cmd_rx.try_recv() {
            self.node.process_exec_command(message);
        }
    }

    fn emit(&mut self, operation_id: UUID4, outcome: ConvertExecutionOutcome) {
        self.emitter.send_convert_event(ConvertExecutionEvent {
            operation_id,
            account_id: self.account_id,
            client_id: self.client_id,
            run_id: None,
            outcome,
        });
        self.node
            .drain_exec_events_before_reconciliation(&mut self.channels.exec_evt_rx);
    }

    fn last_result(&self) -> ConvertResult {
        self.callbacks.borrow().results.last().unwrap().clone()
    }

    fn state(&self, state: TradingState) {
        self.node
            .kernel
            .risk_engine()
            .borrow_mut()
            .set_trading_state(state);
    }

    fn transaction() -> ConvertTransaction {
        ConvertTransaction {
            wallet: "TEST-WALLET".into(),
            relayer_transaction_id: Some("RELAY-001".into()),
            transaction_hash: None,
            nonce: Some("6".into()),
            call_summary: None,
        }
    }

    fn confirmation(&self, operation_id: UUID4) -> ConvertExecutionOutcome {
        ConvertExecutionOutcome::Confirmed {
            verified: Box::new(VerifiedConvert {
                operation_id,
                trader_id: self.node.trader_id(),
                strategy_id: self.strategy_id,
                account_id: self.account_id,
                transaction: Self::transaction(),
                inputs: self.inputs(),
                outputs: vec![ConvertTokenAmount {
                    instrument_id: self.yes_c,
                    quantity: dec!(100),
                }],
                affected_instruments: vec![self.no_a, self.no_b, self.yes_c],
                net_cash: Money::from("100 USDC"),
                ts_event: UnixNanos::from(80),
            }),
            pending_inputs: Vec::new(),
        }
    }
}

#[rstest]
#[case(InstrumentCloseType::ContractExpired, true)]
#[case(InstrumentCloseType::EndOfSession, false)]
#[tokio::test]
async fn startup_restores_convert_scope_using_persisted_formal_closes(
    #[case] output_close: InstrumentCloseType,
    #[case] historical_gap: bool,
) {
    let config = LiveNodeConfig {
        exec_engine: crate::config::LiveExecutionEngineConfig {
            load_cache: true,
            reconciliation: true,
            ..Default::default()
        },
        timeout_reconciliation: Duration::from_secs(1),
        delay_post_stop: Duration::ZERO,
        ..Default::default()
    };
    let mut node = LiveNode::build("ConvertRecovery".to_string(), Some(config)).unwrap();
    let account_id = AccountId::from("POLYMARKET-001");
    let client_id = ClientId::from("POLYMARKET");
    let input = InstrumentId::from("NO-A.POLYMARKET");
    let output = InstrumentId::from("YES-B.POLYMARKET");
    let command = SubmitConvert {
        operation_id: UUID4::new(),
        trader_id: node.trader_id(),
        strategy_id: StrategyId::from("CONVERT-001"),
        account_id,
        client_id,
        inputs: vec![ConvertTokenAmount {
            instrument_id: input,
            quantity: dec!(100),
        }],
        ts_init: UnixNanos::default(),
    };
    let mut record = ConvertOperationRecord::new(&command);
    record.dispatched = true;
    record.affected_instruments = vec![input, output];
    record.transaction = Some(ConvertTransaction {
        relayer_transaction_id: None,
        ..ConvertFixture::transaction()
    });

    let (database, _) = TestCacheDatabaseControl::create();
    database
        .add(
            ConvertOperationRecord::cache_key(command.operation_id),
            serde_json::to_vec(&record).unwrap().into(),
        )
        .unwrap();

    for (instrument_id, close_type) in [
        (input, InstrumentCloseType::ContractExpired),
        (output, output_close),
    ] {
        database
            .add_instrument_close(&InstrumentClose::new(
                instrument_id,
                Price::from("0.00"),
                close_type,
                UnixNanos::from(10),
                UnixNanos::from(10),
            ))
            .unwrap();
    }
    node.set_cache_database(Box::new(database)).unwrap();

    let calls = Rc::new(RefCell::new(ClientCalls::default()));
    let client = LiveExecutionClient::new(Box::new(ConvertClient {
        account_id,
        client_id,
        calls: calls.clone(),
        fail_dispatch: Rc::new(Cell::new(false)),
        reporting_cache: Some(node.kernel.cache()),
        connected: false,
    }));
    node.kernel
        .exec_engine
        .borrow_mut()
        .register_client(Box::new(client.clone()))
        .unwrap();
    node.exec_clients.push(client);

    let startup = node.start().await;
    assert_eq!(startup.is_ok(), historical_gap, "{startup:?}");
    {
        let cache = node.kernel.cache.borrow();
        let restored = cache
            .load_convert_operation(command.operation_id)
            .unwrap()
            .unwrap();
        assert_eq!(restored.historical_gap, historical_gap);
        assert_eq!(restored.result().status, ConvertExecutionStatus::Unknown);
        assert_eq!(restored.transaction, record.transaction);
        assert!(!restored.applied);
        assert_eq!(
            cache.convert_scope_account_is_active(account_id),
            !historical_gap
        );
        assert!(calls.borrow().submits.is_empty());
        assert!(calls.borrow().queries.is_empty());
    }

    if historical_gap {
        node.stop().await.unwrap();
    }
    node.dispose();
}

#[rstest]
fn convert_routes_strategy_commands_and_client_events_without_creating_orders() {
    let mut fixture = ConvertFixture::new(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    assert!(fixture.calls.borrow().submits.is_empty());

    fixture.flush_commands();
    let (command, scope) = fixture.calls.borrow().submits[0].clone();
    assert_eq!(command.operation_id, operation_id);
    assert_eq!(scope.inputs, vec![fixture.no_a, fixture.no_b]);
    assert_eq!(scope.outputs, vec![fixture.yes_c]);
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Submitted(ConvertFixture::transaction()),
    );
    assert_eq!(
        fixture.last_result().status,
        ConvertExecutionStatus::Submitted
    );

    fixture.state(TradingState::Halted);
    let mut same_inputs = fixture.inputs();
    same_inputs.reverse();
    fixture.submit(same_inputs, Some(operation_id));
    fixture.flush_commands();
    assert_eq!(
        fixture.last_result().status,
        ConvertExecutionStatus::Submitted
    );
    assert_eq!(fixture.calls.borrow().submits.len(), 1);
    fixture.query(operation_id, fixture.strategy_id, fixture.account_id);
    fixture.flush_commands();
    assert_eq!(
        fixture.calls.borrow().queries[0].0.operation_id,
        operation_id
    );
    assert_eq!(
        fixture.calls.borrow().queries[0].1,
        ConvertFixture::transaction()
    );

    fixture.emit(operation_id, fixture.confirmation(operation_id));
    let applied = fixture.last_result();
    assert_eq!(applied.operation_id, operation_id);
    assert_eq!(applied.status, ConvertExecutionStatus::Confirmed);
    assert!(applied.applied, "{:?}", applied.reason);
    assert!(applied.unapplied_instruments.is_empty());
    assert!(fixture.other_callbacks.borrow().results.is_empty());
    let opened = fixture.callbacks.borrow().opened.clone();
    assert_eq!(opened.len(), 1);
    assert_eq!(opened[0].instrument_id, fixture.yes_c);
    assert!(opened[0].last_px.is_none());
    assert!(opened[0].opening_order_id.is_none());

    fixture.emit(operation_id, fixture.confirmation(operation_id));
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Unknown {
            transaction: None,
            reason: "query timed out".into(),
        },
    );
    fixture.query(operation_id, fixture.strategy_id, fixture.account_id);
    fixture.flush_commands();
    assert_eq!(fixture.last_result(), applied);
    assert_eq!(fixture.calls.borrow().queries.len(), 1);
    assert_eq!(fixture.callbacks.borrow().opened.len(), 1);
    {
        let cache = fixture.node.kernel.cache.borrow();
        assert_eq!(cache.orders_total_count(None, None, None, None, None), 0);
        let position = cache.position(&fixture.position_id(fixture.yes_c)).unwrap();
        assert_eq!(position.quantity, Quantity::from("100.00"));
        assert_eq!(position.cost_open, dec!(50));
        assert!(position.events.is_empty());
    }

    fixture.state(TradingState::Active);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(fixture.node.trader_id())
        .strategy_id(fixture.strategy_id)
        .instrument_id(fixture.yes_c)
        .side(OrderSide::Sell)
        .quantity(Quantity::from("100.00"))
        .price(Price::from("0.700"))
        .build();
    let order_id = order.client_order_id();
    ConvertFixture::strategy(fixture.strategy_id)
        .submit_order(
            order,
            Some(fixture.position_id(fixture.yes_c)),
            Some(fixture.client_id),
            None,
        )
        .unwrap();
    fixture.flush_commands();
    assert_eq!(fixture.calls.borrow().orders[0].client_order_id, order_id);
}

#[rstest]
#[case::missing_mapping("mapping")]
#[case::insufficient("insufficient")]
#[case::zero("zero")]
#[case::unequal("unequal")]
#[case::fractional_chain_unit("fractional")]
#[case::fractional_position_unit("position_precision")]
#[case::output_short("short")]
#[case::expired_output("expired")]
#[case::sell_commitment("sell")]
#[case::dispatched_sell_without_ack("dispatched_sell")]
#[case::halted("halted")]
#[case::reducing_with_output("reducing")]
fn convert_rejects_ineligible_submissions_without_client_dispatch(#[case] problem: &str) {
    let mut fixture = ConvertFixture::new(problem != "mapping");
    let mut inputs = fixture.inputs();
    match problem {
        "insufficient" => inputs
            .iter_mut()
            .for_each(|input| input.quantity = dec!(101)),
        "zero" => inputs
            .iter_mut()
            .for_each(|input| input.quantity = Decimal::ZERO),
        "unequal" => inputs[0].quantity = dec!(99),
        "fractional" => inputs
            .iter_mut()
            .for_each(|input| input.quantity = dec!(1.0000001)),
        "position_precision" => inputs
            .iter_mut()
            .for_each(|input| input.quantity = dec!(1.001)),
        "short" => fixture.seed(fixture.yes_c, OrderSide::Sell, "0.500"),
        "expired" => {
            fixture
                .node
                .kernel
                .cache
                .borrow_mut()
                .add_instrument_close(InstrumentClose::new(
                    fixture.yes_c,
                    Price::from("0.000"),
                    InstrumentCloseType::ContractExpired,
                    UnixNanos::from(10),
                    UnixNanos::from(10),
                ))
                .unwrap();
        }
        "sell" => {
            let mut order = OrderTestBuilder::new(OrderType::Limit)
                .trader_id(fixture.node.trader_id())
                .strategy_id(fixture.strategy_id)
                .instrument_id(fixture.no_a)
                .side(OrderSide::Sell)
                .quantity(Quantity::from("1.00"))
                .price(Price::from("0.900"))
                .build();
            order
                .apply(TestOrderEventStubs::submitted(&order, fixture.account_id))
                .unwrap();
            fixture
                .node
                .kernel
                .cache
                .borrow_mut()
                .add_order(order, None, Some(fixture.client_id), false)
                .unwrap();
        }
        "dispatched_sell" => {
            let order = OrderTestBuilder::new(OrderType::Limit)
                .trader_id(fixture.node.trader_id())
                .strategy_id(fixture.strategy_id)
                .instrument_id(fixture.no_a)
                .side(OrderSide::Sell)
                .quantity(Quantity::from("1.00"))
                .price(Price::from("0.900"))
                .build();
            let order_id = order.client_order_id();
            ConvertFixture::strategy(fixture.strategy_id)
                .submit_order(
                    order,
                    Some(fixture.position_id(fixture.no_a)),
                    Some(fixture.client_id),
                    None,
                )
                .unwrap();
            fixture.flush_commands();
            let cache = fixture.node.kernel.cache.borrow();
            assert!(cache.is_order_dispatched(&order_id));
            assert_eq!(
                cache.order(&order_id).unwrap().status(),
                OrderStatus::Initialized
            );
        }
        "halted" => fixture.state(TradingState::Halted),
        "reducing" => fixture.state(TradingState::Reducing),
        "mapping" => {}
        _ => unreachable!(),
    }

    let operation_id = fixture.submit(inputs, None);
    fixture.flush_commands();
    let result = fixture.last_result();
    assert_eq!(result.operation_id, operation_id);
    assert_eq!(result.status, ConvertExecutionStatus::Rejected);
    assert!(!result.applied);
    assert!(result.reason.is_some());
    assert!(fixture.calls.borrow().submits.is_empty());
}

#[rstest]
fn convert_cash_only_reducing_dispatches_and_conflicts_do_not_overwrite_results() {
    let mut fixture = ConvertFixture::new(true);
    fixture.seed(fixture.no_c, OrderSide::Buy, "0.500");
    fixture.state(TradingState::Reducing);
    let mut inputs = fixture.inputs();
    inputs.push(ConvertTokenAmount {
        instrument_id: fixture.no_c,
        quantity: dec!(100),
    });
    let operation_id = fixture.submit(inputs.clone(), None);
    fixture.flush_commands();
    assert!(fixture.calls.borrow().submits[0].1.outputs.is_empty());
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Failed {
            transaction: Some(ConvertFixture::transaction()),
            reason: "wallet call failed".into(),
        },
    );
    let failed = fixture.last_result();
    assert_eq!(failed.status, ConvertExecutionStatus::Failed);
    assert!(!failed.applied);

    for input in &mut inputs {
        input.quantity = dec!(50);
    }
    fixture.submit(inputs, Some(operation_id));
    fixture.flush_commands();
    assert_eq!(
        fixture.last_result().status,
        ConvertExecutionStatus::Rejected
    );
    assert!(
        fixture
            .last_result()
            .reason
            .unwrap()
            .to_lowercase()
            .contains("conflict")
    );

    fixture.query(operation_id, fixture.other_strategy_id, fixture.account_id);
    fixture.flush_commands();
    let foreign = fixture.other_callbacks.borrow().results[0].clone();
    assert_eq!(foreign.status, ConvertExecutionStatus::Rejected);
    assert!(foreign.transaction.is_none());
    fixture.query(
        operation_id,
        fixture.strategy_id,
        AccountId::from("POLYMARKET-002"),
    );
    fixture.flush_commands();
    assert_eq!(
        fixture.last_result().status,
        ConvertExecutionStatus::Rejected
    );
    assert!(fixture.last_result().transaction.is_none());
    assert_eq!(fixture.calls.borrow().submits.len(), 1);
    assert!(fixture.calls.borrow().queries.is_empty());
}

#[cfg(feature = "python")]
#[rstest]
fn convert_python_strategy_receives_applied_after_the_fill_free_position_event() {
    use nautilus_trading::python::strategy::PyStrategy;
    use pyo3::{prelude::*, types::PyModule};

    Python::initialize();
    let mut strategy = None;
    let mut fixture = ConvertFixture::with_primary_strategy(true, |node, strategy_id, _| {
        strategy = Some(Python::attach(|py| {
            let module = PyModule::new(py, "_convert_strategy_test").unwrap();
            module.add("Strategy", py.get_type::<PyStrategy>()).unwrap();
            py.run(
                c"
class ConvertProbe(Strategy):
    observed = []

    def on_position_opened(self, event):
        self.observed.append(event)

    def on_convert_result(self, result):
        super().on_convert_result(result)
        self.observed.append(result)
",
                Some(&module.dict()),
                None,
            )
            .unwrap();
            let config = StrategyConfig {
                strategy_id: Some(strategy_id),
                ..Default::default()
            };
            let instance = module
                .getattr("ConvertProbe")
                .unwrap()
                .call1((config,))
                .unwrap()
                .unbind();
            node.kernel
                .trader
                .borrow_mut()
                .add_python_strategy_instance(&instance)
                .unwrap();
            instance
        }));
    });
    let strategy = strategy.unwrap();
    let operation_id = Python::attach(|py| {
        let quantity = py
            .import("decimal")
            .unwrap()
            .getattr("Decimal")
            .unwrap()
            .call1(("100.00",))
            .unwrap();
        strategy
            .bind(py)
            .call_method1(
                "submit_convert",
                (
                    vec![(fixture.no_a, quantity.clone()), (fixture.no_b, quantity)],
                    fixture.account_id,
                    fixture.client_id,
                ),
            )
            .unwrap()
            .extract::<UUID4>()
            .unwrap()
    });
    fixture.flush_commands();
    assert_eq!(
        fixture.calls.borrow().submits[0].0.operation_id,
        operation_id
    );
    fixture.emit(operation_id, fixture.confirmation(operation_id));

    Python::attach(|py| {
        let observed = strategy.bind(py).getattr("observed").unwrap();
        assert_eq!(observed.len().unwrap(), 2);
        let opened = observed.get_item(0).unwrap();
        let result = observed.get_item(1).unwrap();
        assert!(opened.getattr("last_px").unwrap().is_none());
        assert!(opened.getattr("opening_order_id").unwrap().is_none());
        assert_eq!(
            opened
                .getattr("opening_operation_id")
                .unwrap()
                .extract::<UUID4>()
                .unwrap(),
            operation_id,
        );
        assert_eq!(
            result
                .getattr("operation_id")
                .unwrap()
                .extract::<UUID4>()
                .unwrap(),
            operation_id,
        );
        assert!(
            result
                .getattr("applied")
                .unwrap()
                .extract::<bool>()
                .unwrap()
        );
        assert_eq!(
            result
                .getattr("status")
                .unwrap()
                .extract::<ConvertExecutionStatus>()
                .unwrap(),
            ConvertExecutionStatus::Confirmed,
        );
    });
}

#[rstest]
fn convert_dispatch_error_is_rejected_and_repeated_request_does_not_retry() {
    let mut fixture = ConvertFixture::new(true);
    fixture.fail_dispatch.set(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    fixture.flush_commands();
    let rejected = fixture.last_result();
    assert_eq!(rejected.status, ConvertExecutionStatus::Rejected);
    assert!(
        rejected
            .reason
            .as_deref()
            .unwrap()
            .contains("could not dispatch")
    );
    fixture.fail_dispatch.set(false);
    fixture.submit(fixture.inputs(), Some(operation_id));
    fixture.flush_commands();
    assert_eq!(fixture.last_result(), rejected);
    assert_eq!(fixture.calls.borrow().submits.len(), 1);
}

#[rstest]
fn convert_pre_sign_rejection_is_terminal_for_contradictory_evidence() {
    let mut fixture = ConvertFixture::new(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    fixture.flush_commands();

    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Rejected("pre-sign refusal".into()),
    );
    let rejected = fixture.last_result();
    assert_eq!(rejected.status, ConvertExecutionStatus::Rejected);

    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Failed {
            transaction: Some(ConvertFixture::transaction()),
            reason: "contradictory provider failure".into(),
        },
    );
    fixture.emit(operation_id, fixture.confirmation(operation_id));

    let retained = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(retained.status, Some(ConvertExecutionStatus::Rejected));
    assert!(retained.transaction.is_none());
    assert!(!retained.applied);
    assert_eq!(fixture.last_result(), rejected);
    assert!(fixture.callbacks.borrow().opened.is_empty());

    let cache = fixture.node.kernel.cache.borrow();
    assert!(!cache.convert_scope_is_active(fixture.yes_c, fixture.account_id));
    assert!(
        cache
            .positions_open(
                None,
                Some(&fixture.yes_c),
                None,
                Some(&fixture.account_id),
                None
            )
            .is_empty()
    );
}

#[rstest]
fn convert_confirmed_historical_gap_accepts_compatible_transaction_enrichment() {
    let mut fixture = ConvertFixture::new(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    fixture.flush_commands();
    let ConvertExecutionOutcome::Confirmed { verified, .. } = fixture.confirmation(operation_id)
    else {
        unreachable!();
    };
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Confirmed {
            verified,
            pending_inputs: vec![fixture.no_a],
        },
    );
    assert!(!fixture.last_result().applied);

    {
        let mut cache = fixture.node.kernel.cache.borrow_mut();
        for instrument_id in [fixture.no_a, fixture.no_b, fixture.yes_c] {
            cache
                .add_instrument_close(InstrumentClose::new(
                    instrument_id,
                    Price::from("0.000"),
                    InstrumentCloseType::ContractExpired,
                    UnixNanos::from(90),
                    UnixNanos::from(90),
                ))
                .unwrap();
        }
    }

    let ConvertExecutionOutcome::Confirmed {
        mut verified,
        pending_inputs,
    } = fixture.confirmation(operation_id)
    else {
        unreachable!();
    };
    verified.transaction.call_summary = Some("compatible call metadata".into());
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Confirmed {
            verified,
            pending_inputs,
        },
    );

    let result = fixture.last_result();
    assert_eq!(result.status, ConvertExecutionStatus::Confirmed);
    assert!(!result.applied);
    assert!(result.reason.as_deref().unwrap().contains("historical gap"));
    assert_eq!(
        result
            .transaction
            .as_ref()
            .and_then(|transaction| transaction.call_summary.as_deref()),
        Some("compatible call metadata"),
    );
    assert!(fixture.callbacks.borrow().opened.is_empty());
}

#[rstest]
fn convert_returns_unknown_locally_and_preserves_confirmed_pending_accounting() {
    let mut fixture = ConvertFixture::new(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    fixture.flush_commands();
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Unknown {
            transaction: None,
            reason: "reply lost after dispatch".into(),
        },
    );
    let unknown = fixture.last_result();
    fixture.query(operation_id, fixture.strategy_id, fixture.account_id);
    fixture.flush_commands();
    assert_eq!(unknown.status, ConvertExecutionStatus::Unknown);
    assert_eq!(fixture.last_result(), unknown);
    assert!(fixture.calls.borrow().queries.is_empty());

    let ConvertExecutionOutcome::Confirmed { verified, .. } = fixture.confirmation(operation_id)
    else {
        unreachable!();
    };
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Confirmed {
            verified,
            pending_inputs: vec![fixture.no_a],
        },
    );
    let pending = fixture.last_result();
    assert_eq!(pending.status, ConvertExecutionStatus::Confirmed);
    assert!(!pending.applied);
    assert!(pending.reason.is_some());
    assert!(pending.unapplied_instruments.contains(&fixture.no_a));
    assert!(fixture.callbacks.borrow().opened.is_empty());
    fixture.query(operation_id, fixture.strategy_id, fixture.account_id);
    fixture.flush_commands();
    assert_eq!(fixture.calls.borrow().queries.len(), 1);
    fixture.emit(operation_id, fixture.confirmation(operation_id));
    assert!(fixture.last_result().applied);
}

#[rstest]
#[case::unregistered("MISSING")]
#[case::wrong_account("OTHER-ACCOUNT")]
fn convert_submit_does_not_fall_back_or_dispatch_to_another_account(#[case] route: &str) {
    let mut fixture = ConvertFixture::new(true);
    let client_id = ClientId::from(route);
    fixture
        .node
        .kernel
        .exec_engine
        .borrow_mut()
        .register_client(Box::new(ConvertClient {
            account_id: AccountId::from("POLYMARKET-002"),
            client_id: ClientId::from("OTHER-ACCOUNT"),
            calls: fixture.calls.clone(),
            fail_dispatch: fixture.fail_dispatch.clone(),
            reporting_cache: None,
            connected: true,
        }))
        .unwrap();

    let operation_id = ConvertFixture::strategy(fixture.strategy_id)
        .submit_convert(fixture.inputs(), fixture.account_id, client_id, None)
        .unwrap();
    fixture.flush_commands();

    let result = fixture.last_result();
    assert_eq!(result.operation_id, operation_id);
    assert_eq!(result.status, ConvertExecutionStatus::Rejected);
    assert!(result.reason.is_some());
    assert!(fixture.calls.borrow().submits.is_empty());
}

#[rstest]
fn recovered_convert_with_unfixed_plan_can_complete_after_pending_input_clears() {
    let mut fixture = ConvertFixture::new(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    fixture.flush_commands();
    let ConvertExecutionOutcome::Confirmed { verified, .. } = fixture.confirmation(operation_id)
    else {
        unreachable!();
    };
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Confirmed {
            verified,
            pending_inputs: vec![fixture.no_a],
        },
    );
    assert_eq!(
        fixture.last_result().status,
        ConvertExecutionStatus::Confirmed
    );
    assert!(!fixture.last_result().applied);
    let recovered = fixture
        .node
        .kernel
        .exec_engine
        .borrow_mut()
        .recover_convert_operation(operation_id)
        .unwrap();
    assert!(!recovered.applied);
    assert!(!recovered.plan_lost);

    fixture.query(operation_id, fixture.strategy_id, fixture.account_id);
    fixture.flush_commands();
    fixture.emit(operation_id, fixture.confirmation(operation_id));
    assert!(fixture.last_result().applied);
}

#[rstest]
#[case(false)]
#[case(true)]
fn convert_query_does_not_rebuild_a_lost_applied_plan(#[case] stale_core: bool) {
    let mut fixture = ConvertFixture::new(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    fixture.flush_commands();
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Submitted(ConvertFixture::transaction()),
    );

    if stale_core {
        let ConvertExecutionOutcome::Confirmed { verified, .. } =
            fixture.confirmation(operation_id)
        else {
            unreachable!();
        };
        let record = fixture
            .node
            .kernel
            .exec_engine
            .borrow_mut()
            .apply_verified_convert(*verified, &[fixture.no_a])
            .unwrap();
        assert!(record.plan.is_none());
        assert!(!record.plan_fixed);
    }
    {
        let mut cache = fixture.node.kernel.cache.borrow_mut();
        let mut record = cache.load_convert_operation(operation_id).unwrap().unwrap();
        record.status = Some(ConvertExecutionStatus::Confirmed);
        record.applied = true;
        cache.add_convert_operation(&record).unwrap();
    }
    let recovered = fixture
        .node
        .kernel
        .exec_engine
        .borrow_mut()
        .recover_convert_operation(operation_id)
        .unwrap();
    assert!(!recovered.applied);

    fixture.query(operation_id, fixture.strategy_id, fixture.account_id);
    fixture.flush_commands();
    assert_eq!(fixture.calls.borrow().queries.len(), 1);
    fixture.emit(operation_id, fixture.confirmation(operation_id));

    let result = fixture.last_result();
    assert_eq!(result.status, ConvertExecutionStatus::Confirmed);
    assert_eq!(result.transaction, Some(ConvertFixture::transaction()));
    assert!(!result.applied);
    assert_eq!(
        result.unapplied_instruments,
        recovered.unapplied_instruments
    );
    assert!(result.reason.is_some());
    let cache = fixture.node.kernel.cache.borrow();
    assert_eq!(
        cache
            .position(&fixture.position_id(fixture.no_a))
            .unwrap()
            .quantity,
        Quantity::from("100.00")
    );
    assert!(
        cache
            .position(&fixture.position_id(fixture.yes_c))
            .is_none()
    );
}

#[rstest]
fn convert_does_not_query_wallet_context_without_a_transaction_locator() {
    let mut fixture = ConvertFixture::new(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    fixture.flush_commands();
    let transaction = ConvertTransaction {
        relayer_transaction_id: None,
        ..ConvertFixture::transaction()
    };
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Unknown {
            transaction: Some(transaction.clone()),
            reason: "submission response lost".into(),
        },
    );
    let unknown = fixture.last_result();
    fixture.query(operation_id, fixture.strategy_id, fixture.account_id);
    fixture.flush_commands();

    assert_eq!(unknown.status, ConvertExecutionStatus::Unknown);
    assert_eq!(unknown.transaction, Some(transaction));
    assert_eq!(fixture.last_result(), unknown);
    assert!(fixture.calls.borrow().queries.is_empty());
}

#[rstest]
fn convert_keeps_the_original_transaction_when_later_evidence_conflicts() {
    let mut fixture = ConvertFixture::new(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    fixture.flush_commands();
    let original = ConvertTransaction {
        transaction_hash: Some("ORIGINAL-HASH".into()),
        ..ConvertFixture::transaction()
    };
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Submitted(original.clone()),
    );
    let conflict = ConvertTransaction {
        relayer_transaction_id: Some("OTHER-RELAY".into()),
        transaction_hash: Some("OTHER-HASH".into()),
        ..ConvertFixture::transaction()
    };
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Failed {
            transaction: Some(conflict),
            reason: "unrelated transaction failed".into(),
        },
    );
    fixture.query(operation_id, fixture.strategy_id, fixture.account_id);
    fixture.flush_commands();

    assert_eq!(
        fixture.last_result().status,
        ConvertExecutionStatus::Submitted
    );
    assert_eq!(fixture.last_result().transaction, Some(original.clone()));
    assert_eq!(fixture.calls.borrow().queries[0].1, original);
}

#[rstest]
fn convert_rejects_confirmation_with_an_output_outside_the_admitted_scope() {
    let mut fixture = ConvertFixture::new(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    fixture.flush_commands();
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Submitted(ConvertFixture::transaction()),
    );
    let submitted = fixture.last_result();
    let ConvertExecutionOutcome::Confirmed {
        mut verified,
        pending_inputs,
    } = fixture.confirmation(operation_id)
    else {
        unreachable!();
    };
    verified.outputs[0].instrument_id = fixture.no_c;
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Confirmed {
            verified,
            pending_inputs,
        },
    );
    assert_eq!(fixture.last_result().status, submitted.status);
    assert!(!fixture.last_result().applied);
    assert!(fixture.callbacks.borrow().opened.is_empty());
}

#[rstest]
#[case::settled_before_confirmation(false)]
#[case::settled_after_confirmation(true)]
fn convert_retains_a_confirmed_history_gap_after_scope_settlement(
    #[case] confirmed_before_settlement: bool,
) {
    let mut fixture = ConvertFixture::new(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    fixture.flush_commands();

    if confirmed_before_settlement {
        let ConvertExecutionOutcome::Confirmed { verified, .. } =
            fixture.confirmation(operation_id)
        else {
            unreachable!();
        };
        fixture.emit(
            operation_id,
            ConvertExecutionOutcome::Confirmed {
                verified,
                pending_inputs: vec![fixture.no_a],
            },
        );
        assert_eq!(
            fixture.last_result().status,
            ConvertExecutionStatus::Confirmed
        );
        assert!(!fixture.last_result().applied);
    }
    {
        let mut cache = fixture.node.kernel.cache.borrow_mut();
        for instrument_id in [fixture.no_a, fixture.no_b, fixture.yes_c] {
            cache
                .add_instrument_close(InstrumentClose::new(
                    instrument_id,
                    Price::from("0.000"),
                    InstrumentCloseType::ContractExpired,
                    UnixNanos::from(90),
                    UnixNanos::from(90),
                ))
                .unwrap();
        }
    }

    if !confirmed_before_settlement {
        fixture.emit(operation_id, fixture.confirmation(operation_id));
    }
    fixture.query(operation_id, fixture.strategy_id, fixture.account_id);
    fixture.flush_commands();
    let gap = fixture.last_result();

    assert_eq!(gap.status, ConvertExecutionStatus::Confirmed);
    assert!(!gap.applied);
    assert!(gap.reason.as_deref().unwrap().contains("historical"));
    assert!(gap.unapplied_instruments.contains(&fixture.yes_c));
    assert!(fixture.callbacks.borrow().opened.is_empty());
    assert!(fixture.calls.borrow().queries.is_empty());
}

#[rstest]
fn convert_historical_gap_queries_original_locator_without_reapplying_accounting() {
    let mut fixture = ConvertFixture::new(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    fixture.flush_commands();
    let original = ConvertFixture::transaction();

    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Unknown {
            transaction: Some(original.clone()),
            reason: "original execution remains unknown".into(),
        },
    );
    {
        let mut cache = fixture.node.kernel.cache.borrow_mut();
        for instrument_id in [fixture.no_a, fixture.no_b, fixture.yes_c] {
            cache
                .add_instrument_close(InstrumentClose::new(
                    instrument_id,
                    Price::from("0.000"),
                    InstrumentCloseType::ContractExpired,
                    UnixNanos::from(90),
                    UnixNanos::from(90),
                ))
                .unwrap();
        }
    }

    let retained = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(operation_id)
        .unwrap()
        .unwrap();
    assert!(retained.historical_gap);
    assert_eq!(retained.transaction, Some(original.clone()));

    fixture.query(operation_id, fixture.strategy_id, fixture.account_id);
    fixture.flush_commands();
    let calls = fixture.calls.borrow();
    assert_eq!(calls.queries.len(), 1);
    assert_eq!(calls.queries[0].0.operation_id, operation_id);
    assert_eq!(calls.queries[0].1, original);
    drop(calls);

    fixture.emit(operation_id, fixture.confirmation(operation_id));
    let result = fixture.last_result();
    assert_eq!(result.status, ConvertExecutionStatus::Confirmed);
    assert!(!result.applied);
    assert_eq!(result.transaction, Some(ConvertFixture::transaction()));
    assert!(result.unapplied_instruments.contains(&fixture.yes_c));
    assert!(result.reason.as_deref().unwrap().contains("historical"));
    assert!(fixture.callbacks.borrow().opened.is_empty());
    let retained = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(operation_id)
        .unwrap()
        .unwrap();
    assert!(retained.historical_gap);
    assert!(!retained.applied);
}

#[rstest]
fn convert_failed_scope_requires_explicit_invalid_authorization_to_release() {
    let mut fixture = ConvertFixture::new(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    fixture.flush_commands();
    let original = ConvertFixture::transaction();

    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Submitted(original.clone()),
    );
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Failed {
            transaction: Some(original.clone()),
            reason: "provider attempt failed".into(),
        },
    );
    let retained = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(retained.status, Some(ConvertExecutionStatus::Failed));
    assert!(!retained.authorization_invalid);
    assert!(
        fixture
            .node
            .kernel
            .cache
            .borrow()
            .convert_scope_is_active(fixture.yes_c, fixture.account_id)
    );

    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::Rejected("post-authorization refusal".into()),
    );
    assert_eq!(fixture.last_result().status, ConvertExecutionStatus::Failed);

    fixture.query(operation_id, fixture.strategy_id, fixture.account_id);
    fixture.flush_commands();
    assert_eq!(fixture.calls.borrow().queries.len(), 1);

    let mut conflicting = original.clone();
    conflicting.wallet = "OTHER-WALLET".into();
    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::AuthorizationInvalid {
            transaction: Some(conflicting),
            reason: "conflicting authorization proof".into(),
        },
    );
    let retained = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(operation_id)
        .unwrap()
        .unwrap();
    assert!(!retained.authorization_invalid);
    assert_eq!(retained.status, Some(ConvertExecutionStatus::Failed));
    assert!(
        fixture
            .node
            .kernel
            .cache
            .borrow()
            .convert_scope_is_active(fixture.yes_c, fixture.account_id)
    );
    assert_eq!(fixture.calls.borrow().queries.len(), 1);

    fixture.emit(
        operation_id,
        ConvertExecutionOutcome::AuthorizationInvalid {
            transaction: Some(original),
            reason: "authorization proven invalid".into(),
        },
    );
    let released = fixture.last_result();
    assert_eq!(released.status, ConvertExecutionStatus::Failed);
    let retained = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(operation_id)
        .unwrap()
        .unwrap();
    assert!(retained.authorization_invalid);
    assert!(
        !fixture
            .node
            .kernel
            .cache
            .borrow()
            .convert_scope_is_active(fixture.yes_c, fixture.account_id)
    );

    let query_count = fixture.calls.borrow().queries.len();
    fixture.query(operation_id, fixture.strategy_id, fixture.account_id);
    fixture.flush_commands();
    assert_eq!(fixture.calls.borrow().queries.len(), query_count);

    fixture.emit(operation_id, fixture.confirmation(operation_id));
    assert_eq!(fixture.last_result().status, ConvertExecutionStatus::Failed);
    assert!(fixture.callbacks.borrow().opened.is_empty());
}

#[rstest]
fn convert_applied_scope_release_notifies_execution_manager_activity_on_dispatch() {
    let mut fixture = ConvertFixture::new(true);
    let operation_id = fixture.submit(fixture.inputs(), None);
    fixture.flush_commands();
    let key = (fixture.yes_c, fixture.account_id);
    let before = fixture.node.exec_manager.position_activity_revision(&key);

    fixture.emit(operation_id, fixture.confirmation(operation_id));

    assert!(fixture.node.exec_manager.position_activity_revision(&key) > before);
    assert!(fixture.node.exec_manager.position_activity_is_recent(&key));
}
