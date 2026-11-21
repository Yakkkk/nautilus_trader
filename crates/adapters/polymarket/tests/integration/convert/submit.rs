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

//! Live conversion through the production strategy, execution client, and local venue.

use std::{
    cell::Cell,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy::sol_types::{SolCall, SolEvent, SolValue};
use alloy_primitives::{Address, B256, Bytes, U256, hex};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
#[cfg(target_os = "linux")]
use nautilus_common::cache::CacheConfig;
use nautilus_common::{
    actor::DataActor,
    cache::Cache,
    enums::Environment,
    messages::execution::{ConvertExecutionStatus, ConvertResult, ConvertTokenAmount},
    testing::wait_until_async,
};
use nautilus_core::{UUID4, UnixNanos};
#[cfg(target_os = "linux")]
use nautilus_infrastructure::redis::cache::{
    RedisCacheConfig, RedisCacheDatabase, RedisCacheDatabaseAdapter,
};
use nautilus_live::{
    builder::LiveNodeBuilder,
    config::{LiveExecutionEngineConfig, LiveNodeConfig},
    node::LiveNode,
};
use nautilus_model::{
    data::InstrumentClose,
    enums::{InstrumentCloseType, OmsType, OrderSide, OrderStatus, OrderType, TimeInForce},
    events::{OrderAccepted, OrderDenied, OrderFilled, OrderRejected},
    identifiers::{
        AccountId, ClientOrderId, InstrumentId, StrategyId, Symbol, TraderId, VenueOrderId,
    },
    instruments::{
        InstrumentAny,
        neg_risk::{CtfNegRiskEvent, NegRiskCondition},
        stubs::binary_option,
    },
    orders::{Order, OrderTestBuilder},
    position::{Position, PositionReplayEvent},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_polymarket::{
    common::{
        consts::{POLYMARKET_CLIENT_ID, POLYMARKET_PRICE_PRECISION},
        credential::{EvmPrivateKey, RelayerApiKey},
        enums::PolymarketSignatureType,
    },
    factories::PolymarketExecutionClientFactory,
    positions::PolymarketPositionClient,
    signing::eip712::{DEPOSIT_WALLET_FACTORY, NEG_RISK_CTF_COLLATERAL_ADAPTER},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};
use rstest::rstest;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{Value, json};
use ustr::Ustr;

use super::{
    Evidence, HASH, MockChain, RELAYER_ID, SAMPLE, encode_factory_call, event_mapping,
    factory_call, log_index, position_id, relayer, replace_word, rpc,
};
use crate::{
    harness::{ACCOUNT_ID, STRATEGY_ID, TRADER_ID},
    mock_venue::{
        RequestGate, TEST_PRIVATE_KEY, TEST_SIGNER_ADDRESS, TestServerState,
        execution_config_with_retries, load_json, start_mock_server,
    },
};

const DEADLINE: Duration = Duration::from_secs(5);
const CHAIN_AMOUNT: u64 = 100_000_000;
const NONCE: usize = 7;
const ORDER_IDS: [&str; 4] = [
    "O-CONVERT-NO-A",
    "O-CONVERT-NO-B",
    "O-CONVERT-YES-C",
    "O-CONVERT-LATE-NO-A",
];
const PRICES: [&str; 4] = ["0.80", "0.70", "0.70", "0.80"];

alloy::sol! {
    function convertPositions(bytes32 marketId, uint256 indexSet, uint256 amount);
    function id() external view returns (bytes32);
    function owner() external view returns (address);
    function nonce() external view returns (uint256);
    function predictWalletAddress(bytes32 id) external view returns (address);
    function isApprovedForAll(address owner, address operator) external view returns (bool);
    function balanceOf(address owner, uint256 id) external view returns (uint256);
    event Execution(address indexed target, uint256 value, bytes data, bytes result);
}

struct LiveChain {
    chain: Arc<MockChain>,
    wallet: Address,
    calldata: Bytes,
    submitted: tokio::sync::Mutex<Option<Value>>,
    nonce: AtomicUsize,
    approved: AtomicBool,
    approval_reads: AtomicUsize,
    supports_finalized: AtomicBool,
    omit_locator: AtomicBool,
    hash_only: AtomicBool,
    submit_gate: RequestGate,
    transaction_gate: RequestGate,
}

impl LiveChain {
    fn new(mapping: &CtfNegRiskEvent, wallet: Address) -> Self {
        let mut sample: Value = serde_json::from_str(SAMPLE).unwrap();
        let mut batch = factory_call(&sample).batches.remove(0);
        let amount = U256::from(CHAIN_AMOUNT);
        let market = sample["market"].as_str().unwrap().parse::<B256>().unwrap();
        let index_set = U256::from(3_u8);
        let calldata = Bytes::from(
            convertPositionsCall {
                marketId: market,
                indexSet: index_set,
                amount,
            }
            .abi_encode(),
        );
        batch.wallet = wallet;
        batch.nonce = U256::from(NONCE);
        batch.calls[0].data = calldata.clone();
        let input = encode_factory_call(&[batch], &[Bytes::from(vec![1_u8; 65])]);
        sample["input"] = json!([input]);

        let conditions = mapping.conditions();
        let no = conditions[..2]
            .iter()
            .map(|condition| U256::from_be_bytes(condition.no.token_id))
            .collect::<Vec<_>>();
        let yes = vec![U256::from_be_bytes(conditions[2].yes.token_id)];
        let logs = sample["logs"].as_array_mut().unwrap();
        let wallet_topic = format!("0x{:0>64}", hex::encode(wallet));
        let no_log = log_index(logs, "0x25f");
        logs[no_log]["topics"][2] = json!(wallet_topic);
        logs[no_log]["data"] = json!(format!(
            "0x{}",
            hex::encode((no, vec![amount; 2]).abi_encode_params())
        ));
        let yes_log = log_index(logs, "0x26c");
        logs[yes_log]["topics"][3] = json!(wallet_topic);
        logs[yes_log]["data"] = json!(format!(
            "0x{}",
            hex::encode((yes, vec![amount]).abi_encode_params())
        ));

        let adapter_log = log_index(logs, "0x26d");
        logs[adapter_log]["topics"][1] = json!(wallet_topic);
        logs[adapter_log]["topics"][3] = json!(format!("0x{index_set:064x}"));
        logs[adapter_log]["data"] = json!(format!("0x{amount:064x}{amount:064x}"));

        for (index, topic) in [("0x26f", 2), ("0x271", 3)] {
            let cash_log = log_index(logs, index);
            logs[cash_log]["topics"][topic] = json!(wallet_topic);
            let data = logs[cash_log]["data"].as_str().unwrap();
            logs[cash_log]["data"] = json!(replace_word(data, 0, CHAIN_AMOUNT));
        }
        let execution = log_index(logs, "0x272");
        logs[execution]["address"] = json!(wallet);
        logs[execution]["data"] = json!(format!(
            "0x{}",
            hex::encode(
                Execution {
                    target: NEG_RISK_CTF_COLLATERAL_ADAPTER,
                    value: U256::ZERO,
                    data: calldata.clone(),
                    result: Bytes::new(),
                }
                .encode_data()
            )
        ));
        let batch = log_index(logs, "0x273");
        logs[batch]["address"] = json!(wallet);
        logs[batch]["topics"][1] = json!(format!("0x{NONCE:064x}"));

        Self {
            chain: Arc::new(MockChain::new(sample, Evidence::Valid, false)),
            wallet,
            calldata,
            submitted: tokio::sync::Mutex::new(None),
            nonce: AtomicUsize::new(NONCE),
            approved: AtomicBool::new(true),
            approval_reads: AtomicUsize::new(0),
            supports_finalized: AtomicBool::new(true),
            omit_locator: AtomicBool::new(false),
            hash_only: AtomicBool::new(false),
            submit_gate: RequestGate::default(),
            transaction_gate: RequestGate::default(),
        }
    }
}

async fn live_rpc(State(state): State<Arc<LiveChain>>, Json(request): Json<Value>) -> Json<Value> {
    if request["method"] == "eth_getTransactionByHash" {
        state.transaction_gate.wait().await;
    }
    let result = match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber"
            if request["params"][0] == "finalized"
                && !state.supports_finalized.load(Ordering::Acquire) =>
        {
            return Json(json!({
                "jsonrpc": "2.0", "id": request["id"],
                "error": {"code": -32602, "message": "Unsupported block tag"}
            }));
        }
        "eth_getCode" => json!("0x1234"),
        "eth_call" => {
            let data = request["params"][0]["data"].as_str().unwrap();
            let selector = hex::decode(&data[2..10]).unwrap();
            let word = if selector == idCall::SELECTOR {
                U256::from(1_u8)
            } else if selector == ownerCall::SELECTOR {
                U256::from_be_slice(&TEST_SIGNER_ADDRESS.parse::<Address>().unwrap().0.0)
            } else if selector == predictWalletAddressCall::SELECTOR {
                U256::from_be_slice(&state.wallet.0.0)
            } else if selector == nonceCall::SELECTOR {
                U256::from(state.nonce.load(Ordering::Acquire))
            } else if selector == isApprovedForAllCall::SELECTOR {
                state.approval_reads.fetch_add(1, Ordering::AcqRel);
                U256::from(u8::from(state.approved.load(Ordering::Acquire)))
            } else if selector == balanceOfCall::SELECTOR {
                U256::from(CHAIN_AMOUNT)
            } else {
                panic!("Unexpected wallet read: {data}");
            };
            json!(format!("0x{word:064x}"))
        }
        _ => return rpc(State(state.chain.clone()), Json(request)).await,
    };
    Json(json!({"jsonrpc": "2.0", "id": request["id"], "result": result}))
}

async fn live_submit(State(state): State<Arc<LiveChain>>, Json(body): Json<Value>) -> Json<Value> {
    state.chain.submits.fetch_add(1, Ordering::AcqRel);
    *state.submitted.lock().await = Some(body);
    state.submit_gate.wait().await;
    if state.omit_locator.load(Ordering::Acquire) {
        Json(json!({"state": "STATE_NEW"}))
    } else if state.hash_only.load(Ordering::Acquire) {
        Json(json!({"transactionHash": HASH, "state": "STATE_NEW"}))
    } else {
        Json(json!({"transactionID": RELAYER_ID, "transactionHash": HASH, "state": "STATE_NEW"}))
    }
}

async fn start_chain(mapping: &CtfNegRiskEvent) -> (SocketAddr, Arc<LiveChain>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let wallet = Address::from_word(B256::from(U256::from(addr.port()).to_be_bytes::<32>()));
    let state = Arc::new(LiveChain::new(mapping, wallet));
    let router = Router::new()
        .route("/transaction", get(relayer))
        .with_state(state.chain.clone())
        .route("/", post(live_rpc))
        .route("/submit", post(live_submit))
        .with_state(state.clone());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (addr, state)
}

#[derive(Debug)]
struct LiveConvertStrategy {
    core: StrategyCore,
    instruments: [InstrumentId; 3],
    operation_id: UUID4,
    next_leg: usize,
    resume: bool,
    late_input: bool,
    late_order_queued: bool,
    accepted: tokio::sync::mpsc::UnboundedSender<OrderAccepted>,
    results: tokio::sync::mpsc::UnboundedSender<ConvertResult>,
}

impl LiveConvertStrategy {
    fn trade(&mut self, index: usize) -> anyhow::Result<()> {
        let instrument = self.instruments[if index == 3 { 0 } else { index }];
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(TraderId::from(TRADER_ID))
            .strategy_id(StrategyId::from(STRATEGY_ID))
            .instrument_id(instrument)
            .client_order_id(ClientOrderId::from(ORDER_IDS[index]))
            .side(if index == 2 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            })
            .price(Price::from(PRICES[index]))
            .quantity(Quantity::from(if index == 3 {
                "1.0000"
            } else {
                "100.0000"
            }))
            .time_in_force(TimeInForce::Gtc)
            .build();
        self.submit_order(order, None, Some(*POLYMARKET_CLIENT_ID), None)
    }
}

struct StrategyEvents {
    orders: tokio::sync::mpsc::UnboundedReceiver<OrderAccepted>,
    conversions: tokio::sync::mpsc::UnboundedReceiver<ConvertResult>,
}

fn add_strategy(
    node: &mut LiveNode,
    instruments: [InstrumentId; 3],
    operation_id: UUID4,
    admission: Admission,
    resume: bool,
) -> StrategyEvents {
    let (accepted, orders) = tokio::sync::mpsc::unbounded_channel();
    let (results, conversions) = tokio::sync::mpsc::unbounded_channel();
    node.add_strategy(LiveConvertStrategy {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(STRATEGY_ID)),
            oms_type: Some(OmsType::Netting),
            ..Default::default()
        }),
        instruments,
        operation_id,
        next_leg: if resume { 3 } else { 0 },
        resume,
        late_input: admission == Admission::LateInput,
        late_order_queued: false,
        accepted,
        results,
    })
    .unwrap();
    StrategyEvents {
        orders,
        conversions,
    }
}

impl DataActor for LiveConvertStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        if self.resume {
            let output = self.cache().position(&position_id(
                self.instruments[2],
                StrategyId::from(STRATEGY_ID),
            ));
            assert_recovered_output(output, self.operation_id);
            return self.trade(2);
        }
        self.trade(self.next_leg)
    }
}

fn assert_recovered_output(output: Option<Position>, operation_id: UUID4) {
    let output = output.expect("startup recovery has opened the YES position");
    assert_eq!(output.opening_operation_id, Some(operation_id));
    assert_eq!(output.quantity.as_decimal(), dec!(100));
    assert_eq!(output.cost_open, dec!(50));
    assert!(output.events.is_empty());
}

nautilus_strategy!(LiveConvertStrategy, {
    fn on_order_accepted(&mut self, event: OrderAccepted) {
        self.accepted.send(event).unwrap();
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        panic!("Unexpected CLOB denial: {event}");
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        panic!("Unexpected CLOB rejection: {event}");
    }

    fn on_order_filled(&mut self, fill: &OrderFilled) {
        if fill.client_order_id.as_str() == ORDER_IDS[3] {
            return;
        }

        match self.next_leg {
            0 => {
                self.next_leg = 1;
                self.trade(1).unwrap();
            }
            1 => {
                self.next_leg = 2;
                let account_id = AccountId::from(ACCOUNT_ID);
                let inputs = self.instruments[..2]
                    .iter()
                    .map(|&instrument_id| ConvertTokenAmount {
                        instrument_id,
                        quantity: dec!(100),
                    })
                    .collect::<Vec<_>>();
                for _ in 0..2 {
                    let id = self
                        .submit_convert(
                            inputs.clone(),
                            account_id,
                            *POLYMARKET_CLIENT_ID,
                            Some(self.operation_id),
                        )
                        .unwrap();
                    assert_eq!(id, self.operation_id);
                }
            }
            3 => self
                .query_account(fill.account_id, Some(*POLYMARKET_CLIENT_ID), None)
                .unwrap(),
            _ => unreachable!(),
        }
    }

    fn on_convert_result(&mut self, result: ConvertResult) {
        if self.late_input
            && result.status == ConvertExecutionStatus::Submitted
            && !self.late_order_queued
        {
            self.late_order_queued = true;
            self.trade(3).unwrap();
        }
        let first_applied = result.applied && self.next_leg == 2;
        self.results.send(result).unwrap();

        if first_applied {
            self.next_leg = 3;
            self.query_convert(
                self.operation_id,
                AccountId::from(ACCOUNT_ID),
                *POLYMARKET_CLIENT_ID,
            )
            .unwrap();
            self.trade(2).unwrap();
        }
    }
});

fn instrument(condition: &NegRiskCondition, yes: bool) -> InstrumentAny {
    let mut binary = binary_option();
    let outcome = if yes { &condition.yes } else { &condition.no };
    let token_id = U256::from_be_bytes(outcome.token_id).to_string();
    binary.id = outcome.instrument_id;
    binary.raw_symbol = Symbol::from(token_id.as_str());
    binary.currency = Currency::pUSD();
    binary.expiration_ns = UnixNanos::default();
    binary.price_precision = POLYMARKET_PRICE_PRECISION;
    binary.price_increment = Price::from("0.0001");
    binary.size_precision = 4;
    binary.size_increment = Quantity::from("0.0001");
    binary.outcome = Some(Ustr::from(if yes { "Yes" } else { "No" }));
    binary.info = Some(
        serde_json::from_value(json!({
            "market_version": "v1",
            "condition_id": format!("{:#x}", B256::from(condition.condition_id)),
            "token_id": token_id,
            "neg_risk": true,
            "fee_schedule": { "rate": if yes { "0.01" } else { "0" }, "exponent": "1" },
        }))
        .unwrap(),
    );
    InstrumentAny::BinaryOption(binary)
}

fn build_node(venue_addr: SocketAddr, chain_addr: SocketAddr, wallet: Address) -> LiveNode {
    let mut execution = execution_config_with_retries(venue_addr, 50);
    execution.signature_type = PolymarketSignatureType::Poly1271;
    execution.funder = Some(format!("{wallet:#x}"));
    execution.rpc_url = Some(format!("http://{chain_addr}").into());
    execution.base_url_relayer = Some(format!("http://{chain_addr}"));
    execution.relayer_api_key = Some("offline-relayer-key".into());
    execution.relayer_signer_address = Some(TEST_SIGNER_ADDRESS.into());
    LiveNodeBuilder::from_config(LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from(TRADER_ID),
        exec_engine: LiveExecutionEngineConfig {
            reconciliation: false,
            ..Default::default()
        },
        delay_post_stop: Duration::from_millis(20),
        ..Default::default()
    })
    .unwrap()
    .with_name("PolymarketLiveConvert")
    .add_exec_client(
        None,
        Box::new(PolymarketExecutionClientFactory),
        Box::new(execution),
    )
    .unwrap()
    .build()
    .unwrap()
}

fn load_instruments(node: &LiveNode, mapping: &CtfNegRiskEvent) {
    let kernel = node.kernel();

    for condition in mapping.conditions() {
        for yes in [true, false] {
            let instrument = instrument(condition, yes);
            kernel
                .cache
                .borrow_mut()
                .add_instrument(instrument.clone())
                .unwrap();
            kernel
                .exec_engine
                .borrow_mut()
                .get_client_adapter_mut(&POLYMARKET_CLIENT_ID)
                .unwrap()
                .on_instrument(instrument);
        }
    }
    kernel
        .cache
        .borrow_mut()
        .add_convert_event(mapping.clone())
        .unwrap();
}

fn stage_trade(
    index: usize,
    mapping: &CtfNegRiskEvent,
    wallet: Address,
    venue_order_id: VenueOrderId,
) -> Value {
    let condition = &mapping.conditions()[if index == 3 { 0 } else { index }];
    let outcome = if index == 2 {
        &condition.yes
    } else {
        &condition.no
    };
    let mut trade = load_json("ws_user_trade_full.json");
    trade["id"] = json!(format!("trade-convert-{index}"));
    trade["event_type"] = json!("trade");
    trade["market"] = json!(format!("{:#x}", B256::from(condition.condition_id)));
    trade["asset_id"] = json!(U256::from_be_bytes(outcome.token_id).to_string());
    trade["outcome"] = json!(if index == 2 { "Yes" } else { "No" });
    trade["side"] = json!(if index == 2 { "SELL" } else { "BUY" });
    trade["price"] = json!(PRICES[index]);
    if index == 3 {
        trade["size"] = json!("1.0000");
        trade["maker_orders"][0]["matched_amount"] = json!("1.0000");
    }
    trade["maker_address"] = json!(format!("{wallet:#x}"));
    trade["maker_orders"][0]["asset_id"] = trade["asset_id"].clone();
    trade["maker_orders"][0]["outcome"] = trade["outcome"].clone();
    trade["maker_orders"][0]["price"] = trade["price"].clone();
    trade["taker_order_id"] = json!(venue_order_id.as_str());
    trade
}

async fn set_order_id(venue: &TestServerState, index: usize) {
    let mut response = venue.order_response.lock().await;
    response.as_mut().unwrap()["orderID"] = json!(format!("0x{:064x}", index + 1));
}

#[rstest]
#[case::applied(Admission::Ready)]
#[case::rejected_before_signing(Admission::Unapproved)]
#[case::unknown_without_locator(Admission::MissingLocator)]
#[case::applied_without_confirmation(Admission::Unconfirmed)]
#[case::pending_fill_after_submit(Admission::LateInput)]
#[case::balance_refresh_failure(Admission::RefreshFailure)]
#[case::stop_while_submission_in_flight(Admission::StopDuringSubmit)]
#[case::rpc_without_finalized(Admission::UnsupportedFinalized)]
#[case::hash_only_retained_before_verification(Admission::HashOnly)]
#[tokio::test]
async fn live_convert_uses_wallet_evidence_and_preserves_scope(#[case] admission: Admission) {
    exercise_live_convert(admission, None).await;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Admission {
    Ready,
    Unapproved,
    MissingLocator,
    Unconfirmed,
    LateInput,
    RefreshFailure,
    StopDuringSubmit,
    UnsupportedFinalized,
    HashOnly,
}

#[cfg(target_os = "linux")]
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Requires a local Redis service; run explicitly for Convert restart acceptance"]
async fn live_convert_recovers_original_submission_from_redis_and_sells() {
    exercise_live_convert(Admission::Ready, Some(UUID4::new())).await;
}

#[cfg(target_os = "linux")]
async fn redis_database(id: UUID4) -> RedisCacheDatabaseAdapter {
    let database = RedisCacheDatabase::new(
        TraderId::from(TRADER_ID),
        id,
        CacheConfig {
            use_instance_id: true,
            ..Default::default()
        },
        RedisCacheConfig {
            host: Some("127.0.0.1".into()),
            port: Some(
                std::env::var("NAUTILUS_TEST_REDIS_PORT")
                    .map_or(6379, |port| port.parse().unwrap()),
            ),
            connection_timeout: 1,
            number_of_retries: 0,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    RedisCacheDatabaseAdapter { database }
}

#[cfg(target_os = "linux")]
async fn configure_redis(node: &mut LiveNode, id: UUID4, mapping: &CtfNegRiskEvent) {
    let mut seed_cache = Cache::new(None, Some(Box::new(redis_database(id).await)));
    for condition in mapping.conditions() {
        seed_cache
            .add_instrument(instrument(condition, true))
            .unwrap();
        seed_cache
            .add_instrument(instrument(condition, false))
            .unwrap();
    }
    seed_cache.dispose();

    node.set_cache_database(Box::new(redis_database(id).await))
        .unwrap();
    node.kernel()
        .cache
        .borrow_mut()
        .add_convert_event(mapping.clone())
        .unwrap();
}

async fn exercise_live_convert(admission: Admission, redis_instance: Option<UUID4>) {
    let signed = !matches!(
        admission,
        Admission::Unapproved | Admission::Unconfirmed | Admission::UnsupportedFinalized
    );
    let omit_locator = admission == Admission::MissingLocator;
    let sample = serde_json::from_str(SAMPLE).unwrap();
    let mapping = event_mapping(&sample, 3, 0);
    let instruments = [
        mapping.conditions()[0].no.instrument_id,
        mapping.conditions()[1].no.instrument_id,
        mapping.conditions()[2].yes.instrument_id,
    ];
    let (chain_addr, chain) = start_chain(&mapping).await;
    chain
        .approved
        .store(admission != Admission::Unapproved, Ordering::Release);
    chain.omit_locator.store(omit_locator, Ordering::Release);
    chain.supports_finalized.store(
        admission != Admission::UnsupportedFinalized,
        Ordering::Release,
    );

    if admission == Admission::StopDuringSubmit {
        chain.submit_gate.enable();
    }

    if admission == Admission::HashOnly {
        chain.hash_only.store(true, Ordering::Release);
        chain.transaction_gate.enable();
    }
    let venue = TestServerState::default();
    venue.configure_default_order_success().await;
    let position_market = format!("{:#x}", B256::from(mapping.conditions()[0].condition_id));
    venue.market_responses.lock().await.insert(
        position_market.clone(),
        (
            StatusCode::OK,
            json!({
                "condition_id": position_market,
                "neg_risk": true,
                "closed": false,
                "version": "v1",
                "tokens": [],
            }),
        ),
    );
    set_order_id(&venue, 0).await;
    let venue_addr = start_mock_server(venue.clone()).await;
    let mut node = build_node(venue_addr, chain_addr, chain.wallet);
    #[cfg(target_os = "linux")]
    if let Some(instance_id) = redis_instance {
        configure_redis(&mut node, instance_id, &mapping).await;
    }

    if redis_instance.is_none() {
        load_instruments(&node, &mapping);
    }
    let handle = node.handle();
    let cache = node.kernel().cache.clone();
    let operation_id = UUID4::new();
    let account_id = AccountId::from(ACCOUNT_ID);
    let strategy_id = StrategyId::from(STRATEGY_ID);
    let StrategyEvents {
        mut orders,
        mut conversions,
    } = add_strategy(&mut node, instruments, operation_id, admission, false);

    let driver = async {
        for index in 0..2 {
            let event = orders.recv().await.unwrap();
            assert_eq!(event.client_order_id.as_str(), ORDER_IDS[index]);
            assert_eq!(event.instrument_id, instruments[index]);
            set_order_id(
                &venue,
                if index == 1 && admission == Admission::LateInput {
                    3
                } else {
                    index + 1
                },
            )
            .await;
            venue.balance_response.lock().await["balance"] =
                json!(if index == 0 { "920000000" } else { "850000000" });
            let mut trade = stage_trade(index, &mapping, chain.wallet, event.venue_order_id);
            if index == 1 && admission == Admission::Unconfirmed {
                trade["status"] = json!("MATCHED");
            }

            if redis_instance.is_some() {
                let trade_id = trade["id"].as_str().unwrap().to_string();
                venue.trades_response_by_id.lock().await.insert(
                    trade_id,
                    (
                        StatusCode::OK,
                        json!({"data": [trade], "next_cursor": "LTE="}),
                    ),
                );
            }
            venue.send_user(trade).await;
        }

        if signed {
            wait_until_async(
                || async { chain.submitted.lock().await.is_some() },
                DEADLINE,
            )
            .await;
            let request = chain.submitted.lock().await.clone().unwrap();
            assert_eq!(request["nonce"], NONCE.to_string());
            assert_eq!(request["to"], format!("{DEPOSIT_WALLET_FACTORY:#x}"));
            assert_eq!(
                request["depositWalletParams"]["depositWallet"],
                format!("{:#x}", chain.wallet)
            );
            let calls = request["depositWalletParams"]["calls"].as_array().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(
                calls[0]["target"],
                format!("{NEG_RISK_CTF_COLLATERAL_ADAPTER:#x}")
            );
            assert_eq!(
                calls[0]["data"],
                format!("0x{}", hex::encode(&chain.calldata))
            );
            assert!(!request["signature"].as_str().unwrap().is_empty());
            for instrument in instruments {
                assert!(
                    cache
                        .borrow()
                        .convert_scope_is_active(instrument, account_id)
                );
            }
            assert!(
                cache
                    .borrow()
                    .position(&position_id(instruments[2], strategy_id))
                    .is_none()
            );
            assert_eq!(chain.chain.submits.load(Ordering::Acquire), 1);
        }

        if !signed || omit_locator {
            let expected = if omit_locator {
                ConvertExecutionStatus::Unknown
            } else {
                ConvertExecutionStatus::Rejected
            };

            loop {
                let result = conversions.recv().await.unwrap();
                if result.status == expected && (!omit_locator || result.transaction.is_some()) {
                    assert!(!result.applied);
                    if omit_locator {
                        let transaction = result.transaction.unwrap();
                        assert_eq!(transaction.nonce.as_deref(), Some("7"));
                        assert!(!transaction.has_original_locator());
                    }
                    break;
                }
            }
            assert_eq!(
                chain.chain.submits.load(Ordering::Acquire),
                usize::from(signed)
            );

            match admission {
                Admission::Unconfirmed => {
                    assert_eq!(chain.approval_reads.load(Ordering::Acquire), 0);
                }
                Admission::Unapproved => assert!(chain.approval_reads.load(Ordering::Acquire) > 0),
                _ => {}
            }
            assert_eq!(
                cache
                    .borrow()
                    .position(&position_id(instruments[1], strategy_id))
                    .unwrap()
                    .quantity
                    .as_decimal(),
                dec!(100),
            );

            for instrument in instruments {
                assert_eq!(
                    cache
                        .borrow()
                        .convert_scope_is_active(instrument, account_id),
                    signed
                );
            }

            if omit_locator {
                assert_historical_gap(&mut cache.borrow_mut(), operation_id, instruments);
            }
            handle.stop();
            return;
        }

        if redis_instance.is_some() {
            wait_until_async(
                || async {
                    cache
                        .borrow()
                        .load_convert_operation(operation_id)
                        .unwrap()
                        .and_then(|record| record.transaction)
                        .is_some_and(|transaction| {
                            transaction.relayer_transaction_id.as_deref() == Some(RELAYER_ID)
                        })
                },
                DEADLINE,
            )
            .await;
            assert!(
                cache
                    .borrow()
                    .position(&position_id(instruments[2], strategy_id))
                    .is_none()
            );
            handle.stop();
            return;
        }

        if admission == Admission::StopDuringSubmit {
            assert_eq!(chain.submit_gate.started(), 1);
            handle.stop();
            return;
        }

        if admission == Admission::HashOnly {
            wait_until_async(|| async { chain.transaction_gate.started() > 0 }, DEADLINE).await;

            loop {
                let result = conversions.recv().await.unwrap();
                if let Some(transaction) = result.transaction {
                    assert_eq!(result.status, ConvertExecutionStatus::Unknown);
                    assert!(!result.applied);
                    assert!(transaction.relayer_transaction_id.is_none());
                    assert_eq!(transaction.transaction_hash.as_deref(), Some(HASH));
                    assert_eq!(transaction.nonce.as_deref(), Some("7"));
                    break;
                }
            }
            let record = cache
                .borrow()
                .load_convert_operation(operation_id)
                .unwrap()
                .unwrap();
            assert_eq!(
                record.transaction.unwrap().transaction_hash.as_deref(),
                Some(HASH),
            );
            assert_eq!(chain.chain.receipt_reads.load(Ordering::Acquire), 0);
            handle.stop();
            return;
        }

        if admission == Admission::LateInput {
            let order = orders.recv().await.unwrap();
            assert_eq!(order.client_order_id.as_str(), ORDER_IDS[3]);
            let mut trade = stage_trade(3, &mapping, chain.wallet, order.venue_order_id);
            trade["status"] = json!("MATCHED");
            venue.send_user(trade).await;
            wait_until_async(
                || async {
                    cache
                        .borrow()
                        .position(&position_id(instruments[0], strategy_id))
                        .is_some_and(|position| position.quantity.as_decimal() == dec!(101))
                },
                DEADLINE,
            )
            .await;
            chain.nonce.store(NONCE + 1, Ordering::Release);
            chain.chain.finalized.store(true, Ordering::Release);

            loop {
                let result = conversions.recv().await.unwrap();
                if result.status == ConvertExecutionStatus::Confirmed {
                    assert!(!result.applied);
                    assert!(result.unapplied_instruments.contains(&instruments[2]));
                    break;
                }
            }
            assert!(
                cache
                    .borrow()
                    .convert_scope_is_active(instruments[2], account_id)
            );
            assert!(
                cache
                    .borrow()
                    .position(&position_id(instruments[2], strategy_id))
                    .is_none()
            );
            assert_eq!(chain.chain.submits.load(Ordering::Acquire), 1);
            handle.stop();
            return;
        }

        wait_until_async(
            || async { chain.chain.receipt_reads.load(Ordering::Acquire) > 0 },
            DEADLINE,
        )
        .await;
        wait_until_async(
            || async { total_cash(&cache.borrow(), account_id) == Some(dec!(850)) },
            DEADLINE,
        )
        .await;
        let balance_gate = venue.balance_response_gate.clone();
        balance_gate.enable();

        if admission == Admission::RefreshFailure {
            *venue.balance_response_status.lock().await = StatusCode::BAD_GATEWAY;
        }
        venue.balance_response.lock().await["balance"] = json!("950000000");
        chain.nonce.store(NONCE + 1, Ordering::Release);
        chain.chain.finalized.store(true, Ordering::Release);

        loop {
            let result = conversions.recv().await.unwrap();
            if result.applied {
                assert_eq!(result.status, ConvertExecutionStatus::Confirmed);
                break;
            }
        }
        {
            let cache = cache.borrow();
            assert_eq!(total_cash(&cache, account_id), Some(dec!(850)));
            for instrument in &instruments[..2] {
                let position = cache
                    .position(&position_id(*instrument, strategy_id))
                    .unwrap();
                assert!(position.is_closed());
                assert_eq!(position.events.len(), 1);
            }
            let output = cache
                .position(&position_id(instruments[2], strategy_id))
                .unwrap();
            assert_eq!(output.quantity.as_decimal(), dec!(100));
            assert_eq!(output.cost_open, dec!(50));
            assert_eq!(output.opening_operation_id, Some(operation_id));
            assert!(output.events.is_empty());
        }
        let sell = orders.recv().await.unwrap();
        assert_eq!(sell.client_order_id.as_str(), ORDER_IDS[2]);

        if admission == Admission::RefreshFailure {
            wait_until_async(|| async { balance_gate.started() > 0 }, DEADLINE).await;
            *venue.balance_response_status.lock().await = StatusCode::OK;
        }
        venue.balance_response.lock().await["balance"] = json!("1019790000");
        venue
            .send_user(stage_trade(2, &mapping, chain.wallet, sell.venue_order_id))
            .await;
        wait_until_async(
            || async {
                cache
                    .borrow()
                    .order(&ClientOrderId::from(ORDER_IDS[2]))
                    .is_some_and(|order| order.status() == OrderStatus::Filled)
            },
            DEADLINE,
        )
        .await;
        wait_until_async(|| async { balance_gate.started() > 0 }, DEADLINE).await;
        let released = Cell::new(0);
        wait_until_async(
            || async {
                let started = balance_gate.started();
                for _ in released.get()..started {
                    balance_gate.release();
                }
                released.set(started);
                total_cash(&cache.borrow(), account_id) == Some(dec!(1019.79))
            },
            DEADLINE,
        )
        .await;
        handle.stop();
    };

    let (result, ()) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(node.run(), driver)
    })
    .await
    .expect("live Convert flow completed");
    result.unwrap();

    if admission == Admission::HashOnly {
        chain.transaction_gate.release();
    }

    #[cfg(target_os = "linux")]
    if let Some(id) = redis_instance {
        node.dispose();
        drop(node);
        assert_eq!(chain.chain.submits.load(Ordering::Acquire), 1);
        chain.nonce.store(NONCE + 1, Ordering::Release);
        chain.chain.finalized.store(true, Ordering::Release);
        venue.balance_response.lock().await["balance"] = json!("950000000");

        let mut recovered = build_node(venue_addr, chain_addr, chain.wallet);
        recovered
            .set_cache_database(Box::new(redis_database(id).await))
            .unwrap();
        let cache = recovered.kernel().cache.clone();
        cache
            .borrow_mut()
            .add_convert_event(mapping.clone())
            .unwrap();
        let handle = recovered.handle();
        let mut events = add_strategy(
            &mut recovered,
            instruments,
            operation_id,
            Admission::Ready,
            true,
        );

        let driver = async {
            let sell = events.orders.recv().await.unwrap();
            assert_eq!(sell.client_order_id.as_str(), ORDER_IDS[2]);
            venue.balance_response.lock().await["balance"] = json!("1019790000");
            venue
                .send_user(stage_trade(2, &mapping, chain.wallet, sell.venue_order_id))
                .await;
            wait_until_async(
                || async {
                    cache
                        .borrow()
                        .position(&position_id(instruments[2], strategy_id))
                        .is_some_and(|position| position.is_closed())
                },
                DEADLINE,
            )
            .await;
            handle.stop();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::join!(recovered.run(), driver)
        })
        .await
        .unwrap();
        result.unwrap();
        let record = cache
            .borrow()
            .load_convert_operation(operation_id)
            .unwrap()
            .unwrap();
        assert!(record.applied);
        assert_eq!(
            record
                .transaction
                .unwrap()
                .relayer_transaction_id
                .as_deref(),
            Some(RELAYER_ID)
        );
        assert_closed_output(&cache.borrow(), instruments[2], strategy_id);
        assert_eq!(chain.chain.submits.load(Ordering::Acquire), 1);
        return;
    }

    if matches!(admission, Admission::Ready | Admission::RefreshFailure) {
        assert_closed_output(&cache.borrow(), instruments[2], strategy_id);
        assert_eq!(chain.chain.submits.load(Ordering::Acquire), 1);
    } else if omit_locator || admission == Admission::StopDuringSubmit {
        if admission == Admission::StopDuringSubmit {
            chain.nonce.store(NONCE + 1, Ordering::Release);
            chain.chain.finalized.store(true, Ordering::Release);
            chain.submit_gate.release();
            let cache = cache.borrow();
            let record = cache.load_convert_operation(operation_id).unwrap().unwrap();
            assert!(!record.applied);
            assert!(cache.convert_scope_is_active(instruments[2], account_id));
        }
        let client = PolymarketPositionClient::new(
            &EvmPrivateKey::new(TEST_PRIVATE_KEY).unwrap(),
            &format!("{:#x}", chain.wallet),
            RelayerApiKey::new("offline-relayer-key".into(), TEST_SIGNER_ADDRESS).unwrap(),
            Some(format!("http://{chain_addr}")),
            Some(format!("http://{venue_addr}")),
            Some(1),
            None,
        )
        .unwrap()
        .with_rpc_url(format!("http://{chain_addr}"));
        let error = client
            .split_position(&position_market, dec!(1))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("outcome is unknown"), "{error}");
        assert_eq!(chain.chain.submits.load(Ordering::Acquire), 1);
    }
}

fn total_cash(cache: &Cache, account_id: AccountId) -> Option<Decimal> {
    cache
        .account(&account_id)
        .and_then(|account| account.balance(Some(Currency::pUSD())).copied())
        .map(|balance| balance.total.as_decimal())
}

fn assert_historical_gap(cache: &mut Cache, operation_id: UUID4, instruments: [InstrumentId; 3]) {
    let account_id = AccountId::from(ACCOUNT_ID);

    for (index, instrument) in instruments.into_iter().enumerate() {
        cache
            .add_instrument_close(InstrumentClose::new(
                instrument,
                Price::from("0.00"),
                InstrumentCloseType::ContractExpired,
                UnixNanos::default(),
                UnixNanos::default(),
            ))
            .unwrap();
        assert_eq!(
            cache.convert_scope_is_active(instruments[2], account_id),
            index < 2
        );
    }
    let record = cache.load_convert_operation(operation_id).unwrap().unwrap();
    assert!(record.historical_gap);
    assert!(!record.applied);
    assert!(!record.authorization_invalid);
    assert_eq!(record.status, Some(ConvertExecutionStatus::Unknown));
}

fn assert_closed_output(cache: &Cache, output: InstrumentId, strategy: StrategyId) {
    let output = cache.position(&position_id(output, strategy)).unwrap();
    assert!(output.is_closed());
    assert_eq!(
        output.closing_order_id,
        Some(ClientOrderId::from(ORDER_IDS[2]))
    );
    assert_eq!(output.realized_pnl, Some(Money::from("19.79 pUSD")));
    assert_eq!(output.commissions(), vec![Money::from("0.21 pUSD")]);
    assert!(matches!(
        output.replay_events.as_slice(),
        [
            PositionReplayEvent::Converted(_),
            PositionReplayEvent::Filled(_)
        ]
    ));
}
