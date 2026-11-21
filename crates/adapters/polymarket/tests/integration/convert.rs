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

mod submit;

use std::{
    cell::RefCell,
    collections::HashMap,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy::{sol, sol_types::SolCall};
use alloy_primitives::{Address, B256, Bytes, U256, hex};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    clock::VirtualClock,
    live::runner::set_exec_event_sender,
    messages::{
        ExecutionEvent,
        execution::{
            ConvertExecutionEvent, ConvertExecutionOutcome, ConvertExecutionStatus,
            ConvertOperationRecord, ConvertTokenAmount, ConvertTransaction, QueryConvert,
            SubmitConvert,
        },
    },
    msgbus::{self, MessageBus, MessagingSwitchboard},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_execution::engine::ExecutionEngine;
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType, OrderSide},
    events::order::spec::OrderFilledSpec,
    identifiers::{AccountId, ClientOrderId, InstrumentId, PositionId, StrategyId, TraderId},
    instruments::{
        InstrumentAny,
        neg_risk::{CtfNegRiskEvent, NegRiskCondition, NegRiskOutcome},
        stubs::binary_option,
    },
    position::Position,
    types::{Currency, Money, Price, Quantity},
};
use nautilus_polymarket::{
    common::{
        consts::{POLYMARKET_CLIENT_ID, POLYMARKET_VENUE},
        enums::PolymarketSignatureType,
    },
    config::PolymarketExecutionClientConfig,
    execution::PolymarketExecutionClient,
};
use rstest::rstest;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{Value, json};

const SAMPLE: &str = include_str!("../../test_data/chain_convert_deposit_wallet.json");
const WALLET: &str = "0x1387d145aaf01f6e33b66525dda6e1f51f6955f8";
const OTHER_WALLET: &str = "0x2222222222222222222222222222222222222222";
const HASH: &str = "0x53ce0373717a3922e5f07153f59156ebc2748f2b071ad8991c72ad0aab21e806";
const WRONG_HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const RELAYER_ID: &str = "0190b317-a1d3-7bec-9b91-eeb6dcd3a620";
const NONZERO_FEE_BPS: u16 = 100;
const NONZERO_FEE_OUTPUT: u64 = 990_000;
const NONZERO_FEE_CASH: u64 = 7_920_000;

sol! {
    struct TestCall {
        address target;
        uint256 value;
        bytes data;
    }

    struct TestBatch {
        address wallet;
        uint256 nonce;
        uint256 deadline;
        TestCall[] calls;
    }

    function proxy(TestBatch[] batches, bytes[] signatures);
}

fn factory_call(sample: &Value) -> proxyCall {
    let words: Vec<String> = serde_json::from_value(sample["input"].clone()).unwrap();
    let input = hex::decode(words.concat()).unwrap();
    proxyCall::abi_decode_validate(&input).unwrap()
}

fn encode_factory_call(batches: &[TestBatch], signatures: &[Bytes]) -> String {
    format!(
        "0x{}",
        hex::encode(
            proxyCall {
                batches: batches.to_vec(),
                signatures: signatures.to_vec(),
            }
            .abi_encode()
        )
    )
}

fn other_batch(deadline: U256) -> TestBatch {
    TestBatch {
        wallet: OTHER_WALLET.parse::<Address>().unwrap(),
        nonce: U256::from(1),
        deadline,
        calls: vec![TestCall {
            target: Address::ZERO,
            value: U256::ZERO,
            data: Bytes::new(),
        }],
    }
}

fn other_wallet_topic() -> String {
    format!(
        "0x000000000000000000000000{}",
        OTHER_WALLET.trim_start_matches("0x")
    )
}

fn replace_word(data: &str, word: usize, value: u64) -> String {
    let start = 2 + word * 64;
    let end = start + 64;
    format!("{}{:064x}{}", &data[..start], value, &data[end..])
}

fn replace_last_words(data: &str, count: usize, value: u64) -> String {
    let start = data.len() - count * 64;
    let suffix = format!("{value:064x}").repeat(count);
    format!("{}{suffix}", &data[..start])
}

fn log_index(logs: &[Value], index: &str) -> usize {
    logs.iter()
        .position(|log| log["logIndex"].as_str() == Some(index))
        .unwrap()
}

// Every non-Valid variant is a deterministic mutation of the captured fixture, not another live
// chain sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Evidence {
    Valid,
    WrongChain,
    WrongBlock,
    ReorganizedBlock,
    MissingBlockTransaction,
    WrongFactory,
    WrongWallet,
    WrongNonce,
    WrongTarget,
    WrongAmount,
    MissingBatch,
    MissingOutput,
    ExtraDebit,
    Reverted,
    PendingTransactionOnce,
    MissingReceiptOnce,
    PartialTransactionInclusion,
    RelayerHashChanges,
    RetainedHashRelayerConflict,
    RetainedHashRelayerNotFound,
    RelayerIdNotFoundOnly,
    MultiWalletBatch,
    SameWalletSecondBatch,
    ExtraWalletCall,
    NonZeroFee,
    WrongConsumedAmount,
    PendingReceipt,
}

struct MockChain {
    sample: Value,
    evidence: Evidence,
    finalized: AtomicBool,
    relay_reads: AtomicUsize,
    rpc_reads: AtomicUsize,
    transaction_reads: AtomicUsize,
    receipt_reads: AtomicUsize,
    finalized_reads: AtomicUsize,
    submits: AtomicUsize,
}

impl MockChain {
    fn new(sample: Value, evidence: Evidence, finalized: bool) -> Self {
        Self {
            sample,
            evidence,
            finalized: AtomicBool::new(finalized),
            relay_reads: AtomicUsize::new(0),
            rpc_reads: AtomicUsize::new(0),
            transaction_reads: AtomicUsize::new(0),
            receipt_reads: AtomicUsize::new(0),
            finalized_reads: AtomicUsize::new(0),
            submits: AtomicUsize::new(0),
        }
    }

    fn transaction(&self) -> Value {
        let mut transaction = self.sample["transaction"].clone();
        let mut words: Vec<String> = serde_json::from_value(self.sample["input"].clone()).unwrap();

        match self.evidence {
            Evidence::WrongFactory => transaction["to"] = json!(WALLET),
            Evidence::WrongWallet => words[5] = format!("{:064x}", 1),
            Evidence::WrongNonce => words[6] = format!("{:064x}", 96_471),
            Evidence::WrongTarget => words[11] = format!("{:064x}", 1),
            Evidence::WrongAmount => words[18].replace_range(..8, "001e8480"),
            Evidence::MultiWalletBatch => {
                let decoded = factory_call(&self.sample);
                let mut batches = decoded.batches;
                let signature = decoded.signatures[0].clone();
                batches.push(other_batch(batches[0].deadline));
                transaction["input"] = json!(encode_factory_call(
                    &batches,
                    &[signature.clone(), signature]
                ));
                return transaction;
            }
            Evidence::SameWalletSecondBatch => {
                let decoded = factory_call(&self.sample);
                let mut batches = decoded.batches;
                let signature = decoded.signatures[0].clone();
                let mut second = batches[0].clone();
                second.nonce += U256::from(1u8);
                batches.push(second);
                transaction["input"] = json!(encode_factory_call(
                    &batches,
                    &[signature.clone(), signature]
                ));
                return transaction;
            }
            Evidence::ExtraWalletCall => {
                let mut decoded = factory_call(&self.sample);
                let signature = decoded.signatures[0].clone();
                decoded.batches[0].calls.push(TestCall {
                    target: Address::ZERO,
                    value: U256::ZERO,
                    data: Bytes::new(),
                });
                transaction["input"] = json!(encode_factory_call(&decoded.batches, &[signature]));
                return transaction;
            }
            _ => {}
        }
        transaction["input"] = json!(words.concat());
        transaction
    }

    fn receipt(&self) -> Value {
        let transaction = &self.sample["transaction"];
        let mut logs = self.sample["logs"].as_array().unwrap().clone();
        match self.evidence {
            Evidence::MissingBatch | Evidence::Reverted => logs.clear(),
            Evidence::MissingOutput => logs.retain(|entry| entry["logIndex"] != "0x26c"),
            Evidence::ExtraDebit => {
                let mut debit = logs[4].clone();
                debit["logIndex"] = json!("0x270");
                debit["topics"].as_array_mut().unwrap().swap(1, 2);
                logs.insert(5, debit);
            }
            Evidence::NonZeroFee => {
                let positions = log_index(&logs, "0x26d");
                let positions_data = logs[positions]["data"].as_str().unwrap();
                logs[positions]["data"] =
                    json!(replace_word(positions_data, 1, NONZERO_FEE_OUTPUT));

                let yes_transfer = log_index(&logs, "0x26c");
                let yes_data = logs[yes_transfer]["data"].as_str().unwrap();
                logs[yes_transfer]["data"] =
                    json!(replace_last_words(yes_data, 2, NONZERO_FEE_OUTPUT));

                for index in ["0x26f", "0x271"] {
                    let log = log_index(&logs, index);
                    let data = logs[log]["data"].as_str().unwrap();
                    logs[log]["data"] = json!(replace_word(data, 0, NONZERO_FEE_CASH));
                }
            }
            Evidence::WrongConsumedAmount => {
                let input = log_index(&logs, "0x25f");
                let data = logs[input]["data"].as_str().unwrap();
                logs[input]["data"] = json!(replace_last_words(data, 9, 2_000_000));
            }
            Evidence::MultiWalletBatch => {
                let mut other_transfer = logs[log_index(&logs, "0x26c")].clone();
                other_transfer["logIndex"] = json!("0x280");
                other_transfer["topics"][3] = json!(other_wallet_topic());
                logs.push(other_transfer);

                let mut other_conversion = logs[log_index(&logs, "0x26d")].clone();
                other_conversion["logIndex"] = json!("0x281");
                other_conversion["topics"][1] = json!(other_wallet_topic());
                logs.push(other_conversion);
            }
            _ => {}
        }

        for log in &mut logs {
            log["blockNumber"] = transaction["blockNumber"].clone();
            log["blockHash"] = transaction["blockHash"].clone();
            log["transactionHash"] = transaction["hash"].clone();
            log["transactionIndex"] = transaction["transactionIndex"].clone();
            log["removed"] = json!(false);
        }

        let status = if self.evidence == Evidence::Reverted {
            "0x0"
        } else {
            "0x1"
        };
        json!({
            "transactionHash": transaction["hash"],
            "transactionIndex": transaction["transactionIndex"],
            "blockHash": transaction["blockHash"],
            "blockNumber": transaction["blockNumber"],
            "from": transaction["from"],
            "to": transaction["to"],
            "status": status,
            "logs": logs
        })
    }
}

fn event_mapping(sample: &Value, count: usize, fee_bps: u16) -> CtfNegRiskEvent {
    let conditions = sample["conditions"].as_array().unwrap()[..count]
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let condition_id = row[0].as_str().unwrap().parse::<B256>().unwrap();
            let outcome = |token: &Value| {
                let token_id = token.as_str().unwrap().parse::<B256>().unwrap();
                let token_number = U256::from_be_bytes(token_id.0);
                NegRiskOutcome {
                    instrument_id: InstrumentId::from(format!(
                        "{condition_id:#x}-{token_number}.POLYMARKET"
                    )),
                    token_id: token_id.0,
                }
            };
            NegRiskCondition {
                index: u8::try_from(index).unwrap(),
                condition_id: condition_id.0,
                yes: outcome(&row[1]),
                no: outcome(&row[2]),
            }
        })
        .collect();
    let market_id = sample["market"].as_str().unwrap().parse::<B256>().unwrap();
    CtfNegRiskEvent::new_checked(market_id.0, fee_bps, conditions).unwrap()
}

async fn rpc(State(chain): State<Arc<MockChain>>, Json(request): Json<Value>) -> Json<Value> {
    chain.rpc_reads.fetch_add(1, Ordering::Relaxed);
    let params = &request["params"];
    let result = match request["method"].as_str().unwrap() {
        "eth_chainId" => json!(if chain.evidence == Evidence::WrongChain {
            "0x1"
        } else {
            "0x89"
        }),
        "eth_getTransactionByHash" => {
            let reads = chain.transaction_reads.fetch_add(1, Ordering::Relaxed);
            if chain.evidence == Evidence::RelayerHashChanges && params[0] == WRONG_HASH {
                Value::Null
            } else {
                assert_eq!(params[0], HASH);
                let mut transaction = chain.transaction();
                if chain.evidence == Evidence::PendingTransactionOnce && reads == 0 {
                    transaction["blockNumber"] = Value::Null;
                    transaction["blockHash"] = Value::Null;
                    transaction["transactionIndex"] = Value::Null;
                } else if chain.evidence == Evidence::PartialTransactionInclusion {
                    transaction["blockHash"] = Value::Null;
                    transaction["transactionIndex"] = Value::Null;
                }
                transaction
            }
        }
        "eth_getTransactionReceipt" => {
            let reads = chain.receipt_reads.fetch_add(1, Ordering::Relaxed);
            if chain.evidence == Evidence::PendingReceipt
                || (chain.evidence == Evidence::RelayerHashChanges && params[0] == WRONG_HASH)
                || (chain.evidence == Evidence::MissingReceiptOnce && reads == 0)
            {
                Value::Null
            } else {
                assert_eq!(params[0], HASH);
                chain.receipt()
            }
        }
        "eth_getBlockByNumber" => {
            let mut block = chain.sample["block"].clone();
            block["transactions"] = json!([HASH]);

            match params[0].as_str().unwrap() {
                "finalized" if !chain.finalized.load(Ordering::Relaxed) => {
                    block["number"] = json!("0x5d1a26b");
                    block["hash"] = json!(WRONG_HASH);
                }
                "finalized" => {
                    chain.finalized_reads.fetch_add(1, Ordering::Relaxed);
                    if chain.evidence == Evidence::ReorganizedBlock {
                        block["number"] = json!("0x5d1a26d");
                        block["hash"] = json!(WRONG_HASH);
                    } else if chain.evidence == Evidence::WrongBlock {
                        block["hash"] = json!(WRONG_HASH);
                    } else if chain.evidence == Evidence::MissingBlockTransaction {
                        block["transactions"] = json!([]);
                    }
                }
                "0x5d1a26c" => {
                    if chain.evidence == Evidence::MissingBlockTransaction {
                        block["transactions"] = json!([]);
                    }

                    if chain.evidence == Evidence::WrongBlock {
                        block["hash"] = json!(WRONG_HASH);
                    }

                    if chain.evidence == Evidence::ReorganizedBlock
                        && chain.finalized_reads.load(Ordering::Relaxed) > 0
                    {
                        block["hash"] = json!(WRONG_HASH);
                    }
                }
                tag => panic!("Unexpected block tag: {tag}"),
            }
            block
        }
        method => panic!("Unexpected RPC call: {method}"),
    };
    Json(json!({"jsonrpc": "2.0", "id": request["id"], "result": result}))
}

async fn relayer(
    State(chain): State<Arc<MockChain>>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    assert_eq!(query.get("id").map(String::as_str), Some(RELAYER_ID));
    let reads = chain.relay_reads.fetch_add(1, Ordering::Relaxed);
    if matches!(
        chain.evidence,
        Evidence::RetainedHashRelayerNotFound | Evidence::RelayerIdNotFoundOnly
    ) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let hash = if chain.evidence == Evidence::RetainedHashRelayerConflict
        || (chain.evidence == Evidence::RelayerHashChanges && reads == 0)
    {
        WRONG_HASH
    } else {
        HASH
    };
    Json(
        json!([{"transactionID": RELAYER_ID, "transactionHash": hash,
        "state": "STATE_CONFIRMED", "type": "WALLET"}]),
    )
    .into_response()
}

async fn reject_submit(State(chain): State<Arc<MockChain>>) -> StatusCode {
    chain.submits.fetch_add(1, Ordering::Relaxed);
    StatusCode::METHOD_NOT_ALLOWED
}

struct Fixture {
    engine: Rc<RefCell<ExecutionEngine>>,
    cache: Rc<RefCell<Cache>>,
    chain: Arc<MockChain>,
    receiver: tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    command: QueryConvert,
    inputs: Vec<InstrumentId>,
    outputs: Vec<InstrumentId>,
    server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(evidence: Evidence, relayer_id: bool, finalized: bool) -> Self {
        let chain = Arc::new(MockChain::new(
            serde_json::from_str(SAMPLE).unwrap(),
            evidence,
            finalized,
        ));
        let router = Router::new()
            .route("/", post(rpc))
            .route("/transaction", get(relayer))
            .route("/submit", post(reject_submit))
            .with_state(chain.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        msgbus::set_message_bus(Rc::new(RefCell::new(MessageBus::default())));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let command = QueryConvert {
            operation_id: UUID4::new(),
            trader_id: TraderId::from("TESTER-001"),
            strategy_id: StrategyId::from("CONVERT-001"),
            account_id: AccountId::from("POLYMARKET-001"),
            client_id: *POLYMARKET_CLIENT_ID,
            ts_init: UnixNanos::default(),
        };
        let fee_bps = if evidence == Evidence::NonZeroFee {
            NONZERO_FEE_BPS
        } else {
            0
        };
        let mapping = event_mapping(&chain.sample, 11, fee_bps);
        let conditions = mapping.conditions();
        let inputs: Vec<_> = conditions[1..10]
            .iter()
            .map(|item| item.no.instrument_id)
            .collect();
        let outputs = vec![
            conditions[0].yes.instrument_id,
            conditions[10].yes.instrument_id,
        ];
        let scope = mapping.convert_scope(&inputs).unwrap();
        cache.borrow_mut().add_convert_event(mapping).unwrap();
        let mut request = SubmitConvert {
            operation_id: command.operation_id,
            trader_id: command.trader_id,
            strategy_id: command.strategy_id,
            account_id: command.account_id,
            client_id: command.client_id,
            inputs: inputs
                .iter()
                .map(|&instrument_id| ConvertTokenAmount {
                    instrument_id,
                    quantity: Decimal::ONE,
                })
                .collect(),
            ts_init: command.ts_init,
        };
        let mut record = ConvertOperationRecord::new(&request);
        record.affected_instruments = inputs.iter().chain(&outputs).copied().collect();
        let both_locators = matches!(
            evidence,
            Evidence::RetainedHashRelayerConflict | Evidence::RetainedHashRelayerNotFound
        );
        let retained_relayer_id = relayer_id || both_locators;
        let retained_hash = !relayer_id || both_locators;
        record.transaction = Some(ConvertTransaction {
            wallet: WALLET.to_string(),
            relayer_transaction_id: retained_relayer_id.then(|| RELAYER_ID.to_string()),
            transaction_hash: retained_hash.then(|| HASH.to_string()),
            nonce: Some("96470".to_string()),
            call_summary: None,
        });
        record.dispatched = true;
        record.status = Some(if evidence == Evidence::Valid && relayer_id {
            ConvertExecutionStatus::Submitted
        } else {
            ConvertExecutionStatus::Unknown
        });

        for &instrument_id in &record.affected_instruments {
            let mut binary = binary_option();
            binary.id = instrument_id;
            binary.currency = Currency::pUSD();
            let instrument = InstrumentAny::BinaryOption(binary);
            let mut cache = cache.borrow_mut();
            cache.add_instrument(instrument.clone()).unwrap();

            if inputs.contains(&instrument_id) {
                let event_id = UUID4::new();
                let fill = OrderFilledSpec::builder()
                    .trader_id(command.trader_id)
                    .strategy_id(command.strategy_id)
                    .account_id(command.account_id)
                    .instrument_id(instrument_id)
                    .client_order_id(ClientOrderId::new(format!("O-{event_id}")))
                    .position_id(position_id(instrument_id, command.strategy_id))
                    .order_side(OrderSide::Buy)
                    .last_qty(Quantity::from("1.00"))
                    .last_px(Price::from("0.95"))
                    .currency(Currency::pUSD())
                    .event_id(event_id)
                    .build();
                cache
                    .add_position(&Position::new(&instrument, fill), OmsType::Netting)
                    .unwrap();
            }
        }
        cache.borrow_mut().add_convert_operation(&record).unwrap();
        let core = ExecutionClientCore::new(
            command.trader_id,
            command.client_id,
            *POLYMARKET_VENUE,
            OmsType::Netting,
            command.account_id,
            AccountType::Cash,
            None,
            cache.clone(),
        );
        let (max_retries, retry_delay_ms) = match evidence {
            Evidence::PendingTransactionOnce | Evidence::MissingReceiptOnce => (1, 1),
            Evidence::PendingReceipt => (20, 20),
            _ => (0, 1),
        };
        let config = PolymarketExecutionClientConfig::builder()
            .private_key(
                "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".into(),
            )
            .api_key("offline-api-key".into())
            .api_secret(
                base64::Engine::encode(
                    &base64::engine::general_purpose::URL_SAFE,
                    b"offline-test-secret",
                )
                .into(),
            )
            .passphrase("offline-passphrase".into())
            .funder(WALLET.to_string())
            .signature_type(PolymarketSignatureType::Poly1271)
            .rpc_url(url.clone().into())
            .base_url_http(url.clone())
            .base_url_relayer(url)
            .http_timeout_secs(1)
            .max_retries(max_retries)
            .retry_delay_initial_ms(retry_delay_ms)
            .retry_delay_max_ms(retry_delay_ms)
            .build();
        let client = PolymarketExecutionClient::new(core, config).unwrap();
        request.operation_id = UUID4::new();
        assert!(client.submit_convert(request, scope).is_err());

        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        set_exec_event_sender(sender);
        let engine = Rc::new(RefCell::new(ExecutionEngine::new(
            Rc::new(RefCell::new(VirtualClock::new())),
            cache.clone(),
            None,
        )));
        ExecutionEngine::register_msgbus_handlers(&engine);
        engine
            .borrow_mut()
            .register_client(Box::new(client))
            .unwrap();
        engine
            .borrow_mut()
            .register_oms_type(command.strategy_id, OmsType::Netting);
        engine.borrow_mut().start();
        engine.borrow_mut().begin_convert_live_run();
        Self {
            engine,
            cache,
            chain,
            receiver,
            command,
            inputs,
            outputs,
            server,
        }
    }

    async fn query(&mut self) -> ConvertExecutionEvent {
        assert!(self.engine.borrow().query_convert(&self.command));
        let event = tokio::time::timeout(Duration::from_secs(5), self.receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let ExecutionEvent::Convert(event) = event else {
            panic!("Expected Convert execution evidence, was {event:?}");
        };
        assert_eq!(event.operation_id, self.command.operation_id);
        assert_eq!(event.account_id, self.command.account_id);
        assert_eq!(event.client_id, self.command.client_id);
        assert_eq!(event.run_id, self.engine.borrow().convert_live_run_id());
        self.deliver(&event);
        event
    }

    fn deliver(&self, event: &ConvertExecutionEvent) {
        msgbus::send_any(MessagingSwitchboard::exec_engine_process_convert(), event);
    }

    fn record(&self) -> ConvertOperationRecord {
        self.cache
            .borrow()
            .load_convert_operation(self.command.operation_id)
            .unwrap()
            .unwrap()
    }

    fn position(&self, instrument_id: InstrumentId) -> Option<Position> {
        self.cache
            .borrow()
            .position_owned(&position_id(instrument_id, self.command.strategy_id))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.engine.borrow_mut().stop();
        self.server.abort();
    }
}

fn position_id(instrument_id: InstrumentId, strategy_id: StrategyId) -> PositionId {
    PositionId::new(format!("{instrument_id}-{strategy_id}"))
}

#[rstest]
#[case::hash(false)]
#[case::relayer_id(true)]
#[tokio::test]
async fn convert_query_waits_for_finality_then_applies_once(#[case] relayer_id: bool) {
    let mut fixture = Fixture::new(Evidence::Valid, relayer_id, false).await;
    let mut foreign = fixture.command.clone();
    foreign.account_id = AccountId::from("POLYMARKET-002");
    assert!(!fixture.engine.borrow().query_convert(&foreign));
    assert_eq!(fixture.chain.rpc_reads.load(Ordering::Relaxed), 0);

    let pending = fixture.query().await;
    assert!(matches!(
        pending.outcome,
        ConvertExecutionOutcome::Unknown { .. }
    ));
    assert!(!fixture.record().applied);
    if relayer_id {
        let pending_record = fixture.record();
        assert_eq!(
            pending_record.status,
            Some(ConvertExecutionStatus::Submitted)
        );
        assert!(
            pending_record
                .reason
                .is_some_and(|reason| reason.contains("finalized"))
        );
    }
    assert!(
        fixture
            .outputs
            .iter()
            .all(|id| fixture.position(*id).is_none())
    );

    fixture.chain.finalized.store(true, Ordering::Relaxed);
    let confirmed = fixture.query().await;
    let ConvertExecutionOutcome::Confirmed {
        verified,
        pending_inputs,
    } = &confirmed.outcome
    else {
        panic!("Expected Confirmed, was {:?}", confirmed.outcome);
    };
    assert_eq!(verified.transaction.transaction_hash.as_deref(), Some(HASH));
    assert_eq!(verified.net_cash, Money::from("8 pUSD"));
    assert_eq!(verified.outputs.len(), 2);
    assert!(pending_inputs.is_empty());
    assert!(fixture.record().applied, "{:?}", fixture.record());

    fixture.deliver(&confirmed);
    fixture.deliver(&pending);
    assert!(!fixture.engine.borrow().query_convert(&fixture.command));
    assert_eq!(
        fixture.record().status,
        Some(ConvertExecutionStatus::Confirmed)
    );
    assert!(
        fixture
            .inputs
            .iter()
            .all(|id| fixture.position(*id).unwrap().is_closed())
    );

    for &output in &fixture.outputs {
        let position = fixture.position(output).unwrap();
        assert_eq!(position.quantity, Quantity::from("1.00"));
        assert_eq!(position.cost_open, dec!(0.275));
        assert!(position.events.is_empty());
    }
    assert_eq!(
        fixture.chain.relay_reads.load(Ordering::Relaxed) > 0,
        relayer_id
    );
    assert_eq!(fixture.chain.submits.load(Ordering::Relaxed), 0);
}

#[rstest]
#[case::chain(Evidence::WrongChain)]
#[case::factory(Evidence::WrongFactory)]
#[case::wallet(Evidence::WrongWallet)]
#[case::nonce(Evidence::WrongNonce)]
#[case::target(Evidence::WrongTarget)]
#[case::amount(Evidence::WrongAmount)]
#[case::internal_not_proven(Evidence::MissingBatch)]
#[case::missing_output(Evidence::MissingOutput)]
#[case::extra_debit(Evidence::ExtraDebit)]
#[case::reverted(Evidence::Reverted)]
#[tokio::test]
async fn convert_query_never_applies_unproven_effects(#[case] evidence: Evidence) {
    let mut fixture = Fixture::new(evidence, false, true).await;
    let event = fixture.query().await;

    if evidence == Evidence::Reverted {
        assert!(matches!(
            event.outcome,
            ConvertExecutionOutcome::Failed { .. }
        ));
    } else {
        assert!(matches!(
            event.outcome,
            ConvertExecutionOutcome::Unknown { .. }
        ));
    }
    assert!(!fixture.record().applied);
    assert!(!fixture.record().authorization_invalid);
    assert!(fixture.record().reason.is_some());
    assert!(
        fixture
            .outputs
            .iter()
            .all(|id| fixture.position(*id).is_none())
    );
    assert!(
        fixture
            .inputs
            .iter()
            .all(|id| { fixture.position(*id).unwrap().quantity == Quantity::from("1.00") })
    );
    assert_eq!(fixture.chain.submits.load(Ordering::Relaxed), 0);
}

#[rstest]
#[case::wrong_block(Evidence::WrongBlock)]
#[case::reorganized_block(Evidence::ReorganizedBlock)]
#[case::missing_transaction(Evidence::MissingBlockTransaction)]
#[tokio::test]
async fn convert_query_checks_membership_on_the_finalized_chain(#[case] evidence: Evidence) {
    let mut fixture = Fixture::new(evidence, false, true).await;
    let event = fixture.query().await;
    assert!(matches!(
        event.outcome,
        ConvertExecutionOutcome::Unknown { .. }
    ));
    assert!(!fixture.record().applied);
}

#[tokio::test]
async fn convert_query_does_not_report_a_nonfinalized_revert_as_definitive() {
    let mut fixture = Fixture::new(Evidence::Reverted, false, false).await;
    assert!(matches!(
        fixture.query().await.outcome,
        ConvertExecutionOutcome::Unknown { .. }
    ));
    fixture.chain.finalized.store(true, Ordering::Relaxed);
    assert!(matches!(
        fixture.query().await.outcome,
        ConvertExecutionOutcome::Failed { .. }
    ));
    assert!(!fixture.record().authorization_invalid);
}

#[tokio::test]
async fn convert_query_retries_a_known_transaction_that_is_not_yet_mined() {
    let mut fixture = Fixture::new(Evidence::PendingTransactionOnce, false, true).await;
    assert!(matches!(
        fixture.query().await.outcome,
        ConvertExecutionOutcome::Confirmed { .. }
    ));
    assert_eq!(fixture.chain.transaction_reads.load(Ordering::Relaxed), 2);
    assert!(fixture.record().applied);
}

#[tokio::test]
async fn convert_query_rejects_partial_transaction_inclusion() {
    let mut fixture = Fixture::new(Evidence::PartialTransactionInclusion, false, true).await;
    assert!(matches!(
        fixture.query().await.outcome,
        ConvertExecutionOutcome::Unknown { .. }
    ));
    assert_eq!(fixture.chain.transaction_reads.load(Ordering::Relaxed), 1);
    assert!(
        fixture
            .record()
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("partial block inclusion"))
    );
    assert!(!fixture.record().applied);
}

#[tokio::test]
async fn convert_query_retries_a_receipt_not_yet_indexed() {
    let mut fixture = Fixture::new(Evidence::MissingReceiptOnce, false, true).await;
    assert!(matches!(
        fixture.query().await.outcome,
        ConvertExecutionOutcome::Confirmed { .. }
    ));
    assert_eq!(fixture.chain.receipt_reads.load(Ordering::Relaxed), 2);
    assert!(fixture.record().applied);
}

#[tokio::test]
async fn convert_query_does_not_pin_an_unbound_relayer_hash() {
    let mut fixture = Fixture::new(Evidence::RelayerHashChanges, true, true).await;
    assert!(matches!(
        fixture.query().await.outcome,
        ConvertExecutionOutcome::Unknown { .. }
    ));
    let transaction = fixture.record().transaction.unwrap();
    assert_eq!(
        transaction.relayer_transaction_id.as_deref(),
        Some(RELAYER_ID)
    );
    assert_eq!(transaction.transaction_hash, None);
    assert!(matches!(
        fixture.query().await.outcome,
        ConvertExecutionOutcome::Confirmed { .. }
    ));
    assert!(fixture.record().applied);
}

#[tokio::test]
async fn convert_query_rejects_an_explicit_relayer_hash_conflict() {
    let mut fixture = Fixture::new(Evidence::RetainedHashRelayerConflict, false, true).await;
    assert!(matches!(
        fixture.query().await.outcome,
        ConvertExecutionOutcome::Unknown { .. }
    ));
    let transaction = fixture.record().transaction.unwrap();
    assert_eq!(transaction.transaction_hash.as_deref(), Some(HASH));
    assert_eq!(
        transaction.relayer_transaction_id.as_deref(),
        Some(RELAYER_ID)
    );
    assert!(!fixture.record().applied);
}

#[tokio::test]
async fn convert_query_uses_retained_hash_when_relayer_no_longer_has_the_id() {
    let mut fixture = Fixture::new(Evidence::RetainedHashRelayerNotFound, false, true).await;
    assert!(matches!(
        fixture.query().await.outcome,
        ConvertExecutionOutcome::Confirmed { .. }
    ));
    assert_eq!(fixture.chain.relay_reads.load(Ordering::Relaxed), 1);
    assert!(fixture.record().applied);
}

#[tokio::test]
async fn convert_query_keeps_id_only_not_found_unknown() {
    let mut fixture = Fixture::new(Evidence::RelayerIdNotFoundOnly, true, true).await;
    assert!(matches!(
        fixture.query().await.outcome,
        ConvertExecutionOutcome::Unknown { .. }
    ));
    let transaction = fixture.record().transaction.unwrap();
    assert_eq!(
        transaction.relayer_transaction_id.as_deref(),
        Some(RELAYER_ID)
    );
    assert_eq!(transaction.transaction_hash, None);
    assert!(!fixture.record().applied);
}

#[tokio::test]
async fn convert_query_confirms_other_wallet_batches_without_counting_them() {
    let mut fixture = Fixture::new(Evidence::MultiWalletBatch, false, true).await;
    let event = fixture.query().await;
    let ConvertExecutionOutcome::Confirmed { verified, .. } = event.outcome else {
        panic!("Expected Confirmed, was {event:?}");
    };

    assert_eq!(verified.outputs.len(), 2);
    assert!(
        verified
            .outputs
            .iter()
            .all(|output| output.quantity == Decimal::ONE)
    );
    assert!(fixture.record().applied);
    for &output in &fixture.outputs {
        let position = fixture.position(output).unwrap();
        assert_eq!(position.quantity, Quantity::from("1.00"));
        assert_eq!(position.cost_open, dec!(0.275));
    }
}

#[rstest]
#[case::same_wallet(Evidence::SameWalletSecondBatch)]
#[case::extra_call(Evidence::ExtraWalletCall)]
#[tokio::test]
async fn convert_query_rejects_unattributable_wallet_batches(#[case] evidence: Evidence) {
    let mut fixture = Fixture::new(evidence, false, true).await;
    assert!(matches!(
        fixture.query().await.outcome,
        ConvertExecutionOutcome::Unknown { .. }
    ));
    assert!(!fixture.record().applied);
}

#[tokio::test]
async fn convert_query_verifies_nonzero_fee_cash_and_outputs() {
    let mut fixture = Fixture::new(Evidence::NonZeroFee, false, true).await;
    let event = fixture.query().await;
    let ConvertExecutionOutcome::Confirmed {
        verified,
        pending_inputs,
    } = event.outcome
    else {
        panic!("Expected Confirmed, was {event:?}");
    };

    assert_eq!(verified.net_cash, Money::from("7.92 pUSD"));
    assert_eq!(verified.outputs.len(), 2);
    assert!(
        verified
            .outputs
            .iter()
            .all(|output| output.quantity == dec!(0.99))
    );
    assert!(pending_inputs.is_empty());
    assert!(fixture.record().applied);
    for &output in &fixture.outputs {
        let position = fixture.position(output).unwrap();
        assert_eq!(position.quantity, Quantity::from("0.99"));
        assert_eq!(position.cost_open, dec!(0.315));
    }
}

#[tokio::test]
async fn convert_query_rejects_on_chain_consumed_amount_mismatch() {
    let mut fixture = Fixture::new(Evidence::WrongConsumedAmount, false, true).await;
    assert!(matches!(
        fixture.query().await.outcome,
        ConvertExecutionOutcome::Unknown { .. }
    ));
    assert!(!fixture.record().applied);
    assert!(
        fixture
            .record()
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("CTF token")),
        "{:?}",
        fixture.record().reason
    );
    assert!(
        fixture
            .inputs
            .iter()
            .all(|id| fixture.position(*id).unwrap().quantity == Quantity::from("1.00"))
    );
}

#[tokio::test]
async fn convert_query_stop_cancels_retry_without_emitting() {
    let mut fixture = Fixture::new(Evidence::PendingReceipt, false, true).await;
    assert!(fixture.engine.borrow().query_convert(&fixture.command));

    for _ in 0..100 {
        if fixture.chain.receipt_reads.load(Ordering::Relaxed) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        fixture.chain.receipt_reads.load(Ordering::Relaxed) > 0,
        "query did not reach the receipt read"
    );

    fixture.engine.borrow_mut().stop();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let settled_reads = fixture.chain.receipt_reads.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        fixture.chain.receipt_reads.load(Ordering::Relaxed),
        settled_reads,
        "Convert query kept retrying after stop"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), fixture.receiver.recv())
            .await
            .is_err(),
        "Convert query emitted after stop"
    );
    assert!(!fixture.record().applied);
}
