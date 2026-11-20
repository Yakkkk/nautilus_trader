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

//! Finalized Polygon verification for retained Polymarket Convert operations.

use std::{
    collections::{HashMap, HashSet},
    fmt,
};

use alloy::sol_types::{SolCall, SolEvent};
use alloy_primitives::{Address, B256, Bytes, U256, address, hex};
use nautilus_common::messages::execution::{
    ConvertExecutionOutcome, ConvertOperationRecord, ConvertTokenAmount, ConvertTransaction,
    VerifiedConvert,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::neg_risk::CtfNegRiskEvent,
    types::{Currency, Money},
};
use nautilus_network::retry::{RetryConfig, RetryError, RetryManager};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    amounts::{base_units_to_pusd, pusd_to_base_units},
    convert_abi::{
        BatchExecuted, Execution, PositionsConverted, Transfer, TransferBatch, TransferSingle,
        Wrapped, encode_convert_positions, proxyCall,
    },
    wallet::WalletVerifier,
};
use crate::{
    config::PolymarketExecutionClientConfig,
    http::{
        error::Error as HttpError,
        relayer::{PolymarketRelayerPublicClient, RelayerTransactionState},
    },
    signing::eip712::{
        CONDITIONAL_TOKENS, DEPOSIT_WALLET_FACTORY, NEG_RISK_CTF_COLLATERAL_ADAPTER,
        POLYGON_CHAIN_ID, POLYMARKET_COLLATERAL_TOKEN, parse_address, parse_bytes32,
    },
};

const FEE_DENOMINATOR: u64 = 10_000;
const USDC_E: Address = address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");
const QUERY_ELAPSED_BUDGET_MS: u64 = 180_000;

/// Queries and verifies the original, retained Convert transaction.
///
/// The function never signs or submits a transaction. It reads the configured RPC and, when a
/// Relayer ID was retained, the public read-only Relayer transaction endpoint.
pub(crate) async fn query_original_convert(
    config: &PolymarketExecutionClientConfig,
    record: &ConvertOperationRecord,
    mapping: &CtfNegRiskEvent,
    cancellation: &CancellationToken,
) -> ConvertExecutionOutcome {
    let Some(mut transaction) = record.transaction.clone() else {
        return ConvertExecutionOutcome::Unknown {
            transaction: None,
            reason: "retained Convert operation has no original transaction context".to_string(),
        };
    };

    let expected = match ExpectedConvert::new(record, mapping) {
        Ok(expected) => expected,
        Err(failure) => return failure.into_outcome(transaction),
    };

    let proxy_url = match config.validated_proxy_url() {
        Ok(proxy_url) => proxy_url,
        Err(e) => {
            return ConvertExecutionOutcome::Unknown {
                transaction: Some(transaction),
                reason: format!("invalid configured proxy URL: {e}"),
            };
        }
    };
    let Some(rpc_url) = config
        .rpc_url
        .as_ref()
        .map(|url| url.expose_secret().to_string())
    else {
        return ConvertExecutionOutcome::Unknown {
            transaction: Some(transaction),
            reason: "Polymarket Convert queries require a configured Polygon RPC URL".to_string(),
        };
    };
    let mut rpc = match WalletVerifier::new(config.http_timeout_secs, proxy_url.clone()) {
        Ok(rpc) => rpc,
        Err(e) => {
            return ConvertExecutionOutcome::Unknown {
                transaction: Some(transaction),
                reason: format!("failed to create Polygon RPC client: {e}"),
            };
        }
    };
    rpc.set_rpc_url(rpc_url);

    let relayer = if transaction.relayer_transaction_id.is_some() {
        match PolymarketRelayerPublicClient::new(
            config.base_url_relayer.clone(),
            config.http_timeout_secs,
            proxy_url,
        ) {
            Ok(relayer) => Some(relayer),
            Err(_) if transaction.transaction_hash.is_some() => None,
            Err(_) => {
                return ConvertExecutionOutcome::Unknown {
                    transaction: Some(transaction),
                    reason: "failed to create the public Relayer HTTP client".to_string(),
                };
            }
        }
    } else {
        None
    };

    let retry_config = RetryConfig {
        max_retries: config.max_retries,
        initial_delay_ms: config.retry_delay_initial_ms,
        max_delay_ms: config.retry_delay_max_ms,
        backoff_factor: 2.0,
        jitter_ms: 0,
        operation_timeout_ms: Some(config.http_timeout_secs.saturating_mul(1_000)),
        immediate_first: false,
        max_elapsed_ms: Some(QUERY_ELAPSED_BUDGET_MS),
    };
    let manager = RetryManager::<CheckFailure>::new(retry_config);
    let result = {
        let operation = || verify_once(&rpc, relayer.as_ref(), &expected, &transaction, record);
        manager
            .invocation(
                "Polymarket Convert query",
                operation,
                |e| e.failure.is_retryable(),
                CheckFailure::from_retry,
            )
            .cancellation_token(cancellation)
            .execute()
            .await
    };

    match result {
        Ok(verified) => ConvertExecutionOutcome::Confirmed {
            verified: Box::new(verified),
            pending_inputs: Vec::new(),
        },
        Err(failure) => {
            if transaction.transaction_hash.is_none()
                && let Some(hash) = failure.retained_hash
            {
                transaction.transaction_hash = Some(hash);
            }
            failure.failure.into_outcome(transaction)
        }
    }
}

struct ExpectedConvert {
    wallet: Address,
    market_id: B256,
    index_set: U256,
    amount: U256,
    amount_out: U256,
    net_cash: U256,
    inputs: Vec<ConvertTokenAmount>,
    outputs: Vec<ConvertTokenAmount>,
    affected_instruments: Vec<InstrumentId>,
    token_expectations: HashMap<U256, TokenExpectation>,
    calldata: Bytes,
}

impl ExpectedConvert {
    fn new(record: &ConvertOperationRecord, mapping: &CtfNegRiskEvent) -> Result<Self, Failure> {
        if record.inputs.is_empty() {
            return Err(Failure::unknown("retained Convert operation has no inputs"));
        }

        let wallet = parse_address(
            &record
                .transaction
                .as_ref()
                .ok_or_else(|| Failure::unknown("retained Convert transaction is missing"))?
                .wallet,
            "wallet",
        )
        .map_err(|e| Failure::unknown(format!("invalid retained Convert wallet: {e}")))?;

        let input_ids = record
            .inputs
            .iter()
            .map(|input| input.instrument_id)
            .collect::<Vec<_>>();
        let scope = mapping.convert_scope(&input_ids).map_err(|e| {
            Failure::unknown(format!(
                "retained Convert inputs do not form a valid negative-risk conversion: {e}"
            ))
        })?;
        let selected = scope
            .inputs
            .iter()
            .copied()
            .collect::<HashSet<InstrumentId>>();

        let quantity = record.inputs[0].quantity;
        if quantity <= Decimal::ZERO || record.inputs.iter().any(|input| input.quantity != quantity)
        {
            return Err(Failure::unknown(
                "retained Convert inputs did not share one positive quantity",
            ));
        }
        let amount = pusd_to_base_units(quantity).map_err(|e| {
            Failure::unknown(format!(
                "retained Convert quantity was not an exact positive chain amount: {e}"
            ))
        })?;

        let market_id = B256::from(mapping.market_id());
        let mut index_set = U256::ZERO;
        for condition in mapping
            .conditions()
            .iter()
            .filter(|condition| selected.contains(&condition.no.instrument_id))
        {
            index_set |= U256::from(1u8) << usize::from(condition.index);
        }

        let fee = amount
            .checked_mul(U256::from(u64::from(mapping.fee_bps())))
            .ok_or_else(|| Failure::unknown("Convert fee calculation overflowed"))?
            / U256::from(FEE_DENOMINATOR);
        let amount_out = amount
            .checked_sub(fee)
            .ok_or_else(|| Failure::unknown("Convert output amount underflowed"))?;
        let net_cash = if scope.inputs.len() > 1 {
            amount_out
                .checked_mul(U256::from(scope.inputs.len() - 1))
                .ok_or_else(|| Failure::unknown("Convert cash calculation overflowed"))?
        } else {
            U256::ZERO
        };

        // CTF outcome shares and pUSD both use six-decimal chain units in this path.
        let input_quantity = base_units_to_pusd(amount)
            .map_err(|e| Failure::unknown(format!("invalid Convert input amount: {e}")))?;
        let output_quantity = base_units_to_pusd(amount_out)
            .map_err(|e| Failure::unknown(format!("invalid Convert output amount: {e}")))?;
        let mut inputs = Vec::with_capacity(scope.inputs.len());
        let mut outputs = Vec::with_capacity(scope.outputs.len());
        let mut affected_instruments = Vec::with_capacity(mapping.conditions().len());
        let mut token_expectations =
            HashMap::with_capacity(mapping.conditions().len().saturating_mul(2));

        for condition in mapping.conditions() {
            let no_token = U256::from_be_bytes(condition.no.token_id);
            let yes_token = U256::from_be_bytes(condition.yes.token_id);
            if selected.contains(&condition.no.instrument_id) {
                inputs.push(ConvertTokenAmount {
                    instrument_id: condition.no.instrument_id,
                    quantity: input_quantity,
                });
                affected_instruments.push(condition.no.instrument_id);
                token_expectations.insert(no_token, TokenExpectation::Debit(amount));
                token_expectations.insert(yes_token, TokenExpectation::Unchanged);
            } else {
                outputs.push(ConvertTokenAmount {
                    instrument_id: condition.yes.instrument_id,
                    quantity: output_quantity,
                });
                affected_instruments.push(condition.yes.instrument_id);
                token_expectations.insert(no_token, TokenExpectation::Unchanged);
                token_expectations.insert(yes_token, TokenExpectation::Credit(amount_out));
            }
        }

        let mut expected_affected = affected_instruments.clone();
        expected_affected.sort_unstable();
        let mut retained_affected = record.affected_instruments.clone();
        retained_affected.sort_unstable();
        if retained_affected != expected_affected {
            return Err(Failure::unknown(
                "retained affected scope did not match the mapped Convert inputs and outputs",
            ));
        }

        Ok(Self {
            wallet,
            market_id,
            index_set,
            amount,
            amount_out,
            net_cash,
            inputs,
            outputs,
            affected_instruments,
            token_expectations,
            calldata: encode_convert_positions(market_id, index_set, amount),
        })
    }
}

async fn verify_once(
    rpc: &WalletVerifier,
    relayer: Option<&PolymarketRelayerPublicClient>,
    expected: &ExpectedConvert,
    transaction: &ConvertTransaction,
    record: &ConvertOperationRecord,
) -> Result<VerifiedConvert, CheckFailure> {
    let resolved = resolve_hash(relayer, transaction).await?;
    let hash = resolved.hash;
    let hash_hex = format!("{hash:#x}");

    let chain_id = rpc
        .rpc_json("eth_chainId", json!([]))
        .await
        .map_err(|e| rpc_failure("chain ID read", &e))?;
    let chain_id = parse_u64_hex_value(&chain_id, "chain ID")?;
    if chain_id != POLYGON_CHAIN_ID {
        return Err(Failure::unknown(format!(
            "configured RPC reported chain {chain_id}, expected {POLYGON_CHAIN_ID}"
        ))
        .into());
    }

    let transaction_value = rpc
        .rpc_json("eth_getTransactionByHash", json!([hash_hex]))
        .await
        .map_err(|e| rpc_failure("transaction read", &e))?;
    if transaction_value.is_null() {
        return Err(Failure::retryable(format!(
            "configured RPC did not return transaction {hash}"
        ))
        .into());
    }
    let onchain_transaction: RpcTransaction = serde_json::from_value(transaction_value)
        .map_err(|e| Failure::unknown(format!("invalid transaction response: {e}")))?;

    match (
        &onchain_transaction.block_number,
        &onchain_transaction.block_hash,
        &onchain_transaction.transaction_index,
    ) {
        (None, None, None) => {
            return Err(Failure::retryable(format!(
                "configured RPC transaction {hash} is known but not yet mined"
            ))
            .into());
        }
        (Some(_), Some(_), Some(_)) => {}
        _ => {
            return Err(Failure::unknown(
                "transaction response contained partial block inclusion fields",
            )
            .into());
        }
    }

    let receipt_value = rpc
        .rpc_json("eth_getTransactionReceipt", json!([hash_hex]))
        .await
        .map_err(|e| rpc_failure("transaction receipt read", &e))?;
    if receipt_value.is_null() {
        return Err(Failure::retryable(format!(
            "configured RPC did not return a receipt for transaction {hash}"
        ))
        .into());
    }
    let receipt: RpcReceipt = serde_json::from_value(receipt_value)
        .map_err(|e| Failure::unknown(format!("invalid receipt response: {e}")))?;

    verify_transaction_binding(&onchain_transaction, &receipt, hash)?;

    let block = verify_finality(rpc, &receipt, hash).await?;
    let block_timestamp = parse_u64_hex(&block.timestamp, "block timestamp")?
        .checked_mul(1_000_000_000)
        .ok_or_else(|| Failure::unknown("block timestamp overflowed nanoseconds"))?;
    let ts_event = UnixNanos::from(block_timestamp);

    let onchain_input = parse_bytes(&onchain_transaction.input, "transaction input")?;
    if onchain_input.len() < 4 || onchain_input[..4] != proxyCall::SELECTOR {
        return Err(Failure::unknown(
            "original transaction was not a Deposit Wallet Factory proxy call",
        )
        .into());
    }
    let factory_call = proxyCall::abi_decode_validate(&onchain_input)
        .map_err(|e| Failure::unknown(format!("invalid Factory proxy calldata: {e}")))?;
    if factory_call.batches.len() != factory_call.signatures.len() {
        return Err(Failure::unknown("Factory proxy batch and signature counts differed").into());
    }

    let matching_batches = factory_call
        .batches
        .iter()
        .filter(|batch| batch.wallet == expected.wallet)
        .count();

    if matching_batches != 1 {
        return Err(Failure::unknown(
            "original transaction did not contain exactly one retained Deposit Wallet batch",
        )
        .into());
    }
    let Some(batch_index) = factory_call
        .batches
        .iter()
        .position(|batch| batch.wallet == expected.wallet)
    else {
        return Err(Failure::unknown(
            "original transaction did not contain the retained Deposit Wallet batch",
        )
        .into());
    };
    let batch = &factory_call.batches[batch_index];
    if batch.calls.len() != 1 {
        return Err(Failure::unknown(
            "retained Deposit Wallet batch contained mixed contract calls",
        )
        .into());
    }

    if factory_call.signatures[batch_index].is_empty() {
        return Err(Failure::unknown(
            "retained Deposit Wallet batch had no authorization signature",
        )
        .into());
    }

    let retained_nonce = transaction
        .nonce
        .as_deref()
        .ok_or_else(|| Failure::unknown("retained Convert wallet nonce is missing"))?;
    let retained_nonce = parse_u256(retained_nonce, "retained wallet nonce")?;
    if batch.nonce != retained_nonce {
        return Err(Failure::unknown(
            "original Deposit Wallet batch nonce did not match the retained nonce",
        )
        .into());
    }

    let call = &batch.calls[0];
    if call.target != NEG_RISK_CTF_COLLATERAL_ADAPTER {
        return Err(Failure::unknown(
            "Deposit Wallet call target was not the NegRisk collateral adapter",
        )
        .into());
    }

    if call.value != U256::ZERO {
        return Err(
            Failure::unknown("Deposit Wallet call carried an unexpected native value").into(),
        );
    }

    if call.data != expected.calldata {
        return Err(Failure::unknown(
            "Deposit Wallet call parameters did not match the retained Convert request",
        )
        .into());
    }

    match parse_u64_hex(&receipt.status, "receipt status")? {
        0 => {
            return Err(CheckFailure {
                failure: Failure::failed("original Convert transaction reverted on chain"),
                retained_hash: resolved.discovered.clone(),
            });
        }
        1 => {}
        status => {
            return Err(
                Failure::unknown(format!("receipt status was not a boolean: {status}")).into(),
            );
        }
    }

    let logs = parse_logs(&receipt.logs, hash, &receipt)?;
    let (executions, batch_events) = wallet_events(&logs, expected.wallet)?;
    if executions.len() != 1 || batch_events.len() != 1 {
        return Err(Failure::unknown(
            "Deposit Wallet did not provide exactly one successful execution and batch event",
        )
        .into());
    }

    let execution = &executions[0];
    if execution.target != NEG_RISK_CTF_COLLATERAL_ADAPTER
        || execution.value != U256::ZERO
        || execution.data != expected.calldata
    {
        return Err(Failure::unknown(
            "Deposit Wallet execution event did not prove the retained Convert call",
        )
        .into());
    }

    if batch_events[0].nonce != batch.nonce {
        return Err(Failure::unknown(
            "Deposit Wallet batch event nonce did not match the factory batch",
        )
        .into());
    }

    let conversions = adapter_conversions(&logs, expected)?;
    if conversions.len() != 1 {
        return Err(Failure::unknown(
            "NegRisk collateral adapter did not provide exactly one matching conversion event",
        )
        .into());
    }
    let conversion = &conversions[0];
    if conversion.amount != expected.amount || conversion.amountOut != expected.amount_out {
        return Err(Failure::unknown(
            "adapter conversion amounts differed from the retained request and fee",
        )
        .into());
    }

    verify_ctf_token_flows(&logs, expected)?;
    verify_cash_flows(&logs, expected)?;

    let net_cash = base_units_to_pusd(expected.net_cash)
        .map_err(|e| Failure::unknown(format!("invalid Convert net cash: {e}")))?;
    let net_cash = Money::from_decimal(net_cash, Currency::pUSD())
        .map_err(|e| Failure::unknown(format!("invalid Convert net cash value: {e}")))?;

    let mut verified_transaction = transaction.clone();
    if verified_transaction.transaction_hash.is_none()
        && let Some(hash) = resolved.discovered
    {
        verified_transaction.transaction_hash = Some(hash);
    }

    Ok(VerifiedConvert {
        operation_id: record.operation_id,
        trader_id: record.trader_id,
        strategy_id: record.strategy_id,
        account_id: record.account_id,
        transaction: verified_transaction,
        inputs: expected.inputs.clone(),
        outputs: expected.outputs.clone(),
        affected_instruments: expected.affected_instruments.clone(),
        net_cash,
        ts_event,
    })
}

struct HashResolution {
    hash: B256,
    discovered: Option<String>,
}

async fn resolve_hash(
    relayer: Option<&PolymarketRelayerPublicClient>,
    transaction: &ConvertTransaction,
) -> Result<HashResolution, Failure> {
    let retained_hash = transaction
        .transaction_hash
        .as_deref()
        .map(|hash| {
            parse_bytes32(hash, "transaction_hash")
                .map_err(|e| Failure::unknown(format!("invalid retained transaction hash: {e}")))
        })
        .transpose()?;

    let Some(transaction_id) = transaction.relayer_transaction_id.as_deref() else {
        let hash = retained_hash.ok_or_else(|| {
            Failure::unknown("retained Convert operation has no Relayer ID or transaction hash")
        })?;
        return Ok(HashResolution {
            hash,
            discovered: None,
        });
    };
    let Some(relayer) = relayer else {
        let hash = retained_hash.ok_or_else(|| {
            Failure::unknown(
                "public Relayer client was not initialized for the retained transaction",
            )
        })?;
        return Ok(HashResolution {
            hash,
            discovered: None,
        });
    };
    let remote = relayer.get_transaction(transaction_id).await;
    let remote = match remote {
        Ok(remote) => remote,
        Err(e) => {
            let Some(hash) = retained_hash else {
                return Err(relayer_failure(&e));
            };
            return if relayer_allows_retained_hash(&e) {
                Ok(HashResolution {
                    hash,
                    discovered: None,
                })
            } else {
                Err(Failure::unknown(
                    "Relayer transaction query failed without proving the retained hash",
                ))
            };
        }
    };

    if remote.transaction_id.as_deref() != Some(transaction_id) {
        return Err(Failure::unknown(if retained_hash.is_some() {
            "Relayer response did not match the retained transaction ID or hash"
        } else {
            "Relayer response did not match the retained transaction ID"
        }));
    }

    let remote_hash = match remote.transaction_hash.as_deref() {
        Some(hash) => Some(
            parse_bytes32(hash, "Relayer transaction_hash")
                .map_err(|e| Failure::unknown(format!("invalid Relayer transaction hash: {e}")))?,
        ),
        None => None,
    };

    if let Some(retained_hash) = retained_hash {
        match &remote.state {
            RelayerTransactionState::Failed
            | RelayerTransactionState::Invalid
            | RelayerTransactionState::Other(_) => {
                return Err(Failure::unknown(
                    "Relayer state conflicted with the retained on-chain transaction hash",
                ));
            }
            RelayerTransactionState::Confirmed if remote_hash.is_none() => {
                return Err(Failure::unknown(
                    "Relayer confirmation did not provide a hash to compare with the retained hash",
                ));
            }
            RelayerTransactionState::New | RelayerTransactionState::Confirmed => {}
        }

        if remote_hash.is_some_and(|hash| hash != retained_hash) {
            return Err(Failure::unknown(
                "Relayer transaction hash conflicted with the retained transaction hash",
            ));
        }
        return Ok(HashResolution {
            hash: retained_hash,
            discovered: None,
        });
    }

    match &remote.state {
        RelayerTransactionState::Confirmed => {
            let hash = remote_hash.ok_or_else(|| {
                Failure::unknown(
                    "Relayer reported confirmation without an on-chain transaction hash",
                )
            })?;
            Ok(HashResolution {
                hash,
                discovered: Some(format!("{hash:#x}")),
            })
        }
        RelayerTransactionState::Failed | RelayerTransactionState::Invalid => Err(Failure::failed(
            format!("Relayer transaction is {}", remote.state.as_str()),
        )),
        state => Err(Failure::retryable(format!(
            "Relayer transaction is not confirmed (state {})",
            state.as_str()
        ))),
    }
}

fn verify_transaction_binding(
    transaction: &RpcTransaction,
    receipt: &RpcReceipt,
    hash: B256,
) -> Result<(), Failure> {
    let transaction_hash = parse_bytes32(&transaction.hash, "transaction hash")
        .map_err(|e| Failure::unknown(format!("invalid transaction hash: {e}")))?;
    if transaction_hash != hash {
        return Err(Failure::unknown(
            "RPC transaction hash did not match the retained hash",
        ));
    }

    let receipt_hash = parse_bytes32(&receipt.transaction_hash, "receipt transaction hash")
        .map_err(|e| Failure::unknown(format!("invalid receipt transaction hash: {e}")))?;
    if receipt_hash != hash {
        return Err(Failure::unknown(
            "receipt transaction hash did not match the retained hash",
        ));
    }

    let transaction_block_hash = parse_bytes32(
        transaction
            .block_hash
            .as_deref()
            .ok_or_else(|| Failure::unknown("transaction block hash was missing"))?,
        "transaction block hash",
    )
    .map_err(|e| Failure::unknown(format!("invalid transaction block hash: {e}")))?;
    let receipt_block_hash = parse_bytes32(&receipt.block_hash, "receipt block hash")
        .map_err(|e| Failure::unknown(format!("invalid receipt block hash: {e}")))?;
    if transaction_block_hash != receipt_block_hash {
        return Err(Failure::unknown(
            "transaction and receipt block hashes differed",
        ));
    }

    let transaction_block_number = parse_u256(
        transaction
            .block_number
            .as_deref()
            .ok_or_else(|| Failure::unknown("transaction block number was missing"))?,
        "transaction block number",
    )?;
    let receipt_block_number = parse_u256(&receipt.block_number, "receipt block number")?;
    if transaction_block_number != receipt_block_number {
        return Err(Failure::unknown(
            "transaction and receipt block numbers differed",
        ));
    }
    let transaction_index = parse_u256(
        transaction
            .transaction_index
            .as_deref()
            .ok_or_else(|| Failure::unknown("transaction index was missing"))?,
        "transaction index",
    )?;
    let receipt_index = parse_u256(&receipt.transaction_index, "receipt transaction index")?;
    if transaction_index != receipt_index {
        return Err(Failure::unknown("transaction and receipt indices differed"));
    }

    let from = parse_address(&transaction.from, "transaction sender")
        .map_err(|e| Failure::unknown(format!("invalid transaction sender: {e}")))?;
    if let Some(receipt_from) = &receipt.from {
        let receipt_from = parse_address(receipt_from, "receipt sender")
            .map_err(|e| Failure::unknown(format!("invalid receipt sender: {e}")))?;
        if from != receipt_from {
            return Err(Failure::unknown("transaction and receipt senders differed"));
        }
    }

    let Some(to) = transaction.to.as_deref() else {
        return Err(Failure::unknown(
            "original transaction did not target the Deposit Wallet Factory",
        ));
    };
    let to = parse_address(to, "transaction target")
        .map_err(|e| Failure::unknown(format!("invalid transaction target: {e}")))?;
    if to != DEPOSIT_WALLET_FACTORY {
        return Err(Failure::unknown(
            "original transaction did not target the Deposit Wallet Factory",
        ));
    }

    if let Some(receipt_to) = &receipt.to {
        let receipt_to = parse_address(receipt_to, "receipt target")
            .map_err(|e| Failure::unknown(format!("invalid receipt target: {e}")))?;
        if receipt_to != DEPOSIT_WALLET_FACTORY {
            return Err(Failure::unknown(
                "receipt did not target the Deposit Wallet Factory",
            ));
        }
    }

    if parse_u256(&transaction.value, "transaction value")? != U256::ZERO {
        return Err(Failure::unknown(
            "original transaction carried an unexpected native value",
        ));
    }

    if let Some(chain_id) = transaction.chain_id.as_deref() {
        let chain_id = parse_u64_hex(chain_id, "transaction chain ID")?;
        if chain_id != POLYGON_CHAIN_ID {
            return Err(Failure::unknown(format!(
                "transaction chain ID was {chain_id}, expected {POLYGON_CHAIN_ID}"
            )));
        }
    }

    Ok(())
}

async fn verify_finality(
    rpc: &WalletVerifier,
    receipt: &RpcReceipt,
    hash: B256,
) -> Result<RpcBlock, Failure> {
    let finalized = block_by_number(rpc, "finalized", "finalized block").await?;
    let finalized_number = parse_u256(&finalized.number, "finalized block number")?;
    let receipt_block_number = parse_u256(&receipt.block_number, "receipt block number")?;
    let receipt_block_hash = parse_bytes32(&receipt.block_hash, "receipt block hash")
        .map_err(|e| Failure::unknown(format!("invalid receipt block hash: {e}")))?;
    if finalized_number < receipt_block_number {
        return Err(Failure::retryable(format!(
            "receipt block {receipt_block_number} has not reached the RPC finalized block {finalized_number}"
        )));
    }
    let block = if finalized_number == receipt_block_number {
        finalized
    } else {
        let block_tag = format!("{receipt_block_number:#x}");
        block_by_number(rpc, &block_tag, "receipt block").await?
    };

    if parse_u256(&block.number, "block number")? != receipt_block_number {
        return Err(Failure::unknown(
            "receipt block number was not bound to the reported block",
        ));
    }
    let block_hash = parse_bytes32(&block.hash, "block hash")
        .map_err(|e| Failure::unknown(format!("invalid block hash: {e}")))?;
    if block_hash != receipt_block_hash {
        return Err(Failure::unknown(
            "receipt block hash was not bound to the reported block",
        ));
    }

    if !block_contains_transaction(&block, hash) {
        return Err(Failure::unknown(
            "receipt block did not list the original transaction",
        ));
    }

    Ok(block)
}

async fn block_by_number(
    rpc: &WalletVerifier,
    tag: &str,
    field: &str,
) -> Result<RpcBlock, Failure> {
    let value = rpc
        .rpc_json("eth_getBlockByNumber", json!([tag, false]))
        .await
        .map_err(|e| rpc_failure(field, &e))?;
    if value.is_null() {
        return Err(Failure::retryable(format!(
            "configured RPC did not return the {field}"
        )));
    }
    serde_json::from_value(value)
        .map_err(|e| Failure::unknown(format!("invalid {field} response: {e}")))
}

fn block_contains_transaction(block: &RpcBlock, hash: B256) -> bool {
    let Some(transactions) = block.transactions.as_ref() else {
        return false;
    };

    if transactions.is_empty() {
        return false;
    }

    transactions.iter().any(|transaction| {
        transaction
            .as_str()
            .and_then(|value| parse_bytes32(value, "block transaction hash").ok())
            .is_some_and(|value| value == hash)
            || transaction
                .get("hash")
                .and_then(Value::as_str)
                .and_then(|value| parse_bytes32(value, "block transaction hash").ok())
                .is_some_and(|value| value == hash)
    })
}

fn parse_logs(
    logs: &[RpcLog],
    hash: B256,
    receipt: &RpcReceipt,
) -> Result<Vec<ParsedLog>, Failure> {
    let receipt_block_hash = parse_bytes32(&receipt.block_hash, "receipt block hash")
        .map_err(|e| Failure::unknown(format!("invalid receipt block hash: {e}")))?;
    let receipt_block_number = parse_u256(&receipt.block_number, "receipt block number")?;
    let receipt_transaction_index =
        parse_u256(&receipt.transaction_index, "receipt transaction index")?;
    let mut parsed = Vec::with_capacity(logs.len());

    for log in logs {
        if log.removed {
            return Err(Failure::unknown("receipt contained a removed log"));
        }

        if let Some(log_hash) = log.transaction_hash.as_deref() {
            let log_hash = parse_bytes32(log_hash, "log transaction hash")
                .map_err(|e| Failure::unknown(format!("invalid log transaction hash: {e}")))?;
            if log_hash != hash {
                return Err(Failure::unknown(
                    "receipt log belonged to a different transaction",
                ));
            }
        }

        if let Some(log_block_hash) = log.block_hash.as_deref() {
            let log_block_hash = parse_bytes32(log_block_hash, "log block hash")
                .map_err(|e| Failure::unknown(format!("invalid log block hash: {e}")))?;
            if log_block_hash != receipt_block_hash {
                return Err(Failure::unknown(
                    "receipt log belonged to a different block",
                ));
            }
        }

        if let Some(log_block_number) = log.block_number.as_deref()
            && parse_u256(log_block_number, "log block number")? != receipt_block_number
        {
            return Err(Failure::unknown(
                "receipt log belonged to a different block number",
            ));
        }

        if let Some(log_transaction_index) = log.transaction_index.as_deref()
            && parse_u256(log_transaction_index, "log transaction index")?
                != receipt_transaction_index
        {
            return Err(Failure::unknown(
                "receipt log belonged to a different transaction index",
            ));
        }

        let address = parse_address(&log.address, "log address")
            .map_err(|e| Failure::unknown(format!("invalid log address: {e}")))?;
        let topics = log
            .topics
            .iter()
            .map(|topic| {
                parse_bytes32(topic, "log topic")
                    .map_err(|e| Failure::unknown(format!("invalid log topic: {e}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let data = parse_bytes(&log.data, "log data")?;
        parsed.push(ParsedLog {
            address,
            topics,
            data,
        });
    }

    Ok(parsed)
}

fn wallet_events(
    logs: &[ParsedLog],
    wallet: Address,
) -> Result<(Vec<Execution>, Vec<BatchExecuted>), Failure> {
    let mut executions = Vec::new();
    let mut batches = Vec::new();

    for log in logs.iter().filter(|log| log.address == wallet) {
        let Some(topic) = log.topics.first() else {
            return Err(Failure::unknown(
                "Deposit Wallet emitted a log without an event signature",
            ));
        };

        if *topic == Execution::SIGNATURE_HASH {
            executions.push(decode_event(log)?);
        } else if *topic == BatchExecuted::SIGNATURE_HASH {
            batches.push(decode_event(log)?);
        } else {
            return Err(Failure::unknown(
                "Deposit Wallet emitted an unexpected event during the transaction",
            ));
        }
    }

    Ok((executions, batches))
}

fn adapter_conversions(
    logs: &[ParsedLog],
    expected: &ExpectedConvert,
) -> Result<Vec<PositionsConverted>, Failure> {
    let mut conversions = Vec::new();

    for log in logs
        .iter()
        .filter(|log| log.address == NEG_RISK_CTF_COLLATERAL_ADAPTER)
    {
        if log.topics.first() != Some(&PositionsConverted::SIGNATURE_HASH) {
            continue;
        }
        let conversion: PositionsConverted = decode_event(log)?;
        if conversion.initiator != expected.wallet {
            continue;
        }

        if conversion.marketId != expected.market_id || conversion.indexSet != expected.index_set {
            return Err(Failure::unknown(
                "adapter conversion event parameters differed from the retained request",
            ));
        }
        conversions.push(conversion);
    }
    Ok(conversions)
}

#[derive(Clone, Copy, Default)]
struct TokenFlow {
    debited: U256,
    credited: U256,
}

impl TokenFlow {
    fn is_zero(self) -> bool {
        self.debited == U256::ZERO && self.credited == U256::ZERO
    }
}

#[derive(Clone, Copy)]
enum TokenExpectation {
    Debit(U256),
    Credit(U256),
    Unchanged,
}

fn verify_ctf_token_flows(logs: &[ParsedLog], expected: &ExpectedConvert) -> Result<(), Failure> {
    let mut flows: HashMap<U256, TokenFlow> = HashMap::new();

    for log in logs.iter().filter(|log| log.address == CONDITIONAL_TOKENS) {
        let Some(topic) = log.topics.first() else {
            continue;
        };

        if *topic == TransferBatch::SIGNATURE_HASH {
            let transfer: TransferBatch = decode_event(log)?;
            if transfer.ids.len() != transfer.values.len() {
                return Err(Failure::unknown(
                    "CTF TransferBatch id and value counts differed",
                ));
            }

            if transfer.from != expected.wallet && transfer.to != expected.wallet {
                continue;
            }
            verify_ctf_counterparty(transfer.operator, transfer.from, transfer.to, expected)?;
            for (&id, &value) in transfer.ids.iter().zip(transfer.values.iter()) {
                add_token_flow(&mut flows, id, value, transfer.from == expected.wallet)?;
            }
        } else if *topic == TransferSingle::SIGNATURE_HASH {
            let transfer: TransferSingle = decode_event(log)?;
            if transfer.from != expected.wallet && transfer.to != expected.wallet {
                continue;
            }
            verify_ctf_counterparty(transfer.operator, transfer.from, transfer.to, expected)?;
            add_token_flow(
                &mut flows,
                transfer.id,
                transfer.value,
                transfer.from == expected.wallet,
            )?;
        }
    }

    verify_condition_flows(expected, &flows)
}

fn verify_condition_flows(
    expected: &ExpectedConvert,
    flows: &HashMap<U256, TokenFlow>,
) -> Result<(), Failure> {
    for (token_id, expectation) in &expected.token_expectations {
        let flow = flows.get(token_id).copied().unwrap_or_default();
        let valid = match *expectation {
            TokenExpectation::Debit(amount) => {
                flow.debited == amount && flow.credited == U256::ZERO
            }
            TokenExpectation::Credit(amount) => {
                flow.credited == amount && flow.debited == U256::ZERO
            }
            TokenExpectation::Unchanged => flow.is_zero(),
        };

        if !valid {
            return Err(Failure::unknown(format!(
                "CTF token {token_id} flow did not match the retained Convert effect"
            )));
        }
    }

    for (token_id, flow) in flows {
        if !flow.is_zero() && !expected.token_expectations.contains_key(token_id) {
            return Err(Failure::unknown(format!(
                "unexpected CTF token {token_id} movement involved the retained wallet"
            )));
        }
    }

    Ok(())
}

fn verify_ctf_counterparty(
    operator: Address,
    from: Address,
    to: Address,
    expected: &ExpectedConvert,
) -> Result<(), Failure> {
    if operator != NEG_RISK_CTF_COLLATERAL_ADAPTER {
        return Err(Failure::unknown(
            "CTF token movement was not operated by the NegRisk collateral adapter",
        ));
    }

    if from == expected.wallet && to != NEG_RISK_CTF_COLLATERAL_ADAPTER {
        return Err(Failure::unknown(
            "retained wallet token debit did not go to the NegRisk collateral adapter",
        ));
    }

    if to == expected.wallet && from != NEG_RISK_CTF_COLLATERAL_ADAPTER {
        return Err(Failure::unknown(
            "retained wallet token credit did not come from the NegRisk collateral adapter",
        ));
    }
    Ok(())
}

fn add_token_flow(
    flows: &mut HashMap<U256, TokenFlow>,
    id: U256,
    value: U256,
    debit: bool,
) -> Result<(), Failure> {
    let flow = flows.entry(id).or_default();
    let target = if debit {
        &mut flow.debited
    } else {
        &mut flow.credited
    };
    *target = target
        .checked_add(value)
        .ok_or_else(|| Failure::unknown("CTF token flow amount overflowed"))?;
    Ok(())
}

fn verify_cash_flows(logs: &[ParsedLog], expected: &ExpectedConvert) -> Result<(), Failure> {
    let mut transfers = Vec::new();
    let mut wraps = Vec::new();

    for log in logs
        .iter()
        .filter(|log| log.address == POLYMARKET_COLLATERAL_TOKEN)
    {
        let Some(topic) = log.topics.first() else {
            continue;
        };

        if *topic == Transfer::SIGNATURE_HASH {
            let transfer: Transfer = decode_event(log)?;
            if transfer.from == expected.wallet || transfer.to == expected.wallet {
                transfers.push(transfer);
            }
        } else if *topic == Wrapped::SIGNATURE_HASH {
            let wrapped: Wrapped = decode_event(log)?;
            if wrapped.to == expected.wallet {
                wraps.push(wrapped);
            }
        }
    }

    if expected.net_cash.is_zero() {
        if !transfers.is_empty() || !wraps.is_empty() {
            return Err(Failure::unknown(
                "retained Convert wallet had cash movements despite a zero net return",
            ));
        }
        return Ok(());
    }

    if transfers.len() != 1 {
        return Err(Failure::unknown(
            "retained Convert wallet did not receive exactly one pUSD transfer",
        ));
    }
    let transfer = &transfers[0];
    if transfer.from != Address::ZERO
        || transfer.to != expected.wallet
        || transfer.value != expected.net_cash
    {
        return Err(Failure::unknown(
            "pUSD transfer did not match the expected net cash",
        ));
    }

    if wraps.len() != 1 {
        return Err(Failure::unknown(
            "retained Convert wallet did not receive exactly one pUSD wrap",
        ));
    }
    let wrapped = &wraps[0];
    if wrapped.caller != NEG_RISK_CTF_COLLATERAL_ADAPTER
        || wrapped.asset != USDC_E
        || wrapped.to != expected.wallet
        || wrapped.amount != expected.net_cash
    {
        return Err(Failure::unknown(
            "pUSD wrap did not match the adapter, asset, recipient, or net cash",
        ));
    }

    Ok(())
}

fn decode_event<E: SolEvent>(log: &ParsedLog) -> Result<E, Failure> {
    E::decode_raw_log_validate(log.topics.iter().copied(), log.data.as_ref())
        .map_err(|e| Failure::unknown(format!("invalid {} event: {e}", E::SIGNATURE)))
}

fn rpc_failure(context: &str, error: &HttpError) -> Failure {
    if error.is_retryable() {
        Failure::retryable(format!("Polygon RPC {context} failed: {error}"))
    } else {
        Failure::unknown(format!("Polygon RPC {context} failed: {error}"))
    }
}

fn relayer_failure(error: &HttpError) -> Failure {
    if error.is_retryable() {
        Failure::retryable("Relayer transaction query was temporarily unavailable")
    } else {
        Failure::unknown("Relayer transaction query failed; transaction outcome is unknown")
    }
}

fn relayer_allows_retained_hash(error: &HttpError) -> bool {
    error.is_retryable() || matches!(error, HttpError::Http { status: 404, .. })
}

fn parse_u64_hex_value(value: &Value, field: &str) -> Result<u64, Failure> {
    let value = value
        .as_str()
        .ok_or_else(|| Failure::unknown(format!("{field} was not a string")))?;
    parse_u64_hex(value, field)
}

fn parse_u64_hex(value: &str, field: &str) -> Result<u64, Failure> {
    let value = parse_u256(value, field)?;
    u64::try_from(value).map_err(|_| Failure::unknown(format!("{field} exceeded a u64")))
}

fn parse_u256(value: &str, field: &str) -> Result<U256, Failure> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Failure::unknown(format!("{field} was empty")));
    }

    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        U256::from_str_radix(hex, 16).map_err(|e| Failure::unknown(format!("invalid {field}: {e}")))
    } else {
        trimmed
            .parse()
            .map_err(|e| Failure::unknown(format!("invalid {field}: {e}")))
    }
}

fn parse_bytes(value: &str, field: &str) -> Result<Bytes, Failure> {
    let trimmed = value.trim();
    let value = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .ok_or_else(|| Failure::unknown(format!("{field} was not 0x-prefixed hex")))?;
    hex::decode(value)
        .map(Bytes::from)
        .map_err(|e| Failure::unknown(format!("invalid {field}: {e}")))
}

#[derive(Clone, Copy, Debug)]
enum FailureKind {
    Unknown,
    Retryable,
    Failed,
    Cancelled,
}

#[derive(Debug)]
struct Failure {
    kind: FailureKind,
    reason: String,
}

impl Failure {
    fn unknown(reason: impl Into<String>) -> Self {
        Self {
            kind: FailureKind::Unknown,
            reason: reason.into(),
        }
    }

    fn retryable(reason: impl Into<String>) -> Self {
        Self {
            kind: FailureKind::Retryable,
            reason: reason.into(),
        }
    }

    fn failed(reason: impl Into<String>) -> Self {
        Self {
            kind: FailureKind::Failed,
            reason: reason.into(),
        }
    }

    fn cancelled() -> Self {
        Self {
            kind: FailureKind::Cancelled,
            reason: "Convert query was canceled".to_string(),
        }
    }

    fn is_retryable(&self) -> bool {
        matches!(self.kind, FailureKind::Retryable)
    }

    fn into_outcome(self, transaction: ConvertTransaction) -> ConvertExecutionOutcome {
        let transaction = Some(transaction);

        match self.kind {
            FailureKind::Failed => ConvertExecutionOutcome::Failed {
                transaction,
                reason: self.reason,
            },
            FailureKind::Unknown | FailureKind::Retryable | FailureKind::Cancelled => {
                ConvertExecutionOutcome::Unknown {
                    transaction,
                    reason: self.reason,
                }
            }
        }
    }
}

#[derive(Debug)]
struct CheckFailure {
    failure: Failure,
    retained_hash: Option<String>,
}

impl CheckFailure {
    fn from_retry(error: RetryError) -> Self {
        let failure = match error {
            RetryError::Canceled => Failure::cancelled(),
            RetryError::OperationTimeout { timeout_ms } => Failure::retryable(format!(
                "Convert query attempt timed out after {timeout_ms}ms"
            )),
            error @ (RetryError::ElapsedBudgetExceeded { .. }
            | RetryError::InvalidConfiguration { .. }) => {
                Failure::unknown(format!("Convert query retry budget ended: {error}"))
            }
        };
        Self {
            failure,
            retained_hash: None,
        }
    }
}

impl From<Failure> for CheckFailure {
    fn from(failure: Failure) -> Self {
        Self {
            failure,
            retained_hash: None,
        }
    }
}

impl fmt::Display for CheckFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.failure.reason)
    }
}

impl std::error::Error for CheckFailure {}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcTransaction {
    hash: String,
    chain_id: Option<String>,
    block_number: Option<String>,
    block_hash: Option<String>,
    transaction_index: Option<String>,
    from: String,
    to: Option<String>,
    value: String,
    input: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcReceipt {
    transaction_hash: String,
    transaction_index: String,
    block_hash: String,
    block_number: String,
    from: Option<String>,
    to: Option<String>,
    status: String,
    #[serde(default)]
    logs: Vec<RpcLog>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcLog {
    address: String,
    topics: Vec<String>,
    data: String,
    #[serde(default)]
    removed: bool,
    transaction_hash: Option<String>,
    transaction_index: Option<String>,
    block_hash: Option<String>,
    block_number: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcBlock {
    number: String,
    hash: String,
    timestamp: String,
    transactions: Option<Vec<Value>>,
}

struct ParsedLog {
    address: Address,
    topics: Vec<B256>,
    data: Bytes,
}

#[cfg(test)]
mod tests {
    use nautilus_common::messages::execution::{ConvertExecutionStatus, SubmitConvert};
    use nautilus_core::UUID4;
    use nautilus_model::{
        identifiers::{AccountId, ClientId, StrategyId, TraderId},
        instruments::neg_risk::{NegRiskCondition, NegRiskOutcome},
    };
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;

    const WALLET: &str = "0x1387d145aaf01f6e33b66525dda6e1f51f6955f8";

    fn mapping(count: u8, fee_bps: u16) -> CtfNegRiskEvent {
        let mut market_id = [0u8; 32];
        market_id[0] = 0x55;
        let conditions = (0..count)
            .map(|index| {
                let mut condition_id = [0u8; 32];
                condition_id[0] = 0x11;
                condition_id[31] = index + 1;
                let token_id = |prefix: u8| {
                    let mut token = [0u8; 32];
                    token[0] = prefix;
                    token[31] = index + 1;
                    token
                };
                NegRiskCondition {
                    index,
                    condition_id,
                    yes: NegRiskOutcome {
                        token_id: token_id(0x91),
                        instrument_id: InstrumentId::from(
                            format!("YES-{index}.POLYMARKET").as_str(),
                        ),
                    },
                    no: NegRiskOutcome {
                        token_id: token_id(0xa1),
                        instrument_id: InstrumentId::from(
                            format!("NO-{index}.POLYMARKET").as_str(),
                        ),
                    },
                }
            })
            .collect();
        CtfNegRiskEvent::new_checked(market_id, fee_bps, conditions).unwrap()
    }

    fn record(
        mapping: &CtfNegRiskEvent,
        selected: &[usize],
        quantity: Decimal,
    ) -> ConvertOperationRecord {
        let inputs = selected
            .iter()
            .map(|index| ConvertTokenAmount {
                instrument_id: mapping.conditions()[*index].no.instrument_id,
                quantity,
            })
            .collect::<Vec<_>>();
        let scope = mapping
            .convert_scope(
                &inputs
                    .iter()
                    .map(|input| input.instrument_id)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        let request = SubmitConvert {
            operation_id: UUID4::new(),
            trader_id: TraderId::from("TESTER-001"),
            strategy_id: StrategyId::from("CONVERT-001"),
            account_id: AccountId::from("POLYMARKET-001"),
            client_id: ClientId::from("POLYMARKET"),
            inputs,
            ts_init: UnixNanos::default(),
        };
        let mut record = ConvertOperationRecord::new(&request);
        record.affected_instruments = scope
            .inputs
            .iter()
            .chain(scope.outputs.iter())
            .copied()
            .collect();
        record.status = Some(ConvertExecutionStatus::Unknown);
        record.dispatched = true;
        record.transaction = Some(ConvertTransaction {
            wallet: WALLET.to_string(),
            relayer_transaction_id: None,
            transaction_hash: Some(format!("{:#x}", B256::from([0x11; 32]))),
            nonce: Some("42".to_string()),
            call_summary: None,
        });
        record
    }

    #[rstest]
    fn test_fee_and_multi_selected_cash_are_exact() {
        let mapping = mapping(3, 100);
        let record = record(&mapping, &[0, 1], dec!(1.5));
        let expected = ExpectedConvert::new(&record, &mapping).unwrap();

        assert_eq!(expected.amount, U256::from(1_500_000));
        assert_eq!(expected.amount_out, U256::from(1_485_000));
        assert_eq!(expected.net_cash, U256::from(1_485_000));
        assert_eq!(expected.inputs.len(), 2);
        assert_eq!(expected.inputs[0].quantity, dec!(1.5));
        assert_eq!(expected.outputs.len(), 1);
        assert_eq!(expected.outputs[0].quantity, dec!(1.485));

        let selected_no = U256::from_be_bytes(mapping.conditions()[0].no.token_id);
        match expected.token_expectations[&selected_no] {
            TokenExpectation::Debit(amount) => assert_eq!(amount, expected.amount),
            _ => panic!("selected NO token was not a debit"),
        }
        let selected_yes = U256::from_be_bytes(mapping.conditions()[0].yes.token_id);
        assert!(matches!(
            expected.token_expectations[&selected_yes],
            TokenExpectation::Unchanged
        ));
        let unselected_yes = U256::from_be_bytes(mapping.conditions()[2].yes.token_id);
        match expected.token_expectations[&unselected_yes] {
            TokenExpectation::Credit(amount) => assert_eq!(amount, expected.amount_out),
            _ => panic!("unselected YES token was not a credit"),
        }
    }

    #[rstest]
    fn test_ctf_flows_require_exact_token_attribution() {
        let mapping = mapping(3, 0);
        let record = record(&mapping, &[0, 1], dec!(1));
        let expected = ExpectedConvert::new(&record, &mapping).unwrap();
        let mut flows = HashMap::new();
        for (token_id, expectation) in &expected.token_expectations {
            let flow = flows.entry(*token_id).or_insert_with(TokenFlow::default);
            match *expectation {
                TokenExpectation::Debit(amount) => flow.debited = amount,
                TokenExpectation::Credit(amount) => flow.credited = amount,
                TokenExpectation::Unchanged => {}
            }
        }
        assert!(verify_condition_flows(&expected, &flows).is_ok());

        flows.insert(
            U256::from(0xdead_u64),
            TokenFlow {
                debited: U256::from(1u8),
                credited: U256::ZERO,
            },
        );
        assert!(verify_condition_flows(&expected, &flows).is_err());
    }

    #[rstest]
    fn test_block_transaction_inclusion_is_strict_when_listed() {
        let hash = B256::from([0x11; 32]);
        let mut block = RpcBlock {
            number: "0x1".to_string(),
            hash: format!("{:#x}", B256::from([0x22; 32])),
            timestamp: "0x1".to_string(),
            transactions: Some(vec![Value::String(format!("{hash:#x}"))]),
        };
        assert!(block_contains_transaction(&block, hash));

        block.transactions = Some(vec![Value::String(format!(
            "{:#x}",
            B256::from([0x33; 32])
        ))]);
        assert!(!block_contains_transaction(&block, hash));

        block.transactions = Some(Vec::new());
        assert!(!block_contains_transaction(&block, hash));
        block.transactions = None;
        assert!(!block_contains_transaction(&block, hash));
    }
}
