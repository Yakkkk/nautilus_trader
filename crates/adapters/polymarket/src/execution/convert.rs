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

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{Address, B256, U256};
use nautilus_common::{
    cache::Cache,
    messages::execution::{
        ConvertExecutionEvent, ConvertExecutionOutcome, ConvertOperationRecord, ConvertTransaction,
        QueryConvert, SubmitConvert,
    },
};
use nautilus_core::{UUID4, time::AtomicTime};
use nautilus_execution::engine::polymarket_convert::convert_position_id;
use nautilus_live::{ExecutionEventEmitter, task::TaskSpawner};
use nautilus_model::{
    identifiers::{AccountId, ClientId, InstrumentId, TradeId},
    instruments::neg_risk::{ConvertScope, CtfNegRiskEvent},
};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::PolymarketExecutionClient;
use crate::{
    common::{credential::RelayerApiKey, enums::PolymarketSignatureType},
    config::PolymarketExecutionClientConfig,
    execution::{
        collateral::CollateralLocks, reports::fetch_and_emit_account_state,
        settlement::SettlementRegistry,
    },
    http::clob::PolymarketClobHttpClient,
    positions::{
        PolymarketPositionClient, SubmittedWalletBatch, WalletSubmitFailure,
        calldata::PositionCall,
        convert::{ExpectedConvert, query_original_convert},
    },
    signing::eip712::{NEG_RISK_CTF_COLLATERAL_ADAPTER, parse_address},
};

const CONVERT_INPUT_EVIDENCE_WAIT: Duration = Duration::from_millis(1_000);
const CONVERT_INPUT_EVIDENCE_POLL: Duration = Duration::from_millis(25);

impl PolymarketExecutionClient {
    pub(super) fn submit_convert_command(
        &self,
        cmd: &SubmitConvert,
        scope: &ConvertScope,
        run_id: Option<UUID4>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.account_id == self.core.account_id
                && cmd.client_id == self.core.client_id
                && cmd.trader_id == self.core.trader_id,
            "Convert submission does not belong to this execution client"
        );
        self.check_not_faulted()?;
        self.settlement.ensure_submission_allowed()?;
        anyhow::ensure!(
            self.config.signature_type == PolymarketSignatureType::Poly1271,
            "Polymarket Convert submissions require a Deposit Wallet"
        );
        let funder = self.secrets.funder.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "Polymarket Convert submissions require a configured Deposit Wallet funder"
            )
        })?;
        let wallet = parse_address(funder, "funder")?;
        let Some(rpc_url) = &self.config.rpc_url else {
            anyhow::bail!("Polymarket Convert submissions require a configured Polygon RPC URL");
        };

        let record = {
            let cache = self.core.cache();
            let record = cache
                .load_convert_operation(cmd.operation_id)?
                .ok_or_else(|| anyhow::anyhow!("Unknown admitted Convert operation"))?;
            anyhow::ensure!(
                record.matches_request(cmd),
                "Convert submission conflicts with its retained request"
            );
            anyhow::ensure!(
                record.dispatched
                    && record.status.is_none()
                    && record.transaction.is_none()
                    && !record.applied,
                "Convert operation is not awaiting its first submission"
            );
            record
        };

        let submission = {
            let cache = self.core.cache();
            prepare_convert_submission(&cache, &record, scope, wallet, &self.settlement)?
        };

        let relayer_key = RelayerApiKey::resolve_with_default_address(
            self.config.relayer_api_key.clone(),
            self.config.relayer_signer_address.clone(),
            &self.secrets.address,
        )?;
        let proxy_url = self.config.validated_proxy_url()?;
        let position_client = PolymarketPositionClient::new(
            &self.secrets.private_key,
            funder,
            relayer_key,
            self.config.base_url_relayer.clone(),
            self.config.base_url_http.clone(),
            Some(self.config.http_timeout_secs),
            proxy_url,
        )?
        .with_rpc_url(rpc_url.expose_secret().to_string());

        let spawner = self
            .pending_tasks
            .spawner()
            .map_err(|e| anyhow::anyhow!("Polymarket Convert task admission is closed: {e}"))?;
        let cancellation = spawner.cancellation_token();
        let task = ConvertSubmitTask {
            position_client,
            config: self.config.clone(),
            record,
            mapping: submission.mapping,
            call: submission.call,
            input_token_ids: submission.input_token_ids,
            input_evidence: submission.input_evidence,
            wallet,
            amount: submission.amount,
            market_id: submission.market_id,
            index_set: submission.index_set,
            operation_id: cmd.operation_id,
            account_id: cmd.account_id,
            client_id: cmd.client_id,
            run_id,
            emitter: self.emitter.clone(),
            spawner: spawner.clone(),
            settlement: self.settlement.clone(),
            http_client: self.http_client.clone(),
            clock: self.clock,
            signature_type: self.config.signature_type,
            collateral_locks: self.collateral_locks.clone(),
            cancellation,
        };
        spawner
            .spawn(Box::pin(task.run()))
            .map_err(|e| anyhow::anyhow!("Polymarket Convert task admission is closed: {e}"))?;

        Ok(())
    }

    pub(super) fn query_convert_command(
        &self,
        cmd: &QueryConvert,
        transaction: &ConvertTransaction,
        run_id: Option<UUID4>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.account_id == self.core.account_id
                && cmd.client_id == self.core.client_id
                && cmd.trader_id == self.core.trader_id,
            "Convert query does not belong to this execution client"
        );
        anyhow::ensure!(
            self.config.signature_type == PolymarketSignatureType::Poly1271,
            "Polymarket Convert queries require a Deposit Wallet"
        );
        let funder = self.secrets.funder.as_deref().ok_or_else(|| {
            anyhow::anyhow!("Polymarket Convert queries require a configured Deposit Wallet funder")
        })?;
        anyhow::ensure!(
            parse_address(funder, "funder")?
                == parse_address(&transaction.wallet, "Convert wallet")?,
            "Retained Convert wallet does not match the execution client funder"
        );
        anyhow::ensure!(
            self.config.rpc_url.is_some(),
            "Polymarket Convert queries require a configured Polygon RPC URL"
        );

        let (record, mapping, input_evidence) = {
            let cache = self.core.cache();
            let record = cache
                .load_convert_operation(cmd.operation_id)?
                .filter(|record| record.is_owned_by(cmd) && record.dispatched)
                .ok_or_else(|| anyhow::anyhow!("Unknown retained Convert operation"))?;
            anyhow::ensure!(
                record.transaction.as_ref() == Some(transaction)
                    && transaction.has_original_locator(),
                "Convert query requires the retained original transaction locator"
            );
            let mapping = record
                .inputs
                .first()
                .and_then(|input| cache.convert_event_for_instrument(&input.instrument_id))
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("No complete event mapping for retained Convert"))?;
            let input_evidence = record
                .inputs
                .iter()
                .map(|input| {
                    let position_id = convert_position_id(input.instrument_id, record.strategy_id);
                    let trade_ids =
                        cache
                            .position_owned(&position_id)
                            .map_or_else(Vec::new, |position| {
                                position
                                    .events
                                    .iter()
                                    .filter(|fill| fill.instrument_id == input.instrument_id)
                                    .map(|fill| fill.trade_id)
                                    .collect::<Vec<_>>()
                            });
                    (input.instrument_id, trade_ids)
                })
                .collect::<Vec<_>>();
            (record, mapping, input_evidence)
        };

        let config = self.config.clone();
        let settlement = self.settlement.clone();
        let emitter = self.emitter.clone();
        let http_client = self.http_client.clone();
        let clock = self.clock;
        let signature_type = self.config.signature_type;
        let collateral_locks = self.collateral_locks.clone();
        let operation_id = cmd.operation_id;
        let account_id = cmd.account_id;
        let client_id = cmd.client_id;
        let spawner = self
            .pending_tasks
            .spawner()
            .map_err(|e| anyhow::anyhow!("Polymarket Convert task admission is closed: {e}"))?;
        let cancellation = spawner.cancellation_token();
        let admitted = self.spawn_task("Convert query", async move {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            let outcome = query_original_convert(&config, &record, &mapping, &cancellation).await;
            if cancellation.is_cancelled() {
                return Ok(());
            }

            if matches!(&outcome, ConvertExecutionOutcome::Confirmed { .. }) {
                wait_for_convert_input_evidence(&settlement, &input_evidence, &cancellation).await;
                if cancellation.is_cancelled() {
                    return Ok(());
                }
            }

            match outcome {
                ConvertExecutionOutcome::Confirmed {
                    verified,
                    mut pending_inputs,
                } => {
                    settlement.with_convert_input_pending(&input_evidence, |pending| {
                        pending_inputs.extend_from_slice(pending);
                        emitter.send_convert_event(ConvertExecutionEvent {
                            operation_id,
                            account_id,
                            client_id,
                            outcome: ConvertExecutionOutcome::Confirmed {
                                verified,
                                pending_inputs,
                            },
                            run_id,
                        });
                    });
                    spawn_refresh_task(
                        &spawner,
                        &cancellation,
                        http_client,
                        emitter,
                        clock,
                        signature_type,
                        collateral_locks,
                        settlement,
                    );
                }
                outcome => emitter.send_convert_event(ConvertExecutionEvent {
                    operation_id,
                    account_id,
                    client_id,
                    outcome,
                    run_id,
                }),
            }
            Ok(())
        });
        anyhow::ensure!(
            admitted,
            "Polymarket Convert query task admission is closed"
        );
        Ok(())
    }
}

struct ConvertSubmission {
    mapping: CtfNegRiskEvent,
    amount: U256,
    market_id: B256,
    index_set: U256,
    call: PositionCall,
    input_token_ids: Vec<U256>,
    input_evidence: Vec<(InstrumentId, Vec<TradeId>)>,
}

fn prepare_convert_submission(
    cache: &Cache,
    record: &ConvertOperationRecord,
    scope: &ConvertScope,
    wallet: Address,
    settlement: &SettlementRegistry,
) -> anyhow::Result<ConvertSubmission> {
    let input_ids = record
        .inputs
        .iter()
        .map(|input| input.instrument_id)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !input_ids.is_empty(),
        "retained Convert operation has no inputs"
    );
    let mapping = cache
        .convert_event_for_instrument(&input_ids[0])
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no complete negative-risk event mapping for input {}",
                input_ids[0]
            )
        })?;
    let expected = ExpectedConvert::new_for_submission(record, &mapping, wallet)
        .map_err(|e| anyhow::anyhow!(e.reason))?;
    let scope_inputs = sorted_instruments(&scope.inputs);
    let scope_outputs = sorted_instruments(&scope.outputs);
    let expected_inputs = sorted_instruments(
        &expected
            .inputs
            .iter()
            .map(|input| input.instrument_id)
            .collect::<Vec<_>>(),
    );
    let expected_outputs = sorted_instruments(
        &expected
            .outputs
            .iter()
            .map(|output| output.instrument_id)
            .collect::<Vec<_>>(),
    );
    anyhow::ensure!(
        scope_inputs == expected_inputs && scope_outputs == expected_outputs,
        "dispatched Convert scope does not match the authoritative retained scope"
    );

    let mut input_evidence = Vec::with_capacity(input_ids.len());
    for input in &input_ids {
        let position_id = convert_position_id(*input, record.strategy_id);
        let position = cache.position_owned(&position_id).ok_or_else(|| {
            anyhow::anyhow!(
                "conversion input {input} has no position for strategy {}",
                record.strategy_id
            )
        })?;
        anyhow::ensure!(
            !position.events.is_empty(),
            "conversion input {input} has no applied CLOB fill evidence"
        );
        let trade_ids = position
            .events
            .iter()
            .map(|fill| {
                anyhow::ensure!(
                    fill.instrument_id == *input,
                    "conversion input {input} position contained another instrument's fill"
                );
                Ok(fill.trade_id)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        settlement.request_convert_input_resolution(*input, &trade_ids);
        settlement.ensure_convert_input_evidence(*input, &trade_ids)?;
        input_evidence.push((*input, trade_ids));
    }

    let call = PositionCall {
        target: NEG_RISK_CTF_COLLATERAL_ADAPTER,
        data: expected.calldata,
    };

    Ok(ConvertSubmission {
        mapping,
        amount: expected.amount,
        market_id: expected.market_id,
        index_set: expected.index_set,
        call,
        input_token_ids: expected.input_token_ids,
        input_evidence,
    })
}

fn sorted_instruments(instruments: &[InstrumentId]) -> Vec<InstrumentId> {
    let mut instruments = instruments.to_vec();
    instruments.sort_unstable();
    instruments
}

struct ConvertSubmitTask {
    position_client: PolymarketPositionClient,
    config: PolymarketExecutionClientConfig,
    record: ConvertOperationRecord,
    mapping: CtfNegRiskEvent,
    call: PositionCall,
    input_token_ids: Vec<U256>,
    input_evidence: Vec<(InstrumentId, Vec<TradeId>)>,
    wallet: Address,
    amount: U256,
    market_id: B256,
    index_set: U256,
    operation_id: UUID4,
    account_id: AccountId,
    client_id: ClientId,
    run_id: Option<UUID4>,
    emitter: ExecutionEventEmitter,
    spawner: TaskSpawner,
    settlement: Arc<SettlementRegistry>,
    http_client: PolymarketClobHttpClient,
    clock: &'static AtomicTime,
    signature_type: PolymarketSignatureType,
    collateral_locks: Arc<Mutex<CollateralLocks>>,
    cancellation: CancellationToken,
}

impl ConvertSubmitTask {
    async fn run(self) {
        if self.cancellation.is_cancelled() {
            return;
        }

        if let Err(e) = self.position_client.verify_convert_rpc().await {
            if !self.cancellation.is_cancelled() {
                self.emit(ConvertExecutionOutcome::Rejected(format!(
                    "Convert RPC preflight failed: {e}"
                )));
            }
            return;
        }

        if self.cancellation.is_cancelled() {
            return;
        }

        if let Err(e) = self
            .position_client
            .verify_convert_wallet(&self.input_token_ids, self.amount)
            .await
        {
            if !self.cancellation.is_cancelled() {
                self.emit(ConvertExecutionOutcome::Rejected(format!(
                    "Convert wallet preflight failed: {e}"
                )));
            }
            return;
        }

        if self.cancellation.is_cancelled() {
            return;
        }

        for (instrument_id, trade_ids) in &self.input_evidence {
            self.settlement
                .request_convert_input_resolution(*instrument_id, trade_ids);
        }

        if let Some((instrument_id, _)) =
            self.input_evidence
                .iter()
                .find(|(instrument_id, trade_ids)| {
                    self.settlement
                        .convert_input_evidence_pending(*instrument_id, trade_ids)
                })
        {
            self.emit(ConvertExecutionOutcome::Rejected(format!(
                "input {instrument_id} settlement evidence changed before signing"
            )));
            return;
        }

        let metadata = format!(
            "Convert {} market={:#x} index_set={} amount={}",
            self.operation_id, self.market_id, self.index_set, self.amount
        );
        let submitted = match self
            .position_client
            .submit_convert_call(self.operation_id, self.call.clone(), &metadata)
            .await
        {
            Ok(submitted) => submitted,
            Err(failure) => {
                self.handle_submit_failure(&failure);
                return;
            }
        };

        let transaction = self.transaction(&submitted);
        if transaction.relayer_transaction_id.is_some() {
            self.emit(ConvertExecutionOutcome::Submitted(transaction.clone()));
        } else if transaction.has_original_locator() {
            self.emit(ConvertExecutionOutcome::Unknown {
                transaction: Some(transaction.clone()),
                reason: "original transaction hash retained; verification is in progress"
                    .to_string(),
            });
        } else {
            self.emit(ConvertExecutionOutcome::Unknown {
                transaction: Some(transaction),
                reason:
                    "Relayer submit response omitted transaction_id; transaction outcome is unknown"
                        .to_string(),
            });
            return;
        }

        let mut record = self.record.clone();
        record.transaction = Some(transaction);
        let outcome =
            query_original_convert(&self.config, &record, &self.mapping, &self.cancellation).await;

        if self.cancellation.is_cancelled() {
            return;
        }

        if matches!(&outcome, ConvertExecutionOutcome::Confirmed { .. }) {
            wait_for_convert_input_evidence(
                &self.settlement,
                &self.input_evidence,
                &self.cancellation,
            )
            .await;

            if self.cancellation.is_cancelled() {
                return;
            }
        }

        match outcome {
            ConvertExecutionOutcome::Confirmed {
                verified,
                mut pending_inputs,
            } => {
                self.settlement
                    .with_convert_input_pending(&self.input_evidence, |pending| {
                        pending_inputs.extend_from_slice(pending);
                        self.emit(ConvertExecutionOutcome::Confirmed {
                            verified,
                            pending_inputs,
                        });
                    });
                self.spawn_refresh();
            }
            outcome => self.emit(outcome),
        }
    }

    fn handle_submit_failure(&self, failure: &WalletSubmitFailure) {
        let reason = failure.error.to_string();
        if failure.ambiguous {
            if let Some(nonce) = failure.nonce {
                let transaction = self.transaction_from_parts(nonce, None, None);
                self.emit(ConvertExecutionOutcome::Unknown {
                    transaction: Some(transaction),
                    reason,
                });
            } else {
                self.emit(ConvertExecutionOutcome::Unknown {
                    transaction: None,
                    reason,
                });
            }
            return;
        }

        if !self.cancellation.is_cancelled() {
            self.emit(ConvertExecutionOutcome::Rejected(reason));
        }
    }

    fn transaction(&self, submitted: &SubmittedWalletBatch) -> ConvertTransaction {
        self.transaction_from_parts(
            submitted.nonce,
            submitted.transaction_id.clone(),
            submitted.transaction_hash.clone(),
        )
    }

    fn transaction_from_parts(
        &self,
        nonce: U256,
        relayer_transaction_id: Option<String>,
        transaction_hash: Option<String>,
    ) -> ConvertTransaction {
        ConvertTransaction {
            wallet: format!("{:#x}", self.wallet),
            relayer_transaction_id,
            transaction_hash,
            nonce: Some(nonce.to_string()),
            call_summary: Some(format!(
                "convertPositions marketId={:#x} indexSet={} amount={}",
                self.market_id, self.index_set, self.amount
            )),
        }
    }

    fn emit(&self, outcome: ConvertExecutionOutcome) {
        self.emitter.send_convert_event(ConvertExecutionEvent {
            operation_id: self.operation_id,
            account_id: self.account_id,
            client_id: self.client_id,
            outcome,
            run_id: self.run_id,
        });
    }

    fn spawn_refresh(&self) {
        spawn_refresh_task(
            &self.spawner,
            &self.cancellation,
            self.http_client.clone(),
            self.emitter.clone(),
            self.clock,
            self.signature_type,
            self.collateral_locks.clone(),
            self.settlement.clone(),
        );
    }
}

async fn wait_for_convert_input_evidence(
    settlement: &SettlementRegistry,
    input_evidence: &[(InstrumentId, Vec<TradeId>)],
    cancellation: &CancellationToken,
) -> Vec<InstrumentId> {
    let deadline = Instant::now() + CONVERT_INPUT_EVIDENCE_WAIT;

    loop {
        for (instrument_id, trade_ids) in input_evidence {
            settlement.request_convert_input_resolution(*instrument_id, trade_ids);
        }
        let pending =
            settlement.with_convert_input_pending(input_evidence, |pending| pending.to_vec());
        if pending.is_empty() || cancellation.is_cancelled() || Instant::now() >= deadline {
            return pending;
        }
        tokio::time::sleep(CONVERT_INPUT_EVIDENCE_POLL).await;
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the refresh task captures the existing client components without a wrapper type"
)]
fn spawn_refresh_task(
    spawner: &TaskSpawner,
    cancellation: &CancellationToken,
    http_client: PolymarketClobHttpClient,
    emitter: ExecutionEventEmitter,
    clock: &'static AtomicTime,
    signature_type: PolymarketSignatureType,
    collateral_locks: Arc<Mutex<CollateralLocks>>,
    settlement: Arc<SettlementRegistry>,
) {
    let spawner = spawner.clone();
    let cancellation = cancellation.clone();

    if let Err(e) = spawner.spawn(async move {
        if cancellation.is_cancelled() {
            return;
        }

        if let Err(e) = fetch_and_emit_account_state(
            &http_client,
            &emitter,
            clock,
            signature_type,
            &collateral_locks,
            &settlement,
        )
        .await
        {
            log::warn!("Failed to refresh account after Convert confirmation: {e}");
        }
    }) {
        log::debug!("Cannot spawn Convert account refresh: {e}");
    }
}
