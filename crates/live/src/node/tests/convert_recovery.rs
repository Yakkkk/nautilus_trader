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

use std::{cell::Cell, rc::Rc, time::Duration};

use async_trait::async_trait;
use nautilus_common::{
    cache::Cache,
    clients::ConvertQueryDispatchError,
    messages::execution::{
        ConvertExecutionEvent, ConvertExecutionOutcome, ConvertExecutionStatus,
        ConvertOperationRecord, ConvertTokenAmount, ConvertTransaction, QueryConvert,
    },
};
use nautilus_core::{Params, UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, OmsType},
    identifiers::{AccountId, ClientId, InstrumentId, StrategyId},
    reports::ExecutionMassStatus,
    types::{AccountBalance, Currency, MarginBalance},
};

use super::*;
use crate::execution::emitter::ExecutionEventEmitter;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryBehavior {
    EmitInvalidAuthorization,
    Silent,
}

#[derive(Debug)]
struct RecoveryClient {
    account_id: AccountId,
    client_id: ClientId,
    behavior: QueryBehavior,
    connected: Rc<Cell<bool>>,
    queries: Rc<Cell<usize>>,
    query_failures: Rc<Cell<usize>>,
    permanent_query_failure: Rc<Cell<bool>>,
    mass_status_checks: Rc<Cell<usize>>,
    last_query_run_id: Rc<Cell<Option<UUID4>>>,
    restores: Rc<Cell<usize>>,
    emitter: ExecutionEventEmitter,
    reporting_cache: Option<Rc<RefCell<Cache>>>,
}

#[async_trait(?Send)]
impl ExecutionClient for RecoveryClient {
    fn is_connected(&self) -> bool {
        self.connected.get()
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
        self.connected.set(true);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.connected.set(false);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected.set(false);
        Ok(())
    }

    fn query_convert(
        &self,
        command: QueryConvert,
        transaction: &ConvertTransaction,
    ) -> anyhow::Result<()> {
        self.query_convert_for_run(command, transaction, None)
    }

    fn query_convert_for_run(
        &self,
        command: QueryConvert,
        transaction: &ConvertTransaction,
        run_id: Option<UUID4>,
    ) -> anyhow::Result<()> {
        self.queries.set(self.queries.get() + 1);
        self.last_query_run_id.set(run_id);

        if self.permanent_query_failure.get() {
            return Err(ConvertQueryDispatchError::permanent(
                "permanent Convert query configuration error",
            )
            .into());
        }

        let failures = self.query_failures.get();
        if failures > 0 {
            self.query_failures.set(failures - 1);
            return Err(ConvertQueryDispatchError::retryable(
                "transient Convert query dispatch failure",
            )
            .into());
        }

        if self.behavior == QueryBehavior::EmitInvalidAuthorization {
            let outcome = ConvertExecutionOutcome::AuthorizationInvalid {
                transaction: Some(transaction.clone()),
                reason: "query proved the original authorization invalid".to_string(),
            };
            self.emitter.send_convert_event(ConvertExecutionEvent {
                operation_id: command.operation_id,
                account_id: self.account_id,
                client_id: self.client_id,
                outcome,
                run_id,
            });
        }

        Ok(())
    }

    async fn restore_convert_context(
        &self,
        _operation_id: UUID4,
        _transaction: &ConvertTransaction,
    ) -> anyhow::Result<()> {
        self.restores.set(self.restores.get() + 1);
        Ok(())
    }

    async fn generate_mass_status(
        &self,
        _lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        self.mass_status_checks
            .set(self.mass_status_checks.get() + 1);

        if let Some(cache) = &self.reporting_cache {
            cache
                .borrow()
                .prepare_convert_position_report(self.account_id, None)?;
        }
        Ok(None)
    }
}

struct RecoveryFixture {
    node: LiveNode,
    operation_id: UUID4,
    output: InstrumentId,
    transaction: ConvertTransaction,
    queries: Rc<Cell<usize>>,
    query_failures: Rc<Cell<usize>>,
    permanent_query_failure: Rc<Cell<bool>>,
    mass_status_checks: Rc<Cell<usize>>,
    last_query_run_id: Rc<Cell<Option<UUID4>>>,
    restores: Rc<Cell<usize>>,
    client: LiveExecutionClient,
}

impl RecoveryFixture {
    fn new(
        reconciliation: bool,
        timeout: Duration,
        behavior: QueryBehavior,
        with_locator: bool,
    ) -> Self {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                load_cache: true,
                reconciliation,
                ..Default::default()
            },
            timeout_reconciliation: timeout,
            delay_post_stop: Duration::ZERO,
            ..Default::default()
        };
        let mut node = LiveNode::build("ConvertRecoveryQuery".to_string(), Some(config)).unwrap();
        let account_id = AccountId::from("POLYMARKET-001");
        let client_id = ClientId::from("POLYMARKET");
        let strategy_id = StrategyId::from("CONVERT-001");
        let input = InstrumentId::from("NO-A.POLYMARKET");
        let output = InstrumentId::from("YES-B.POLYMARKET");
        let operation_id = UUID4::new();
        let transaction = ConvertTransaction {
            wallet: "0x1111111111111111111111111111111111111111".to_string(),
            relayer_transaction_id: with_locator.then(|| "RELAY-001".to_string()),
            transaction_hash: None,
            nonce: Some("7".to_string()),
            call_summary: None,
        };
        let record = ConvertOperationRecord {
            operation_id,
            trader_id: node.trader_id(),
            strategy_id,
            account_id,
            client_id,
            inputs: vec![ConvertTokenAmount {
                instrument_id: input,
                quantity: dec!(100),
            }],
            affected_instruments: vec![input, output],
            status: None,
            dispatched: true,
            historical_gap: false,
            authorization_invalid: false,
            transaction: Some(transaction.clone()),
            applied: false,
            simulated_cash_credited: false,
            plan_lost: false,
            recovery_blocked: false,
            unapplied_instruments: vec![input, output],
            reason: None,
        };
        let (database, _) = TestCacheDatabaseControl::create();
        database
            .add(
                ConvertOperationRecord::cache_key(operation_id),
                serde_json::to_vec(&record).unwrap().into(),
            )
            .unwrap();
        node.set_cache_database(Box::new(database)).unwrap();

        let queries = Rc::new(Cell::new(0));
        let query_failures = Rc::new(Cell::new(0));
        let permanent_query_failure = Rc::new(Cell::new(false));
        let mass_status_checks = Rc::new(Cell::new(0));
        let last_query_run_id = Rc::new(Cell::new(None));
        let restores = Rc::new(Cell::new(0));
        let mut emitter = ExecutionEventEmitter::new(
            get_atomic_clock_realtime(),
            node.trader_id(),
            account_id,
            AccountType::Cash,
            Some(Currency::USDC()),
        );
        emitter.set_sender(get_exec_event_sender());
        let client = LiveExecutionClient::new(Box::new(RecoveryClient {
            account_id,
            client_id,
            behavior,
            connected: Rc::new(Cell::new(true)),
            queries: queries.clone(),
            query_failures: query_failures.clone(),
            permanent_query_failure: permanent_query_failure.clone(),
            mass_status_checks: mass_status_checks.clone(),
            last_query_run_id: last_query_run_id.clone(),
            restores: restores.clone(),
            emitter,
            reporting_cache: Some(node.kernel.cache()),
        }));
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(client.clone()))
            .unwrap();
        node.exec_clients.push(client.clone());

        Self {
            node,
            operation_id,
            output,
            transaction,
            queries,
            query_failures,
            permanent_query_failure,
            mass_status_checks,
            last_query_run_id,
            restores,
            client,
        }
    }
}

#[rstest]
#[tokio::test]
async fn test_startup_query_result_reaches_normal_convert_route_before_mass_status() {
    let mut fixture = RecoveryFixture::new(
        true,
        Duration::from_secs(1),
        QueryBehavior::EmitInvalidAuthorization,
        true,
    );
    fixture.node.start().await.unwrap();

    let record = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(record.status, Some(ConvertExecutionStatus::Failed));
    assert!(record.authorization_invalid);
    assert_eq!(fixture.queries.get(), 1);
    assert!(fixture.mass_status_checks.get() >= 1);
    assert_eq!(fixture.restores.get(), 1);
    assert!(
        !fixture
            .node
            .kernel
            .cache
            .borrow()
            .convert_scope_is_active(fixture.output, record.account_id)
    );

    fixture.node.stop().await.unwrap();
    fixture.node.dispose();
}

#[rstest]
#[tokio::test]
async fn test_startup_query_retries_a_transient_dispatch_failure() {
    let mut fixture = RecoveryFixture::new(
        false,
        Duration::from_secs(1),
        QueryBehavior::EmitInvalidAuthorization,
        true,
    );
    fixture.query_failures.set(1);
    fixture.node.start().await.unwrap();

    let record = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(fixture.queries.get(), 2);
    assert_eq!(record.status, Some(ConvertExecutionStatus::Failed));
    assert!(record.authorization_invalid);
    assert_eq!(
        fixture.last_query_run_id.get(),
        fixture
            .node
            .kernel
            .exec_engine
            .borrow()
            .convert_live_run_id(),
    );

    fixture.node.stop().await.unwrap();
    fixture.node.dispose();
}

#[rstest]
#[tokio::test]
async fn test_startup_query_permanent_dispatch_error_is_not_retried() {
    let mut fixture = RecoveryFixture::new(
        false,
        Duration::from_secs(1),
        QueryBehavior::EmitInvalidAuthorization,
        true,
    );
    fixture.permanent_query_failure.set(true);
    fixture.node.start().await.unwrap();

    let record = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(fixture.queries.get(), 1);
    assert_eq!(record.status, None);
    assert!(!record.authorization_invalid);
    assert!(
        fixture
            .node
            .kernel
            .cache
            .borrow()
            .convert_scope_is_active(fixture.output, record.account_id)
    );

    fixture.node.stop().await.unwrap();
    fixture.node.dispose();
}

#[rstest]
#[tokio::test]
async fn test_startup_query_budget_exhaustion_remains_pending_and_allows_explicit_requery() {
    let mut fixture = RecoveryFixture::new(
        false,
        Duration::from_secs(1),
        QueryBehavior::EmitInvalidAuthorization,
        true,
    );
    fixture.query_failures.set(3);
    fixture.node.start().await.unwrap();

    let record = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(fixture.queries.get(), 3);
    assert_eq!(record.status, None);
    assert!(!record.authorization_invalid);
    assert!(
        fixture
            .node
            .kernel
            .cache
            .borrow()
            .convert_scope_is_active(fixture.output, record.account_id)
    );

    fixture.query_failures.set(0);
    let command = QueryConvert {
        operation_id: fixture.operation_id,
        trader_id: fixture.node.trader_id(),
        strategy_id: StrategyId::from("CONVERT-001"),
        account_id: AccountId::from("POLYMARKET-001"),
        client_id: ClientId::from("POLYMARKET"),
        ts_init: UnixNanos::default(),
    };
    assert!(
        fixture
            .node
            .kernel
            .exec_engine
            .borrow()
            .query_convert(&command)
    );
    fixture.node.drain_runner_pending();

    let record = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(fixture.queries.get(), 4);
    assert_eq!(record.status, Some(ConvertExecutionStatus::Failed));
    assert!(record.authorization_invalid);

    fixture.node.stop().await.unwrap();
    fixture.node.dispose();
}

#[rstest]
#[tokio::test]
async fn test_run_query_result_is_consumed_before_mass_status() {
    let mut fixture = RecoveryFixture::new(
        true,
        Duration::from_secs(1),
        QueryBehavior::EmitInvalidAuthorization,
        true,
    );
    let handle = fixture.node.handle();
    let mass_status_checks = fixture.mass_status_checks.clone();

    let result = {
        let run = fixture.node.run_with_mode(NodeRunMode::Hosted);
        tokio::pin!(run);
        let drive = async {
            wait_until_async(
                || async { handle.is_running() && mass_status_checks.get() > 0 },
                Duration::from_secs(2),
            )
            .await;
            handle.stop();
        };

        let (result, ()) = tokio::join!(run, drive);
        result
    };
    result.unwrap();

    let record = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(fixture.queries.get(), 1);
    assert!(fixture.mass_status_checks.get() >= 1);
    assert!(record.authorization_invalid);
    assert!(
        !fixture
            .node
            .kernel
            .cache
            .borrow()
            .convert_scope_is_active(fixture.output, record.account_id)
    );

    fixture.node.dispose();
}

#[rstest]
#[tokio::test]
async fn test_startup_query_without_result_ends_without_retry_and_keeps_scope() {
    let mut fixture = RecoveryFixture::new(
        false,
        Duration::from_millis(50),
        QueryBehavior::Silent,
        true,
    );
    fixture.node.start().await.unwrap();

    let record = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(record.status, None);
    assert_eq!(fixture.queries.get(), 1);
    assert!(
        fixture
            .node
            .kernel
            .cache
            .borrow()
            .convert_scope_is_active(fixture.output, record.account_id)
    );

    fixture.node.stop().await.unwrap();
    fixture.node.dispose();
}

#[rstest]
#[tokio::test]
async fn test_current_run_evidence_without_locator_is_accepted_with_core_ownership_checks() {
    let mut fixture = RecoveryFixture::new(
        false,
        Duration::from_millis(50),
        QueryBehavior::Silent,
        false,
    );
    fixture.node.start().await.unwrap();
    let run_id = fixture
        .node
        .kernel
        .exec_engine
        .borrow()
        .convert_live_run_id();
    let before = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();

    AsyncRunner::handle_exec_event(ExecutionEvent::Convert(ConvertExecutionEvent {
        operation_id: fixture.operation_id,
        account_id: AccountId::from("OTHER-ACCOUNT"),
        client_id: before.client_id,
        outcome: ConvertExecutionOutcome::AuthorizationInvalid {
            transaction: Some(fixture.transaction.clone()),
            reason: "wrong account evidence".to_string(),
        },
        run_id,
    }));
    let retained = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(retained, before);

    AsyncRunner::handle_exec_event(ExecutionEvent::Convert(ConvertExecutionEvent {
        operation_id: fixture.operation_id,
        account_id: before.account_id,
        client_id: before.client_id,
        outcome: ConvertExecutionOutcome::AuthorizationInvalid {
            transaction: Some(fixture.transaction.clone()),
            reason: "current run evidence without locator".to_string(),
        },
        run_id,
    }));
    let retained = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert!(retained.authorization_invalid);
    assert_eq!(retained.status, Some(ConvertExecutionStatus::Failed));

    fixture.node.stop().await.unwrap();
    fixture.node.dispose();
}

#[rstest]
#[tokio::test]
async fn test_initiate_shutdown_rejects_direct_engine_convert_evidence() {
    let mut fixture = RecoveryFixture::new(
        false,
        Duration::from_millis(50),
        QueryBehavior::Silent,
        false,
    );
    fixture.node.start().await.unwrap();
    let previous_run = fixture
        .node
        .kernel
        .exec_engine
        .borrow()
        .convert_live_run_id()
        .unwrap();

    fixture.node.initiate_shutdown();
    assert!(
        fixture
            .node
            .kernel
            .exec_engine
            .borrow()
            .convert_live_run_id()
            .is_none()
    );

    let before = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();

    for run_id in [Some(previous_run), None] {
        AsyncRunner::handle_exec_event(ExecutionEvent::Convert(ConvertExecutionEvent {
            operation_id: fixture.operation_id,
            account_id: before.account_id,
            client_id: before.client_id,
            outcome: ConvertExecutionOutcome::AuthorizationInvalid {
                transaction: Some(fixture.transaction.clone()),
                reason: "late direct-engine result".to_string(),
            },
            run_id,
        }));
    }
    let after = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(before, after);

    fixture.node.finalize_stop().await.unwrap();
    fixture.node.dispose();
}

#[rstest]
#[tokio::test]
async fn test_startup_restores_wallet_context_without_dispatching_a_query() {
    let mut fixture = RecoveryFixture::new(
        false,
        Duration::from_millis(50),
        QueryBehavior::Silent,
        false,
    );
    fixture.node.start().await.unwrap();

    assert_eq!(fixture.restores.get(), 1);
    assert_eq!(fixture.queries.get(), 0);
    assert_eq!(fixture.client.client_id(), ClientId::from("POLYMARKET"));

    fixture.node.stop().await.unwrap();
    fixture.node.dispose();
}

#[rstest]
#[tokio::test]
async fn test_convert_result_after_stop_does_not_change_the_retained_operation() {
    let mut fixture = RecoveryFixture::new(
        false,
        Duration::from_millis(50),
        QueryBehavior::Silent,
        false,
    );
    fixture.node.start().await.unwrap();
    fixture.node.stop().await.unwrap();

    let before = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    fixture
        .node
        .process_exec_event(ExecutionEvent::Convert(ConvertExecutionEvent {
            operation_id: fixture.operation_id,
            account_id: before.account_id,
            client_id: before.client_id,
            outcome: ConvertExecutionOutcome::AuthorizationInvalid {
                transaction: Some(fixture.transaction),
                reason: "late result".to_string(),
            },
            run_id: None,
        }));
    let after = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert_eq!(before, after);

    fixture.node.dispose();
}

#[rstest]
#[tokio::test]
async fn test_old_live_run_convert_evidence_is_ignored_after_restart() {
    let mut fixture = RecoveryFixture::new(
        false,
        Duration::from_millis(50),
        QueryBehavior::Silent,
        true,
    );
    fixture.node.start().await.unwrap();

    let engine = fixture.node.kernel.exec_engine.clone();
    let command = QueryConvert {
        operation_id: fixture.operation_id,
        trader_id: fixture.node.trader_id(),
        strategy_id: StrategyId::from("CONVERT-001"),
        account_id: AccountId::from("POLYMARKET-001"),
        client_id: ClientId::from("POLYMARKET"),
        ts_init: UnixNanos::default(),
    };
    assert!(engine.borrow().query_convert(&command));
    let previous_run = fixture.last_query_run_id.get().unwrap();

    fixture.node.stop().await.unwrap();
    fixture.node.kernel_mut().reset();
    fixture.node.start().await.unwrap();

    assert!(engine.borrow().query_convert(&command));
    let current_run = fixture.last_query_run_id.get().unwrap();
    assert_ne!(previous_run, current_run);

    let outcome = || ConvertExecutionOutcome::AuthorizationInvalid {
        transaction: Some(fixture.transaction.clone()),
        reason: "live-run-fenced evidence".to_string(),
    };
    fixture
        .node
        .process_exec_event(ExecutionEvent::Convert(ConvertExecutionEvent {
            operation_id: fixture.operation_id,
            account_id: AccountId::from("POLYMARKET-001"),
            client_id: ClientId::from("POLYMARKET"),
            outcome: outcome(),
            run_id: Some(previous_run),
        }));

    let retained = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert!(!retained.authorization_invalid);
    assert_eq!(retained.status, None);

    fixture
        .node
        .process_exec_event(ExecutionEvent::Convert(ConvertExecutionEvent {
            operation_id: fixture.operation_id,
            account_id: AccountId::from("POLYMARKET-001"),
            client_id: ClientId::from("POLYMARKET"),
            outcome: outcome(),
            run_id: None,
        }));
    let retained = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert!(!retained.authorization_invalid);

    fixture
        .node
        .process_exec_event(ExecutionEvent::Convert(ConvertExecutionEvent {
            operation_id: fixture.operation_id,
            account_id: AccountId::from("POLYMARKET-001"),
            client_id: ClientId::from("POLYMARKET"),
            outcome: outcome(),
            run_id: Some(current_run),
        }));
    let retained = fixture
        .node
        .kernel
        .cache
        .borrow()
        .load_convert_operation(fixture.operation_id)
        .unwrap()
        .unwrap();
    assert!(retained.authorization_invalid);

    fixture.node.stop().await.unwrap();
    fixture.node.dispose();
}
