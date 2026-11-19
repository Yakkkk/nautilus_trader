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

//! Python wrappers for conversion results.

use nautilus_common::messages::execution::{
    ConvertExecutionStatus, ConvertResult, ConvertTransaction,
};
use nautilus_core::UUID4;
use nautilus_model::identifiers::{AccountId, ClientId, InstrumentId, StrategyId, TraderId};
use pyo3::prelude::*;

/// Python view of the original wallet and transaction context.
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.trading")]
#[pyclass(
    module = "nautilus_trader.trading",
    name = "ConvertTransaction",
    frozen,
    eq,
    from_py_object
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyConvertTransaction {
    #[pyo3(get)]
    pub wallet: String,
    #[pyo3(get)]
    pub relayer_transaction_id: Option<String>,
    #[pyo3(get)]
    pub transaction_hash: Option<String>,
    #[pyo3(get)]
    pub nonce: Option<String>,
    #[pyo3(get)]
    pub call_summary: Option<String>,
}

impl From<ConvertTransaction> for PyConvertTransaction {
    fn from(value: ConvertTransaction) -> Self {
        Self {
            wallet: value.wallet,
            relayer_transaction_id: value.relayer_transaction_id,
            transaction_hash: value.transaction_hash,
            nonce: value.nonce,
            call_summary: value.call_summary,
        }
    }
}

/// Python view of the execution evidence and local accounting result.
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.trading")]
#[pyclass(
    module = "nautilus_trader.trading",
    name = "ConvertResult",
    frozen,
    eq,
    from_py_object
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyConvertResult {
    #[pyo3(get)]
    pub operation_id: UUID4,
    #[pyo3(get)]
    pub trader_id: TraderId,
    #[pyo3(get)]
    pub strategy_id: StrategyId,
    #[pyo3(get)]
    pub account_id: AccountId,
    #[pyo3(get)]
    pub client_id: ClientId,
    #[pyo3(get)]
    pub status: ConvertExecutionStatus,
    #[pyo3(get)]
    pub applied: bool,
    #[pyo3(get)]
    pub transaction: Option<PyConvertTransaction>,
    #[pyo3(get)]
    pub unapplied_instruments: Vec<InstrumentId>,
    #[pyo3(get)]
    pub reason: Option<String>,
}

impl From<ConvertResult> for PyConvertResult {
    fn from(value: ConvertResult) -> Self {
        Self {
            operation_id: value.operation_id,
            trader_id: value.trader_id,
            strategy_id: value.strategy_id,
            account_id: value.account_id,
            client_id: value.client_id,
            status: value.status,
            applied: value.applied,
            transaction: value.transaction.map(Into::into),
            unapplied_instruments: value.unapplied_instruments,
            reason: value.reason,
        }
    }
}
