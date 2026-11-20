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

//! Commands and results for negative-risk asset conversion.

use std::{collections::HashSet, fmt::Display, sync::Weak};

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    identifiers::{AccountId, ClientId, InstrumentId, StrategyId, TraderId},
    types::Money,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Requests one conversion of the selected NO inputs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub struct SubmitConvert {
    pub operation_id: UUID4,
    pub trader_id: TraderId,
    pub strategy_id: StrategyId,
    pub account_id: AccountId,
    pub client_id: ClientId,
    pub inputs: Vec<ConvertTokenAmount>,
    pub ts_init: UnixNanos,
}

impl Display for SubmitConvert {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "SubmitConvert(operation_id={}, client_id={}, account_id={})",
            self.operation_id, self.client_id, self.account_id
        )
    }
}

/// Queries the original operation without starting a new conversion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub struct QueryConvert {
    pub operation_id: UUID4,
    pub trader_id: TraderId,
    pub strategy_id: StrategyId,
    pub account_id: AccountId,
    pub client_id: ClientId,
    pub ts_init: UnixNanos,
}

impl Display for QueryConvert {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "QueryConvert(operation_id={}, client_id={}, account_id={})",
            self.operation_id, self.client_id, self.account_id
        )
    }
}

/// Original wallet and transaction context supplied by the client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvertTransaction {
    pub wallet: String,
    pub relayer_transaction_id: Option<String>,
    pub transaction_hash: Option<String>,
    /// The original wallet nonce as an unsigned decimal string.
    pub nonce: Option<String>,
    pub call_summary: Option<String>,
}

impl ConvertTransaction {
    /// Returns whether this context locates the original external transaction.
    #[must_use]
    pub fn has_original_locator(&self) -> bool {
        self.relayer_transaction_id.is_some() || self.transaction_hash.is_some()
    }
}

/// A token amount in instrument units, not raw chain units.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvertTokenAmount {
    pub instrument_id: InstrumentId,
    pub quantity: Decimal,
}

/// The verified effects of a single Convert; network verification belongs to the client.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedConvert {
    pub operation_id: UUID4,
    pub trader_id: TraderId,
    pub strategy_id: StrategyId,
    pub account_id: AccountId,
    pub transaction: ConvertTransaction,
    pub inputs: Vec<ConvertTokenAmount>,
    pub outputs: Vec<ConvertTokenAmount>,
    /// Includes every consumed NO and every potential YES, including zero or absent outputs.
    pub affected_instruments: Vec<InstrumentId>,
    pub net_cash: Money,
    pub ts_event: UnixNanos,
}

/// Evidence for the external execution, independent of local accounting.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(C)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        frozen,
        eq,
        eq_int,
        module = "nautilus_trader.trading",
        from_py_object,
        rename_all = "SCREAMING_SNAKE_CASE",
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.trading")
)]
pub enum ConvertExecutionStatus {
    /// Refused before any wallet authorization was dispatched.
    Rejected = 1,
    Submitted = 2,
    Unknown = 3,
    /// A provider attempt failed; authorization validity is not established.
    Failed = 4,
    Confirmed = 5,
}

/// The result delivered to the requesting strategy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvertResult {
    pub operation_id: UUID4,
    pub trader_id: TraderId,
    pub strategy_id: StrategyId,
    pub account_id: AccountId,
    pub client_id: ClientId,
    pub status: ConvertExecutionStatus,
    /// Whether the core has applied every position change and published its lifecycle event.
    pub applied: bool,
    pub transaction: Option<ConvertTransaction>,
    pub unapplied_instruments: Vec<InstrumentId>,
    pub reason: Option<String>,
}

impl ConvertResult {
    /// Creates a locally rejected result for a request that was not executed.
    #[must_use]
    pub fn rejected(
        operation_id: UUID4,
        trader_id: TraderId,
        strategy_id: StrategyId,
        account_id: AccountId,
        client_id: ClientId,
        reason: String,
    ) -> Self {
        Self {
            operation_id,
            trader_id,
            strategy_id,
            account_id,
            client_id,
            status: ConvertExecutionStatus::Rejected,
            applied: false,
            transaction: None,
            unapplied_instruments: Vec::new(),
            reason: Some(reason),
        }
    }
}

/// Prefix under which conversion operations are retained in general cache state.
pub const CONVERT_OPERATION_CACHE_PREFIX: &str = "polymarket_convert_operation:";

/// The locally retained request, dispatch, and execution state for a conversion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvertOperationRecord {
    pub operation_id: UUID4,
    pub trader_id: TraderId,
    pub strategy_id: StrategyId,
    pub account_id: AccountId,
    pub client_id: ClientId,
    pub inputs: Vec<ConvertTokenAmount>,
    /// The complete input and potential output scope fixed at admission.
    #[serde(default)]
    pub affected_instruments: Vec<InstrumentId>,
    /// The execution evidence, or `None` while admission or dispatch is pending.
    pub status: Option<ConvertExecutionStatus>,
    /// Whether the admitted request has been authorized for, or handed to, its execution client.
    pub dispatched: bool,
    /// Whether the complete scope formally settled before accounting could be applied.
    #[serde(default)]
    pub historical_gap: bool,
    /// Whether the original wallet authorization is proven unable to execute the conversion.
    #[serde(default)]
    pub authorization_invalid: bool,
    pub transaction: Option<ConvertTransaction>,
    pub applied: bool,
    /// Whether simulated cash for this operation has already been posted.
    #[serde(default)]
    pub simulated_cash_credited: bool,
    pub unapplied_instruments: Vec<InstrumentId>,
    pub reason: Option<String>,
}

impl ConvertOperationRecord {
    /// Creates an admitted record awaiting dispatch from the original request.
    #[must_use]
    pub fn new(command: &SubmitConvert) -> Self {
        Self {
            operation_id: command.operation_id,
            trader_id: command.trader_id,
            strategy_id: command.strategy_id,
            account_id: command.account_id,
            client_id: command.client_id,
            inputs: command.inputs.clone(),
            affected_instruments: Vec::new(),
            status: None,
            dispatched: false,
            historical_gap: false,
            authorization_invalid: false,
            transaction: None,
            applied: false,
            simulated_cash_credited: false,
            unapplied_instruments: Vec::new(),
            reason: None,
        }
    }

    /// Creates a locally rejected record for a request that was never dispatched.
    #[must_use]
    pub fn rejected(command: &SubmitConvert, reason: String) -> Self {
        let mut record = Self::new(command);
        record.status = Some(ConvertExecutionStatus::Rejected);
        record.reason = Some(reason);
        record
    }

    /// Returns the cache key under which this operation is retained.
    #[must_use]
    pub fn cache_key(operation_id: UUID4) -> String {
        format!("{CONVERT_OPERATION_CACHE_PREFIX}{operation_id}")
    }

    /// Returns whether the request identity matches, ignoring input order and `ts_init`.
    ///
    /// The original operation ID is not part of the request comparison because a caller
    /// supplies it to find an existing operation.
    #[must_use]
    pub fn matches_request(&self, command: &SubmitConvert) -> bool {
        self.trader_id == command.trader_id
            && self.strategy_id == command.strategy_id
            && self.account_id == command.account_id
            && self.client_id == command.client_id
            && normalized_inputs(&self.inputs) == normalized_inputs(&command.inputs)
    }

    /// Returns whether the requesting strategy, account, and client own this operation.
    #[must_use]
    pub fn is_owned_by(&self, command: &QueryConvert) -> bool {
        self.trader_id == command.trader_id
            && self.strategy_id == command.strategy_id
            && self.account_id == command.account_id
            && self.client_id == command.client_id
    }

    /// Retains the accounting gap without changing the external execution evidence.
    pub fn mark_historical_gap(&mut self) {
        if self.unapplied_instruments.is_empty() {
            self.unapplied_instruments = self.affected_instruments.clone();
        }
        self.historical_gap = true;
        self.reason = Some(
            "all affected instruments formally expired; accounting retained as a historical gap"
                .to_string(),
        );
    }

    /// Returns the result conveyed to the strategy.
    #[must_use]
    pub fn result(&self) -> ConvertResult {
        ConvertResult {
            operation_id: self.operation_id,
            trader_id: self.trader_id,
            strategy_id: self.strategy_id,
            account_id: self.account_id,
            client_id: self.client_id,
            status: self.status.unwrap_or(ConvertExecutionStatus::Unknown),
            applied: self.applied,
            transaction: self.transaction.clone(),
            unapplied_instruments: self.unapplied_instruments.clone(),
            reason: self.reason.clone(),
        }
    }
}

/// Returns normalized input identities and quantities, independent of caller order.
#[must_use]
pub fn normalized_inputs(inputs: &[ConvertTokenAmount]) -> Vec<(InstrumentId, Decimal)> {
    let mut inputs: Vec<_> = inputs
        .iter()
        .map(|input| (input.instrument_id, input.quantity))
        .collect();
    inputs.sort_unstable_by_key(|(instrument_id, _)| *instrument_id);
    inputs
}

/// Returns whether all input instruments are unique.
#[must_use]
pub fn has_unique_inputs(inputs: &[ConvertTokenAmount]) -> bool {
    let mut instruments = HashSet::with_capacity(inputs.len());
    inputs
        .iter()
        .all(|input| instruments.insert(input.instrument_id))
}

/// Evidence returned by an execution client for an original conversion request.
#[derive(Clone, Debug)]
pub struct ConvertExecutionEvent {
    pub operation_id: UUID4,
    pub account_id: AccountId,
    pub client_id: ClientId,
    pub outcome: ConvertExecutionOutcome,
}

/// A simulated conversion outcome scoped to one execution-client run.
///
/// The weak lease lets a delayed delivery check whether the producing run is still active
/// without retaining that run or its state.
#[derive(Clone, Debug)]
pub struct SimulatedConvertExecutionEvent {
    pub lease: Weak<()>,
    /// Whether the simulation should post its net cash through an account-state event.
    pub apply_cash: bool,
    pub event: ConvertExecutionEvent,
}

impl SimulatedConvertExecutionEvent {
    /// Returns whether the producing run is still active.
    #[must_use]
    pub fn is_current(&self) -> bool {
        self.lease.upgrade().is_some()
    }
}

impl Display for SimulatedConvertExecutionEvent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "SimulatedConvertExecutionEvent(operation_id={}, client_id={}, account_id={})",
            self.event.operation_id, self.event.client_id, self.event.account_id
        )
    }
}

/// Separates client execution evidence from the core's local application result.
#[derive(Clone, Debug)]
pub enum ConvertExecutionOutcome {
    /// The request was refused before any wallet authorization was dispatched.
    Rejected(String),
    Submitted(ConvertTransaction),
    Unknown {
        transaction: Option<ConvertTransaction>,
        reason: String,
    },
    Failed {
        transaction: Option<ConvertTransaction>,
        reason: String,
    },
    /// The original authorization is proven unable to execute and no conversion succeeded.
    AuthorizationInvalid {
        transaction: Option<ConvertTransaction>,
        reason: String,
    },
    Confirmed {
        verified: Box<VerifiedConvert>,
        pending_inputs: Vec<InstrumentId>,
    },
}
