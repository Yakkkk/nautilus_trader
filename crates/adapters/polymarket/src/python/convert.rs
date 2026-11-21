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

//! Python registration for explicit Polymarket negative-risk Convert mappings.

use alloy_primitives::{B256, U256};
use nautilus_common::python::cache::PyCache;
use nautilus_core::python::to_pyvalue_err;
use nautilus_model::identifiers::InstrumentId;
use pyo3::prelude::*;

use crate::{
    common::consts::POLYMARKET_VENUE,
    convert::{CtfNegRiskMarketMetadata, build_neg_risk_event},
};

/// Builds and registers one complete CTF negative-risk event mapping.
///
/// `cache` is the live node or strategy cache holding the full event's loaded instruments.
/// `token_ids` contains `(YES, NO)` decimal token IDs in chain condition-index order. This helper
/// performs no discovery or extra chain reads; it uses the supplied event metadata and loaded
/// Gamma definitions.
#[pyfunction]
#[pyo3(name = "register_convert_event")]
#[pyo3(signature = (cache, anchor, market_id, question_count, fee_bps, token_ids))]
#[pyo3_stub_gen::derive::gen_stub_pyfunction(module = "nautilus_trader.adapters.polymarket")]
#[expect(
    clippy::needless_pass_by_value,
    reason = "PyO3 extracts owned Python arguments for the existing binding"
)]
pub fn py_register_convert_event(
    cache: PyCache,
    anchor: InstrumentId,
    market_id: String,
    question_count: u8,
    fee_bps: u16,
    token_ids: Vec<(String, String)>,
) -> PyResult<()> {
    let market_id = market_id
        .parse::<B256>()
        .map_err(|e| to_pyvalue_err(format!("invalid Convert market ID: {e}")))?;
    let token_ids = token_ids
        .iter()
        .map(|(yes, no)| {
            let yes = U256::from_str_radix(yes.trim(), 10)
                .map_err(|e| to_pyvalue_err(format!("invalid YES token ID: {e}")))?;
            let no = U256::from_str_radix(no.trim(), 10)
                .map_err(|e| to_pyvalue_err(format!("invalid NO token ID: {e}")))?;
            Ok([yes, no])
        })
        .collect::<PyResult<Vec<_>>>()?;

    let cache_rc = cache.cache_rc();
    let instruments = {
        let cache = cache_rc.borrow();
        cache
            .instruments(&POLYMARKET_VENUE, None)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>()
    };
    let metadata = CtfNegRiskMarketMetadata {
        market_id,
        question_count,
        fee_bps,
        token_ids: &token_ids,
    };
    let event = build_neg_risk_event(anchor, instruments.iter(), &metadata)
        .map_err(|e| to_pyvalue_err(e.to_string()))?;
    cache_rc
        .borrow_mut()
        .add_convert_event(event)
        .map_err(to_pyvalue_err)
}
