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

use nautilus_common::messages::execution::{
    ConvertExecutionEvent, ConvertExecutionOutcome, ConvertTransaction, QueryConvert,
};
use nautilus_core::UUID4;

use super::PolymarketExecutionClient;
use crate::{
    common::enums::PolymarketSignatureType, positions::convert::query_original_convert,
    signing::eip712::parse_address,
};

impl PolymarketExecutionClient {
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

        let (record, mapping) = {
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
            (record, mapping)
        };

        let config = self.config.clone();
        let settlement = self.settlement.clone();
        let emitter = self.emitter.clone();
        let operation_id = cmd.operation_id;
        let account_id = cmd.account_id;
        let client_id = cmd.client_id;
        let cancellation = self.pending_tasks.cancellation_token();
        let admitted = self.spawn_task("Convert query", async move {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            let mut outcome =
                query_original_convert(&config, &record, &mapping, &cancellation).await;
            if cancellation.is_cancelled() {
                return Ok(());
            }

            if let ConvertExecutionOutcome::Confirmed { pending_inputs, .. } = &mut outcome {
                pending_inputs.extend(record.inputs.iter().filter_map(|input| {
                    settlement
                        .ensure_resolved(Some(input.instrument_id), "Convert accounting")
                        .err()
                        .map(|_| input.instrument_id)
                }));
            }

            if cancellation.is_cancelled() {
                return Ok(());
            }
            emitter.send_convert_event(ConvertExecutionEvent {
                operation_id,
                account_id,
                client_id,
                outcome,
                run_id,
            });
            Ok(())
        });
        anyhow::ensure!(
            admitted,
            "Polymarket Convert query task admission is closed"
        );
        Ok(())
    }
}
