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

//! Polygon reads for Deposit Wallet identity and V2 position operations.

use std::{collections::HashMap, fmt::Debug};

use alloy::{sol, sol_types::SolCall};
use alloy_primitives::{Address, Bytes, U256};
use nautilus_core::string::secret::REDACTED;
use nautilus_network::{
    http::{HttpClient, HttpRedirectPolicy, Method, create_standard_nautilus_headers},
    websocket::proxy::ProxyUrl,
};
use serde_json::json;

use crate::{
    http::error::{Error, Result},
    signing::eip712::{
        CONDITIONAL_TOKENS, DEPOSIT_WALLET_FACTORY, NEG_RISK_CTF_COLLATERAL_ADAPTER,
        POLYMARKET_COLLATERAL_TOKEN, POSITION_MANAGER, ROUTER,
    },
};

const DEFAULT_RPC_URL: &str = "https://polygon.drpc.org";

sol! {
    function id() external view returns (bytes32);
    function owner() external view returns (address);
    function nonce() external view returns (uint256);
    function predictWalletAddress(bytes32 id) external view returns (address);
    function predictLegacyWalletAddress(bytes32 id) external view returns (address);
    function balanceOf(address account) external view returns (uint256);
    function allowance(address owner, address spender) external view returns (uint256);
    function isApprovedForAll(address owner, address operator) external view returns (bool);
    interface PositionLedger {
        function balanceOf(address owner, uint256 id) external view returns (uint256);
    }
    function getPayout(uint256 positionId, uint256 amount) external view returns (uint256);
}

pub(crate) struct WalletVerifier {
    client: HttpClient,
    rpc_url: String,
}

impl WalletVerifier {
    pub(super) fn new(timeout_secs: u64, proxy_url: Option<ProxyUrl>) -> Result<Self> {
        let mut headers: HashMap<String, String> =
            create_standard_nautilus_headers().into_iter().collect();
        headers.insert("Content-Type".into(), "application/json".into());

        let client = HttpClient::builder()
            .headers(headers)
            .redirect_policy(HttpRedirectPolicy::Reject)
            .redact_url(true)
            .timeout_secs(timeout_secs)
            .maybe_proxy_url(proxy_url.map(|url| url.expose().to_string()))
            .build()
            .map_err(Error::from_http_client)?;
        Ok(Self {
            client,
            rpc_url: DEFAULT_RPC_URL.into(),
        })
    }

    pub(super) fn set_rpc_url(&mut self, rpc_url: String) {
        self.rpc_url = rpc_url;
    }

    pub(super) async fn verify(&self, signer: Address, wallet: Address) -> Result<U256> {
        let code = self
            .rpc("eth_getCode", json!([format!("{wallet:#x}"), "latest"]))
            .await?;

        if code.is_empty() {
            return Err(Error::bad_request("Deposit Wallet is not deployed"));
        }

        let identity = self
            .call(wallet, idCall {}.abi_encode())
            .await
            .map_err(|e| match e {
                Error::Decode(message) => Error::decode(format!(
                    "Deposit Wallet identity call failed; check wallet type and RPC: {message}"
                )),
                error => error,
            })?;

        let id = idCall::abi_decode_returns_validate(&identity).map_err(|_| {
            Error::bad_request(
                "Funder lacks Deposit Wallet identity; Safe and Proxy wallets are unsupported",
            )
        })?;

        let predicted = self
            .predict(predictWalletAddressCall { id }.abi_encode())
            .await?;

        if predicted != wallet {
            let legacy = self
                .predict(predictLegacyWalletAddressCall { id }.abi_encode())
                .await?;

            if legacy != wallet {
                return Err(Error::bad_request(
                    "Funder is not a canonical Deposit Wallet for this signer; Safe, Proxy, and EOA wallets are unsupported",
                ));
            }
        }

        let ownership = self.call(wallet, ownerCall {}.abi_encode()).await?;
        let owner = ownerCall::abi_decode_returns_validate(&ownership)
            .map_err(|e| Error::decode(format!("Invalid Deposit Wallet owner: {e}")))?;
        if owner != signer {
            return Err(Error::bad_request("Deposit Wallet signer is not its owner"));
        }

        let nonce = self.call(wallet, nonceCall {}.abi_encode()).await?;
        nonceCall::abi_decode_returns_validate(&nonce)
            .map_err(|e| Error::decode(format!("Invalid Deposit Wallet nonce: {e}")))
    }

    pub(super) async fn verify_v2_split(&self, owner: Address, amount: U256) -> Result<()> {
        let balance = self
            .read(
                POLYMARKET_COLLATERAL_TOKEN,
                balanceOfCall { account: owner },
            )
            .await?;

        if balance < amount {
            return Err(Error::bad_request(
                "Deposit Wallet has insufficient pUSD for split",
            ));
        }

        let allowance = self
            .read(
                POLYMARKET_COLLATERAL_TOKEN,
                allowanceCall {
                    owner,
                    spender: ROUTER,
                },
            )
            .await?;

        if allowance < amount {
            return Err(Error::bad_request(
                "Deposit Wallet pUSD allowance for Router is insufficient",
            ));
        }

        Ok(())
    }

    pub(super) async fn verify_v2_positions(
        &self,
        owner: Address,
        position_ids: &[U256],
        amount: U256,
    ) -> Result<()> {
        let approved = self
            .read(
                POSITION_MANAGER,
                isApprovedForAllCall {
                    owner,
                    operator: ROUTER,
                },
            )
            .await?;

        if !approved {
            return Err(Error::bad_request(
                "Deposit Wallet must approve Router as a PositionManager operator",
            ));
        }

        for &id in position_ids {
            let balance = self
                .read(
                    POSITION_MANAGER,
                    PositionLedger::balanceOfCall { owner, id },
                )
                .await?;

            if balance < amount {
                return Err(Error::bad_request(format!(
                    "Deposit Wallet has insufficient V2 position balance for {id}"
                )));
            }
        }

        Ok(())
    }

    pub(super) async fn verify_ctf_convert(
        &self,
        owner: Address,
        token_ids: &[U256],
        amount: U256,
    ) -> Result<()> {
        let approved = self
            .read(
                CONDITIONAL_TOKENS,
                isApprovedForAllCall {
                    owner,
                    operator: NEG_RISK_CTF_COLLATERAL_ADAPTER,
                },
            )
            .await?;

        if !approved {
            return Err(Error::bad_request(
                "Deposit Wallet must approve the NegRisk collateral adapter as a Conditional Tokens operator",
            ));
        }

        for &id in token_ids {
            let balance = self
                .read(
                    CONDITIONAL_TOKENS,
                    PositionLedger::balanceOfCall { owner, id },
                )
                .await?;

            if balance < amount {
                return Err(Error::bad_request(format!(
                    "Deposit Wallet has insufficient NO balance for CTF token {id}"
                )));
            }
        }

        Ok(())
    }

    pub(super) async fn payout(&self, position_id: U256, amount: U256) -> Result<U256> {
        self.read(
            POSITION_MANAGER,
            getPayoutCall {
                positionId: position_id,
                amount,
            },
        )
        .await
    }

    async fn read<C: SolCall>(&self, target: Address, call: C) -> Result<C::Return> {
        let response = self.call(target, call.abi_encode()).await?;
        C::abi_decode_returns_validate(&response)
            .map_err(|_| Error::decode(format!("Invalid Polygon RPC result for {}", C::SIGNATURE)))
    }

    async fn predict(&self, data: Vec<u8>) -> Result<Address> {
        let response = self.call(DEPOSIT_WALLET_FACTORY, data).await?;
        predictWalletAddressCall::abi_decode_returns_validate(&response)
            .map_err(|e| Error::decode(format!("Invalid Deposit Wallet prediction: {e}")))
    }

    async fn call(&self, target: Address, data: Vec<u8>) -> Result<Bytes> {
        self.rpc("eth_call", json!([
            {"to": format!("{target:#x}"), "data": format!("0x{}", alloy_primitives::hex::encode(data))},
            "latest"
        ])).await
    }

    /// Executes a read-only Polygon JSON-RPC request and returns its raw result.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport fails, the response is malformed, or the RPC reports an
    /// error.
    pub(super) async fn rpc_json(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let body = serde_json::to_vec(
            &json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}),
        )?;
        let response = self
            .client
            .request(
                Method::POST,
                self.rpc_url.clone(),
                None,
                None,
                Some(body),
                None,
                None,
            )
            .await
            .map_err(Error::from_http_client)?;
        if !response.status.is_success() {
            return Err(Error::from_status_code(
                response.status.as_u16(),
                b"Polygon RPC request failed",
            ));
        }
        decode_rpc_response(&response.body, method)
    }

    async fn rpc(&self, method: &str, params: serde_json::Value) -> Result<Bytes> {
        serde_json::from_value(self.rpc_json(method, params).await?)
            .map_err(|_| Error::decode(format!("Invalid Polygon RPC result for {method}")))
    }
}

fn decode_rpc_response(body: &[u8], method: &str) -> Result<serde_json::Value> {
    let response: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| Error::decode("Polygon RPC returned an invalid response"))?;
    let object = response
        .as_object()
        .ok_or_else(|| Error::decode("Polygon RPC response was not an object"))?;
    let Some(id) = object.get("id").and_then(serde_json::Value::as_u64) else {
        return Err(Error::decode("Polygon RPC returned an invalid response"));
    };
    let Some(jsonrpc) = object.get("jsonrpc").and_then(serde_json::Value::as_str) else {
        return Err(Error::decode("Polygon RPC returned an invalid response"));
    };

    if jsonrpc != "2.0" || id != 1 {
        return Err(Error::decode(
            "Polygon RPC did not return a successful matching response",
        ));
    }

    if let Some(error) = object.get("error").filter(|e| !e.is_null()) {
        let message = match error.get("code").and_then(serde_json::Value::as_i64) {
            Some(code) => format!("Polygon RPC {method} failed (code {code})"),
            None => format!("Polygon RPC {method} failed"),
        };

        return Err(Error::decode(message));
    }

    object
        .get("result")
        .cloned()
        .ok_or_else(|| Error::decode("Polygon RPC omitted its result"))
}

impl Debug for WalletVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(WalletVerifier))
            .field("rpc_url", &REDACTED)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::{
        common::credential::{EvmPrivateKey, RelayerApiKey},
        http::relayer::PolymarketRelayerHttpClient,
        signing::eip712::OrderSigner,
    };

    #[rstest]
    fn test_rpc_url_is_redacted_from_debug() {
        let mut verifier = WalletVerifier::new(2, None).unwrap();
        verifier.set_rpc_url("https://rpc.example/dummy-path-secret?key=dummy-query-secret".into());
        assert_eq!(
            format!("{verifier:?}"),
            "WalletVerifier { rpc_url: \"<redacted>\", .. }"
        );
    }

    #[rstest]
    fn test_rpc_response_distinguishes_null_from_omitted_result() {
        let null_result = decode_rpc_response(
            br#"{"jsonrpc":"2.0","id":1,"result":null}"#,
            "eth_getTransactionReceipt",
        )
        .unwrap();
        assert_eq!(null_result, serde_json::Value::Null);

        let missing_result =
            decode_rpc_response(br#"{"jsonrpc":"2.0","id":1}"#, "eth_getTransactionReceipt")
                .unwrap_err();
        assert!(missing_result.to_string().contains("omitted its result"));

        let null_error = decode_rpc_response(
            br#"{"jsonrpc":"2.0","id":1,"result":"0x","error":null}"#,
            "eth_call",
        )
        .unwrap();
        assert_eq!(null_error, serde_json::Value::from("0x"));
    }

    #[tokio::test]
    #[ignore = "requires Polymarket credentials and network access to Polygon and the Relayer"]
    async fn test_live_deposit_wallet_security_reads() {
        let private_key = EvmPrivateKey::new(&std::env::var("POLYMARKET_PK").unwrap()).unwrap();
        let signer = OrderSigner::new(&private_key).unwrap();
        let wallet = std::env::var("POLYMARKET_FUNDER").unwrap().parse().unwrap();
        WalletVerifier::new(20, None)
            .unwrap()
            .verify(signer.address(), wallet)
            .await
            .unwrap();
        PolymarketRelayerHttpClient::new(RelayerApiKey::from_env().unwrap(), None, 20)
            .unwrap()
            .get_wallet_nonce(signer.address())
            .await
            .unwrap();
    }
}
