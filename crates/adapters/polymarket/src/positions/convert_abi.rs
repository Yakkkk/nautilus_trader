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

//! Verified ABI helpers for Deposit Wallet Convert transaction verification.

use alloy::{primitives::Bytes, sol, sol_types::SolCall};
use alloy_primitives::{B256, U256};

sol! {
    struct DepositWalletCall {
        address target;
        uint256 value;
        bytes data;
    }

    struct DepositWalletBatch {
        address wallet;
        uint256 nonce;
        uint256 deadline;
        DepositWalletCall[] calls;
    }

    function proxy(DepositWalletBatch[] batches, bytes[] signatures);

    function convertPositions(bytes32 marketId, uint256 indexSet, uint256 amount);

    event Execution(address indexed target, uint256 value, bytes data, bytes result);
    event BatchExecuted(uint256 indexed nonce);
    event PositionsConverted(
        address indexed initiator,
        bytes32 indexed marketId,
        uint256 indexed indexSet,
        uint256 amount,
        uint256 amountOut
    );
    event Transfer(address indexed from, address indexed to, uint256 value);
    event TransferSingle(
        address indexed operator,
        address indexed from,
        address indexed to,
        uint256 id,
        uint256 value
    );
    event TransferBatch(
        address indexed operator,
        address indexed from,
        address indexed to,
        uint256[] ids,
        uint256[] values
    );
    event Wrapped(
        address indexed caller,
        address indexed asset,
        address indexed to,
        uint256 amount
    );
}

/// Encodes the verified NegRisk collateral adapter `convertPositions` call.
#[must_use]
pub(crate) fn encode_convert_positions(market_id: B256, index_set: U256, amount: U256) -> Bytes {
    Bytes::from(
        convertPositionsCall {
            marketId: market_id,
            indexSet: index_set,
            amount,
        }
        .abi_encode(),
    )
}

#[cfg(test)]
mod tests {
    use alloy::sol_types::SolEvent;
    use alloy_primitives::{address, hex};
    use rstest::rstest;
    use serde_json::Value;

    use super::*;
    use crate::signing::eip712::NEG_RISK_CTF_COLLATERAL_ADAPTER;

    const SAMPLE: &str = include_str!("../../test_data/chain_convert_deposit_wallet.json");

    #[rstest]
    fn test_deposit_wallet_factory_abi_matches_captured_selector() {
        assert_eq!(proxyCall::SELECTOR, [0x0a, 0x3c, 0x44, 0x05]);
        assert_eq!(
            Execution::SIGNATURE_HASH.0,
            hex!("fde9c64d8b0b21fe439285c271603188269ad7c561364f724d48e9846120d7b6")
        );
        assert_eq!(
            BatchExecuted::SIGNATURE_HASH.0,
            hex!("e9cc9bbe7de1cbb097a6357ca3182909488efe8d793e25665befc064a02d10a7")
        );
        assert_eq!(
            PositionsConverted::SIGNATURE_HASH.0,
            hex!("1b8b64a5dd5755bb862006962dbcd07c11ab463091508bcaa350192a43022319")
        );
        assert_eq!(
            Wrapped::SIGNATURE_HASH.0,
            hex!("c00a5c84859ae82a7f5e6a2773283fb525335d5b3195f61174aa1ecc7e15dd84")
        );
        assert_eq!(
            TransferBatch::SIGNATURE_HASH.0,
            hex!("4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb")
        );
        assert_eq!(
            Transfer::SIGNATURE_HASH.0,
            hex!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
        );
    }

    #[rstest]
    fn test_decode_deposit_wallet_factory_capture() {
        let sample: Value = serde_json::from_str(SAMPLE).unwrap();
        let words: Vec<String> = serde_json::from_value(sample["input"].clone()).unwrap();
        let input = hex::decode(words.concat()).unwrap();
        let decoded = proxyCall::abi_decode_validate(&input).unwrap();
        assert_eq!(decoded.batches.len(), 1);
        assert_eq!(decoded.signatures.len(), 1);
        let batch = &decoded.batches[0];
        assert_eq!(
            batch.wallet,
            address!("0x1387d145aaf01f6e33b66525dda6e1f51f6955f8")
        );
        assert_eq!(batch.nonce, U256::from(96_470));
        assert_eq!(batch.deadline, U256::from(0x6affb5b5));
        assert_eq!(batch.calls.len(), 1);
        assert_eq!(batch.calls[0].target, NEG_RISK_CTF_COLLATERAL_ADAPTER);
        assert_eq!(batch.calls[0].value, U256::ZERO);
        assert_eq!(batch.calls[0].data.len(), 100);
        assert_eq!(&batch.calls[0].data[..4], convertPositionsCall::SELECTOR);
        let expected = encode_convert_positions(
            B256::from(hex!(
                "318963409a59098f8393d3db2ad7db7208b2975ca7d657bc66e897ed92244300"
            )),
            U256::from(0x3fe),
            U256::from(1_000_000),
        );
        assert_eq!(batch.calls[0].data, expected);
    }
}
