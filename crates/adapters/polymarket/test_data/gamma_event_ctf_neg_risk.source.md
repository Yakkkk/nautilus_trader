# CTF negative-risk conversion sample

The [Gamma event](https://gamma-api.polymarket.com/events/slug/bitcoin-dominance-on-november-20-2026)
was fetched on November 19, 2026. The fixture retains the full six-market array, with only
fields unrelated to conversion removed. It is a CTF (`v1`) event, not an augmented event.

The [current contracts list](https://docs.polymarket.com/resources/contracts.md) identifies the
Polygon `NegRiskCtfCollateralAdapter` at `0xadA2005600Dec949baf300f4C6120000bDB6eAab`.
Its [verified ABI and source](https://sourcify.dev/server/v2/contract/137/0xadA2005600Dec949baf300f4C6120000bDB6eAab?fields=abi,sources)
show that `convertPositions(bytes32,uint256,uint256)` accepts a market ID and a bitset
of NO indices, delegates to `NEG_RISK_ADAPTER`, and wraps returned USDC.e into pUSD.

The [delegated adapter](https://sourcify.dev/server/v2/contract/137/0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296?fields=abi,sources)
encodes a question index in the final byte of the question ID. Zeroing that byte gives the
chain market ID. The adapter derives the CTF condition ID from its own address, the question
ID, and the binary outcome count; its position IDs use CTF index set `1` for YES and `2` for NO.
Every bit set in the conversion mask selects one NO. Every unset bit below the question count
selects one potential YES output. Net output per item is `amount - floor(amount * fee_bps / 10000)`
in base units. Before wrapping, the adapter returns that amount times `(selected_count - 1)`
in collateral.

Read-only `eth_call` at finalized Polygon block `0x5d05e40`
(`0xa32de12505b52c99072e5cce62937820126aac856335791e3f5ad94524165e66`) against
`https://polygon.drpc.org` confirmed:

- `NEG_RISK_ADAPTER()` is `0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296`.
- `COLLATERAL_TOKEN()` is pUSD at `0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB`.
- `getQuestionCount(market_id) = 6` and `getFeeBips(market_id) = 0`.
- For indices 0 through 5, `getConditionId(question_id)` and
  `getPositionId(question_id, true/false)` match the condition and YES/NO IDs in the fixture.

Tests use these recorded values offline. No RPC or credentials are required.
