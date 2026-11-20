# Polygon Deposit Wallet Convert sample

The [Polygon transaction](https://polygonscan.com/tx/0x53ce0373717a3922e5f07153f59156ebc2748f2b071ad8991c72ad0aab21e806)
was read using `eth_getTransactionByHash`, `eth_getTransactionReceipt`, and `eth_getBlockByNumber`
from `https://polygon-bor-rpc.publicnode.com` on November 20, 2026. The fixture projects the
transaction and receipt to fields used by the verifier, and omits logs not touching the wallet
or carrying its collateral adapter execution evidence. Repeated receipt metadata is restored
by the local RPC fixture. No signature was requested and no transaction was sent.

The Factory input is `proxy((address,uint256,uint256,(address,uint256,bytes)[])[],bytes[])`.
It contains one batch at wallet nonce `96470`, with a single call to the
[verified NegRiskCtfCollateralAdapter](https://sourcify.dev/server/v2/contract/137/0xadA2005600Dec949baf300f4C6120000bDB6eAab?fields=abi,sources).
The wallet's `Execution(address,uint256,bytes,bytes)` and `BatchExecuted(uint256)` evidence
follows that call's asset effects. `convertPositions` consumed one share for each NO in mask
`0x3fe`, returned one share of each complementary YES, and minted 8 pUSD to the wallet.

`getQuestionCount`, `getFeeBips`, `getConditionId`, and `getPositionId` on the delegated
NegRiskAdapter were read with `eth_call` at `finalized` to record all eleven `[condition, YES, NO]`
triples in chain index order and the zero fee.
