# QBFT

Package `qbft` is a PoC implementation of the ["The Istanbul BFT Consensus Algorithm"](https://arxiv.org/pdf/2002.03613.pdf) by Henrique Moniz
as referenced by the [QBFT spec](https://github.com/ConsenSys/qbft-formal-spec-and-verification).

## Features

- Simple API, just a single function: `qbft.Run`.
- Transport abstracted and not provided.
- No dependencies.

## TODO

 - Double check future/past round messages.
 - Double check if rules need to trigger on round change.
 - Add Byzantium tests.