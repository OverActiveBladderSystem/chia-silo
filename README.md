# Silo

Silo is a Chia 2.7.5 full node and the wallet that syncs from it. Block rules, validation, and farming are the stock Chia rules. This is not a different chain, and it is not an official Chia Network release.

The change is what the full node stores after a block is accepted. A stock node keeps a full record of every coin that was ever created, plus indexes so it can list coins by address. Silo keeps a short row for each coin that is still unspent, one day of spent marks so a normal reorg can be undone, and the blocks and filters that already contain the history.

The wallet in this tree reads those filters and blocks and keeps its own list of coins. A stock wallet pointed only at a Silo node is told there is no address index, and it will not see its coins from this node. A stock wallet pointed at a stock full node is unchanged.

The design is written out in [docs/silo.md](docs/silo.md): what is stored, what the node will and will not answer, how the wallet syncs and spends, and what that means for other wallets and for explorers.

## Try it

Use a new database folder. Do not open a stock coin database with Silo, and do not open a Silo database with a stock node. An older Silo folder is refused. The format is `silo-v4`.

Python versions follow Chia 2.7.5: 3.10 through 3.14. Free-threaded 3.13 is refused. Install from this tree the same way you install Chia, then run the node and the wallet from this tree. Keys stay in the keyring on the machine that runs it. They are not part of this repository.

Farming still asks the node for signage points and challenges. That path is unchanged. The wallet is the part that has to be this one, or another wallet that syncs from block filters instead of an address index.

## License

Apache 2.0, the same license as Chia. This tree started from Chia 2.7.5. Chia Network has not reviewed or released it.
