# Silo

Silo is a Chia 2.7.5 full node and the wallet that knows how to sync from it. The chain, the block rules, and farming are the stock rules. What changed is the full-node database, and every wallet or service that used to ask that database for the history of every coin.

## The idea

A stock 2.7.5 node stores a full row for every coin that was ever created, spent or not. The row holds the coin id, the parent, the puzzle hash, the amount, the birth height, the spent height, and whether it was a reward. Indexes sit on top of that so the node can answer “coins at this address,” “children of this coin,” and “what this old block added” without opening the block. That table grows for the life of the chain, including coins that can never be spent again.

Silo stores the coins that are still live, one day of undo, and the blocks and filters that already contain the history. A question that used to be an index lookup is answered by reading a block. A question that would be a lie if the index is gone is refused, instead of being answered with an empty list.

The format marker is `silo-v4`. Schema version 4. An older Silo folder is refused. It has to be a new database folder. The node uses RocksDB. SQLite coin-store code remains in the tree for tests and is not the database a Silo node runs.

## What the node stores

| Thing | What is kept | How long |
| --- | --- | --- |
| Unspent coin | The 32-byte coin id, and a 4-byte creation height. Not the parent, the puzzle, or the amount. | Until the coin is spent. It does not age out. |
| Spent coin | An 8-byte mark: creation height and spent height. | 4,608 blocks, then the row is deleted. |
| One-day undo lists | The ids of coins spent at each height, so a reorg can unspend them. A fast-forward side key for a recent singleton. | The same 4,608 blocks. |
| Fast-forward row | 64 bytes: the parent id and that parent’s parent. Only for a singleton created in the last day that still has the same puzzle and the same amount as its parent. | Until it is spent, or until the day rolls off. An empty value is never written. |
| Block | The full block. | Forever. A reorg clears the “on the main chain” flag. It does not delete the block. |
| Filters | The committed filter, and a hint filter built beside it. Page size 32. | Forever. Reward puzzle hashes are in the committed filter. |
| Block time | The exact timestamp of each transaction block, in seconds. | Forever. Time locks compare this number. It is not rounded. |
| Chain metadata | Peak, undo floor, unspent count, sub-epoch summaries, which blocks are on the main chain, which blocks still have a compact-proof form. | With the chain. |

4,608 blocks is one day at the 18.75 second target. That window is part of the design. It is not shortened on a small machine and not lengthened on a large one.

These are not stored, and an older database drops them on open: an address index, a parent index, a hint index, a height index of every coin, and a second forever-copy of spent coins. A birth journal of every coin is not written on the normal save. The parent, the puzzle, and the amount stay in the block and in the spend that uses the coin.

A reward coin is an ordinary unspent row. It is marked as a coinbase. It is never a fast-forward row. Its puzzle hash is in the filter for that block.

Two coin ids in one block’s spend list are rejected. The node does not silently keep one of them.

## How a block is saved

Every transaction block goes through the same save. A block during a long sync, a batch sync, and a block from a peer are not three different coin paths. If the coin ids in that block cannot already exist, the save does not probe the whole coin table once per coin. If they might already exist, it checks.

The coin rows are written in the same save as that block’s filters and its timestamp. On a reorg, the coins are rolled back before the block flags are cleared, and the block files stay.

While a long sync is running, file packing is paused, and it stays paused until that sync is finished. Cache and memtable sizes follow the machine’s memory and its CPU count. They are not pinned to one computer’s cores.

A backup refuses to copy a database whose migration was only half finished. Opening a folder that is still marked as an older full-coin format is refused rather than treated as a Silo database.

## How a spend is checked

The mempool and block validation read the short row and the spend. They do not open the coin’s birth block on the normal path.

A 4-byte row means the coin is unspent. The spend itself carries the parent, the puzzle, and the amount. Those three must hash to the coin id, or the spend is skipped. An 8-byte mark means the coin is already spent. A second spend of it is a double spend, unless the spend is a fast-forward of a singleton onto the current coin. No row at all is not a success and not an unspent coin. It is rejected as unknown.

The time allowed for that check stays at the stock 2 seconds.

A one-block step forward keeps a mempool spend whose coin was not spent in that block, and drops a spend whose coin was. The fast path of a new peak only updates which height the mempool is standing on. It does not rewrite the stored bundle. The slow path, a reorg or a peak whose previous transaction block does not match, can move a fast-forward bundle onto the current singleton. If that move fails, the original bundle is kept.

Replacing a fast-forward spend is allowed when the new bundle spends the current singleton and still covers the fee coins. Dropping the fast-forward flag is rejected. If the fast-forward row has already aged out and the spend still carries the flag, a replacement that spends the current coin is still allowed.

## Fast-forward

Some singletons are spent into a child with the same puzzle and the same amount. For one day after that child is created, Silo keeps a 64-byte lineage so several already-signed spends can be lined up without opening the birth block. After that day the row is removed even if the coin is still unspent. A spend with a real lineage proof still works. A fake lineage fails. A reorg restores the fast-forward row only when the lineage is exactly 64 bytes and the coin was created inside the undo window. If the birth list has already aged out, the reorg restores only the 4-byte unspent row.

## Reorgs

Inside the last 4,608 blocks, the node unspends coins the abandoned fork spent and deletes coins that fork created. It uses the one-day spent-id lists and the blocks it still has. The block blobs stay.

Older than that, those lists are gone. The node reads the stored blocks and rebuilds. It does not invent a coin body from a deleted row.

A compact proof replaces the VDF witness inside a block under the same header hash. It does not change the coin table and it does not remove the generator. A block that points at an earlier generator reads that generator from the stored block. A missing reference fails validation. It is not treated as an empty generator.

Sub-epoch summaries that sit above the current peak are dropped when the node loads. A summary sitting exactly at the peak is dropped if that block does not actually contain one. After a reorg deletes summaries, the summary file is rewritten on the next flush.

## What the node will answer

| Question | Answer |
| --- | --- |
| Is this coin id unspent? | Yes, at any age. The 4-byte row is the answer. The puzzle and the amount are read from the block that created it when someone needs them. |
| Give me this block, header, or filter | Yes, at any height. These are never refused because a coin row aged out. |
| What did this block add and remove? | Inside the last 4,608 blocks, the real heights. Older than that, refused with `COIN_HISTORY_NOT_KEPT`. The block itself is still served. |
| All coins for this address | Refused. The text is `Silo node has no address index. Sync from block filters.` An empty list is not used, because an empty list looks like an empty wallet. |
| Children of this coin | There is no parent index. While the spend is still in the one-day window, the children can be read from that block. After the mark is gone, the node returns an error. An empty list means it looked and there were no children. An unspent coin has no children, and an empty list there is correct. |
| A coin id whose spent mark was deleted | Not found. That is not “unspent,” and it does not say whether the coin was spent or never existed. |
| Hints as their own table | Not stored. The hint filter and the block are the record. |

## How the Silo wallet syncs

The wallet’s database is separate from the full-node coin table. The node will not list this wallet’s coins by address later, so the wallet keeps its own copy: the coin, when it was created, when it was spent, and the lineage of anything that is not a plain coin.

On connect it still asks for the old address subscription. This node answers with the refusal sentence above. The Silo wallet treats that sentence as the instruction to read filters. It does not keep subscribing to addresses. A stock wallet does not contain that walk, which is why a stock wallet pointed only at Silo does not see its coins.

The walk reads both filters, 32 blocks at a time. A match on a puzzle hash, a spent coin id, or a hint is checked by opening the block. Rewards are included. A hinted coin is kept only when the block really pays it to this wallet. A stranger’s coin that is created and spent in the same block is not kept, and neither is a child of that stranger. A coin of ours that is created and spent in the same block is followed, including the child, so a payment that moves again before the block ends is not missed. A fee-only spend is recorded as a spend with no children, so the wallet does not go ask for children that were never created.

A coin just stored is watched for the rest of that same walk. That matters for a pool singleton: the first pool coin is saved while the launcher block is read, and a later join, leave, or claim in the same catch-up is then seen and applied.

If a filter read fails, the saved height does not move. The peer that failed the filter read is dropped, so the wallet does not pretend it finished.

If the wallet is off and comes back later, the first peak is a fresh sync. It steps back 256 blocks from the saved height and reads filters to the tip. Filters and blocks are still there after the one-day spent marks are gone. If it is stopped in the middle, the saved height stays where the walk stopped.

A new address is walked from its birthday. A request to watch one coin by id reads the unspent row from the node first, because that row never ages out, and then reads the filters so a spend whose mark is already gone is still applied. An empty lookup does not invent an unspent coin.

## How the wallet spends what it holds

A plain coin is signed from the wallet’s own key. The node checks the 4-byte row and the fields inside the spend.

A CAT, a revocable CAT, a credential, a DID, an NFT, or a pool singleton needs the lineage saved when the coin arrived: the parent, that parent’s inner puzzle, and that parent’s amount. If the parent’s spent row is already gone, the wallet reads the creation block. It does not stop with “parent not found” on a spend it should be able to build, and it does not treat a missing parent as an unspent parent.

Recovering a DID uses the parent coin for that lineage, not the current coin. Using the current coin names the parent one step too short, and the chain rejects the next spend. If the wallet already has that DID, the search keeps the lineage it already saved instead of wiping it. Importing a DID whose launcher lookup fails reads the launcher spend from the block when that height is known, or when the node still has the launcher’s spent row. The first DID coin’s lineage is the launcher coin. If that first coin was also spent and its row is gone, and the launcher height was not saved, the import still stops. The launcher block does not contain the later coin.

A clawback the wallet already holds is claimed from the coin and the metadata saved with it. It does not need the parent row. Sending a notification spends a coin the wallet holds now. Neither one depends on a deleted parent row.

A pool or plot-NFT reward is claimed from the launcher and the height stored with the reward. The claim does not look up the reward coin’s parent in the coin table.

An offer file carries the puzzle reveal for every coin it spends. Creating an offer does not need the parent row. Taking an offer checks that the other coin is still unspent. An unspent coin of any age passes. A coin whose spent mark is gone is not found, and that is a rejection, not an acceptance. Coins reserved by an open offer stay locked for ordinary XCH and CAT sends, and also for an NFT transfer, an NFT metadata or owner change, and a data-layer root update. The lock is written after the offer spend is built, so creating the offer itself is not blocked by its own lock.

## Restarts and reorgs on the wallet

The wallet rolls its own coin table back with the chain: coins created above the fork are deleted, and coins spent above the fork are marked unspent again. It then reads the filters for the new chain.

A peer that never shares a header with this wallet does not get to roll it back to height zero. The wallet keeps its coins and does not follow that chain. A block that extends the saved peak is still accepted after a restart, even if that peak is not in the in-memory header map. A weight proof is what fills that map. An equal or lighter peak is ignored unless it connects to a header the wallet already has, including the saved peak.

A notification from a dropped block is deleted, including the id that would have blocked the same message from being saved again. A retry saved for a coin above the fork is dropped, including a retry that was stored with no fork height. A clawback that was already spent is tied to the spend height, so undoing that spend makes it claimable again. If the reorg also removes the coin, the claim note is not put back. A note written only because the wallet first saw a coin already spent is not turned into a pending send, so the restored coin is not locked.

While a sync step is still running, the balance number can be the figure from before the block being applied. The coin list reads the wallet table and is current. When the step finishes, the next balance request is current. A send uses the coin table, not that cached figure.

## Other wallets and other software

A stock wallet pointed only at Silo asks for the address subscription, receives the refusal, and does not walk filters. It will not learn its coins from this node. Point that wallet at a stock full node and nothing about Silo applies. The chain is the same. People who are not running Silo do not change their wallet.

A wallet connected to both still gets address answers from the stock node. Silo can serve blocks and filters and can say whether one coin id is unspent. It is not a substitute for the address index.

An explorer, a pool dashboard, or any service that builds “coins at this address,” “children of this coin,” or “additions in this old block” by calling the node will be refused. It needs a stock node, or it needs to read the blocks and filters and keep its own copy. A service that only fetches blocks, filters, or one coin id can talk to Silo.

Farming does not use the address index. The farmer asks this node for signage points and block challenges. That path is the stock path.

## Limits

A trade that was marked confirmed above a fork can stay marked confirmed. The trade record is not rolled back with the coins. The coins themselves do roll back. A credential record is also not rewound. An interested-address subscription has no height, so deleting a pool wallet removes the farmer entry and does not by itself remove that address from the subscription list.

The mempool’s fast path does not rewrite a stored fast-forward bundle. That rewrite happens on the slow path. An already-signed spend that names a singleton coin older than a day is not silently moved onto the current coin. It is rejected until it is signed against the current coin, or until it still carries a real lineage the node can check.

A same-height replacement, this block swapped for another block at the same height, is rolled back and re-read when the wallet is following Silo by filters. A wallet that is instead trusting a stock node, one that still has the address index, does not take that rollback from the pushed coin states alone.

A data-layer mirror is skipped if the header read fails at that moment. A later successful read of the same block records it. An unhinted CAT whose parent was also created in the same block is not picked up by the walk that follows a child of a coin the wallet just noticed.

Finding a lost DID or NFT reads the creation block when the parent row is gone. Importing a DID still stops if the first singleton was spent, that spent row is gone, and the launcher height was not saved with the backup.

## What did not change

Block validation, header validation, and the VDF proofs are the stock 2.7.5 checks. A compact proof is still just a smaller witness for the same header. The mempool still has two seconds. The farmer still talks to the node the same way. A stock node and a stock wallet, talking to each other, are unaffected. Silo is a different memory of the same chain, and a wallet that is willing to read that memory from the blocks.
