# Keystore walkthrough

Questions an interviewer is likely to ask about this project, with short answers grounded in the code. Each answer
names the file or test to open. This covers day 1 (the memtable, the write-ahead log, recovery, the store); later
days add sections.

**Reading order:** DESIGN.md "Write path" and "Recovery" → `Store.write` → `memtable/SkipList.put` →
`wal/WalFormat` (the table at the top) → `wal/WalReader.read` → `StoreFaultTest` and `StoreLockTest`.

## The big picture

**What is it, in one sentence?**
A key-value store built like LevelDB: writes go to a write-ahead log and a sorted in-memory table; on day 2 full
tables become immutable sorted files (SSTables) that compaction merges, and on day 3 a TCP server and log-shipping
replication go on top.

**Why an LSM tree and not a B-tree?**
An LSM tree turns random writes into sequential ones: a write is one append to the log plus an in-memory insert,
and data reaches disk in large sorted batches. The price is paid on reads (a key may be in several files, which
Bloom filters mitigate) and in compaction's background I/O. B-trees update pages in place, which reads well but
writes randomly. Write-heavy workloads (logs, metrics, messaging) favour the LSM shape.

**Walk me through a put.**
`Store.put` checks the limits and copies the caller's arrays, then `Store.write` runs under the write lock: next
sequence number, `WriteAheadLog.append` (encode, then a loop over `FileChannel.write`), `sync()` under
`EVERY_WRITE`, `MemTable.put`, return. Nothing is visible or acknowledged before it is in the log.

**What does an acknowledged write promise?**
Under `SyncPolicy.EVERY_WRITE`: it survives a power loss, because the log was forced to the device
(`fcntl(F_FULLFSYNC)` on macOS, `fdatasync` on Linux) before `put` returned. Under `SyncPolicy.NEVER`: it survives
the process being killed (the bytes are in the OS page cache), but not an OS crash or power loss.

## The memtable

**Why a skip list?**
It keeps keys sorted (needed for the SSTable flush and for range scans) and has a simple safe read path with one
writer, which is what an LSM memtable needs. A balanced tree needs rotations, which are hard to make safe for
lock-free readers. LevelDB, RocksDB and HBase use skip lists.

**How can readers skip the lock while a writer inserts?** (`SkipList.put`)
The writer fills in the new node completely (key, entry, its own next pointers), then links it in from level 0 up,
each link a volatile write (`AtomicReferenceArray.set`). A reader follows links with volatile reads. A volatile write
happens-before every read that sees it, so a reader that reaches the node through any link sees everything written
before that link. Nodes are never removed, so there's no use-after-free equivalent.

**What if you linked the node before filling in its next pointers?**
A reader could reach the node and find a null next pointer, and conclude the list ends there: a missing key, a short
iteration. That is the mutation `SkipListConcurrencyTest` was checked against: all 3 runs fail within 0.2 s
("iteration saw 98 keys after step 189").

**How do you know the concurrency test isn't just lucky?**
The writer publishes its progress through an `AtomicInteger`; a reader reads it first, then checks every key up to
that point. Reading the counter creates a happens-before edge with every insert before it, so a miss is a real bug,
not a race the test lost. And the test was checked to fail when the publication order is broken.

**What about the list's height, which the writer raises?**
It is raised before the new levels are linked. A reader that sees the new height and a null at the head's top level
treats null as "past the end" and goes down a level, which is correct, just slower.

**Why branching factor 4 and 12 levels?**
Pugh's analysis gives about `(1/p) log_{1/p} n` comparisons per search: `2 log2 n` for both p = 1/2 and p = 1/4.
But p = 1/4 needs 1.33 pointers per node instead of 2. Twelve levels serve about 4^12 = 16 million keys at the
expected cost. Same as LevelDB.

**Why overwrite the entry in place instead of keeping versions?**
There are no snapshots yet, so old versions would only cost memory. The swap is one volatile write of an immutable
`Entry`, so a reader sees the old entry or the new one. If day 4's snapshots need versions, the key becomes
(user key, sequence number), as in LevelDB.

**What's the catch with in-place overwrites?**
A million overwrites of one key leave a tiny memtable and a huge log, so day 2's flush must be triggered by the
log's size, not the memtable's (`StoreTest.overwritesGrowTheLogNotTheMemtable`). A first version counted "bytes
written" in the memtable; a review measured it 22 times too small for 1-byte deletes, since it left out each
record's framing, and it was removed: the log's own size is exact.

## The log and recovery

**What's in a log record?** (`WalFormat`)
`length` (4 bytes), `checksum` (4: CRC32C of the length and the payload), then the payload: `seq` (8), `op` (1),
key length and key, and for a put the value length and value. Big-endian. `WalFormatTest.theLayoutIsFixed` pins the
bytes against a CRC-32C written out bit by bit.

**Why does the checksum cover the length?**
A damaged length makes the reader take the wrong bytes as the payload. Those bytes almost never checksum correctly
anyway, but covering the length costs nothing and closes the gap.

**Why check the fields after the checksum passes?**
A record with a valid checksum and nonsense fields (an unknown op, a key length past the payload) can only come
from a bug, and treating it as damage is safer than guessing. The scan after damage checks the fields *first*,
because they are cheap and reject almost every offset without computing a checksum.

**What can a crash leave at the end of the log?**
Under `EVERY_WRITE`, at most one incomplete record: a write starts only after the previous flush returned. Its bytes
may be partly there, or the file may have grown with its data missing (zeros). Under `NEVER`, a power loss can tear
several records, in any order of pages.

**How does recovery tell a torn tail from damage?** (`WalReader.read`)
It replays good records until the first bad one, then tries to decode an intact record at every byte offset after
it. None: a torn tail, which it truncates. One or more: writes were made after the damage, so it's damage in the
middle; by default the store refuses to open (`WalCorruptionException`), and `RecoveryMode.POINT_IN_TIME` drops the
rest on request.

**Why refuse instead of just truncating, like a crash?**
Truncating at mid-log damage silently loses acknowledged writes. A refusal costs an operator a decision, and
`keystore dump` shows what would be dropped. The default errs on the side of refusing: a torn record whose value
happens to contain an encoded record makes an ordinary crash look like damage
(`WalReaderTest.aValueHoldingAnEncodedRecordMakesATornTailLookLikeDamage`). I chose a false refusal over a false
"torn tail".

**Can damage to the last record be detected?**
No: a flipped bit in the last record looks exactly like a torn write, and it is dropped as one
(`WalReaderTest.aFlipInTheLastRecordIsATornTail`). Telling them apart would need to know how far the log had been
synced, which would cost another write per sync.

**Why check sequence numbers during replay?**
A checksum proves a record is intact, not that it belongs there. Old bytes beyond a truncation, or a bug that
reuses a number, give intact records in the wrong place; a gap or a repeat in the sequence catches them
(`WalReaderTest.aRecordOutOfSequenceIsDamageEvenIfItIsIntact`).

**Why force the truncation before appending?**
If the truncation isn't durable and the machine crashes after new appends, the old bytes can reappear beyond the
new records, and the next recovery would see them.

**How did you test recovery?**
Exhaustively on a small log: cut at every byte offset (the recovered records must be exactly those that ended before
the cut), and every bit of every record flipped (the last record's flips are torn tails; any earlier record's flips
are damage at that record's offset, refused by default). Plus zero-filled and random tails, and out-of-sequence and
repeated records.

## Failures while running

**What happens if a write fails halfway?** (`StoreFaultTest.aWriteThatFailsPartwayStopsWritesUntilRecovery`)
The log may now end in part of a record. Appending after it would turn a torn tail into mid-log damage. So the
store remembers the failure and refuses every later write with `StoreFailedException`; reads go on. Reopening runs
recovery, which drops the partial record.

**And if fsync fails?**
The same, plus a lesson from PostgreSQL's 2018 "fsyncgate": after a failed `fsync`, Linux may have dropped the dirty
pages, so retrying the flush can succeed without the data ever being written. You can't retry; you stop. The
caller's outcome is unknown: the record was written in full and may well be durable, and in
`aFailedFlushLeavesTheOutcomeUnknown` it is there after a reopen.

**What happens if someone interrupts a writing thread?**
`FileChannel` is interruptible: the JDK closes the channel and throws `ClosedByInterruptException`. The channel is
shared, so the store fails as above and stays failed until a reopen
(`StoreFaultTest.anInterruptedWriterClosesTheLogAndStopsWrites`). The fix is to do the log's I/O on its own thread,
which group commit needs anyway.

**How do you stop two processes from opening the same store?**
A `LOCK` file locked with `FileChannel.tryLock`. The subtle part is the same JVM: POSIX record locks belong to the
process, and closing any descriptor for the file releases them all. So a second open in the same JVM that opened
`LOCK`, failed, and closed it would release the first store's lock. The store therefore refuses a second open from a
JVM-wide set of open directories before touching `LOCK`. `StoreLockTest` uses a child JVM: with the set removed,
the child opens a directory that is still open here.

## What's next

**What would you build next?**
Day 2: flush the memtable to SSTables (sorted blocks, a sparse index, a Bloom filter), a manifest, log rotation, and
size-tiered compaction. Then the TCP server and replication (day 3), crash tests with a killed process and JMH
benchmarks (day 4), and group commit if the numbers call for it.
