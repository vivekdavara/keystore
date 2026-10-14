# Keystore walkthrough

Questions an interviewer is likely to ask about this project, with short answers grounded in the code. Each answer
names the file or test to open. Day 1 covered the memtable, the write-ahead log, recovery and the store; day 2 adds
tables, the flush, the manifest, versions and compaction ("Tables and compaction", below).

**Reading order:** DESIGN.md "Write path" and "Recovery" → `Store.write` → `memtable/SkipList.put` →
`wal/WalFormat` (the table at the top) → `wal/WalReader.read` → `StoreFaultTest` and `StoreLockTest`. For day 2:
DESIGN.md "Tables", "The flush", "Versions" and "Compaction" → `sstable/TableFormat` (the format at the top) →
`sstable/Table.get` → `Store.flushMemtable`, `Store.get`, `Store.merge` → `Version` → `TableCrashCycleTest` and
`StoreConcurrencyTest`.

## The big picture

**What is it, in one sentence?**
A key-value store built like LevelDB: writes go to a write-ahead log and a sorted in-memory table, which is flushed
to immutable sorted files (SSTables) that compaction merges; on day 3 a TCP server and log-shipping replication go
on top.

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

**Why link bottom-up, if the node is complete before any link?**
For monotonic visibility. The completeness of the node makes any order safe for a single reader; bottom-up makes
it consistent across reads. Once a search can find the node on any level, it is on level 0, so every later search
and iteration finds it. Linked top-down, a `get` could find a key through an upper level that an iteration started
after it would miss, while the put was still in progress.

**Why a `VarHandle` and not an `AtomicReferenceArray`?** (`3707eca`)
`AtomicReferenceArray` is an object wrapping an array: one more object per node (16 bytes) and one more pointer per
step. A plain array read through `MethodHandles.arrayElementVarHandle` with `getVolatile`/`setVolatile` has the same
memory semantics. Measured (README, "Results"): 16 bytes less per key and inserts 21% faster, but lookups no faster
(744,000 vs 722,000 a second, within the noise). I had expected the lookups to improve; they didn't, and the README
says so.

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

## Bugs the review found (day 1)

**Tell me about a bug a review found.** (`StoreLockTest.aSecondRealPathToTheSameDirectoryIsTheSameStore`)
Two stores could write one log. I had already handled the POSIX trap (closing any descriptor on `LOCK` releases the
process's lock) with a JVM-wide set of open directories, keyed by real path. But one directory can have two real
paths: on macOS, `/private/var/...` is also `/System/Volumes/Data/private/var/...` (a firmlink), and `toRealPath`
keeps them apart; a Linux bind mount or a rename does the same. The second open passed the set, `tryLock` failed
with `OverlappingFileLockException`, and closing that channel released the first store's lock. A child process then
opened the store, and in the review's run both wrote sequence number 2 at the same offset: an acknowledged write was
gone, and recovery said "clean". The fix keys the set by file key (device and inode), and never closes a channel
whose lock attempt overlapped one this JVM holds; a second class loader, which has a set of its own, still reaches
that path, and a test uses one to check it.

**Why does the recovery scan have a budget?** (`WalReaderTest.aTornValueBuiltToLookLikeRecordsIsDecidedQuickly`)
The scan checks cheap fields before checksums, which is linear for zeros or random bytes. But a value can repeat a
26-byte fake header that passes the cheap checks at every repeat while claiming a long payload, and each repeat then
costs a checksum over that payload: quadratic. A 4 MiB value torn by a crash took 16.9 s to recover. Any client can
store such a value. With a budget of checksummed bytes (four times the tail plus 1 MiB), the scan gives up early,
and recovery fails closed (refuses) because it can't rule out writes after the damage: 0.03 s.

**What does `lastSequence()` mean?** (`StoreFaultTest.lastSequenceNamesOnlyWritesReadsCanSee`)
The last write reads can see, and so, under `EVERY_WRITE`, the last durable one. It used to advance right after the
append, while the write was still being forced; anything treating it as "readable up to here" (day 3's follower
position, say) would have been wrong. A gate in the test channel holds a put mid-flush to show it.

**What does `close` promise?**
That every acknowledged write is on the device when it returns. The review found the gap: under `NEVER`, close is
where writes get forced, and a store whose append had failed skipped the force and returned normally. Now it forces
after a failed append, doesn't after a failed flush (each acknowledged write was forced on its own, and a second
flush proves nothing), and throws under `NEVER` if an interrupt has left nothing to force with.

**Which tests passed with their mechanism removed?**
Every force (truncation, directories, close), closing the channel on a refused open, close's write lock, and an
in-place replacement the concurrency test accepted either way. Each has a test now (`DurabilityTest` counts forces
per path and file descriptors through `/dev/fd`). One thing still has no test: a plain write where a volatile one
belongs passes the concurrency test, because its own happens-before counter covers the gap. That needs a
jcstress-style harness; the property rests on the argument in DESIGN.md.

**What does a durable write cost here?**
About 3 ms: `EVERY_WRITE`'s median put is 2,990 µs, and a C program doing 128-byte appends with `F_FULLFSYNC`
measures 2,994 µs, against 26 µs for a plain `fsync`, which doesn't flush the drive's cache. So the store does about
360 durable writes a second, and 525,000 under `NEVER`. Group commit is the standard way between the two.

## Tables and compaction (day 2)

**Walk me through a get now.**
`Store.get` pins the current `Version` (one compare-and-set on its reference count), then looks in the memtable, the
frozen memtable if a flush is running, and each table newest first, stopping at the first value or tombstone. For a
table, `Table.get` first checks the key against the table's smallest and largest keys and its Bloom filter, which
costs no I/O; then it binary-searches the in-memory index for the first block whose last key is at least the key,
reads that one block with a positional read, checks its checksum, and scans it. Measured with a million keys in four
tables: a present key p50 2.2 µs, reading 1.02 blocks; an absent key p50 0.1 µs, 0.033 blocks (README, "Results").

**Why is newest-first the right order?**
Each place holds only writes newer than every place after it: the memtable's are newer than the frozen memtable's,
which are newer than any table's, and the tables cover disjoint ranges of sequence numbers in age order (the
manifest checks it). So the first entry found is the newest. That is also why a merge may only take adjacent tables
(below).

**What's in a table file, and why a sparse index?**
Data blocks of about 4 KiB (entries in key order), then the Bloom filter, then the index (each block's last key,
offset and length), then a fixed 72-byte footer with the offsets, counts and a magic number (`sstable/TableFormat`).
One index entry per block instead of per key keeps the index small enough to hold in memory for every table, under
1% of the data here; the price is scanning a few dozen entries inside the block.

**How does the Bloom filter work, and why 10 bits a key?**
Each key sets 7 bits of a bit array, picked by double hashing one 64-bit MurmurHash64A (`BloomFilter.add`); a lookup
checks the same 7 bits, and any zero means "definitely not here". With b bits a key and k probes the false-positive
rate is about `(1 - e^(-k/b))^k`, minimised at `k = b ln 2`: 10 bits gives 0.82% in theory and 0.90% measured
(`BloomFilterTest`). It costs 1.25 bytes a key of memory, and it is what makes a miss cheap: without it, a key a
table doesn't have would still cost a block read in every table whose range covers it.

**Why does the block checksum include the block's offset?**
A review found that bytes alone weren't enough: two blocks of one length, swapped, both passed their checksums, and
a lookup in the wrong block said "not here", so the read went on to an older table and returned an overwritten
value. Putting the offset into the CRC makes a block in the wrong place fail like a damaged one
(`TableTest.swappedBlocksOfEqualLengthFailTheirChecksums`).

**What happens if the machine crashes during a flush?**
The flush (`Store.flushMemtable`) forces and closes the old log and starts a new one, writes and forces the table,
forces the directory, then renames a new manifest into place; only after that are the old logs deleted. Before the
rename, the old manifest is in force: recovery replays the old log and the new one, and deletes the table nobody
names. After it, the new manifest names the table and the new log, and the old log is garbage. `TableCrashCycleTest`
injects a fault at a random write or force anywhere (249 crashes) and checks every acknowledged write survives;
moving the log deletion before the manifest fails it at the fourth cycle.

**Why rewrite the manifest whole instead of appending to it?**
It is tiny (40 bytes plus 32 per table) and changes only on a flush or a merge, so writing a temp file, forcing it
and renaming it over the old one is cheap, and the rename is atomic: a crash leaves the old manifest or the new one.
LevelDB appends edits to a manifest log, which scales to thousands of files but needs its own recovery rules.

**What if the manifest is lost?**
A directory with logs or tables but no `MANIFEST` refuses to open (`StoreTablesTest`). Opening it as a new store
would have been the easy path, and it would have deleted every table as garbage.

**How do you stop a compaction from closing a file a reader is in?**
Reference counts at two levels (`Version`, `Table`). A version holds a reference on each of its tables; a read pins
a version. When a merge installs a new version, the store drops its reference on the old one, but a read still in it
keeps it, and so its tables, alive; the last release closes the files and deletes those the merge marked obsolete.
`StoreConcurrencyTest` shows it: without the pin, readers hit `AsynchronousCloseException`.

**Why does pinning retry?**
A read reads the current version and then tries to increment its count; if the count is already zero, the version
was released between the two steps, which only happens after a newer version was installed, so reading the field
again gives that one. The count never goes from zero back up, so a released version can't be resurrected.

**Why size-tiered compaction, and how does it pick?**
`Compaction.pick` groups adjacent tables, newest first, into tiers of similar size (all small ones together;
otherwise within half to one and a half times the tier's average, Cassandra's defaults) and merges the newest tier
with four tables. It writes each key about `log4(data / flush size)` times. Leveled compaction rewrites more but reads
fewer files; size-tiered is simpler to build and to reason about, and was the plan. Measured: a million random writes
wrote 3.37 bytes of table per byte of data.

**Why must merged tables be adjacent?**
Reads trust newest-first order, and each table covers a range of sequence numbers after the next one's. Merge two
tables with a third between them and the output has to go somewhere: before the third, its old entries hide the
third's newer ones; after it, the reverse. So a tier is a run of adjacent tables, and the output takes the run's
place (`CompactionTest.aTableOfAnotherSizeBetweenSimilarOnesSplitsTheTier`).

**When can a tombstone be dropped?**
Only when the merge includes the oldest table. A tombstone hides older values of its key; dropped while an older
table still has one, the deleted value would come back. When nothing older exists, the key is just gone, and the
tombstone goes with it (`StoreTablesTest.compactAllKeepsTheNewestValueAndDropsTombstones`).

**What did the day-2 reviews find?**
Four bugs. None lost an acknowledged write in a test, but one could have returned wrong data: two swapped blocks
of equal length passed their checksums and made a lookup answer "not here" (a stale read from an older table),
fixed by putting the offset in the checksum. A crafted table with valid checksums claimed 2^31 index entries
and ran the reader out of memory, fixed by checking the count against the index's length first. `close()` reported
possible data loss under `NEVER` after a flush that had already forced everything, fixed by remembering the clean
rotation. And an undeletable garbage file failed the reopen (I'd found that one too). The second review's sweep test
also caught five mechanisms no test covered; it is `FlushMergeFaultCloseTest`. While building I found one more: an
interrupted `get` closed a table's channel for every reader.

**What happens to writes while a flush or merge runs?**
They wait: both run in the writing thread under the write lock. Reads don't wait. The slowest of a million writes in
`bench fill` waited 316 ms for a merge. LevelDB and RocksDB do this work on background threads and only slow writers
down when it falls behind; that's day 4's, with group commit.

## What's next

**What would you build next?**
The TCP server and replication (day 3); crash tests with a killed process, JMH benchmarks, and moving flushes and
merges to a background thread with group commit (day 4). For the tables themselves: a block cache, prefix
compression within blocks, and leveled compaction to compare against size-tiered.
