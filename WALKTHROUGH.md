# Keystore walkthrough

Questions an interviewer is likely to ask about this project, with short answers grounded in the code. Each answer
names the file or test to open. Day 1 covered the memtable, the write-ahead log, recovery and the store; day 2 added
tables, the flush, the manifest, versions and compaction ("Tables and compaction", below); day 3 added the server
and replication ("The server and replication"); day 4 added crash tests with killed processes, snapshots, damage
found while running, and JMH ("Failure and measurement"); day 5 is the v1.0.0 release ("The release", below).

**Reading order:** DESIGN.md "Write path" and "Recovery" → `Store.write` → `memtable/SkipList.put` →
`wal/WalFormat` (the table at the top) → `wal/WalReader.read` → `StoreFaultTest` and `StoreLockTest`. For day 2:
DESIGN.md "Tables", "The flush", "Versions" and "Compaction" → `sstable/TableFormat` (the format at the top) →
`sstable/Table.get` → `Store.flushMemtable`, `Store.get`, `Store.merge` → `Version` → `TableCrashCycleTest` and
`StoreConcurrencyTest`. For day 3: DESIGN.md "The server" and "Replication" → `server/Protocol` (the table at the
top) → `server/Server.serve`, `answer`, `ship`, `checkHistory` → `LogTail.read` and `open` → `server/Follower.run`,
`stream` and `follow` → `Store.replicate`, `appendAndApply` and `awaitSequence` → `ReplicationTest`,
`FollowerStreamTest` and `FollowerCrashCycleTest`. For day 4: DESIGN.md "Snapshots", "Damage found while running"
and "Day 4's tests" → `Workload` → `KillTarget` (`KillSwitch`, `die`) → `KillTest` (`Child`, `check`) →
`FollowerKillTest` → `Store.snapshot` (and `link`, `deleteTree`) → `Manifest` (the checksum, and `decode` of version
1) → `StoreCheck.verify` → `SnapshotTest` and `TableDamageTest` → `jmh/ReadBenchmark`. For day 5: DESIGN.md "The
release" → `About` → `Manifest.decode` and `sstable/TableFormat.decodeFooter` (the format checks) →
`LaterFormatTest` → `scripts/release_check.sh` → `bench/Medians` → `cli/ReadmeTest` → CHANGELOG.md.

## The big picture

**What is it, in one sentence?**
A key-value store built like LevelDB: writes go to a write-ahead log and a sorted in-memory table, which is flushed
to immutable sorted files (SSTables) that compaction merges, with a TCP server and log-shipping replication on
top.

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
tables (medians of 5): a present key p50 2.0 µs, reading 1.02 blocks; an absent key p50 0.1 µs, 0.033 blocks
(README, "Results").

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
table doesn't have would still cost a block read in every table whose range covers it. Measured over four tables, a
miss reads 2.526 blocks at 1 bit a key, 0.365 at 5, 0.033 at 10 and 0.002 at 16, which is four times the formula's
rate each time.

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
`bench fill` waited 254 ms (median of 5 runs) for a flush and a merge. LevelDB and RocksDB do this work on background threads and only slow writers
down when it falls behind; that's day 4's, with group commit.

## The server and replication (day 3)

**What does the protocol look like, and why lengths instead of delimiters?**
One line of ASCII naming the command and the lengths, then the bytes: `PUT 4 6\n` then `cityBoston`, answered with
`OK 1` (the sequence number). It's memcached's text protocol shape. With lengths up front, keys and values can hold
any bytes (newlines included) without escaping, and the server knows how much to read before reading it, so it can
refuse a bad length from the line alone, before allocating (`Protocol.parse`, `ProtocolTest.refusesWhatIsntARequest`).
It's also easy to type at with `nc` while debugging, which a binary protocol isn't.

**How does the server handle many clients?**
One virtual thread a connection (Java 21), running a plain read-answer loop (`Server.serve`), so the code is
straight-line blocking I/O and the JVM runs them all on a few carrier threads. A connection's
replies come in its requests' order, and a reply is flushed only when nothing else is buffered, so a client that
pipelines gets its replies batched (`ServerTest.pipelinedRequestsAreAnsweredInOrder`: 2,000 pairs sent before reading).

**Why can't the server interrupt its handler threads on shutdown?**
Because an interrupt during a `FileChannel` operation closes the channel. If a handler were in `store.put` when
interrupted, the log's channel would close, and the store would refuse every write until a reopen. Day 2 hit the
same thing for table reads. So `Server.close()` closes the sockets: a blocked read fails, a request in progress
finishes its store call and then fails to send its reply.

**What happens with a malformed request?**
`ERR PROTOCOL` and the connection closes, because after a framing error the server doesn't know where the next
request starts. The subtle part is closing: if the client has sent more bytes the server hasn't read, a plain
`close()` sends a TCP reset, and a reset can make the client's read fail before it sees the error. So the server
half-closes and drains for up to a second (`Server.drain`).

**How does replication work, in one breath?**
A follower connects and sends `SYNC n`, where n is one past its store's last sequence number. The leader opens a
`LogTail` at n over its own log files and streams every record from there, exactly as the log holds it, then waits
for new writes and streams those, with a heartbeat when idle. The follower checks each record's checksum and
sequence number, applies batches with `Store.replicate` (the leader's sequence numbers, into its own log), and
acknowledges each batch. Any break: reconnect and send `SYNC` again from wherever its store got to.

**Why read the log files instead of keeping a queue of recent writes in memory?**
Because then a follower that's an hour behind, one that just restarted, and one that's keeping up all take the same
path: read the files from their position. With a queue (Redis's replication backlog is one) the leader holds memory
for each slow follower or drops it, a leader restart loses the queue, and catch-up needs a second code path. Here
the writer never waits for or buffers for a follower; the cost is two positional reads a record through the page
cache (`LogTail.read`).

**How does the tail know a record is safe to read while the store is writing the same file?**
It only reads records with sequence numbers up to `store.lastSequence()`, and the store sets that after the append
has returned. So the record's bytes are already in the file; a write in progress is always past what the tail will
read. And under `EVERY_WRITE` those records are on the device, so a follower never gets a write the leader could
lose in a crash (`LogTail` class comment, `Store.appendAndApply`).

**How does it find the record in the files?**
Logs are in sequence order and each starts where the last ended, so the record is in the last log whose first
record is at or before it (`LogTail.open`). It skips earlier records by their headers alone, and at the clean end of
a log it lists the directory again: the record must be in a newer log, because only the newest log takes writes and
the record is acknowledged. If the store deleted the log mid-read, the tail's open channel still reads it
(`LogTailTest.aTailKeepsReadingALogTheStoreDeleted`).

**What happens when a follower is too far behind?**
Flushed logs are deleted unless `Options.retainLogBytes` keeps them (the newest ones, oldest deleted first, so
what's left is unbroken; `keystore serve` keeps 64 MiB, like Postgres's `wal_keep_size`). If the record the follower
needs is gone, the tail throws `BehindException`, the leader sends `ERR BEHIND oldest`, and the follower stops in
state `BEHIND`: it needs a copy of the leader's tables, which day 4's snapshots will provide
(`ReplicationTest.aFollowerWhoseNextRecordIsGoneStopsAsBehind`).

**Where does a follower keep its position, and what if it crashes?**
Its position is its own store's `lastSequence()`. Nothing else is stored. After a crash the store recovers like any
store, and the follower asks for whatever comes after what it recovered, so the position can never disagree with the
data (`ReplicationTest.aRestartedFollowerCarriesOnFromItsLastSequenceNumber`).

**A follower that forces every write would do about 350 a second here. How does it keep up with a leader doing
hundreds of thousands?**
It applies whatever has arrived as one batch (up to 1,024 records or 4 MiB) with one `Store.replicate` call: one
append loop, one force, one publish (`Follower.stream`, `ReplicateTest.aBatchIsForcedOnce`), split into runs only
where the leader's log would have flushed. It's group commit, done on the follower, where the batches form by
themselves. README "Results" has the catch-up rate under both sync policies.

**How does the leader notice a dead follower, and the follower a dead leader?**
Heartbeats and acknowledgements. An idle leader sends `HEARTBEAT seq` every second; a follower that hears nothing
for 10 seconds reconnects. The follower sends `ACK seq` after each batch and heartbeat, and the leader closes a
stream after ten heartbeats of silence (`Server.readAcks`), which also frees the deleted log a tail might be holding
open (`ReplicationTest.aFollowerThatStopsAckingIsDropped`).

**Is this replication synchronous? What can a client lose?**
Asynchronous: the leader acknowledges a write when its own log has it. If the leader's disk dies before a follower
gets the write, the write is gone. Reads on a follower can be stale by its lag (`info` shows `lag=`). There's no
failover. Semi-synchronous replication would wait for one follower's `ACK` before acknowledging; the ACKs already
exist for it.

**Tell me about a bug the review found on day 3.**
The follower dropped records it had already checked. It collects whatever has arrived into a batch and applies the
batch once nothing more is waiting. But the leader writes its last records and its `ERR` line in one go, so when a
stream ended with an error (the leader's next record was damaged, or its store was closing), the follower read the
error before applying, threw the batch away with the connection, and reconnected. With a damaged record 6, it got
records 1 to 5 on every connection and never kept them; when a leader's store closed after 2,000 writes, the
follower had received all 2,000 and kept 1,024. The fix is one rule in `Follower.stream`: whatever ends a stream,
apply the records that passed their checks first (`review/FollowerKeepsVerifiedRecordsOnStreamErrorTest`).

**What if a catch-up batch is bigger than the room left in the follower's log?**
That was the second review bug. The leader took those writes one at a time, flushing whenever its log reached 4 MiB,
so it never came near its log's size limit; the follower put a whole batch in at once and could hit it, and stopped.
Now `Store.replicate` splits a batch into runs that end where the leader's log would have flushed, with a flush
between them, so a batch never needs more room than the leader's writes did (`ReplicateTest`).

**How do you know a crashed follower comes back consistent?**
`FollowerCrashCycleTest` makes a fault land on a random write or force of the follower's store (a batch's append,
a table, the manifest, a directory) and takes the directory as the crash image. Each image must recover to exactly
the leader's state at the image's own sequence number, a prefix of the leader's history and never a mix, with
nothing the follower acknowledged missing, and then follow the leader to the end. Publishing a batch before
appending it fails it at once.

**Can a follower end up with different data than the leader?**
Under `EVERY_WRITE` on the leader, no: a follower only gets acknowledged, forced records, applied in order with the
same numbers. Under `NEVER`, a leader crash can lose acknowledged writes a follower already has. If the follower
reconnects before the leader has written past it, it is ahead and gets `ERR AHEAD`. If the leader has already taken
new writes under those numbers, `SYNC` catches it: the follower sends the stored checksum of its last write (`SYNC
11 checksum`), the leader reads its own write 10 and compares, and a mismatch is `ERR DIVERGED 10`. It's Raft's
check of the previous entry's term, with the record's CRC32C standing in for the term
(`ReplicationTest.aFollowerWhoseLeaderCameBackWithADifferentHistoryStops`: without it, the follower streamed on to 12
with a mixed history). On day 3 a follower that restarted with every write in its tables didn't know its last
checksum and sent a bare `SYNC`; since day 4 the manifest keeps it (`ReplicationTest.aFollowerRestartedWithEvery-
WriteInItsTablesStillChecksItsHistory`). The gap left: a leader whose log no longer holds that write can't compare,
and then nothing is checked.

## Failure and measurement (day 4)

**How do the kill tests know what the store should hold after a SIGKILL?**
The child's writes are `Workload`'s: write N is a pure function of the seed and N (`Workload.write`), so the test
never needs the child to say what it wrote, only how far it got. The child prints `ack N` after write N returns
(`KillTarget.main`), one `write(2)` to a pipe, so a line arrives whole or not at all. After the death the test reopens
the store, takes its last sequence number L, which must be the last ack or the one after (the write in flight can
have reached the log; it can't be further, since the next write starts only after the ack is printed), replays the
workload's first L writes into a model, and compares every key (`KillTest.check`).

**Why kill a real process instead of simulating the crash, as days 1–3 did?**
The in-process crash tests copy the directory at a fault and recover the copy, which is exact about the files but
shares a JVM: the static set of open directories, the kept-open lock channels, the classes. A SIGKILL leaves nothing
of the process: the kernel closes its files and releases its POSIX lock, and the next process starts cold, as it
would in production. What it doesn't do is lose the page cache, so a kill can't show a missing force; those are
counted (`DurabilityTest`, the order of a snapshot's forces), not killed.

**What about killing the leader, with a follower attached?**
`LeaderKillTest` kills the real `keystore serve` 8 times while a client writes to it over TCP and a follower
streams from it, restarting it on the same port each time. Every write the client got an `OK` for is there after
the restart, and the follower, which reconnects with backoff, never finds itself ahead of the restarted leader or on
a different history. That's two day-3 rules composing with recovery: a leader ships only acknowledged writes (a tail
never reads past `lastSequence()`), and an acknowledged write survives a kill. It's an end-to-end check rather than a
sharp one: since a kill keeps the page cache, a leader that shipped writes before forcing them would pass it too
(only a power cut could tell, which nothing here does).

**Random kills land mostly in one place. How did you get deaths everywhere?**
Most of a write's time is its force (3 ms under `EVERY_WRITE` on this Mac), so random deaths mostly hit forces: 17 of
20 recovered the write in flight. So the child can also kill itself: every channel the store opens is wrapped in a
`KillSwitch` sharing one countdown, and the N-th write or force cuts its write at a random byte and sends the process
SIGKILL (`KillTarget.die`). The test draws N until deaths have landed on all seven kinds of step; for a seed the
file operations are deterministic, so it takes 46 and 36 deaths, here and in CI. With the covered logs deleted
before the manifest names the table, it loses 26 and 24 acknowledged writes.

**Tell me about bugs in your own test harness.**
Three today. `Process.destroyForcibly()` sends SIGKILL and also closes the child's streams, so the acks still in the
pipe were lost and the test's idea of "acknowledged" was short; the children are killed with `kill(1)` instead
(`KillTest.Child.kill`). The review found the chosen-step test reading the child's stderr from the first byte, so any
JVM message ("Picked up JAVA_TOOL_OPTIONS: ...") failed it; it now finds the child's line among others. And CI failed
once on day 3's `FollowerCrashCycleTest`: a fault armed for a follower's writes wasn't used up when the follower
caught up in fewer operations (batches form by timing), and it fired in the cycle's `close()`; the countdown is now
disarmed before close.

**How does a snapshot stay exact while writes go on?**
Under the write lock: flush the memtable, pin the current version, read the last sequence number and checksum. After
the flush the pinned tables hold exactly the writes up to that number. Then, outside the lock, the tables are
hard-linked into a new directory, a manifest naming them is written, and the directory is renamed into place
(`Store.snapshot`). Writes go on meanwhile into new memtables and tables the snapshot never sees. The pin matters
because a merge can replace the tables during the links: `SnapshotTest.theTablesASnapshotCopiesSurviveAMerge-
ThatReplacesThem` holds a snapshot at its first copy, runs `compactAll`, and checks the old files are still there;
without the pin they're gone.

**Why are hard links safe here?**
Because a table never changes once written: `TableWriter` creates it with `CREATE_NEW`, writes it once and forces
it, and `Table` only ever opens it for reading. Merges write new tables and delete old ones, and deleting a link
removes only that name. So the store and its snapshot can share an inode for as long as either keeps it. The cost is
that a snapshot keeps a table's space allocated after the store deletes it, and damage to the bytes is in both.

**Tell me about the bug the review found on day 4.**
The first snapshot built itself in the target directory and, on failure, deleted the tables and the directory, but
not `MANIFEST.tmp` or `MANIFEST`. If the manifest's force failed, the target was left holding `MANIFEST.tmp`, and
`Store.open` took that for a new store: it opened, without an error, as an empty store at sequence 0, though the
source held 300 writes (reproduced with the review's test on `0520e30`). A crash mid-snapshot had the same hole when
there were no tables to give it away. Now a snapshot is built in `.TARGET.snapshot-N` beside the target and renamed
into place last, so the target is a whole snapshot or absent, and a failure deletes what was built;
`SnapshotTest.aSnapshotFailingAnywhereLeavesNothing` fails each of its writes and forces in turn.

**What did the second review find?**
Three bugs in code written after the first review. The worst was day 1's firmlink trap in a new place: a snapshot is
refused inside the store's own directory (it could take a name the store writes, like `MANIFEST.tmp`), and the check
compared real paths, but `/System/Volumes/Data/private/...` and `/private/...` are one directory with two real paths
on macOS. A snapshot there became `STORE/MANIFEST.tmp`, and every flush failed from then on. Any "is this the same
directory" check needs the file key (device and inode), as `Store.open` already used; the check now walks up from
the target's nearest existing ancestor comparing file keys, before creating anything. The others: `verify` counted a
good log as damaged after a damaged one, and my fix for the first review made long target names fail, since the
build directory's name is 30 bytes longer.

**What happens when a table block goes bad?**
Nothing until something reads it: open checks a table's footer, filter and index, not its data blocks. Then a read
of that block fails with `CorruptTableException`, never a wrong value and never "not found" (which would let an older
table's overwritten value through); other reads and writes go on. A merge that reaches it fails the store's writes,
as any failed merge does, and leaves the manifest unchanged. `keystore verify` finds it first by reading every block
(`TableDamageTest`). Writing that test found a server bug: the error reply carried the table's full path and hit
the 128-byte line limit before saying what was wrong; it now names the file alone.

**Why doesn't open just check every block?**
It would make opening a store as slow as reading all of it, every time, to catch damage that is rare and that a read
catches anyway (every block has a checksum covering its offset). Open checks what reads depend on to be safe: the
footer, the filter and the index, all in memory afterwards. A full check is a separate, deliberate act
(`StoreCheck`), like `fsck` or a scrub.

**What does the Bloom filter buy, in numbers?**
With 14 tables and keys that no table's range rules out, an absent key costs 8.84 block reads at 1 bit a key and
0.114 at the default 10; JMH measured 69,409 against 1,735,369 lookups a second, 25 times (`ReadBenchmark`). The
block counts are 14 times the formula's false-positive rate, `(1 - e^(-k/b))^k`, at every setting. Present keys gain
4 times, since their own block is always read.

**Why does JMH's write throughput under `NEVER` vary from 102,000 to 288,000 a second?**
The iterations go on in one store, so flushes and merges land in some and not others, and merges grow as the size
tiers do; they run in the writing thread, under the write lock. It's the cost day 2 measured as a 254 ms stall,
seen as throughput. Moving them to a background thread is the fix, not built yet.

**Why put the last write's checksum in the manifest?**
For day 3's divergence check. A follower proves where its history ends by sending its last write's stored checksum
with `SYNC`; a store knew it only while a log held that write, so after a flush (or when started from a snapshot,
which is all tables) it sent nothing and nothing was checked. The manifest is written at every flush, so it can carry
it: format version 2, and a version-1 manifest still reads, with the checksum unknown (`ManifestTest`).

## The release (day 5)

**What does v1.0.0 promise about its files?**
It writes table format 1 and manifest format 2, and reads manifest format 1 (day 2's) as well; `keystore version`
prints them (`About`). A table's format is in its footer: the magic's last byte, and a version field the footer's
checksum covers. The manifest's is its magic's last byte. A store that a later version wrote, with a manifest or a
table in a format this one doesn't know, is refused by name: "manifest format 3, which this build doesn't read (it
reads 1 to 2): written by a later version of keystore, or damaged" (`Manifest.decode`, `TableFormat.decodeFooter`).

**Why does it matter when the refusal comes, and not only that it comes?**
Because opening a store changes it: recovery truncates a torn tail, and then every file the manifest doesn't account
for is deleted as garbage. A build that read a later manifest as its own could delete the later version's files. So
the format is the first thing checked in the manifest's bytes and each table's footer, before recovery touches
anything (by then the open has only opened, or created, the lock file), and `LaterFormatTest` compares every other
file's bytes before and after the refused open.

**How would you change the log's record format?**
A record has no version field and a log has no header, so nothing in a log's bytes says which format wrote it. A
new format would get a new file name (`000004.wal`, say): recovery knows each file's format from its name and
replays old logs with the old decoder until a flush covers them. Replication ships the leader's records exactly as
its log holds them, so followers have to read both formats before any leader writes the new one: upgrade followers
first. (Adding a version byte to each record now would itself be such a change.)

**What does the release check do, and why run the tests again on a clean export?**
`scripts/release_check.sh`: `mvn -B verify`; the same on a `git archive` of the commit in a temporary directory; the
packaged jar's commands (`scripts/smoke.sh`) and the replication demo; then every measurement in the README again
(five runs of `scripts/bench.sh`, summarised by `Medians`, and JMH with day 4's settings) and the kill tests. The
clean export catches what exists only in my working tree: the build filters `src/main/resources`, and the tests read
`README.md` and `pom.xml`, so a file I never added would pass locally and fail for whoever clones it.

**How do you know the README's examples are right?**
`ReadmeTest` runs "Run it"'s commands as written (put, flush, get, delete and dump; verify and snapshot; version)
and compares what they print with what the README shows. I checked it fails the other way too: in a copy of the
README, a wrong byte count in the snapshot line, a wrong sequence range in verify's report, a wrong oldest manifest
format and a wrong value read from the snapshot each failed a test.

**Why Java, for a storage engine?**
The JDK has what this needs: `FileChannel.force` (which issues `F_FULLFSYNC` on macOS; checked in the JDK's own
code), positional reads, CRC32C, `VarHandle` for the skip list's memory ordering, and virtual threads for a thread a
connection. The costs are real: the memtable lives on the heap (64 bytes of structure a key, measured), so garbage
collection pauses are possible; there's no control over the page cache; and an interrupted `FileChannel` closes for
every thread (the store reopens a table's channel, and never interrupts its own threads). Java storage systems in
production keep their bulk data off the heap (Cassandra's off-heap memtables and Bloom filters, HBase's off-heap
block cache) or leave it to the page cache (Kafka's log); this one keeps the memtable on the heap.

**What would you do differently if you started again?**
Group commit and flushes off the writer's thread from the start. They change the write path's shape (a log thread,
a queue, acknowledgements after a shared force), and adding them now means rewriting the part of the store with the
most tests around it. And a version byte in the log record, for the reason above.

**How would this become a distributed database?**
It has one leader, asynchronous followers and no failover. The next step is a consensus log (Raft) in place of plain
log shipping: a write is acknowledged once a majority has it, and a follower can be elected leader. `SYNC`'s
checksum check is already Raft's previous-entry check, with the record's checksum standing in for the term. After
that, the key space split into ranges, each its own replicated store, with something that places ranges on machines
and splits them as they grow: the shape of TiKV (a Raft group per range, RocksDB on each node) and CockroachDB
(the same idea over Pebble, an LSM tree written in Go).

## What's next

**What would you build next?**
Flushes and merges on a background thread, with group commit for `EVERY_WRITE`'s writers (the JMH write numbers show
the cost of not having them). A `SNAPSHOT` command, so a `BEHIND` follower can fetch a running leader's snapshot over
the connection instead of someone making one on the leader's machine. For replication: walking back to the last
matching write after a divergence, and semi-synchronous writes. For the tables themselves: a block cache, prefix
compression within blocks, and leveled compaction to compare against size-tiered.
