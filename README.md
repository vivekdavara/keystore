# Keystore

An LSM-tree key-value store in Java 21, built the way LevelDB and RocksDB are: a write-ahead log with CRC32C
checksums, an in-memory skip list, SSTables with a sparse index and Bloom filters, and size-tiered compaction; a TCP
server with a small text protocol; and leader-follower replication by shipping the write-ahead log.

**Status: day 3 of 5.** Day 1 built the write path's foundation: the memtable (a hand-written skip list with
lock-free reads), the write-ahead log, crash recovery, and `put`/`get`/`delete`. Day 2 made it an LSM tree: the
memtable is flushed to immutable tables when the log reaches 4 MiB, a manifest names the live tables, reads go
through the memtable and then the tables newest first, and tables of similar size are merged. Day 3 put it on the
network: a server (a virtual thread a connection, lengths before binary data), and followers that apply the
leader's log records under the leader's sequence numbers, read by the leader from its own log files, and catch up
from their own last sequence number after any break. No external dependencies beyond JUnit.
[DESIGN.md](DESIGN.md) has the design and the plan for days 4–5.

## Why

Storage engines are mostly about what happens when things go wrong: a crash in the middle of a write, a flush that
fails, a bit that flips on disk. This project tries to be exact about those cases, with tests that create each one,
rather than only getting the happy path fast. Every claim below has a test or a command next to it.

## Architecture

```mermaid
flowchart LR
    client[client] -->|"put / delete"| store[Store<br/>one writer at a time]
    store -->|"1. append record<br/>2. force (EVERY_WRITE)"| wal[(newest log<br/>CRC32C records)]
    store -->|"3. apply"| mem[memtable<br/>skip list]
    mem -->|"log at 4 MiB:<br/>freeze, flush"| sst[(tables, newest first<br/>sparse index + Bloom filter)]
    sst -->|"merge a tier of 4<br/>similar sizes"| sst
    manifest[(MANIFEST<br/>live tables, oldest log)] -.- sst
    client -->|"get: pin a version (no lock)"| mem
    mem -.->|"miss"| sst
    tcp[Server<br/>PUT / GET / DEL over TCP] --> store
    wal -->|"SYNC: log records,<br/>read from the log files"| follower[Follower<br/>its own Store, read-only server]
    follower -.->|"ACK seq"| tcp
```

- **Write path.** A write is appended to the log, forced to the device under `SyncPolicy.EVERY_WRITE` (the default),
  applied to the memtable, and only then acknowledged. When the log has reached 4 MiB, the write first flushes: the
  log is forced and a new one started, the memtable is frozen and written to a table, and a new manifest names the
  table. Then any tier of four similar-sized tables is merged into one.
- **Read path.** `get` takes no lock: it pins an immutable version (the memtable, the frozen memtable, the tables)
  and looks in that order, newest first. A table whose key range or Bloom filter rules the key out costs no disk
  read; otherwise its sparse index names the one 4 KiB block to read.
- **Recovery.** Opening a store reads the manifest, checks its tables, and replays only the logs the tables don't
  cover. In the newest log, the longest run of good records is applied and what follows the first bad record is
  classified: damage with nothing intact after it is a torn tail, which is what a crash leaves, and is truncated;
  damage followed by intact records means writes were made after it, and the store refuses to open (by default)
  rather than silently drop them. Then files the manifest doesn't account for are deleted.
- **Server.** A request is a line naming a command and the lengths of the key and value that follow it (`PUT 4 6`,
  then `cityBoston`); the reply is a line (`OK 1`). Each connection is a virtual thread; replies come in request
  order and are flushed when no further request is waiting, so pipelined requests share writes.
- **Replication.** A follower sends `SYNC n` with the sequence number after its store's last; the leader streams
  every record from `n` on, exactly as its log holds them (checksum included), read from its own log files, then
  follows new writes. The follower checks each record's checksum and sequence number and applies whatever has
  arrived as one batch (one force) into its own log; it acknowledges each batch, and after any break reconnects
  and asks from wherever its store got to. The leader keeps 64 MiB of flushed logs for followers; one further
  behind is told `BEHIND` and stops (it needs a copy of the tables: day 4).

The details, and why each choice was made, are in [DESIGN.md](DESIGN.md).

## Run it

Java 21 and Maven 3.9 or later. On this machine Maven's default JDK is newer, so point it at Java 21:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
```

Build and run the tests (the CI on Ubuntu and macOS runs the same command):

```bash
mvn verify
```

The `keystore` command works on a store directory (after `mvn package`):

```bash
scripts/keystore put target/demo city Boston
```

```bash
scripts/keystore flush target/demo
```

```bash
scripts/keystore get target/demo city
```

```bash
scripts/keystore delete target/demo city
```

```bash
scripts/keystore dump target/demo
```

`get` prints `Boston`, read from the table the flush wrote. `dump` reads the manifest and the logs without changing
anything; after those four commands it prints:

```
target/demo/MANIFEST: tables cover writes up to #1; logs start at 000002.log; next file number 4
  000003.sst: 152 bytes, writes #1 to #1
target/demo/000002.log: 25 bytes
         0  #2 delete city
clean: 25 bytes, every one a good record
```

For a log the store would refuse to open, it shows where the damage is and lists the intact records after it, the
ones `RecoveryMode.POINT_IN_TIME` would drop, and exits with status 3.

From Java:

```java
try (Store store = Store.open(Path.of("target/demo"))) {
    store.put(Bytes.utf8("k"), Bytes.utf8("v"));
    Optional<byte[]> value = store.get(Bytes.utf8("k"));
    store.delete(Bytes.utf8("k"));
}
```

**A leader and a follower.** Each runs until Ctrl-C (or SIGTERM), then closes its store. In one terminal:

```bash
scripts/keystore serve target/leader 8104
```

In another:

```bash
scripts/keystore follow target/follower 8105 127.0.0.1:8104
```

Then write to the leader and read from the follower:

```bash
scripts/keystore client 127.0.0.1:8104 put city Boston
```

```bash
scripts/keystore client 127.0.0.1:8105 get city
```

```bash
scripts/keystore client 127.0.0.1:8105 info
```

`put` prints the write's sequence number, `1`; `get` prints `Boston`; and the follower's `info` prints
`role=follower seq=1 connections=1 state=STREAMING leader=127.0.0.1:8104 leaderSeq=1 lag=0 connects=1`. A `put` to
the follower gets `READONLY`. `scripts/replication-demo.sh` does all of this with both servers in the background, and
CI runs it on Linux and macOS.

## Tests

`mvn verify` runs them all; the tests that make each failure happen are the interesting ones:

| What | Test |
|---|---|
| The skip list behaves like `TreeMap` under random puts and gets, and its structure holds at every level | `SkipListTest` |
| Readers running alongside the writer see every insert the writer had finished (a happens-before edge through a counter makes a miss a real bug) | `SkipListConcurrencyTest` |
| The record format, byte for byte, against a CRC-32C written out bit by bit; every single-bit flip is caught | `WalFormatTest` |
| A log cut at every byte recovers exactly the whole records before the cut; a flip in any bit of any record is classified correctly | `WalReaderTest` |
| Truncation is on disk and forced; a refused open leaves the file untouched | `WriteAheadLogTest` |
| The store against a `HashMap` model across reopens, concurrent writers, a torn tail, damage refused then dropped | `StoreTest` |
| A child JVM can't open a directory this JVM holds, even after a refused second open here (the POSIX lock trap) | `StoreLockTest` |
| A child JVM can't open the directory through a second real path (a macOS firmlink), a rename, or a second class loader | `StoreLockTest` |
| A write that fails partway, a failed flush, and an interrupted writer each stop writes until a reopen; close forces what it must; a write past the log's limit is refused | `StoreFaultTest` |
| `EVERY_WRITE` forces every write before it returns, `NEVER` only at close | `SyncPolicyTest` |
| Every new directory's parent, the log's directory on every open, and a truncation are forced; a refused open leaks no file descriptor | `DurabilityTest` |
| 200 crash-and-recover cycles (a write dying at a random byte, zero or garbage tails) leave exactly the acknowledged writes, 3 seeds | `CrashCycleTest` |
| The `keystore` command, including a read-only dump of a damaged log | `MainTest` |
| **Day 2** | |
| Tables: every entry reads back; random tables answer every key, prefix and extension as a `TreeMap` does; every bit flip, every cut and two swapped blocks are caught; a crafted index count is refused | `TableTest` |
| The Bloom filter's false-positive rate is near the formula's; its hash matches the reference C code | `BloomFilterTest` |
| The manifest round-trips, every bit flip and cut is caught, a torn temp file leaves the old one, inconsistent fields are refused | `ManifestTest` |
| Reads across the memtable and tables, newest wins, tombstones hide older values; merges keep the count down; a flush failing at the manifest loses nothing; no manifest or a damaged table refuses the open | `StoreTablesTest` |
| Which adjacent tables a merge takes | `CompactionTest` |
| 360 crash-and-recover cycles with faults anywhere (appends, tables, the manifest, directory forces), 249 crashes, against the acknowledged writes | `TableCrashCycleTest` |
| A flush and a merge force each file they must, in order | `DurabilityTest` |
| A fault at every single write and force of a flush, a merge and a write that flushes, both sync policies, then close and reopen: nothing acknowledged lost, close's answer right (from the review) | `FlushMergeFaultCloseTest` |
| Four readers alongside constant flushes and merges never miss a key or read a stale value; no file is left open or behind | `StoreConcurrencyTest` |
| **Day 3** | |
| Every command parses; 33 lines that aren't requests (spacing, case, signs, leading zeros, lengths one past each limit, bad `SYNC` checksums) are refused before anything is allocated | `ProtocolTest` |
| Any bytes as keys and values; 2,000 pipelined requests answered in order; bad requests get `ERR PROTOCOL` and a closed connection; a follower refuses writes; the connection limit; 8 concurrent clients get distinct, ordered sequence numbers | `ServerTest` |
| `replicate` takes the leader's numbers or nothing; one force for a batch; `awaitSequence` wakes on a write and on close | `ReplicateTest` |
| Retention keeps an unbroken run of the newest covered logs, across flushes and reopens | `LogRetentionTest` |
| A tail reads every record in order across logs, follows a writer through 20,000 writes, flushes and merges, reads on through a deleted log, says `BEHIND` when a log is gone, and never returns a damaged record | `LogTailTest` |
| A follower gets every write live, catches up late, after its own restart, after a broken connection and after the leader's restart; `BEHIND`, `AHEAD` and `DIVERGED` stop it; a silent follower is dropped; every key compared | `ReplicationTest` |
| `keystore serve`, `follow` and `client` end to end | `MainTest`, `scripts/replication-demo.sh` |
| A follower crashed at any write or force (80 cycles, 56 crashes) recovers to exactly its leader's state at its own sequence number, then follows on | `FollowerCrashCycleTest` |
| A scripted leader sends gaps, repeats, damaged records and junk: the follower applies only what passed its checks and asks again from its position | `FollowerStreamTest` |
| The review's three bugs, as tests that failed before the fixes | `review/` |

Most of the mechanisms these tests cover were also checked the other way round: removed in a copy of the file, a
test had to fail, and the file was restored. DESIGN.md, "Testing", lists which.

## Results

These are plain wall-clock timings, not JMH (that's day 4): `scripts/bench.sh` prints one line per run, each run
after a warm-up run in the same JVM, and each figure below is the median of 5 runs. `Bench` is
`java -cp target/classes:target/test-classes io.github.vivekdavara.keystore.bench.Bench`.

### Day 3: the server and replication

Measured on an Apple M3 (macOS 26.6.2, the internal SSD, APFS) with Homebrew OpenJDK 21.0.12, **on power**
(charging, Low Power Mode off), with nothing else running: `scripts/bench.sh 5 3` at `c239820`, 17:34 to 17:41, and
the pipeline lines it now ends with (`Bench pipeline DEPTH 200000 100`, 5 runs each, added after) at `800f5d3`,
17:53 to 17:55. Everything is over loopback TCP on one machine; keys are 16 bytes and values 100; the data is
synthetic.

| What | Median of 5 (range) | Command |
|---|---|---|
| Puts, 1 connection, one request at a time, to a leader under `NEVER` | 35,096/s; p50 26.6 µs, p99 41.6 µs (34,579–35,241/s) | `Bench server 1 100000 100` |
| Gets, 1 connection | 38,633/s; p50 25.5 µs, p99 36.9 µs (38,422–38,798/s) | the same run |
| Puts, 4 connections | 68,199/s; p50 50.4 µs, p99 108.6 µs (66,277–69,374/s) | `Bench server 4 100000 100` |
| Gets, 4 connections | 86,232/s; p50 43.0 µs, p99 92.4 µs (84,141–87,669/s) | the same run |
| Puts, 16 connections | 68,795/s; p50 216.9 µs, p99 373.9 µs (62,333–73,623/s) | `Bench server 16 100000 100` |
| Gets, 16 connections | 107,105/s; p50 142.3 µs, p99 289.3 µs (104,607–108,756/s) | the same run |
| A new follower catching up on 1,000,000 writes (141 MB of log), its store under `NEVER` | 4,715 ms: 212,089 records/s, 30 MB/s (187,229–224,721/s) | `Bench catchup 1000000 100 NEVER` |
| The same, the follower's store under `EVERY_WRITE` (one force a batch) | 6,549 ms: 152,697 records/s, 22 MB/s (150,124–154,149/s) | `Bench catchup 1000000 100 EVERY_WRITE` |
| One write's replication lag (from the leader's `put` returning to the follower having applied it), follower under `NEVER` | p50 28.0 µs, p99 73.3 µs, slowest 1.4 ms (p50 27.8–28.1 µs) | `Bench lag 10000 100 NEVER` |
| The same, follower under `EVERY_WRITE` | p50 2,959 µs, p99 4,201 µs, slowest 11.0 ms (p50 2,895–2,964 µs) | `Bench lag 1000 100 EVERY_WRITE` |
| One connection, pipelined: 16 requests sent before their replies are read | puts 147,534/s (143,700–148,630); gets 214,907/s (213,172–216,361) | `Bench pipeline 16 200000 100` |
| The same, 128 at a time | puts 175,815/s (175,000–177,837); gets 292,803/s (291,323–294,901) | `Bench pipeline 128 200000 100` |
| The same, one at a time (as `server 1`) | puts 35,253/s; gets 40,140/s | `Bench pipeline 1 200000 100` |
| For comparison, one 128-byte append and force in C (`target/fsync_cost target 1000`) | `F_FULLFSYNC` p50 2,994 µs; `fsync` p50 27 µs | `scripts/bench.sh` |

- **One connection's round trip is about 26 µs**, of which the store's own put is about 2 µs (day 2's p50 under
  `NEVER`); the rest is the client, loopback TCP and the server's thread.
- **Puts stop scaling at 4 connections** (68,199/s at 4, 68,795/s at 16): writes are serialised by the store's write
  lock, and with several connections waiting, each write hands the lock to a parked thread, so more connections only
  queue (the p50 goes from 50 µs to 217 µs). It isn't the lock's capacity: one pipelined connection, which never
  contends for it, puts 147,534 a second. Gets, which take no lock, keep scaling, to 107,105/s on 16 connections.
- **Pipelining pays because the server flushes only when no request is waiting**: 16 requests in flight give 4.2
  times the puts and 5.4 times the gets of one at a time, answered in a few writes rather than one each.
- **Catching up batches itself.** Every one of the 977 batches was full (1,024 records): a follower that is behind
  always has more waiting. That is what makes a forcing follower practical: under `EVERY_WRITE` it applied 152,697
  writes a second, where day 1's store forcing each write managed 361. The forces added 1.8 s, 1.9 ms a batch, less
  than a device flush (3.0 ms), probably because the leader goes on sending while the follower waits on its force.
- **Live replication costs about 28 µs a write** when the follower doesn't force, about one loopback round trip;
  when it forces, the lag is one device flush (2,959 µs against `F_FULLFSYNC`'s 2,994 µs).

### Day 2: flushes, merges, and reads from tables

Measured on an Apple M3 (macOS 26.6.2, the internal SSD, APFS) with Homebrew OpenJDK 21.0.12, **on battery** (Low
Power Mode off), with nothing else running: `scripts/bench.sh 5 2` and five runs of `Bench recover 1000000 100`, at
`bcd744b`, 17:52 to 17:55. Keys are 16 bytes and values 100; the data is synthetic.

| What | Median of 5 (range) | Command |
|---|---|---|
| 1,000,000 puts in random key order under `NEVER`, with the default flush (every 4 MiB of log) and merges (tiers of 4) | 253,593 writes/s (223,601 to 256,178); p50 2.0 µs, p99 4.5 µs, p99.9 8.3 µs; slowest 254 ms (249 to 386) | `Bench fill 1000000 100` |
| What those puts did | 33 flushes and 10 merges, 3 tables left; 3.37 bytes of table written per byte of keys and values: 1.15 by flushes, 2.22 by merges (the same in every run) | same |
| Reopening that store | 9 ms | same |
| `get` of a present key, 1,000,000 keys flushed into 4 tables, 10 bits a key | p50 2.0 µs, p99 3.8 µs; 1.020 data blocks read a get | `Bench reads 1000000 100 10` |
| `get` of an absent key (between two present ones), the same store | p50 0.1 µs, p99 2.3 µs; 0.033 data blocks a get | same |
| 200,000 puts under `NEVER` (day 1's measurement, which now includes 6 flushes and a merge) | 336,578 writes/s (211,400 to 339,257); p50 1.7 µs, p99 3.8 µs; slowest 85 ms | `Bench writes NEVER 200000 100` |
| Recovery: open a store whose one log holds 1,000,000 puts (141.0 MB; the flush disabled, as on day 1) | 250 ms (246 to 296): 4.0 million records/s, 564 MB/s | `Bench recover 1000000 100` |

The Bloom filter's size against the cost of a `get` (`Bench reads 1000000 100 BITS`, the same store each time):

| Bits a key | Probes | Absent key: blocks a get | Absent key: p50, p99 | Present key: blocks a get | Present key: p50, p99 | 4 × the formula's false-positive rate |
|---|---|---|---|---|---|---|
| 1 | 1 | 2.526 | 4.7 µs, 8.1 µs | 2.519 | 4.5 µs, 8.0 µs | 2.53 |
| 5 | 3 | 0.365 | 0.1 µs, 4.1 µs | 1.222 | 2.1 µs, 6.3 µs | 0.37 |
| 10 (default) | 7 | 0.033 | 0.1 µs, 2.3 µs | 1.020 | 2.0 µs, 3.8 µs | 0.033 |
| 16 | 11 | 0.002 | 0.1 µs, 0.2 µs | 1.001 | 2.0 µs, 2.8 µs | 0.002 |

What the numbers say:

- **The Bloom filter does what the formula says.** The keys were put in random order, so every table's key range
  covers almost every key, and an absent key's cost is set by the filters alone: four tables times the false-positive
  rate `(1 - e^(-k/b))^k` predicts the blocks read to the third decimal at every size. At 10 bits a key, a miss
  costs a twentieth of a block; at 1 bit (a filter that says "maybe" 63% of the time) it costs two and a half
  blocks and 4.7 µs instead of 0.1 µs.
- **A hit costs one block read**, a positional read of 4 KiB through the page cache (warm here: this is CPU and
  system-call time, not the SSD) plus a checksum and a scan: about 2 µs. The 0.02 extra blocks are false positives in
  the tables checked before the one that has the key.
- **Writes are as fast as on day 1 between flushes** (p50 1.7 to 2.0 µs), but the write that triggers a flush and
  a merge waits for both: 254 ms for the slowest of a million, 85 ms of 200,000. That is the cost of doing them in
  the writer's thread (DESIGN.md, "Limitations"). The 200,000-put throughput fell from day 1's 525,217 writes/s to
  336,578 with the median write unchanged at 1.7 µs, so the difference is in the writes that waited (day 1 was
  measured on power, this on battery).
- **Size-tiered compaction wrote 3.37 bytes of table for each byte of data** for a million random keys, on top of
  the log. With merges of four, 33 flushes and 10 merges means 8 merges of flushed tables and 2 of their outputs, so
  most keys were rewritten twice by merges, which is the 2.22.
- **Reopening a store with tables is cheap**: 9 ms, since only footers, filters and indexes are read and the newest
  log holds at most about 4 MiB.

### Day 1: the log and the memtable

Measured on an Apple M3 (macOS 26.6, the internal SSD, APFS) with Homebrew OpenJDK 21.0.12, on power, with nothing
else running: `scripts/bench.sh 5` at `1ace995`.

| What | Median of 5 | Command |
|---|---|---|
| A 128-byte append, then `fsync`, in C | p50 26 µs, p99 64 µs | `target/fsync_cost target 1000` |
| A 128-byte append, then `fcntl(F_FULLFSYNC)`, in C | p50 2,994 µs, p99 3,591 µs | same |
| `put` under `EVERY_WRITE` (16-byte key, 100-byte value) | 361 writes/s; p50 2,990 µs, p99 3,977 µs | `Bench writes EVERY_WRITE 1000 100` |
| `put` under `NEVER` (no flushes yet on day 1; see above for day 2) | 525,217 writes/s; p50 1.7 µs, p99 4.2 µs | `Bench writes NEVER 200000 100` |
| Recovery: open a store whose log holds 1,000,000 puts (141.0 MB) | 284 ms: 3.5 million records/s, 496 MB/s | `Bench recover 1000000 100` |
| The scan after damage, over a 64 MiB tail of zeros | 60 ms (1,113 MB/s) | `Bench scan 67108864 zeros` |
| The same over a 64 MiB tail of random bytes | 75 ms (895 MB/s) | `Bench scan 67108864 random` |
| Memtable heap per key, structure only (node, next pointers, entry) | 64.2 bytes | `Bench memory 1000000 100` |
| Skip list, 1,000,000 inserts then lookups, one thread | 877,000 inserts/s, 781,000 lookups/s | `Bench skiplist 1000000` |
| `ConcurrentSkipListMap`, the same | 892,000 inserts/s, 870,000 lookups/s | same |

Rechecked at `d63a17a`, after the review's fixes, with the same commands: `NEVER` 504,305 to 525,399 writes/s
(3 runs), `EVERY_WRITE` 355 to 360 writes/s with a median put of 2,990 µs (3 runs), recovery of the 1,000,000 puts
278 ms (1 run). All within the spread above.

What the numbers say:

- **A durable write costs one drive-cache flush.** `EVERY_WRITE`'s median put (2,990 µs) is the C program's
  `F_FULLFSYNC` (2,994 µs), and a plain `fsync` would be about a hundred times cheaper (26 µs) but doesn't flush the
  drive's cache. That `FileChannel.force` issues `F_FULLFSYNC` on macOS is checked in the JDK's own code
  (DESIGN.md, "Appending"). `NEVER` is about 1,450 times faster because it never waits for the device. Group commit,
  sharing one flush among concurrent writers, is the usual way between the two (day 4).
- **Recovery reads at about 500 MB/s**, so replaying a 64 MB log (a plausible memtable) takes about a tenth of a
  second. The range across the 5 runs was 257 to 517 ms.
- **The scan that tells a torn tail from damage costs about a millisecond per megabyte** over zeros or random bytes:
  most offsets fail on the length or the op byte. A value built to look like records at every offset is decided by
  the scan's budget (see "Review of day 1").
- **The hand-written skip list is close to the JDK's** for inserts (2% slower, median) and 10% slower for lookups.
  The run-to-run spread is wide (lookups 653,000 to 843,000 a second), so these are rough.
- **Holding next pointers in a plain array, through a `VarHandle`**, instead of an `AtomicReferenceArray`
  (`3707eca`): 16 bytes less per key (80.2 to 64.2, `Bench memory`), and inserts went from 726,000 to 881,000 a
  second (median of 5 alternating pairs of runs, the old build in a worktree at `ef9cb7e`). Lookups didn't improve:
  744,000 before and 722,000 after, within the noise. My guess that the extra pointer hop slowed lookups was wrong,
  or at least not measurable this way.

## Review of day 1

Two review agents went over the day's code at `b28ac73`, one on the log and recovery, one on the memtable and the
store's concurrency, each in its own git worktree. They reported only what they had reproduced, and I reproduced
each finding again before fixing it. Every fix has a test that fails without it.

- **Two stores could write one log, and an acknowledged write was lost** (both reviews found it). The set of
  directories open in this JVM was keyed by real path, but one directory can have two: a macOS firmlink
  (`/System/Volumes/Data/private/var/...`), a Linux bind mount, or a rename. The second open passed the set, its
  failed `tryLock` closed a channel on `LOCK`, and POSIX released the first store's lock. Now the set is keyed by
  the directory's file key (device and inode), and a channel whose lock attempt overlaps one this JVM holds is kept
  open, never closed (a second class loader, with a set of its own, reaches that path).
- **Recovery could take minutes.** The scan after damage was quadratic over a value built to look like log records
  at every offset: 16.9 s for a 4 MiB torn value (reproduced here), and 275 s for 16 MiB in the review's run. It now has a checksum
  budget; when it runs out, recovery can't rule out writes after the damage, so the default refuses (0.03 s).
- **The store acknowledged writes it could never recover**, past the 2 GiB recovery reads whole. `Options.maxLogBytes`
  is now checked before anything is written.
- **`lastSequence()` named a write before reads could see it**, while it was still being forced.
- **Close under `NEVER` skipped the force after a failed append**, and returned normally.
- **The log's directory wasn't forced if a crash came between creating the file and forcing it**, and only one of
  several new parent directories was forced.
- **The memtable's count of bytes written** left out each record's framing, 22 times too small for 1-byte deletes,
  though the design said the flush would use it. It's gone; the flush will use the log's size.
- **Tests that passed with their mechanism removed:** every force (truncation, directories, close), the channel
  close on a refused open, close's write lock, and an in-place replacement that readers never saw. Each now has a
  test that fails without it.
- **Not fixable by a test:** a plain write where a volatile one belongs in the skip list passed the concurrency test
  100 times in 100; the counter that makes the test's checks exact also hides a missing edge. That property rests on
  the argument in DESIGN.md, and would need a jcstress-style harness to test.

## Review of day 2

Two review agents started on the day's code at `6314e01`, one on the table format and recovery, one on the store's
flushes, merges and concurrency, each in its own git worktree. Both were cut off by an account usage limit after
about six minutes. The first had already written its findings as tests, which I reproduced and fixed:

- **Two data blocks of the same length, swapped, passed every check**, and a lookup in such a block answered "not
  here" for a key the table has: a store read would fall through to an older table and return an overwritten value.
  The block checksum covered the block's bytes but not where they were. It now covers the offset too, so a block
  found in the wrong place (a misdirected write, say) fails its checksum.
- **A crafted table whose checksums were all valid could claim an index of 2^31 blocks**, and the reader allocated
  arrays for them before noticing the index was far too short, dying of an `OutOfMemoryError`. The count is now
  checked against the index's length first, and block lengths against the data region.
- Its oracle test (random tables over keys on both sides of 0x80, every prefix and extension of every key probed)
  and its maximum-length-key test passed, and are now part of `TableTest`.

Found while building: **one interrupted `get` broke a table for every reader.** `FileChannel` is interruptible, so
the interrupt closed the table's channel, and every later read of that table failed with `ClosedChannelException`
until a reopen (a probe showed it). A table now reopens its channel when a read finds it closed while the table is
still live; the interrupted read still fails.

The second agent left nothing, so it was run again after the limit reset (on `4933ba4`, about 26 minutes), on
failure handling during flushes and merges. It injected a fault at every single write and force of a flush, a
merge and a write that flushes, under both sync policies, then closed, reopened and compared with the acknowledged
writes. Nothing acknowledged was lost and the manifest never named a missing table, but it found:

- **`close()` threw `StoreFailedException` under `NEVER` for nothing**: after a flush had forced and closed the
  old log and then failed to start the new one, close took the closed log for one it couldn't force. Fixed; the
  store notes that the rotation closed the log cleanly.
- **An undeletable garbage file failed the reopen**, and leaked the newest log's channel. I had found and fixed the
  same thing independently while the review was running (`00ed282`, at 17:38).
- **Five mechanisms no test noticed when broken**, which its sweep test (`FlushMergeFaultCloseTest`, now in the
  repo) catches: a merge marking its inputs obsolete before the manifest is written (a failed manifest write would
  then delete files the manifest on disk still names), `compactAll` not failing the store, `flush` and `compactAll`
  running after a failure, and a failed flush taken for a failed per-write force (close then skips a force it needs
  under `NEVER`).

## Review of day 3

One review agent (the account's usage limit stopped two on day 2) started on the day's code at `3195c23`, in its own
git worktree, worded as a correctness review of the server, the tail, the follower and `Store.replicate`, for about
20 minutes. It reported three bugs, each with a test that failed; I reproduced each in the repo before fixing it, and
its tests are in `review/`:

- **The follower dropped records it had checked when a stream ended with an error.** It applied its batch only when
  the batch was full, at a heartbeat, at `ERR BEHIND`, or when nothing more had arrived; the leader writes its last
  records and an `ERR` line together, so any other end threw the batch away. A leader whose next record was damaged
  sent records 1 to 5 on every connection and the follower never kept them (0 applied after 4 connects); when a
  leader's store closed after 2,000 writes, the follower had received all of them and applied 1,024. Now whatever
  ends a stream, the records already checked are applied first.
- **A follower with its leader's options failed during catch-up.** `replicate` put a whole batch into the log at
  once; the leader had taken the same writes one at a time, flushing between them. With a flush at 1 KiB and a cap of
  4 KiB, the leader took 300 writes and the follower stopped on a 12 KiB batch with `LogFullException`. Batches now
  go in runs that end where the leader's log would have flushed.
- **`Server.close()` couldn't end an idle `SYNC` stream**: the stream waited for a write, which closing a socket
  doesn't wake, so close waited 5 s and returned with the stream still running. Streams now wait in 50 ms slices.

From its suspicions, I fixed three more: a flood of refused connections held a socket each for up to a second (now
at most 64 at once); a tail mistook a damaged length in a record it skipped for the end of the log and said `BEHIND`
(reproduced; now damage); a tail leaked a channel when reading a log's first record failed. It also listed mechanisms
no test caught when removed; the follower's sequence check, the tail's gap check, and applying the batch before
stopping on `BEHIND` now have tests (`FollowerStreamTest`, `LogTailTest`), and the drain after a protocol error is
in DESIGN.md's "What the tests can't show": on this Mac nothing fails without it.

While it ran I added `FollowerCrashCycleTest` (a follower crashed at any write or force recovers to a prefix of its
leader's history) and `FollowerStreamTest` (a scripted leader that sends gaps, repeats and damage).

## Design decisions

- **A hand-written skip list, one writer, lock-free readers.** Writes are serialised anyway (one log), so the skip
  list only needs to be safe for one writer and many readers. It fills in a node, then links it bottom-up with
  volatile writes, so a reader can never reach a half-built node. This is LevelDB's model.
- **Fail closed on damage in the middle of the log.** Recovery tells a torn tail (normal after a crash) from damage
  followed by intact records (writes would be lost). The default refuses the second; `POINT_IN_TIME` drops it on
  request; `keystore dump` shows what would go.
- **Sequence numbers are checked on replay.** A checksum can't catch an intact record in the wrong place; a gap or a
  repeat in the sequence can.
- **A failed write stops all writes.** After a failed append or flush the log's state is unknown (and a retried
  flush can falsely succeed: PostgreSQL's "fsyncgate"), so the store refuses further writes until it is reopened.
- **One process per directory, enforced twice.** A `LOCK` file with an OS lock, and a JVM-wide set of open
  directories keyed by file key, because POSIX locks are per process and closing any descriptor releases them.
- **Bounded, fail-closed recovery.** The scan that tells a torn tail from damage has a budget; when it can't decide,
  it refuses rather than guess.
- **The manifest is the only list of live files, rewritten whole and renamed into place.** Anything it doesn't
  name is garbage, deleted once recovery has succeeded; a directory with tables but no manifest is refused rather
  than opened as a new store, which would delete them.
- **Immutable versions with reference counts for reads.** A read pins one snapshot of the memtable, the frozen
  memtable and the tables, so it never sees a key between two places, and a table a merge dropped stays open until
  the last read in it is done. A read costs one atomic increment and one decrement.
- **Size-tiered compaction over adjacent tables only.** Each table covers a contiguous range of sequence numbers, so
  newest-first is a valid read order; merging only adjacent tables keeps it so. Tombstones are dropped only by a
  merge that reaches the oldest table.
- **Block checksums that cover the block's offset**, so a block in the wrong place fails like a damaged one.
- **Flush before the write that finds the log full**, not after, so a failed flush fails a write that was never
  made.
- **Ship the log, as bytes, from the log files.** The leader sends each record exactly as its log holds it, so the
  follower checks the checksum of what was written; and it reads them from its own files rather than a queue in
  memory, so catching up after an hour and following live writes are one code path, and the writer never waits for
  a follower.
- **Ship only acknowledged writes.** A tail never reads past `lastSequence()`, so it never sees a write in progress,
  and under `EVERY_WRITE` a follower never has a write the leader could lose in a crash.
- **Check where the histories join.** `SYNC` carries the stored checksum of the follower's last write, and the
  leader refuses (`DIVERGED`) if its own write there differs: Raft's previous-entry check, with the record's
  checksum standing in for the term.
- **A follower's position is its store's `lastSequence()`.** No separate position file: after a crash it asks for
  whatever comes after what its store recovered.
- **Group commit on the follower.** Whatever has arrived is applied in one batch, with one force, so a follower that
  forces keeps up with a leader that doesn't.
- **Virtual threads, never interrupted.** One per connection keeps the server's code straight-line; an interrupt
  would close the store's log channel, so shutdown closes sockets instead.

## Limitations

- Flushes and merges run in the writing thread, under the write lock: the write that triggers one waits for it
  (reads don't). Background compaction is day 4's, with group commit.
- Writes are serialised, and under `EVERY_WRITE` each waits for its own device flush; there is no group commit yet.
- A merge takes a whole tier, so it needs as much free space as the tier; tombstones live until a merge reaches the
  oldest table.
- No block cache, no compression, no range scans or snapshots yet.
- Damage that reaches the end of the log is indistinguishable from a torn write, so it is dropped as one, however
  many records it covers.
- A value that holds an encoded log record can make recovery refuse to open after an ordinary crash; the default
  errs on the side of refusing (a test shows the case).
- Interrupting a writing thread closes the log's channel and stops the store's writes until a reopen. (An
  interrupted reader only fails its own read.)
- Replication is asynchronous, with no failover: a write the leader acknowledged may be on no follower yet, and a
  follower's reads can be stale by its lag (`info` shows it).
- A follower that falls further behind than the leader's kept logs stops as `BEHIND` until day 4's snapshots.
- A leader under `NEVER` that loses acknowledged writes in a crash and then takes new ones is caught by `SYNC`'s
  checksum of the follower's last write (`DIVERGED`), but only when the follower still knows that checksum (not
  after a restart with every write in its tables) and the leader's log still holds that write.
- No authentication or TLS; the server listens on the loopback address.

DESIGN.md's "Limitations" has the full list.

## License

MIT. See [LICENSE](LICENSE).
