# Changelog

## v1.0.0 (2026-10-17)

The first release: an LSM-tree key-value store in Java 21 with a write-ahead log, SSTables, size-tiered compaction,
a TCP server and leader-follower replication by shipping the log. The store depends on nothing; the tests use JUnit
and the benchmarks JMH. The design is in [DESIGN.md](DESIGN.md), the measured results in the
[README](README.md#results), and the questions an interviewer is likely to ask in [WALKTHROUGH.md](WALKTHROUGH.md).

**Files.** v1.0.0 writes table format 1 and manifest format 2, and reads manifest format 1 (day 2's) too
(`keystore version` prints them). A log record carries no version.

### The log and the memtable (day 1)

- Keys of 1 to 65,536 bytes in unsigned byte order, values up to 16 MiB, deletes as tombstones; every write gets the
  next sequence number.
- A write-ahead log of CRC32C-checked records (`length | checksum | seq | op | key | value`). A write is appended,
  forced to the device under `SyncPolicy.EVERY_WRITE` (the default; `F_FULLFSYNC` on macOS), applied to the
  memtable, and only then acknowledged. Any failure once a write has started stops writes until a reopen.
- A hand-written skip list for the memtable: one writer, lock-free readers, each node linked bottom up with volatile
  writes.
- Recovery replays the longest good prefix of the newest log with gapless sequence numbers, truncates a torn tail,
  and refuses to open (by default) when intact records follow damage; `RecoveryMode.POINT_IN_TIME` drops them on
  request. The scan that tells the two apart has a budget and fails closed.
- One store per directory: a `LOCK` file with an OS lock, and a JVM-wide set of open directories keyed by file key
  (a macOS firmlink gives one directory two real paths).
- `keystore put | get | delete | dump`; `dump` is read-only and shows what recovery would drop.

### Tables and compaction (day 2)

- SSTables: ~4 KiB data blocks, a Bloom filter (10 bits a key, 7 probes, MurmurHash64A), a sparse index, a footer
  with a format version and a checksum. Every block's trailer is the CRC32C of its offset and its bytes, so a block
  in the wrong place fails like a damaged one.
- When the log reaches 4 MiB, the next write first flushes the memtable to a table; a manifest, rewritten whole and
  renamed into place, names the live tables and the oldest log to replay.
- Reads pin an immutable version (memtable, frozen memtable, tables newest first) and take no lock; a table a merge
  dropped stays open until the last read in it is done.
- Size-tiered compaction of adjacent tables (tiers of 4 of similar size); tombstones are dropped only by a merge that
  reaches the oldest table.
- `keystore flush | compact`.

### The server and replication (day 3)

- A TCP server with a line-based protocol (lengths before binary data, as in memcached's): `PING`, `GET`, `PUT`,
  `DEL`, `INFO`, `SYNC`. A virtual thread a connection; replies in request order, flushed when no further request is
  waiting, so pipelined requests share writes.
- Leader-follower replication: a follower sends `SYNC n` and its last write's stored checksum; the leader streams
  every record from `n` exactly as its log holds it, read from its own log files, then follows new writes. The
  follower checks each record, applies whatever has arrived as one batch with one force, acknowledges it, and after
  any break reconnects from wherever its store got to. `BEHIND`, `AHEAD` and `DIVERGED` stop it.
- `keystore serve | follow | client`; SIGTERM closes everything (exit 143).

### Failure and measurement (day 4)

- Kill tests: a child JVM writing to a store, the real `keystore follow`, and the real `keystore serve` are killed
  with SIGKILL again and again (at random moments, and at a chosen write or force of any file); after each death
  the store holds exactly a prefix of its writes, with every acknowledged write in it.
- Snapshots (`Store.snapshot`, `keystore snapshot DIR TARGET`): hard links to the immutable tables and a manifest,
  built beside the target and renamed into place. A `BEHIND` follower can start again from one.
- The manifest (format 2) keeps the last write's checksum, so a store whose writes are all in tables can still show
  a leader where its history ends.
- A damaged table block fails its own read and nothing else; a merge that reaches one stops writes. `keystore verify`
  reads every block and log record and exits 3 on damage.
- JMH benchmarks: writes by sync policy, reads of present and absent keys by Bloom filter size, snapshots.

### The release (day 5)

- `keystore version`: the build's version and the file formats it writes and reads.
- A manifest or table in a format this build doesn't know (a later version's) is refused by name, before the open
  changes any file: `manifest format 3, which this build doesn't read (it reads 1 to 2): written by a later version
  of keystore, or damaged`. It was refused before too, but as a missing magic or "format version 2, not 1".
- `scripts/release_check.sh`: the tests, the tests again on a clean `git archive` of the commit, the packaged command
  and the replication demo, every measurement in the README again, and the kill tests; `quick` for the first four.
  `Medians` prints the medians and ranges of `scripts/bench.sh`'s runs.
- `scripts/smoke.sh`: the packaged command end to end; CI and the release check run it.
- CI runs on pinned images (`ubuntu-24.04`, `macos-26`), the ones every earlier run used.
- The README's "Run it" examples for `verify`, `snapshot` and `version` are checked against what the commands print
  (`ReadmeTest`), as the first five were.
