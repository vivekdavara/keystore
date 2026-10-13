# Keystore

An LSM-tree key-value store in Java 21, built the way LevelDB and RocksDB are: a write-ahead log with CRC32C
checksums, an in-memory skip list, and (from day 2) SSTables with Bloom filters and size-tiered compaction, then a
TCP server and leader-follower replication by log shipping.

**Status: day 1 of 5.** See [DESIGN.md](DESIGN.md) for the design and the plan.

## Build and test

Java 21 and Maven 3.9 or later:

```bash
mvn verify
```

On this machine Maven's default JDK is newer, so point it at Java 21 first:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@21 mvn verify
```

## License

MIT. See [LICENSE](LICENSE).
