package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.server.Follower;
import io.github.vivekdavara.keystore.server.Server;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SnapshotTest {
    private static final int KEYS = 200;

    @TempDir
    Path tmp;

    private static Options options() {
        return Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(8 << 10);
    }

    /** Checks that the store in {@code dir} holds exactly the model's writes, and returns its last sequence number. */
    private static long checkSnapshot(Path dir, Workload.Model model) throws IOException {
        try (Store copy = Store.open(dir, options())) {
            model.advanceTo(copy.lastSequence());
            for (int k = 0; k < KEYS; k++) {
                assertEquals(string(model.get(k)), string(copy.get(Workload.key(k))), "k" + k + " at " + copy.lastSequence());
            }
            return copy.lastSequence();
        }
    }

    /** Linked (one file system), and copied (as across file systems, where links fail). */
    @ParameterizedTest(name = "link {0}")
    @ValueSource(booleans = {true, false})
    void aSnapshotOpensAsAStoreWithExactlyItsWrites(boolean link) throws IOException {
        Workload workload = new Workload(3, KEYS);
        Workload.Model model = new Workload.Model(workload);
        Store.Snapshot snapshot;
        long checksum;
        try (Store store = Store.open(tmp.resolve("store"), options())) {
            for (long seq = 1; seq <= 1_000; seq++) { // tables, and writes still in the memtable
                workload.apply(store, seq);
            }
            assertTrue(store.memtableSize() > 0 && !store.tables().isEmpty());
            checksum = store.lastChecksum();
            snapshot = store.snapshot(tmp.resolve("snapshot"), link);
            assertEquals(1_000, snapshot.lastSequence());
            assertEquals(checksum, snapshot.lastChecksum());
            assertEquals(tmp.resolve("snapshot").toRealPath(), snapshot.directory());
            assertEquals(store.tables().size(), snapshot.tables(), "the store's tables, after its flush");
            assertEquals(link ? snapshot.tables() : 0, snapshot.linked());
            assertEquals(List.of("snapshot", "store"), names(tmp), "nothing else beside it");
            for (long seq = 1_001; seq <= 1_500; seq++) { // the store goes on; the snapshot doesn't change
                workload.apply(store, seq);
            }
        }
        assertEquals(1_000, checkSnapshot(snapshot.directory(), model));
        try (Store copy = Store.open(snapshot.directory(), options())) {
            assertEquals(checksum, copy.lastChecksum(), "the last write's checksum, from the manifest");
            assertEquals(0, copy.recovery().records(), "nothing to replay");
        }
    }

    /** The source's merges delete the tables it linked; the snapshot's links keep them. */
    @Test
    void theSourcesMergesDontTouchTheSnapshot() throws IOException {
        Workload workload = new Workload(5, KEYS);
        Workload.Model model = new Workload.Model(workload);
        Path snapshot = tmp.resolve("snapshot");
        try (Store store = Store.open(tmp.resolve("store"), options())) {
            for (long seq = 1; seq <= 800; seq++) {
                workload.apply(store, seq);
            }
            store.snapshot(snapshot);
            for (long seq = 801; seq <= 1_600; seq++) {
                workload.apply(store, seq);
            }
            store.compactAll();
            assertEquals(1, store.tables().size());
        }
        assertEquals(800, checkSnapshot(snapshot, model));
        try (Store copy = Store.open(snapshot, options())) { // and the snapshot's own writes and merges
            for (long seq = 801; seq <= 1_600; seq++) {
                workload.apply(copy, seq);
            }
            copy.compactAll();
        }
        assertEquals(1_600, checkSnapshot(snapshot, model));
        try (Store store = Store.open(tmp.resolve("store"), options())) {
            assertEquals(1_600, store.lastSequence(), "the source is whole too");
        }
    }

    @Test
    void anEmptyStoresSnapshotIsAnEmptyStore() throws IOException {
        try (Store store = Store.open(tmp.resolve("store"), options())) {
            Store.Snapshot snapshot = store.snapshot(tmp.resolve("a").resolve("b").resolve("snapshot"));
            assertEquals(0, snapshot.lastSequence());
            assertEquals(0, snapshot.tables());
            assertEquals(-1, snapshot.lastChecksum());
        }
        try (Store copy = Store.open(tmp.resolve("a/b/snapshot"), options())) {
            assertEquals(0, copy.lastSequence());
            copy.put(utf8("k"), utf8("v"));
        }
    }

    /** Everything deleted and merged away: no tables, but the sequence number goes on. */
    @Test
    void aSnapshotWithNoTablesKeepsTheSequenceNumber() throws IOException {
        try (Store store = Store.open(tmp.resolve("store"), options())) {
            store.put(utf8("k"), utf8("v"));
            store.delete(utf8("k"));
            store.flush();
            store.compactAll();
            assertEquals(0, store.tables().size());
            Store.Snapshot snapshot = store.snapshot(tmp.resolve("snapshot"));
            assertEquals(2, snapshot.lastSequence());
            assertEquals(0, snapshot.tables());
        }
        try (Store copy = Store.open(tmp.resolve("snapshot"), options())) {
            assertEquals(2, copy.lastSequence());
            assertEquals(Optional.empty(), copy.get(utf8("k")));
            assertEquals(3, copy.put(utf8("k"), utf8("w")));
        }
    }

    @Test
    void theTargetMustNotExist() throws IOException {
        Files.createDirectories(tmp.resolve("empty"));
        Files.writeString(tmp.resolve("file"), "x");
        try (Store store = Store.open(tmp.resolve("store"), options())) {
            store.put(utf8("k"), utf8("v"));
            assertThrows(FileAlreadyExistsException.class, () -> store.snapshot(tmp.resolve("empty")));
            assertThrows(FileAlreadyExistsException.class, () -> store.snapshot(tmp.resolve("file")));
            assertThrows(FileAlreadyExistsException.class, () -> store.snapshot(tmp.resolve("store")));
            // A name the store could use itself, in its own directory.
            assertThrows(IllegalArgumentException.class, () -> store.snapshot(tmp.resolve("store/MANIFEST.tmp")));
            assertEquals(1, store.memtableSize(), "refused before anything was done: nothing flushed");
            assertEquals(0, store.tables().size());
            store.put(utf8("k"), utf8("w"));
        }
        try (Stream<Path> files = Files.list(tmp.resolve("empty"))) {
            assertEquals(0, files.count());
        }
    }

    /**
     * A snapshot that fails partway leaves nothing: neither the target nor the directory it was
     * built in, and the store goes on taking writes (only the flush is the store's, and it worked).
     * Each of the snapshot's writes and forces is failed in turn (a copied table's force, the
     * directory's before the manifest, the manifest's write and force, the directory's after it,
     * the parent's after the rename), until one succeeds. Found by review: a failure while the
     * manifest was written left MANIFEST.tmp (or MANIFEST) in the target, which a store then opened
     * as an empty one at sequence 0, and a retry found the target taken.
     */
    @ParameterizedTest(name = "link {0}")
    @ValueSource(booleans = {true, false})
    void aSnapshotFailingAnywhereLeavesNothing(boolean link) throws IOException {
        Path target = tmp.resolve("snapshot");
        Path storeDir = tmp.toRealPath().resolve("store");
        AtomicInteger left = new AtomicInteger(Integer.MAX_VALUE);
        BiFunction<Path, FileChannel, FileChannel> wrap = (path, channel) -> {
            if (path.startsWith(storeDir) || path.startsWith(tmp.resolve("store"))) {
                return channel; // the store's own files
            }
            return new FaultyChannel(channel) {
                @Override
                public int write(java.nio.ByteBuffer src) throws IOException {
                    if (left.decrementAndGet() == 0) {
                        throw new IOException("injected: write failed");
                    }
                    return super.write(src);
                }

                @Override
                public void force(boolean metaData) throws IOException {
                    if (left.decrementAndGet() == 0) {
                        throw new IOException("injected: force failed");
                    }
                    super.force(metaData);
                }
            };
        };
        try (Store store = Store.open(tmp.resolve("store"), options(), wrap)) {
            int failures = 0;
            for (int n = 1; ; n++) {
                assertTrue(n < 100, "the snapshot never succeeded");
                for (int i = 0; i < 50; i++) {
                    store.put(utf8("k" + n + "-" + i), utf8("v"));
                }
                left.set(n);
                try {
                    store.snapshot(target, link);
                } catch (IOException e) {
                    assertTrue(e.getMessage().startsWith("injected"), e::toString);
                    assertEquals(List.of("store"), names(tmp), "after failure " + n + ": nothing left");
                    failures++;
                    continue;
                }
                left.set(Integer.MAX_VALUE);
                assertTrue(failures >= (link ? 5 : 6), failures + " failure points");
                try (Store copy = Store.open(target, options())) {
                    assertEquals(store.lastSequence(), copy.lastSequence());
                    assertEquals("v", string(copy.get(utf8("k1-0"))).orElseThrow());
                }
                return;
            }
        }
    }

    private static List<String> names(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    /**
     * What a snapshot forces, in order: each copied table, its directory before the manifest is
     * written, the manifest, its directory after the manifest's rename, and the parent after the
     * snapshot's rename into place. No test of contents can see a missing force: these can.
     */
    @ParameterizedTest(name = "link {0}")
    @ValueSource(booleans = {true, false})
    void aSnapshotForcesItsFilesThenItsDirectoryThenItsName(boolean link) throws IOException {
        List<String> events = new CopyOnWriteArrayList<>();
        Path parent = tmp.toRealPath();
        BiFunction<Path, FileChannel, FileChannel> wrap = (path, channel) -> {
            String name = path.equals(parent) ? "parent"
                    : path.getFileName().toString().startsWith(".snapshot.snapshot-") ? "building"
                    : path.getFileName().toString();
            if (!path.startsWith(parent) || path.startsWith(parent.resolve("store"))) {
                return channel;
            }
            return new FaultyChannel(channel) {
                @Override
                public int write(java.nio.ByteBuffer src) throws IOException {
                    events.add("write " + name);
                    return super.write(src);
                }

                @Override
                public void force(boolean metaData) throws IOException {
                    events.add("force " + name);
                    super.force(metaData);
                }
            };
        };
        try (Store store = Store.open(tmp.resolve("store"), options().withMergeWidth(64), wrap)) {
            for (int i = 0; i < 300; i++) {
                store.put(utf8("k" + i), utf8("v" + i));
            }
            store.flush(); // and another with the snapshot's flush
            store.put(utf8("last"), utf8("v"));
            Store.Snapshot snapshot = store.snapshot(tmp.resolve("snapshot"), link);
            List<String> tables = new ArrayList<>();
            for (TableInfo t : store.tables()) {
                tables.add("force " + Store.tableName(t.number()));
            }
            List<String> expected = new ArrayList<>(link ? List.of() : tables);
            expected.addAll(List.of("force building", "write MANIFEST.tmp", "force MANIFEST.tmp", "force building",
                    "force parent"));
            assertEquals(expected, events);
            assertEquals(store.tables().size(), snapshot.tables());
            assertTrue(snapshot.tables() >= 2, "tables to copy: " + snapshot.tables());
        }
    }

    /**
     * The snapshot copies its tables outside the write lock, so a merge can replace them meanwhile;
     * the version it pinned keeps them until it's done. Here a merge runs, and the old tables'
     * last other reference goes, while the snapshot waits at the force of the first table it
     * copied: without the pin, the second table's file is gone when its copy starts.
     */
    @Test
    @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void theTablesASnapshotCopiesSurviveAMergeThatReplacesThem() throws Exception {
        AtomicReference<FaultyChannel.Gate> gate = new AtomicReference<>();
        Path parent = tmp.toRealPath();
        BiFunction<Path, FileChannel, FileChannel> wrap = (path, channel) -> {
            if (path.startsWith(parent) && !path.startsWith(parent.resolve("store"))
                    && path.toString().endsWith(Store.TABLE_SUFFIX) && gate.get() == null) {
                FaultyChannel held = new FaultyChannel(channel);
                gate.set(held.holdNextForce());
                return held;
            }
            return channel;
        };
        Workload workload = new Workload(23, KEYS);
        try (Store store = Store.open(tmp.resolve("store"), options().withMergeWidth(64), wrap)) {
            for (long seq = 1; seq <= 600; seq++) {
                workload.apply(store, seq);
            }
            assertTrue(store.tables().size() >= 2, "several tables to copy");
            AtomicReference<Object> result = new AtomicReference<>();
            Thread snapshot = Thread.ofPlatform().start(() -> {
                try {
                    result.set(store.snapshot(tmp.resolve("snapshot"), false));
                } catch (Throwable e) {
                    result.set(e);
                }
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (gate.get() == null) {
                assertTrue(System.nanoTime() < deadline, "the snapshot reached its first copy");
                Thread.sleep(1);
            }
            gate.get().awaitEntered();
            List<TableInfo> before = store.tables();
            store.compactAll(); // replaces every table; the old ones are now only the snapshot's
            assertEquals(1, store.tables().size());
            for (TableInfo t : before) {
                assertTrue(Files.exists(store.directory().resolve(Store.tableName(t.number()))), "pinned: still there");
            }
            gate.get().open();
            snapshot.join();
            assertTrue(result.get() instanceof Store.Snapshot, () -> "the snapshot: " + result.get());
            for (TableInfo t : before) {
                assertTrue(!Files.exists(store.directory().resolve(Store.tableName(t.number()))),
                        "released by the snapshot, the merged-away tables are deleted");
            }
        }
        Workload.Model model = new Workload.Model(workload);
        assertEquals(600, checkSnapshot(tmp.resolve("snapshot"), model));
    }

    @Test
    void aFailedStoreTakesNoSnapshot() throws IOException {
        AtomicReference<FaultyChannel> log = new AtomicReference<>();
        try (Store store = Store.open(tmp.resolve("store"), options(), (path, channel) -> {
            if (path.toString().endsWith(Store.LOG_SUFFIX)) {
                FaultyChannel faulty = new FaultyChannel(channel);
                log.set(faulty);
                return faulty;
            }
            return channel;
        })) {
            store.put(utf8("k"), utf8("v"));
            log.get().failWritesAfter(3);
            assertThrows(IOException.class, () -> store.put(utf8("k"), utf8("w")));
            assertThrows(StoreFailedException.class, () -> store.snapshot(tmp.resolve("snapshot")));
            assertFalse(Files.exists(tmp.resolve("snapshot")));
        }
    }

    /**
     * Snapshots taken while a writer writes as fast as it can: each holds exactly the writes up to
     * its own sequence number, though its tables were linked while writes, flushes and merges went
     * on.
     */
    @Test
    @Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void snapshotsTakenDuringWritesAreEachAPrefix() throws Exception {
        Workload workload = new Workload(13, KEYS);
        List<Store.Snapshot> snapshots = new ArrayList<>();
        try (Store store = Store.open(tmp.resolve("store"), options())) {
            AtomicBoolean writing = new AtomicBoolean(true);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread writer = Thread.ofPlatform().start(() -> {
                try {
                    for (long seq = 1; writing.get(); seq++) {
                        workload.apply(store, seq);
                    }
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            for (int i = 0; i < 20; i++) {
                TimeUnit.MILLISECONDS.sleep(10);
                snapshots.add(store.snapshot(tmp.resolve("snapshot-" + i)));
            }
            writing.set(false);
            writer.join();
            assertEquals(null, failure.get());
        }
        Workload.Model model = new Workload.Model(workload);
        long previous = 0;
        for (Store.Snapshot s : snapshots) {
            assertTrue(s.lastSequence() >= previous, "in order");
            assertEquals(s.lastSequence(), checkSnapshot(s.directory(), model));
            previous = s.lastSequence();
        }
        assertTrue(previous > 100, "the writer wrote: " + previous);
    }

    /**
     * Day 3 left a follower that is {@code BEHIND} stopped, since its leader had deleted the log it
     * needed. A snapshot of the leader starts it again: opened as the follower's store, it asks for
     * the writes after the snapshot's last, with that write's checksum, which the leader checks.
     */
    @Test
    @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aFollowerThatIsBehindStartsAgainFromASnapshot() throws Exception {
        Workload workload = new Workload(19, KEYS);
        // Keeps 64 KiB of flushed logs: a follower 2,000 writes behind (about 340 KB) finds its next one gone.
        Options leaderOptions = options().withRetainLogBytes(64 << 10);
        try (Store leader = Store.open(tmp.resolve("leader"), leaderOptions);
                Server server = Server.leader(leader, Server.Config.loopback(0))) {
            for (long seq = 1; seq <= 2_000; seq++) {
                workload.apply(leader, seq);
            }
            try (Store behind = Store.open(tmp.resolve("behind"), options());
                    Follower follower = Follower.start(behind, Follower.Config.of(server.address()))) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (follower.status().state() != Follower.State.BEHIND) {
                    assertTrue(System.nanoTime() < deadline, follower.status().toString());
                    Thread.sleep(5);
                }
            }
            Store.Snapshot snapshot = leader.snapshot(tmp.resolve("from-snapshot"));
            for (long seq = 2_001; seq <= 2_300; seq++) { // well within the logs it keeps
                workload.apply(leader, seq);
            }
            try (Store copy = Store.open(snapshot.directory(), options());
                    Follower follower = Follower.start(copy, Follower.Config.of(server.address())
                            .withTimeout(Duration.ofSeconds(5)))) {
                assertEquals(2_000, copy.lastSequence());
                assertEquals(snapshot.lastChecksum(), copy.lastChecksum());
                assertTrue(follower.awaitApplied(2_300, Duration.ofSeconds(30)), follower.status().toString());
                Workload.Model model = new Workload.Model(workload);
                model.advanceTo(2_300);
                for (int k = 0; k < KEYS; k++) {
                    assertEquals(string(model.get(k)), string(copy.get(Workload.key(k))), "k" + k);
                }
            }
        }
    }

    private static Optional<String> string(Optional<byte[]> value) {
        return value.map(v -> new String(v, StandardCharsets.UTF_8));
    }
}
