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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

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

    @Test
    void aSnapshotOpensAsAStoreWithExactlyItsWrites() throws IOException {
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
            snapshot = store.snapshot(tmp.resolve("snapshot"));
            assertEquals(1_000, snapshot.lastSequence());
            assertEquals(checksum, snapshot.lastChecksum());
            assertEquals(store.tables().size(), snapshot.tables(), "the store's tables, after its flush");
            assertEquals(snapshot.tables(), snapshot.linked(), "one file system: every table linked");
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
            store.put(utf8("k"), utf8("w")); // refused before anything was done
        }
        try (Stream<Path> files = Files.list(tmp.resolve("empty"))) {
            assertEquals(0, files.count());
        }
    }

    /**
     * A snapshot that fails partway (here, at the force of its directory before the manifest) is
     * deleted, and the store goes on taking writes: only the flush is the store's, and it worked.
     */
    @Test
    void aFailedSnapshotIsDeletedAndTheStoreGoesOn() throws IOException {
        Path target = tmp.resolve("snapshot");
        AtomicBoolean failNext = new AtomicBoolean();
        BiFunction<Path, FileChannel, FileChannel> wrap = (path, channel) -> {
            if (path.equals(target) && failNext.getAndSet(false)) {
                FaultyChannel faulty = new FaultyChannel(channel);
                faulty.failForces();
                return faulty;
            }
            return channel;
        };
        try (Store store = Store.open(tmp.resolve("store"), options(), wrap)) {
            for (int i = 0; i < 300; i++) {
                store.put(utf8("k" + i), utf8("v" + i));
            }
            failNext.set(true);
            IOException e = assertThrows(IOException.class, () -> store.snapshot(target));
            assertTrue(e.getMessage().contains("injected"), e::toString);
            assertFalse(Files.exists(target), "a failed snapshot is deleted");
            store.put(utf8("after"), utf8("yes"));
            Store.Snapshot again = store.snapshot(target);
            assertEquals(301, again.lastSequence());
        }
        try (Store copy = Store.open(target, options())) {
            assertEquals("yes", string(copy.get(utf8("after"))).orElseThrow());
        }
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
