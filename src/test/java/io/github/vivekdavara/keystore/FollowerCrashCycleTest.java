package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.server.Follower;
import io.github.vivekdavara.keystore.server.Server;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A follower crashed anywhere while it replicates. As in {@link TableCrashCycleTest}, every channel
 * of the follower's store shares one countdown of writes and forces, and the one that runs it out
 * fails (a write torn at a random byte), wherever it is: an append of a batch, a flush's table, the
 * manifest, a directory force. The follower's store then fails, and its directory, which nothing
 * writes to after that, is what a killed process leaves. Each cycle recovers that image and checks
 * that it is exactly the leader's state at the image's own sequence number (a follower is always a
 * prefix of its leader's history, never a mixture), then follows the leader again from there.
 */
class FollowerCrashCycleTest {
    private static final int KEYS = 50;

    @TempDir
    Path tmp;

    /** A channel whose writes and forces count down a shared budget; the last one fails. */
    private static final class Countdown extends FaultyChannel {
        private final AtomicInteger left;
        private final SplittableRandom random;
        private final String kind;
        private final Map<String, Integer> faults;

        Countdown(FileChannel inner, AtomicInteger left, SplittableRandom random, Path path, Map<String, Integer> faults) {
            super(inner);
            this.left = left;
            this.random = random;
            String name = path.getFileName().toString();
            this.kind = name.endsWith(".log") ? "log" : name.endsWith(".sst") ? "table"
                    : name.startsWith("MANIFEST") ? "manifest" : "directory";
            this.faults = faults;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            if (left.decrementAndGet() == 0) {
                synchronized (faults) {
                    faults.merge(kind + " write", 1, Integer::sum);
                }
                failWritesAfter(random.nextInt(Math.max(1, src.remaining()))); // torn: some bytes, never all
            }
            return super.write(src);
        }

        @Override
        public void force(boolean metaData) throws IOException {
            if (left.decrementAndGet() == 0) {
                synchronized (faults) {
                    faults.merge(kind + " force", 1, Integer::sum);
                }
                throw new IOException("injected: force failed");
            }
            super.force(metaData);
        }
    }

    @Test
    @Timeout(value = 180, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aCrashedFollowerRecoversToAPrefixOfItsLeadersHistory() throws Exception {
        Map<String, Integer> faults = new TreeMap<>();
        for (long seed = 1; seed <= 2; seed++) {
            cycles(seed, faults);
        }
        for (String step : new String[] {"log write", "table write", "manifest write", "directory force"}) {
            assertTrue(faults.containsKey(step), "no fault landed on a " + step + ": " + faults);
        }
    }

    private void cycles(long seed, Map<String, Integer> faults) throws Exception {
        SplittableRandom random = new SplittableRandom(seed);
        List<String[]> history = new ArrayList<>(); // history.get(seq - 1): key, then the value or null for a delete
        Options leaderOptions = Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(2048)
                .withRetainLogBytes(Long.MAX_VALUE);
        Options followerOptions = Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(1024).withMergeWidth(3);
        AtomicInteger left = new AtomicInteger(Integer.MAX_VALUE);
        BiFunction<Path, FileChannel, FileChannel> wrap = (path, inner) -> new Countdown(inner, left, random, path, faults);
        Path dir = tmp.resolve("follower-" + seed);
        int crashes = 0;
        long recoveredPastAcknowledged = 0;

        try (Store leader = Store.open(tmp.resolve("leader-" + seed), leaderOptions);
                Server server = Server.leader(leader, Server.Config.loopback(0).withHeartbeat(Duration.ofMillis(20)))) {
            for (int cycle = 0; cycle < 40; cycle++) {
                left.set(Integer.MAX_VALUE);
                Store store = Store.open(dir, followerOptions, wrap);
                try {
                    long m = store.lastSequence();
                    assertTrue(m <= history.size(), "cycle " + cycle + ": the follower is past its leader");
                    Map<String, String> want = stateAt(history, m);
                    for (int k = 0; k < KEYS; k++) {
                        assertEquals(Optional.ofNullable(want.get("k" + k)), string(store.get(utf8("k" + k))),
                                "cycle " + cycle + ", key k" + k + ", at #" + m);
                    }
                    boolean crash = random.nextInt(4) != 0;
                    if (crash) {
                        left.set(random.nextInt(1, 40));
                    }
                    long before = store.lastSequence();
                    Follower follower = Follower.start(store, Follower.Config.of(server.address())
                            .withTimeout(Duration.ofSeconds(5)).withMaxBackoff(Duration.ofMillis(20))
                            .withMaxBatchRecords(1 + random.nextInt(16)));
                    try {
                        int writes = random.nextInt(20, 120);
                        for (int w = 0; w < writes; w++) {
                            String key = "k" + random.nextInt(KEYS);
                            String value = random.nextInt(5) == 0 ? null
                                    : "v" + cycle + "-" + w + "-" + "x".repeat(random.nextInt(0, 40));
                            long seq = value == null ? leader.delete(utf8(key)) : leader.put(utf8(key), utf8(value));
                            history.add(new String[] {key, value});
                            assertEquals(history.size(), seq);
                        }
                        awaitDone(follower, history.size());
                        Follower.Status status = follower.status();
                        long applied = status.applied();
                        if (status.state() == Follower.State.FAILED) {
                            assertTrue(crash, "cycle " + cycle + ": failed with no fault armed: " + status.detail());
                            crashes++;
                            follower.close();
                            Path image = tmp.resolve("image");
                            copy(dir, image); // nothing writes to a failed store's directory: this is the crash
                            closeQuietly(store);
                            delete(dir);
                            Files.move(image, dir);
                            try (Store check = Store.open(dir, followerOptions)) {
                                assertTrue(check.lastSequence() >= applied, "acknowledged by the follower's store, then lost");
                                recoveredPastAcknowledged += check.lastSequence() - applied;
                            }
                            continue;
                        }
                        assertEquals(history.size(), applied, "cycle " + cycle + ": caught up without a fault");
                        assertTrue(applied >= before);
                    } finally {
                        follower.close(); // again, after a crash: closing twice does nothing
                    }
                } catch (IOException | RuntimeException | Error e) {
                    closeQuietly(store);
                    throw e;
                }
                store.close();
            }
            // The last image, followed to the end: the same contents as the leader.
            try (Store store = Store.open(dir, followerOptions);
                    Follower follower = Follower.start(store, Follower.Config.of(server.address()))) {
                assertTrue(follower.awaitApplied(history.size(), Duration.ofSeconds(30)), follower.status().toString());
                Map<String, String> want = stateAt(history, history.size());
                for (int k = 0; k < KEYS; k++) {
                    assertEquals(Optional.ofNullable(want.get("k" + k)), string(store.get(utf8("k" + k))));
                    assertEquals(string(leader.get(utf8("k" + k))), string(store.get(utf8("k" + k))));
                }
            }
        }
        System.out.printf("follower crash cycles, seed %d: 40 cycles, %d crashes, %d writes; %d records recovered past"
                + " what the follower had acknowledged (whole records of a batch whose append or force failed);"
                + " faults so far: %s%n", seed, crashes, history.size(), recoveredPastAcknowledged, faults);
        assertTrue(crashes >= 15, crashes + " crashes");
    }

    /** Waits until the follower has applied {@code seq} or has failed (the injected fault). */
    private static void awaitDone(Follower follower, long seq) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (follower.status().applied() < seq && follower.status().state() != Follower.State.FAILED) {
            assertTrue(System.nanoTime() < deadline, "stuck: " + follower.status());
            follower.awaitApplied(seq, Duration.ofMillis(5));
        }
    }

    private static Map<String, String> stateAt(List<String[]> history, long seq) {
        Map<String, String> state = new HashMap<>();
        for (int i = 0; i < seq; i++) {
            String[] write = history.get(i);
            if (write[1] == null) {
                state.remove(write[0]);
            } else {
                state.put(write[0], write[1]);
            }
        }
        return state;
    }

    private static void closeQuietly(Store store) {
        try {
            store.close();
        } catch (IOException | RuntimeException ignored) {
            // A failed store's close can throw; the crash image is already taken.
        }
    }

    private static void copy(Path from, Path to) throws IOException {
        Files.createDirectories(to);
        try (Stream<Path> files = Files.list(from)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                Files.copy(file, to.resolve(file.getFileName()), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private static void delete(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                Files.delete(file);
            }
        }
        Files.delete(dir);
    }

    private static Optional<String> string(Optional<byte[]> value) {
        return value.map(v -> new String(v, StandardCharsets.UTF_8));
    }
}
