package io.github.vivekdavara.keystore.server;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.Options;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.SyncPolicy;
import io.github.vivekdavara.keystore.server.Follower.State;
import io.github.vivekdavara.keystore.server.Protocol.ErrorCode;
import io.github.vivekdavara.keystore.wal.WalRecord;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ReplicationTest {
    @TempDir
    Path dir;

    private final List<AutoCloseable> open = new ArrayList<>();

    @AfterEach
    void closeAll() throws Exception {
        for (int i = open.size() - 1; i >= 0; i--) {
            open.get(i).close();
        }
    }

    private <T extends AutoCloseable> T track(T closeable) {
        open.add(closeable);
        return closeable;
    }

    private static final long FLUSH = 16 << 10;

    /** A leader keeps every log; small logs, so the tests cross many flushes and merges. */
    private static Options leaderOptions() {
        return Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(FLUSH).withRetainLogBytes(1L << 30);
    }

    private static Options followerOptions() {
        return Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(FLUSH);
    }

    private static final Server.Config FAST = Server.Config.loopback(0).withHeartbeat(Duration.ofMillis(50));

    private Store open(String name, Options options) throws IOException {
        return track(Store.open(dir.resolve(name), options));
    }

    private Follower follow(Store store, Server leader) {
        return track(Follower.start(store, Follower.Config.of(leader.address()).withTimeout(Duration.ofSeconds(5))
                .withMaxBackoff(Duration.ofMillis(200))));
    }

    /** Writes seq from..to: puts of random keys, some deletes. Returns the keys touched. */
    private static List<byte[]> write(Store store, long from, long to, SplittableRandom random) throws IOException {
        List<byte[]> keys = new ArrayList<>();
        for (long seq = from; seq <= to; seq++) {
            byte[] key = utf8("key-" + random.nextInt(2_000));
            long got = random.nextInt(10) == 0 ? store.delete(key) : store.put(key, utf8("value-" + seq));
            assertEquals(seq, got);
            keys.add(key);
        }
        return keys;
    }

    private static void assertSameContents(Store leader, Store follower) throws IOException {
        assertEquals(leader.lastSequence(), follower.lastSequence());
        for (int i = 0; i < 2_000; i++) {
            byte[] key = utf8("key-" + i);
            Optional<byte[]> want = leader.get(key);
            Optional<byte[]> got = follower.get(key);
            assertEquals(want.isPresent(), got.isPresent(), "key-" + i);
            if (want.isPresent()) {
                assertArrayEquals(want.get(), got.get(), "key-" + i);
            }
        }
    }

    private static void await(Follower follower, long seq) throws InterruptedException {
        assertTrue(follower.awaitApplied(seq, Duration.ofSeconds(60)), "applied " + follower.status());
    }

    @Test
    void aFollowerGetsEveryWriteAsItIsMade() throws Exception {
        Store leaderStore = open("leader", leaderOptions());
        Server leader = track(Server.leader(leaderStore, FAST));
        Store followerStore = open("follower", followerOptions());
        Follower follower = follow(followerStore, leader);
        write(leaderStore, 1, 5_000, new SplittableRandom(1));
        await(follower, 5_000);
        assertSameContents(leaderStore, followerStore);
        assertEquals(State.STREAMING, follower.status().state());
        assertEquals(1, follower.status().connects());
        assertTrue(followerStore.stats().flushes() > 5, "the follower flushes on its own");
    }

    @Test
    void aFollowerThatStartsLateCatchesUpFromTheLeadersLogs() throws Exception {
        Store leaderStore = open("leader", leaderOptions());
        Server leader = track(Server.leader(leaderStore, FAST));
        SplittableRandom random = new SplittableRandom(2);
        write(leaderStore, 1, 4_000, random);
        assertTrue(leaderStore.stats().flushes() > 5 && leaderStore.stats().merges() > 0, "the writes are in tables too");
        Store followerStore = open("follower", followerOptions());
        Follower follower = follow(followerStore, leader);
        await(follower, 4_000);
        write(leaderStore, 4_001, 5_000, random); // and then live
        await(follower, 5_000);
        assertSameContents(leaderStore, followerStore);
    }

    @Test
    void aRestartedFollowerCarriesOnFromItsLastSequenceNumber() throws Exception {
        Store leaderStore = open("leader", leaderOptions());
        Server leader = track(Server.leader(leaderStore, FAST));
        SplittableRandom random = new SplittableRandom(3);
        write(leaderStore, 1, 1_000, random);
        Path followerDir = dir.resolve("follower");
        try (Store followerStore = Store.open(followerDir, followerOptions());
                Follower follower = Follower.start(followerStore, Follower.Config.of(leader.address()))) {
            await(follower, 1_000);
        }
        write(leaderStore, 1_001, 3_000, random); // while the follower is down
        Store followerStore = open("follower", followerOptions());
        assertEquals(1_000, followerStore.lastSequence(), "its position is its store's");
        Follower follower = follow(followerStore, leader);
        await(follower, 3_000);
        assertSameContents(leaderStore, followerStore);
    }

    @Test
    void aBrokenConnectionIsResumedWhereItLeftOff() throws Exception {
        Store leaderStore = open("leader", leaderOptions());
        Server first = Server.leader(leaderStore, FAST);
        int port = first.port();
        Store followerStore = open("follower", followerOptions());
        Follower follower = track(Follower.start(followerStore,
                Follower.Config.of(first.address()).withTimeout(Duration.ofSeconds(5)).withMaxBackoff(Duration.ofMillis(100))));
        SplittableRandom random = new SplittableRandom(4);
        write(leaderStore, 1, 1_000, random);
        await(follower, 1_000);
        first.close(); // the stream breaks
        write(leaderStore, 1_001, 2_000, random);
        Server second = track(Server.leader(leaderStore,
                new Server.Config(new InetSocketAddress(first.address().getAddress(), port), 16, Duration.ofMillis(50))));
        await(follower, 2_000);
        assertSameContents(leaderStore, followerStore);
        assertTrue(follower.status().connects() >= 2, follower.status().toString());
        assertEquals(1, second.followers().size());
    }

    @Test
    void aRestartedLeaderServesFromItsRecoveredLogs() throws Exception {
        Path leaderDir = dir.resolve("leader");
        Store followerStore = open("follower", followerOptions());
        SplittableRandom random = new SplittableRandom(5);
        int port;
        Follower follower;
        try (Store leaderStore = Store.open(leaderDir, leaderOptions());
                Server leader = Server.leader(leaderStore, FAST)) {
            port = leader.port();
            follower = track(Follower.start(followerStore, Follower.Config.of(leader.address())
                    .withTimeout(Duration.ofSeconds(5)).withMaxBackoff(Duration.ofMillis(100))));
            write(leaderStore, 1, 1_500, random);
            await(follower, 1_500);
        }
        Store leaderStore = open("leader", leaderOptions());
        write(leaderStore, 1_501, 2_500, random);
        track(Server.leader(leaderStore, new Server.Config(
                new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), port), 16, Duration.ofMillis(50))));
        await(follower, 2_500);
        assertSameContents(leaderStore, followerStore);
    }

    @Test
    void aFollowerWhoseNextRecordIsGoneStopsAsBehind() throws Exception {
        Store leaderStore = open("leader", leaderOptions().withRetainLogBytes(0));
        Server leader = track(Server.leader(leaderStore, FAST));
        write(leaderStore, 1, 2_000, new SplittableRandom(6));
        Store followerStore = open("follower", followerOptions());
        Follower follower = follow(followerStore, leader);
        awaitState(follower, State.BEHIND);
        assertEquals(0, followerStore.lastSequence());
        assertTrue(follower.status().detail().startsWith("BEHIND"), follower.status().detail());
    }

    @Test
    void aFollowerAheadOfTheLeaderStops() throws Exception {
        Store leaderStore = open("leader", leaderOptions());
        Server leader = track(Server.leader(leaderStore, FAST));
        write(leaderStore, 1, 5, new SplittableRandom(7));
        Store followerStore = open("follower", followerOptions());
        List<WalRecord> other = new ArrayList<>();
        for (long seq = 1; seq <= 10; seq++) {
            other.add(WalRecord.put(seq, utf8("other"), utf8("history")));
        }
        followerStore.replicate(other);
        Follower follower = follow(followerStore, leader);
        awaitState(follower, State.AHEAD);
        assertEquals(10, followerStore.lastSequence(), "nothing applied");
    }

    @Test
    void aFollowerWhoseLeaderCameBackWithADifferentHistoryStops() throws Exception {
        // A leader under NEVER that lost writes 6 to 10 in a crash, then took others under the same
        // numbers: the follower got the first 6 to 10. Played here by a second leader on the same port.
        Store first = Store.open(dir.resolve("leader-1"), leaderOptions());
        Server firstServer = Server.leader(first, FAST);
        int port = firstServer.port();
        for (int i = 1; i <= 10; i++) {
            first.put(utf8("k" + i), utf8("first-" + i));
        }
        Store followerStore = open("follower", followerOptions());
        Follower follower = track(Follower.start(followerStore, Follower.Config.of(firstServer.address())
                .withTimeout(Duration.ofSeconds(5)).withMaxBackoff(Duration.ofMillis(50))));
        await(follower, 10);
        firstServer.close();
        first.close();

        Store second = open("leader-2", leaderOptions());
        for (int i = 1; i <= 12; i++) {
            second.put(utf8("k" + i), utf8(i <= 5 ? "first-" + i : "second-" + i)); // the same 1 to 5, then not
        }
        Server secondServer = track(Server.leader(second,
                new Server.Config(new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), port), 16,
                        Duration.ofMillis(50))));
        awaitState(follower, State.DIVERGED);
        assertEquals(10, followerStore.lastSequence(), "nothing of the second history was applied");
        assertArrayEquals(utf8("first-10"), followerStore.get(utf8("k10")).orElseThrow());
        assertTrue(follower.status().detail().startsWith("DIVERGED: 10"), follower.status().detail());
        assertEquals(0, secondServer.followers().size());
    }

    /**
     * The same, with the follower restarted after a flush, so that its last write is in a table and
     * no log holds it. Day 3's store forgot the write's checksum then, sent a bare {@code SYNC 11},
     * and streamed on into the second history; since day 4 the manifest keeps it.
     */
    @Test
    void aFollowerRestartedWithEveryWriteInItsTablesStillChecksItsHistory() throws Exception {
        Store first = Store.open(dir.resolve("leader-1"), leaderOptions());
        Server firstServer = Server.leader(first, FAST);
        int port = firstServer.port();
        for (int i = 1; i <= 10; i++) {
            first.put(utf8("k" + i), utf8("first-" + i));
        }
        Store followerStore = Store.open(dir.resolve("follower"), followerOptions());
        Follower follower = Follower.start(followerStore, Follower.Config.of(firstServer.address()));
        await(follower, 10);
        follower.close();
        followerStore.flush();
        long checksum = followerStore.lastChecksum();
        followerStore.close();
        firstServer.close();
        first.close();

        Store restarted = open("follower", followerOptions());
        assertTrue(restarted.recovery().records() == 0 && restarted.tables().size() == 1, "every write is in a table");
        assertEquals(checksum, restarted.lastChecksum(), "known from the manifest");
        Store second = open("leader-2", leaderOptions());
        for (int i = 1; i <= 12; i++) {
            second.put(utf8("k" + i), utf8(i <= 5 ? "first-" + i : "second-" + i));
        }
        track(Server.leader(second, new Server.Config(
                new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), port), 16, Duration.ofMillis(50))));
        Follower again = track(Follower.start(restarted, Follower.Config.of(firstServer.address())
                .withTimeout(Duration.ofSeconds(5)).withMaxBackoff(Duration.ofMillis(50))));
        awaitState(again, State.DIVERGED);
        assertEquals(10, restarted.lastSequence(), "nothing of the second history was applied");
        assertArrayEquals(utf8("first-10"), restarted.get(utf8("k10")).orElseThrow());
    }

    @Test
    void syncChecksTheChecksumOfTheWriteBeforeFrom() throws Exception {
        Store leaderStore = open("leader", leaderOptions());
        Server leader = track(Server.leader(leaderStore, FAST));
        write(leaderStore, 1, 10, new SplittableRandom(9));
        long seven;
        try (io.github.vivekdavara.keystore.LogTail tail = leaderStore.tail(7)) {
            seven = io.github.vivekdavara.keystore.wal.WalFormat.storedChecksum(tail.poll());
        }
        assertEquals("ERR DIVERGED 7", firstLine(leader, "SYNC 8 " + ((seven + 1) & 0xffffffffL)));
        assertTrue(firstLine(leader, "SYNC 8 " + seven).startsWith("REC "), "the right checksum streams");
        assertTrue(firstLine(leader, "SYNC 8").startsWith("REC "), "no checksum: nothing to check");
    }

    private static String firstLine(Server leader, String request) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(leader.address(), 5_000);
            socket.setSoTimeout(10_000);
            socket.getOutputStream().write((request + "\n").getBytes(StandardCharsets.US_ASCII));
            return Protocol.readLine(new BufferedInputStream(socket.getInputStream()));
        }
    }

    @Test
    void aFollowersServerAnswersReadsAndRefusesWrites() throws Exception {
        Store leaderStore = open("leader", leaderOptions());
        Server leader = track(Server.leader(leaderStore, FAST));
        Store followerStore = open("follower", followerOptions());
        Follower follower = follow(followerStore, leader);
        Server replica = track(Server.follower(followerStore, Server.Config.loopback(0), follower::info));
        try (Client writer = Client.connect(leader.address()); Client reader = Client.connect(replica.address())) {
            long seq = writer.put(utf8("city"), utf8("Boston"));
            await(follower, seq);
            assertArrayEquals(utf8("Boston"), reader.get(utf8("city")).orElseThrow());
            assertEquals(ErrorCode.READONLY, assertThrows(ServerException.class, () -> reader.put(utf8("city"), utf8("x"))).code());
            assertEquals("STREAMING", reader.info("state"));
            assertEquals(Long.toString(seq), reader.info("seq"));
            assertEquals("1", writer.info("followers"));
            awaitTrue(() -> leader.followers().get(0).acked() == seq, () -> "acked " + leader.followers());
            assertEquals(seq, leader.followers().get(0).sent());
        }
    }

    @Test
    void writesFromManyClientsReachTheFollowerInOrder() throws Exception {
        Store leaderStore = open("leader", leaderOptions());
        Server leader = track(Server.leader(leaderStore, FAST));
        Store followerStore = open("follower", followerOptions().withSync(SyncPolicy.EVERY_WRITE));
        Follower follower = follow(followerStore, leader);
        int clients = 6;
        int each = 800;
        List<CompletableFuture<Void>> writers = new ArrayList<>();
        for (int c = 0; c < clients; c++) {
            int id = c;
            writers.add(CompletableFuture.runAsync(() -> {
                try (Client client = Client.connect(leader.address())) {
                    SplittableRandom random = new SplittableRandom(id);
                    for (int i = 0; i < each; i++) {
                        byte[] key = utf8("key-" + random.nextInt(2_000));
                        if (random.nextInt(10) == 0) {
                            client.delete(key);
                        } else {
                            client.put(key, utf8("c" + id + "-" + i));
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for (CompletableFuture<Void> w : writers) {
            w.get(60, TimeUnit.SECONDS);
        }
        await(follower, (long) clients * each);
        assertSameContents(leaderStore, followerStore);
    }

    @Test
    void aFollowerThatStopsAckingIsDropped() throws Exception {
        Store leaderStore = open("leader", leaderOptions());
        Server leader = track(Server.leader(leaderStore, Server.Config.loopback(0).withHeartbeat(Duration.ofMillis(20))));
        try (Socket silent = new Socket()) {
            silent.connect(leader.address(), 5_000);
            silent.setSoTimeout(10_000);
            silent.getOutputStream().write("SYNC 1\n".getBytes(StandardCharsets.US_ASCII));
            InputStream in = new BufferedInputStream(silent.getInputStream());
            assertTrue(Protocol.readLine(in).startsWith("HEARTBEAT "));
            // It never ACKs: after ten heartbeats of silence (200 ms) the leader closes the stream.
            awaitTrue(() -> leader.followers().isEmpty(), leader::followers);
            String line;
            do {
                line = Protocol.readLine(in);
            } while (line != null && line.startsWith("HEARTBEAT "));
            assertEquals(null, line);
        }
    }

    @Test
    void aFollowerAsksFromItsOwnPositionAndTheLeaderSendsLogBytes() throws Exception {
        Store leaderStore = open("leader", leaderOptions());
        Server leader = track(Server.leader(leaderStore, FAST));
        write(leaderStore, 1, 10, new SplittableRandom(8));
        try (Socket socket = new Socket()) {
            socket.connect(leader.address(), 5_000);
            socket.setSoTimeout(10_000);
            socket.getOutputStream().write("SYNC 8\n".getBytes(StandardCharsets.US_ASCII));
            InputStream in = new BufferedInputStream(socket.getInputStream());
            for (long seq = 8; seq <= 10; seq++) {
                String line = Protocol.readLine(in);
                assertTrue(line.startsWith("REC "), line);
                byte[] bytes = Protocol.readExactly(in, (int) Protocol.number(line.substring(4)));
                assertEquals(seq, ((io.github.vivekdavara.keystore.wal.WalFormat.Ok)
                        io.github.vivekdavara.keystore.wal.WalFormat.decode(bytes, 0, bytes.length)).record().seq());
            }
            assertTrue(Protocol.readLine(in).startsWith("HEARTBEAT 10"));
        }
        try (Socket socket = new Socket()) {
            socket.connect(leader.address(), 5_000);
            socket.setSoTimeout(10_000);
            socket.getOutputStream().write("SYNC 12\n".getBytes(StandardCharsets.US_ASCII));
            assertEquals("ERR AHEAD 10", Protocol.readLine(new BufferedInputStream(socket.getInputStream())));
        }
    }

    private static void awaitState(Follower follower, State state) {
        awaitTrue(() -> follower.status().state() == state, follower::status);
    }

    private static void awaitTrue(BooleanSupplier condition, java.util.function.Supplier<Object> what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, () -> "timed out: " + what.get());
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
