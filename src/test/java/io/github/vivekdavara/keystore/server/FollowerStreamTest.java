package io.github.vivekdavara.keystore.server;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.Options;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.SyncPolicy;
import io.github.vivekdavara.keystore.wal.WalFormat;
import io.github.vivekdavara.keystore.wal.WalRecord;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A follower against a scripted leader that sends what a real one shouldn't: a gap, a repeat, a
 * damaged record, a line that isn't part of a stream. The follower must apply nothing it can't
 * check, drop the connection, and ask again from its own position.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FollowerStreamTest {
    @TempDir
    Path dir;

    private ServerSocket listener;
    private Store store;
    private Follower follower;

    @AfterEach
    void close() throws IOException {
        if (follower != null) {
            follower.close();
        }
        if (store != null) {
            store.close();
        }
        if (listener != null) {
            listener.close();
        }
    }

    /** The records a real leader would send for sequence numbers from..to. */
    private static byte[] record(long seq) {
        ByteBuffer encoded = WalFormat.encode(WalRecord.put(seq, utf8("k" + seq), utf8("v" + seq)));
        byte[] bytes = new byte[encoded.remaining()];
        encoded.get(bytes);
        return bytes;
    }

    /** The stored checksum of record(seq), as a follower that applied it sends it in SYNC. */
    private static long checksum(long seq) {
        return WalFormat.storedChecksum(record(seq));
    }

    private static void send(OutputStream out, byte[] record) throws IOException {
        out.write(("REC " + record.length + "\n").getBytes(StandardCharsets.US_ASCII));
        out.write(record);
    }

    /** One scripted connection: what the follower asked for, and a way to answer. */
    private record Connection(Socket socket, String request, InputStream in, OutputStream out) {}

    private final BlockingQueue<Connection> connections = new LinkedBlockingQueue<>();

    private void startFakeLeader() throws IOException {
        listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread.ofPlatform().daemon().start(() -> {
            try {
                for (;;) {
                    Socket s = listener.accept();
                    InputStream in = new BufferedInputStream(s.getInputStream());
                    connections.add(new Connection(s, Protocol.readLine(in), in, s.getOutputStream()));
                }
            } catch (IOException e) {
                // closed
            }
        });
    }

    private Connection next() throws InterruptedException {
        Connection c = connections.poll(20, TimeUnit.SECONDS);
        assertTrue(c != null, "the follower didn't connect; " + follower.status());
        return c;
    }

    private void startFollower() throws IOException {
        store = Store.open(dir, Options.defaults().withSync(SyncPolicy.NEVER));
        follower = Follower.start(store, Follower.Config.of(new InetSocketAddress(InetAddress.getLoopbackAddress(),
                listener.getLocalPort())).withTimeout(Duration.ofSeconds(5)).withMaxBackoff(Duration.ofMillis(20)));
    }

    /** The follower sets STREAMING just after sending SYNC, so a moment after the fake leader reads it. */
    private void awaitStreaming() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (follower.status().state() != Follower.State.STREAMING) {
            assertTrue(System.nanoTime() < deadline, follower.status().toString());
            Thread.sleep(1);
        }
    }

    @Test
    void aGapIsNotAppliedAndTheFollowerAsksAgainFromItsPosition() throws Exception {
        startFakeLeader();
        startFollower();
        Connection first = next();
        assertEquals("SYNC 1", first.request());
        send(first.out(), record(1));
        send(first.out(), record(2));
        first.out().write("HEARTBEAT 2\n".getBytes(StandardCharsets.US_ASCII)); // ends the batch
        first.out().flush();
        assertTrue(follower.awaitApplied(2, Duration.ofSeconds(10)));
        send(first.out(), record(4)); // 3 is missing
        first.out().flush();

        Connection second = next();
        assertEquals("SYNC 3 " + checksum(2), second.request(), "asks again from its own position, and says what #2 was");
        assertEquals(2, store.lastSequence(), "record 4 wasn't applied");
        awaitStreaming();
        assertTrue(follower.status().detail().contains("record 4"), follower.status().detail());
        send(second.out(), record(3));
        send(second.out(), record(4));
        second.out().flush();
        assertTrue(follower.awaitApplied(4, Duration.ofSeconds(10)));
        assertArrayEquals(utf8("v4"), store.get(utf8("k4")).orElseThrow());
        first.socket().close();
        second.socket().close();
    }

    @Test
    void aRepeatAndADamagedRecordAreNotApplied() throws Exception {
        startFakeLeader();
        startFollower();
        Connection first = next();
        send(first.out(), record(1));
        first.out().write("HEARTBEAT 1\n".getBytes(StandardCharsets.US_ASCII));
        first.out().flush();
        assertTrue(follower.awaitApplied(1, Duration.ofSeconds(10)));
        send(first.out(), record(1)); // a repeat
        first.out().flush();
        Connection second = next();
        assertEquals("SYNC 2 " + checksum(1), second.request());
        assertEquals(1, store.lastSequence());

        byte[] damaged = record(2);
        damaged[damaged.length - 1] ^= 1; // the checksum no longer matches
        send(second.out(), damaged);
        second.out().flush();
        Connection third = next();
        assertEquals("SYNC 2 " + checksum(1), third.request());
        assertEquals(1, store.lastSequence());
        assertTrue(follower.status().detail().contains("fails its checks"), follower.status().detail());
        first.socket().close();
        second.socket().close();
        third.socket().close();
    }

    @Test
    void aBurstCutShortByABadRecordKeepsTheGoodRecords() throws Exception {
        startFakeLeader();
        startFollower();
        Connection first = next();
        ByteArrayOutputStream burst = new ByteArrayOutputStream();
        send(burst, record(1));
        send(burst, record(2));
        send(burst, record(2)); // a repeat, in the same burst as the good records
        first.out().write(burst.toByteArray()); // one write, so they arrive as one batch
        first.out().flush();
        Connection second = next();
        // Records 1 and 2 passed their checks before the repeat came, still in the batch: they are
        // applied before the connection is dropped, so the follower asks only for what follows.
        assertEquals("SYNC 3 " + checksum(2), second.request());
        assertEquals(2, store.lastSequence());
        first.socket().close();
        second.socket().close();
    }

    @Test
    void recordsInTheSameBurstAsAnErrorAreApplied() throws Exception {
        startFakeLeader();
        startFollower();
        Connection first = next();
        ByteArrayOutputStream burst = new ByteArrayOutputStream();
        send(burst, record(1));
        send(burst, record(2));
        burst.write("ERR INTERNAL the store is closed\n".getBytes(StandardCharsets.US_ASCII));
        first.out().write(burst.toByteArray());
        first.out().flush();
        Connection second = next();
        assertEquals("SYNC 3 " + checksum(2), second.request(), "the records before the ERR were applied");

        ByteArrayOutputStream behind = new ByteArrayOutputStream();
        send(behind, record(3));
        behind.write("ERR BEHIND 10\n".getBytes(StandardCharsets.US_ASCII));
        second.out().write(behind.toByteArray());
        second.out().flush();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (follower.status().state() != Follower.State.BEHIND) {
            assertTrue(System.nanoTime() < deadline, follower.status().toString());
            Thread.sleep(1);
        }
        assertEquals(3, store.lastSequence(), "and so was the one before BEHIND");
        first.socket().close();
        second.socket().close();
    }

    @Test
    void divergedStopsTheFollower() throws Exception {
        startFakeLeader();
        startFollower();
        Connection first = next();
        send(first.out(), record(1));
        first.out().write("HEARTBEAT 1\n".getBytes(StandardCharsets.US_ASCII));
        first.out().flush();
        assertTrue(follower.awaitApplied(1, Duration.ofSeconds(10)));
        first.socket().close();
        Connection second = next();
        assertEquals("SYNC 2 " + checksum(1), second.request());
        second.out().write("ERR DIVERGED 1\n".getBytes(StandardCharsets.US_ASCII));
        second.out().flush();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (follower.status().state() != Follower.State.DIVERGED) {
            assertTrue(System.nanoTime() < deadline, follower.status().toString());
            Thread.sleep(1);
        }
        assertTrue(connections.poll(200, TimeUnit.MILLISECONDS) == null, "it doesn't reconnect");
        second.socket().close();
    }

    @Test
    void aLineThatIsntPartOfAStreamDropsTheConnection() throws Exception {
        startFakeLeader();
        startFollower();
        List<Connection> seen = new ArrayList<>();
        for (String bad : new String[] {"PONG\n", "REC 5\n", "REC 99999999999\n", "ERR INTERNAL boom\n"}) {
            Connection c = next();
            seen.add(c);
            c.out().write(bad.getBytes(StandardCharsets.US_ASCII));
            c.out().flush();
            if (bad.equals("REC 5\n")) {
                c.out().write(new byte[5]); // too short to be a record
                c.out().flush();
            }
        }
        Connection last = next();
        assertEquals("SYNC 1", last.request());
        assertEquals(0, store.lastSequence());
        awaitStreaming(); // none of those stops it
        assertTrue(follower.status().connects() >= 5);
        for (Connection c : seen) {
            c.socket().close();
        }
        last.socket().close();
    }
}
