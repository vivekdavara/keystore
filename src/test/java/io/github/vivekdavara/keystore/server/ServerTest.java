package io.github.vivekdavara.keystore.server;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.Options;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.SyncPolicy;
import io.github.vivekdavara.keystore.server.Protocol.ErrorCode;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ServerTest {
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

    private Store store(Options options) throws IOException {
        return track(Store.open(dir.resolve("store"), options));
    }

    private Server leader(Store store) throws IOException {
        return track(Server.leader(store, Server.Config.loopback(0)));
    }

    private Client client(Server server) throws IOException {
        return track(Client.connect(server.address()));
    }

    @Test
    void putGetDeleteOverTheWire() throws IOException {
        Store store = store(Options.defaults().withSync(SyncPolicy.NEVER));
        Client client = client(leader(store));
        client.ping();
        assertEquals(1, client.put(utf8("city"), utf8("Boston")));
        assertArrayEquals(utf8("Boston"), client.get(utf8("city")).orElseThrow());
        assertEquals(2, client.delete(utf8("city")));
        assertEquals(Optional.empty(), client.get(utf8("city")));
        assertEquals(Optional.empty(), store.get(utf8("city")));
        assertEquals(2, store.lastSequence());
    }

    @Test
    void keysAndValuesAreAnyBytes() throws IOException {
        Store store = store(Options.defaults().withSync(SyncPolicy.NEVER));
        Client client = client(leader(store));
        byte[] every = new byte[256];
        for (int i = 0; i < every.length; i++) {
            every[i] = (byte) i; // newlines, zeros, bytes that aren't ASCII
        }
        byte[] longestKey = new byte[65_536];
        Arrays.fill(longestKey, (byte) '\n');
        byte[] big = new byte[3 << 20];
        new Random(7).nextBytes(big);

        client.put(every, every);
        client.put(longestKey, new byte[0]);
        client.put(utf8("big"), big);
        assertArrayEquals(every, client.get(every).orElseThrow());
        assertArrayEquals(new byte[0], client.get(longestKey).orElseThrow());
        assertArrayEquals(big, client.get(utf8("big")).orElseThrow());
        assertArrayEquals(big, store.get(utf8("big")).orElseThrow());
    }

    @Test
    void pipelinedRequestsAreAnsweredInOrder() throws IOException {
        Store store = store(Options.defaults().withSync(SyncPolicy.NEVER));
        Server server = leader(store);
        try (Socket socket = new Socket()) {
            socket.connect(server.address(), 5_000);
            socket.setSoTimeout(30_000);
            OutputStream out = socket.getOutputStream();
            StringBuilder requests = new StringBuilder();
            for (int i = 0; i < 2_000; i++) {
                String key = "k" + i;
                requests.append("PUT ").append(key.length()).append(" 1\n").append(key).append('v');
                requests.append("GET ").append(key.length()).append('\n').append(key);
            }
            out.write(requests.toString().getBytes(StandardCharsets.US_ASCII)); // all of it, before reading anything
            out.flush();
            InputStream in = new BufferedInputStream(socket.getInputStream());
            for (int i = 0; i < 2_000; i++) {
                assertEquals("OK " + (i + 1), Protocol.readLine(in));
                assertEquals("VALUE 1", Protocol.readLine(in));
                assertEquals('v', in.read());
            }
        }
    }

    @Test
    void aBadRequestGetsAnErrorAndTheConnectionCloses() throws IOException {
        Store store = store(Options.defaults().withSync(SyncPolicy.NEVER));
        Server server = leader(store);
        for (String bad : List.of("FROB\n", "GET 0\n", "PUT 1 16777217\n", "GET  1\n", "x".repeat(200) + "\n",
                "GET\t1\n")) {
            try (Socket socket = new Socket()) {
                socket.connect(server.address(), 5_000);
                socket.setSoTimeout(30_000);
                socket.getOutputStream().write((bad + "PING\n").getBytes(StandardCharsets.US_ASCII));
                InputStream in = new BufferedInputStream(socket.getInputStream());
                String reply = Protocol.readLine(in);
                assertTrue(reply.startsWith("ERR PROTOCOL "), reply);
                assertNull(Protocol.readLine(in), "the PING after it isn't answered: the connection is closed");
            }
        }
        assertEquals(0, store.lastSequence());
    }

    @Test
    void aFollowerRefusesWritesAndKeepsTheConnection() throws IOException {
        Store store = store(Options.defaults().withSync(SyncPolicy.NEVER));
        store.put(utf8("a"), utf8("1"));
        Server server = track(Server.follower(store, Server.Config.loopback(0), () -> "state=TEST"));
        Client client = client(server);
        ServerException put = assertThrows(ServerException.class, () -> client.put(utf8("a"), utf8("2")));
        assertEquals(ErrorCode.READONLY, put.code());
        ServerException delete = assertThrows(ServerException.class, () -> client.delete(utf8("a")));
        assertEquals(ErrorCode.READONLY, delete.code());
        assertArrayEquals(utf8("1"), client.get(utf8("a")).orElseThrow()); // the refused data was read past
        assertEquals("follower", client.info("role"));
        assertEquals("TEST", client.info("state"));
        assertEquals(1, store.lastSequence());
    }

    @Test
    void connectionsPastTheLimitAreRefused() throws IOException {
        Store store = store(Options.defaults().withSync(SyncPolicy.NEVER));
        Server server = track(Server.leader(store, Server.Config.loopback(0).withMaxConnections(2)));
        Client first = client(server);
        Client second = client(server);
        first.ping();
        second.ping();
        try (Client third = Client.connect(server.address())) {
            ServerException e = assertThrows(ServerException.class, third::ping);
            assertEquals(ErrorCode.BUSY, e.code());
        }
        first.close();
        awaitConnections(server, 1);
        client(server).ping(); // a slot is free again
    }

    @Test
    void aFloodOfRefusedConnectionsHoldsAtMost64AtOnce() throws Exception {
        Store store = store(Options.defaults().withSync(SyncPolicy.NEVER));
        Server server = track(Server.leader(store, Server.Config.loopback(0).withMaxConnections(1)));
        Client holder = client(server);
        holder.ping(); // takes the one slot
        List<Socket> flood = new ArrayList<>();
        try {
            long start = System.nanoTime();
            for (int i = 0; i < 150; i++) {
                Socket socket = new Socket();
                flood.add(socket);
                socket.connect(server.address(), 5_000);
                socket.setSoTimeout(5_000);
            }
            long floodMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            int busy = 0;
            int closedWithoutReply = 0;
            for (Socket socket : flood) {
                String line;
                try {
                    line = Protocol.readLine(new BufferedInputStream(socket.getInputStream()));
                } catch (IOException e) {
                    line = null; // reset
                }
                if (line != null && line.startsWith("ERR BUSY ")) {
                    busy++;
                } else {
                    assertTrue(line == null, line);
                    closedWithoutReply++;
                }
            }
            assertEquals(150, busy + closedWithoutReply);
            assertTrue(busy >= 1, "some were told");
            if (floodMillis < 900) { // each refusal holds its socket for up to a second, so the cap holds within one
                assertTrue(busy <= 64, busy + " refused with a reply at once, in " + floodMillis + " ms");
                assertTrue(closedWithoutReply >= 150 - 64, closedWithoutReply + " closed at once");
            }
        } finally {
            for (Socket socket : flood) {
                socket.close();
            }
        }
        holder.close();
        awaitConnections(server, 0);
        client(server).ping(); // the server still serves
    }

    @Test
    void concurrentClientsGetDistinctSequenceNumbers() throws Exception {
        Store store = store(Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(64 << 10));
        Server server = leader(store);
        int clients = 8;
        int each = 500;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        try {
            List<Future<List<Long>>> results = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                int id = c;
                results.add(pool.submit(() -> {
                    List<Long> seqs = new ArrayList<>();
                    try (Client client = Client.connect(server.address())) {
                        for (int i = 0; i < each; i++) {
                            seqs.add(client.put(utf8(id + ":" + i), utf8("v" + i)));
                        }
                    }
                    return seqs;
                }));
            }
            Set<Long> all = new HashSet<>();
            for (Future<List<Long>> result : results) {
                List<Long> seqs = result.get(60, TimeUnit.SECONDS);
                for (int i = 1; i < seqs.size(); i++) {
                    assertTrue(seqs.get(i) > seqs.get(i - 1), "one client's writes are ordered");
                }
                all.addAll(seqs);
            }
            assertEquals(clients * each, all.size());
            assertEquals(1L, all.stream().mapToLong(Long::longValue).min().orElseThrow());
            assertEquals((long) clients * each, all.stream().mapToLong(Long::longValue).max().orElseThrow());
            Client reader = client(server);
            for (int c = 0; c < clients; c++) {
                assertArrayEquals(utf8("v" + (each - 1)), reader.get(utf8(c + ":" + (each - 1))).orElseThrow());
            }
            assertTrue(store.stats().flushes() > 0, "the writes flushed while being served");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void storeErrorsAreReplies() throws IOException {
        Store store = store(Options.defaults().withSync(SyncPolicy.NEVER).withMaxLogBytes(200));
        Client client = client(leader(store));
        client.put(utf8("a"), new byte[100]);
        ServerException full = assertThrows(ServerException.class, () -> client.put(utf8("b"), new byte[100]));
        assertEquals(ErrorCode.FULL, full.code());
        assertEquals(2, client.put(utf8("c"), new byte[0]), "nothing was written, and the store goes on");

        store.close();
        ServerException closed = assertThrows(ServerException.class, () -> client.get(utf8("a")));
        assertEquals(ErrorCode.INTERNAL, closed.code());
        assertEquals(ErrorCode.INTERNAL, assertThrows(ServerException.class, () -> client.put(utf8("d"), utf8("x"))).code());
    }

    @Test
    void aClientThatLeavesMidRequestWritesNothing() throws IOException {
        Store store = store(Options.defaults().withSync(SyncPolicy.NEVER));
        Server server = leader(store);
        try (Socket socket = new Socket()) {
            socket.connect(server.address(), 5_000);
            socket.getOutputStream().write("PUT 3 5\nabcde".getBytes(StandardCharsets.US_ASCII)); // 2 bytes short
        }
        awaitConnections(server, 0);
        assertEquals(0, store.lastSequence());
    }

    @Test
    void closeEndsConnectionsAndLeavesTheStoreOpen() throws IOException {
        Store store = store(Options.defaults().withSync(SyncPolicy.NEVER));
        Server server = leader(store);
        Client client = client(server);
        client.put(utf8("a"), utf8("1"));
        InetSocketAddress address = server.address();
        server.close();
        assertThrows(IOException.class, client::ping);
        assertThrows(IOException.class, () -> Client.connect(address).ping());
        assertArrayEquals(utf8("1"), store.get(utf8("a")).orElseThrow());
        assertEquals(2, store.put(utf8("b"), utf8("2")));
    }

    private static void awaitConnections(Server server, int n) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (server.connections() != n) {
            assertTrue(System.nanoTime() < deadline, "connections: " + server.connections() + ", expected " + n);
            Thread.onSpinWait();
        }
    }
}
