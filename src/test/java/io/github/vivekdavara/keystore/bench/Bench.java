package io.github.vivekdavara.keystore.bench;

import io.github.vivekdavara.keystore.Bytes;
import io.github.vivekdavara.keystore.Options;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.SyncPolicy;
import io.github.vivekdavara.keystore.memtable.MemTable;
import io.github.vivekdavara.keystore.memtable.SkipList;
import io.github.vivekdavara.keystore.server.Client;
import io.github.vivekdavara.keystore.server.Follower;
import io.github.vivekdavara.keystore.server.Protocol;
import io.github.vivekdavara.keystore.server.Server;
import io.github.vivekdavara.keystore.wal.RecoveryMode;
import io.github.vivekdavara.keystore.wal.WalFormat;
import io.github.vivekdavara.keystore.wal.WalReader;
import io.github.vivekdavara.keystore.wal.WalRecord;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * The measurements in the README, run by {@code scripts/bench.sh}. Plain wall-clock timings, not
 * JMH (that's day 4): the first run of each is a warm-up and is thrown away, and each line reports
 * one run; the script runs several and the README gives medians.
 *
 * <pre>
 * writes SYNC COUNT VALUE_BYTES   puts of 16-byte keys into a new store; throughput and latency
 * recover COUNT VALUE_BYTES       time to open a store whose one log holds COUNT puts (no flush)
 * fill COUNT VALUE_BYTES          COUNT puts in random key order under NEVER with the default flush
 *                                 and merges: throughput, latency, bytes written to tables, reopen
 * reads COUNT VALUE_BYTES BITS    gets of present and absent keys once COUNT keys are in tables
 *                                 with Bloom filters of BITS bits a key
 * scan TAIL_BYTES zeros|random    time to read a log of one record and a damaged tail
 * memory COUNT VALUE_BYTES        heap bytes per key in a memtable, against approximateBytes
 * skiplist COUNT                  inserts and lookups: this skip list against ConcurrentSkipListMap
 * server CONNECTIONS COUNT VALUE_BYTES
 *                                 COUNT puts then COUNT gets over loopback TCP, one request at a time
 *                                 on each of CONNECTIONS connections, to a leader under NEVER
 * catchup COUNT VALUE_BYTES SYNC  a new follower (its store under SYNC) catching up on a leader that
 *                                 holds COUNT puts: time, records/s, batches
 * lag COUNT VALUE_BYTES SYNC      one put at a time to a leader, timed until a follower (under SYNC)
 *                                 has applied it
 * pipeline DEPTH COUNT VALUE_BYTES
 *                                 COUNT puts then COUNT gets on one connection, DEPTH requests sent
 *                                 before their replies are read
 * </pre>
 */
public final class Bench {
    private Bench() {}

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "writes" -> writes(SyncPolicy.valueOf(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            case "recover" -> recover(Integer.parseInt(args[1]), Integer.parseInt(args[2]));
            case "fill" -> fill(Integer.parseInt(args[1]), Integer.parseInt(args[2]));
            case "reads" -> reads(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            case "scan" -> scan(Integer.parseInt(args[1]), args[2]);
            case "memory" -> memory(Integer.parseInt(args[1]), Integer.parseInt(args[2]));
            case "skiplist" -> skiplist(Integer.parseInt(args[1]));
            case "server" -> server(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            case "catchup" -> catchup(Integer.parseInt(args[1]), Integer.parseInt(args[2]), SyncPolicy.valueOf(args[3]));
            case "lag" -> lag(Integer.parseInt(args[1]), Integer.parseInt(args[2]), SyncPolicy.valueOf(args[3]));
            case "pipeline" -> pipeline(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            default -> throw new IllegalArgumentException("unknown benchmark " + args[0]);
        }
    }

    private static void writes(SyncPolicy sync, int count, int valueBytes) throws IOException {
        byte[] value = randomBytes(valueBytes, 1);
        for (int run = 0; run < 2; run++) { // run 0 warms up
            Path dir = Files.createTempDirectory("keystore-bench");
            long[] latencies = new long[count];
            long start;
            long end;
            try (Store store = Store.open(dir, Options.defaults().withSync(sync))) {
                start = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    long t0 = System.nanoTime();
                    store.put(key(i), value);
                    latencies[i] = System.nanoTime() - t0;
                }
                end = System.nanoTime();
            } finally {
                delete(dir);
            }
            if (run == 1) {
                Arrays.sort(latencies);
                System.out.printf(
                        "writes sync=%s count=%d value=%dB: %.0f writes/s, p50 %.1f us, p99 %.1f us, max %.1f us%n",
                        sync, count, valueBytes, count / seconds(start, end),
                        micros(latencies[count / 2]), micros(latencies[(int) (count * 0.99)]),
                        micros(latencies[count - 1]));
            }
        }
    }

    private static void recover(int count, int valueBytes) throws IOException {
        Path dir = Files.createTempDirectory("keystore-bench");
        try {
            byte[] value = randomBytes(valueBytes, 2);
            // No flush, so the log holds every write: this measures replaying a log, as on day 1.
            Options noFlush = Options.defaults().withFlushLogBytes(Long.MAX_VALUE);
            try (Store store = Store.open(dir, noFlush.withSync(SyncPolicy.NEVER))) {
                for (int i = 0; i < count; i++) {
                    store.put(key(i), value);
                }
            }
            long logBytes = Files.size(dir.resolve("000001.log"));
            for (int run = 0; run < 2; run++) {
                long start = System.nanoTime();
                try (Store store = Store.open(dir, noFlush)) {
                    long end = System.nanoTime();
                    if (store.lastSequence() != count) {
                        throw new AssertionError("recovered " + store.lastSequence() + " of " + count);
                    }
                    if (run == 1) {
                        double s = seconds(start, end);
                        System.out.printf(
                                "recover count=%d value=%dB log=%.1f MB: %.0f ms, %.0f records/s, %.0f MB/s%n",
                                count, valueBytes, logBytes / 1e6, s * 1e3, count / s, logBytes / 1e6 / s);
                    }
                }
            }
        } finally {
            delete(dir);
        }
    }

    private static void fill(int count, int valueBytes) throws IOException {
        byte[] value = randomBytes(valueBytes, 7);
        int[] order = shuffled(count, 8);
        Options options = Options.defaults().withSync(SyncPolicy.NEVER);
        for (int run = 0; run < 2; run++) { // run 0 warms up
            Path dir = Files.createTempDirectory("keystore-bench");
            try {
                long[] latencies = new long[count];
                long start;
                long end;
                Store.Stats stats;
                int tables;
                try (Store store = Store.open(dir, options)) {
                    start = System.nanoTime();
                    for (int i = 0; i < count; i++) {
                        long t0 = System.nanoTime();
                        store.put(key(order[i]), value);
                        latencies[i] = System.nanoTime() - t0;
                    }
                    end = System.nanoTime();
                    stats = store.stats();
                    tables = store.tables().size();
                }
                long t0 = System.nanoTime();
                try (Store store = Store.open(dir, options)) {
                    long t1 = System.nanoTime();
                    if (store.lastSequence() != count || store.get(key(order[count / 2])).isEmpty()) {
                        throw new AssertionError("reopened at " + store.lastSequence());
                    }
                    if (run == 1) {
                        Arrays.sort(latencies);
                        double user = (double) count * (16 + valueBytes);
                        System.out.printf("fill count=%d value=%dB: %.0f writes/s, p50 %.1f us, p99 %.1f us, p99.9 %.1f us,"
                                        + " max %.1f ms; %d flushes, %d merges, %d tables; table bytes written per byte of"
                                        + " keys and values %.2f (flushes %.2f, merges %.2f); reopen %.0f ms%n",
                                count, valueBytes, count / seconds(start, end), micros(latencies[count / 2]),
                                micros(latencies[(int) (count * 0.99)]), micros(latencies[(int) (count * 0.999)]),
                                latencies[count - 1] / 1e6, stats.flushes(), stats.merges(), tables,
                                (stats.flushedBytes() + stats.mergedBytes()) / user, stats.flushedBytes() / user,
                                stats.mergedBytes() / user, seconds(t0, t1) * 1e3);
                    }
                }
            } finally {
                delete(dir);
            }
        }
    }

    /**
     * COUNT keys (the even numbers, in random order) put and flushed, so every key is in a table;
     * then 200,000 gets of present keys and 200,000 of absent ones (the odd numbers, between them),
     * twice, the first pass a warm-up. Blocks a get counts the data blocks read, from the tables'
     * own counters. The page cache is warm: these are CPU and system-call costs, not disk reads.
     */
    private static void reads(int count, int valueBytes, int bits) throws IOException {
        byte[] value = randomBytes(valueBytes, 9);
        Options options = Options.defaults().withSync(SyncPolicy.NEVER).withBloomBitsPerKey(bits);
        Path dir = Files.createTempDirectory("keystore-bench");
        try {
            try (Store store = Store.open(dir, options)) {
                for (int i : shuffled(count, 10)) {
                    store.put(key(2 * i), value);
                }
                store.flush();
            }
            try (Store store = Store.open(dir, options)) {
                int gets = 200_000;
                int[] present = new int[gets];
                SplittableRandom random = new SplittableRandom(11);
                for (int i = 0; i < gets; i++) {
                    present[i] = random.nextInt(count);
                }
                for (int run = 0; run < 2; run++) {
                    String[] kinds = {"present", "absent"};
                    StringBuilder line = new StringBuilder();
                    for (String kind : kinds) {
                        long blocksBefore = blocksRead(store);
                        long[] latencies = new long[gets];
                        for (int i = 0; i < gets; i++) {
                            byte[] key = key(kind.equals("present") ? 2 * present[i] : 2 * present[i] + 1);
                            long t0 = System.nanoTime();
                            boolean found = store.get(key).isPresent();
                            latencies[i] = System.nanoTime() - t0;
                            if (found != kind.equals("present")) {
                                throw new AssertionError(kind + " key " + present[i]);
                            }
                        }
                        double blocks = (blocksRead(store) - blocksBefore) / (double) gets;
                        Arrays.sort(latencies);
                        line.append(String.format("; %s: p50 %.1f us, p99 %.1f us, %.3f blocks a get", kind,
                                micros(latencies[gets / 2]), micros(latencies[(int) (gets * 0.99)]), blocks));
                    }
                    if (run == 1) {
                        System.out.printf("reads count=%d value=%dB bits=%d: %d tables%s%n",
                                count, valueBytes, bits, store.tables().size(), line);
                    }
                }
            }
        } finally {
            delete(dir);
        }
    }

    private static long blocksRead(Store store) {
        long sum = 0;
        for (var table : store.tables()) {
            sum += table.blocksRead();
        }
        return sum;
    }

    private static void scan(int tailBytes, String kind) throws IOException {
        ByteBuffer first = WalFormat.encode(WalRecord.put(1, key(0), randomBytes(100, 3)));
        byte[] log = new byte[first.remaining() + tailBytes];
        first.get(log, 0, first.remaining());
        if (kind.equals("random")) {
            byte[] tail = randomBytes(tailBytes, 4);
            System.arraycopy(tail, 0, log, log.length - tailBytes, tailBytes);
        } else if (!kind.equals("zeros")) {
            throw new IllegalArgumentException("zeros or random, not " + kind);
        }
        for (int run = 0; run < 2; run++) {
            long start = System.nanoTime();
            var recovery = WalReader.read(log, log.length, 1, RecoveryMode.TOLERATE_TORN_TAIL, r -> {});
            long end = System.nanoTime();
            if (recovery.droppedBytes() != tailBytes) {
                throw new AssertionError(recovery.toString());
            }
            if (run == 1) {
                double s = seconds(start, end);
                System.out.printf("scan tail=%.1f MB of %s: %.0f ms, %.0f MB/s (%s)%n",
                        tailBytes / 1e6, kind, s * 1e3, tailBytes / 1e6 / s, recovery.outcome());
            }
        }
    }

    private static void memory(int count, int valueBytes) {
        byte[][] keys = new byte[count][];
        byte[][] values = new byte[count][];
        for (int i = 0; i < count; i++) {
            keys[i] = key(i);
            values[i] = new byte[valueBytes];
        }
        long before = usedHeap();
        MemTable table = new MemTable();
        for (int i = 0; i < count; i++) {
            table.put(i + 1, keys[i], values[i]);
        }
        long after = usedHeap();
        // The key and value arrays existed before; what's new is the memtable's own structure.
        double structure = (after - before) / (double) count;
        long arrays = arrayBytes(16) + arrayBytes(valueBytes);
        System.out.printf(
                "memory count=%d key=16B value=%dB: memtable structure %.1f bytes/key, with the arrays %.1f;"
                        + " approximateBytes charges %.1f%n",
                count, valueBytes, structure, structure + arrays, table.approximateBytes() / (double) count);
        if (table.size() != count) {
            throw new AssertionError(table.size());
        }
    }

    /**
     * Single-threaded inserts of COUNT keys in random order, then COUNT lookups of present keys in
     * another random order, into this skip list and into the JDK's ConcurrentSkipListMap with the
     * same comparator. Each structure is timed in its own pass, alternating, after a warm-up pass.
     */
    private static void skiplist(int count) {
        byte[][] keys = new byte[count][];
        for (int i = 0; i < count; i++) {
            keys[i] = key(i);
        }
        int[] insertOrder = shuffled(count, 5);
        int[] lookupOrder = shuffled(count, 6);
        for (int run = 0; run < 2; run++) {
            for (String which : new String[] {"SkipList", "ConcurrentSkipListMap"}) {
                BiConsumer<byte[], Integer> put;
                Function<byte[], Integer> get;
                if (which.equals("SkipList")) {
                    SkipList<byte[], Integer> list = new SkipList<>(Bytes.ORDER);
                    put = list::put;
                    get = list::get;
                } else {
                    ConcurrentSkipListMap<byte[], Integer> map = new ConcurrentSkipListMap<>(Bytes.ORDER);
                    put = map::put;
                    get = map::get;
                }
                long t0 = System.nanoTime();
                for (int i : insertOrder) {
                    put.accept(keys[i], i);
                }
                long t1 = System.nanoTime();
                long found = 0;
                for (int i : lookupOrder) {
                    found += get.apply(keys[i]) == i ? 1 : 0;
                }
                long t2 = System.nanoTime();
                if (found != count) {
                    throw new AssertionError(which + " found " + found);
                }
                if (run == 1) {
                    System.out.printf("skiplist %s count=%d: %.0f inserts/s, %.0f lookups/s%n",
                            which, count, count / seconds(t0, t1), count / seconds(t1, t2));
                }
            }
        }
    }

    /** One request on a client, for key number {@code i}. */
    private interface Request {
        void send(Client client, int i) throws IOException;
    }

    /**
     * The server's cost on loopback: a leader whose store doesn't force (NEVER), so a put's time is
     * the round trip, the server's parsing and the in-memory write. Each connection is a thread
     * sending one request and waiting for its reply; latency is that round trip.
     */
    private static void server(int connections, int count, int valueBytes) throws Exception {
        byte[] value = randomBytes(valueBytes, 11);
        for (int run = 0; run < 2; run++) { // run 0 warms up
            Path dir = Files.createTempDirectory("keystore-bench");
            try (Store store = Store.open(dir, Options.defaults().withSync(SyncPolicy.NEVER));
                    Server server = Server.leader(store, Server.Config.loopback(0))) {
                long[] puts = new long[count];
                long[] gets = new long[count];
                long putNanos = clients(server, connections, count, (client, i) -> client.put(key(i), value), puts);
                long getNanos = clients(server, connections, count, (client, i) -> {
                    if (client.get(key(i)).isEmpty()) {
                        throw new AssertionError("key " + i + " missing");
                    }
                }, gets);
                if (run == 1) {
                    Arrays.sort(puts);
                    Arrays.sort(gets);
                    System.out.printf("server connections=%d count=%d value=%dB: puts %.0f/s, p50 %.1f us, p99 %.1f us;"
                                    + " gets %.0f/s, p50 %.1f us, p99 %.1f us%n",
                            connections, count, valueBytes, count / (putNanos / 1e9), micros(puts[count / 2]),
                            micros(puts[(int) (count * 0.99)]), count / (getNanos / 1e9), micros(gets[count / 2]),
                            micros(gets[(int) (count * 0.99)]));
                }
            } finally {
                delete(dir);
            }
        }
    }

    /** Runs {@code count} requests split over {@code connections} threads; returns the wall time. */
    private static long clients(Server server, int connections, int count, Request request, long[] latencies)
            throws Exception {
        int each = count / connections;
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        List<Throwable> failures = new ArrayList<>();
        for (int c = 0; c < connections; c++) {
            int first = c * each;
            int last = c == connections - 1 ? count : first + each;
            Thread t = new Thread(() -> {
                try (Client client = Client.connect(server.address())) {
                    go.await();
                    for (int i = first; i < last; i++) {
                        long t0 = System.nanoTime();
                        request.send(client, i);
                        latencies[i] = System.nanoTime() - t0;
                    }
                } catch (Throwable e) {
                    synchronized (failures) {
                        failures.add(e);
                    }
                }
            });
            t.start();
            threads.add(t);
        }
        Thread.sleep(100); // let every client connect before the clock starts
        long start = System.nanoTime();
        go.countDown();
        for (Thread t : threads) {
            t.join();
        }
        long end = System.nanoTime();
        if (!failures.isEmpty()) {
            throw new AssertionError("a client failed", failures.get(0));
        }
        return end - start;
    }

    /**
     * Pipelining on one connection: DEPTH requests written before the first reply is read, so the
     * server finds them already buffered and answers them with one write (it flushes only when no
     * request is waiting). DEPTH 1 is the request-reply loop of {@code server 1}. Raw protocol, so
     * the client's own one-at-a-time API doesn't get in the way.
     */
    private static void pipeline(int depth, int count, int valueBytes) throws Exception {
        byte[] value = randomBytes(valueBytes, 14);
        for (int run = 0; run < 2; run++) { // run 0 warms up
            Path dir = Files.createTempDirectory("keystore-bench");
            try (Store store = Store.open(dir, Options.defaults().withSync(SyncPolicy.NEVER));
                    Server server = Server.leader(store, Server.Config.loopback(0));
                    Socket socket = new Socket()) {
                socket.connect(server.address(), 5_000);
                socket.setTcpNoDelay(true);
                OutputStream out = new BufferedOutputStream(socket.getOutputStream(), 1 << 20);
                InputStream in = new BufferedInputStream(socket.getInputStream(), 1 << 20);
                byte[] putLine = ("PUT 16 " + valueBytes + "\n").getBytes(StandardCharsets.US_ASCII);
                byte[] getLine = "GET 16\n".getBytes(StandardCharsets.US_ASCII);
                long t0 = System.nanoTime();
                for (int i = 0; i < count; i += depth) {
                    int n = Math.min(depth, count - i);
                    for (int j = 0; j < n; j++) {
                        out.write(putLine);
                        out.write(key(i + j));
                        out.write(value);
                    }
                    out.flush();
                    for (int j = 0; j < n; j++) {
                        String reply = Protocol.readLine(in);
                        if (reply == null || !reply.startsWith("OK ")) {
                            throw new AssertionError("put " + (i + j) + ": " + reply);
                        }
                    }
                }
                long t1 = System.nanoTime();
                for (int i = 0; i < count; i += depth) {
                    int n = Math.min(depth, count - i);
                    for (int j = 0; j < n; j++) {
                        out.write(getLine);
                        out.write(key(i + j));
                    }
                    out.flush();
                    for (int j = 0; j < n; j++) {
                        String reply = Protocol.readLine(in);
                        if (reply == null || !reply.equals("VALUE " + valueBytes)) {
                            throw new AssertionError("get " + (i + j) + ": " + reply);
                        }
                        Protocol.readExactly(in, valueBytes);
                    }
                }
                long t2 = System.nanoTime();
                if (run == 1) {
                    System.out.printf("pipeline depth=%d count=%d value=%dB: puts %.0f/s, gets %.0f/s%n",
                            depth, count, valueBytes, count / seconds(t0, t1), count / seconds(t1, t2));
                }
            } finally {
                delete(dir);
            }
        }
    }

    /**
     * A leader holding COUNT puts (in tables by now, every log kept), then a new follower whose
     * store forces under SYNC: the time until it has applied them all.
     */
    private static void catchup(int count, int valueBytes, SyncPolicy sync) throws Exception {
        byte[] value = randomBytes(valueBytes, 12);
        for (int run = 0; run < 2; run++) { // run 0 warms up
            Path leaderDir = Files.createTempDirectory("keystore-bench");
            Path followerDir = Files.createTempDirectory("keystore-bench");
            try (Store leader = Store.open(leaderDir,
                            Options.defaults().withSync(SyncPolicy.NEVER).withRetainLogBytes(Long.MAX_VALUE));
                    Server server = Server.leader(leader, Server.Config.loopback(0))) {
                for (int i = 0; i < count; i++) {
                    leader.put(key(i), value);
                }
                long logBytes = 0;
                try (Stream<Path> files = Files.list(leaderDir)) {
                    for (Path f : files.filter(f -> f.toString().endsWith(".log")).toList()) {
                        logBytes += Files.size(f);
                    }
                }
                try (Store store = Store.open(followerDir, Options.defaults().withSync(sync))) {
                    long t0 = System.nanoTime();
                    try (Follower follower = Follower.start(store, Follower.Config.of(server.address()))) {
                        if (!follower.awaitApplied(count, Duration.ofMinutes(10))) {
                            throw new AssertionError("caught up to " + follower.status());
                        }
                        long t1 = System.nanoTime();
                        Follower.Status status = follower.status();
                        if (run == 1) {
                            double s = seconds(t0, t1);
                            System.out.printf("catchup count=%d value=%dB sync=%s: %.0f ms, %.0f records/s, %.0f MB/s"
                                            + " of log, %d batches (%.0f records a batch), follower flushes %d%n",
                                    count, valueBytes, sync, s * 1e3, count / s, logBytes / 1e6 / s, status.batches(),
                                    (double) count / status.batches(), store.stats().flushes());
                        }
                    }
                }
            } finally {
                delete(leaderDir);
                delete(followerDir);
            }
        }
    }

    /**
     * One put at a time to a leader (in process, NEVER, so the leader's own force isn't counted),
     * each timed from the put's return until a follower whose store forces under SYNC has applied
     * it: what replication adds (the leader's tail waking, the read of its log, TCP, the follower's
     * checks and append, and its force under EVERY_WRITE).
     */
    private static void lag(int count, int valueBytes, SyncPolicy sync) throws Exception {
        byte[] value = randomBytes(valueBytes, 13);
        for (int run = 0; run < 2; run++) { // run 0 warms up
            Path leaderDir = Files.createTempDirectory("keystore-bench");
            Path followerDir = Files.createTempDirectory("keystore-bench");
            try (Store leader = Store.open(leaderDir,
                            Options.defaults().withSync(SyncPolicy.NEVER).withRetainLogBytes(64L << 20));
                    Server server = Server.leader(leader, Server.Config.loopback(0));
                    Store store = Store.open(followerDir, Options.defaults().withSync(sync));
                    Follower follower = Follower.start(store, Follower.Config.of(server.address()))) {
                long[] latencies = new long[count];
                for (int i = 0; i < count; i++) {
                    long seq = leader.put(key(i), value);
                    long t0 = System.nanoTime();
                    if (!follower.awaitApplied(seq, Duration.ofSeconds(30))) {
                        throw new AssertionError("not applied: " + follower.status());
                    }
                    latencies[i] = System.nanoTime() - t0;
                }
                if (run == 1) {
                    Arrays.sort(latencies);
                    System.out.printf("lag count=%d value=%dB sync=%s: p50 %.1f us, p99 %.1f us, max %.1f us%n",
                            count, valueBytes, sync, micros(latencies[count / 2]), micros(latencies[(int) (count * 0.99)]),
                            micros(latencies[count - 1]));
                }
            } finally {
                delete(leaderDir);
                delete(followerDir);
            }
        }
    }

    private static int[] shuffled(int n, long seed) {
        int[] order = new int[n];
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }
        SplittableRandom random = new SplittableRandom(seed);
        for (int i = n - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            int t = order[i];
            order[i] = order[j];
            order[j] = t;
        }
        return order;
    }

    /** A byte array's heap size on a 64-bit JVM: a 16-byte header, then the data, rounded up to 8. */
    private static long arrayBytes(int length) {
        return (16L + length + 7) / 8 * 8;
    }

    /** 16 bytes: "key-" and 12 digits. */
    static byte[] key(int i) {
        return String.format("key-%012d", i).getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] randomBytes(int n, long seed) {
        byte[] bytes = new byte[n];
        new SplittableRandom(seed).nextBytes(bytes);
        return bytes;
    }

    private static long usedHeap() {
        Runtime runtime = Runtime.getRuntime();
        for (int i = 0; i < 5; i++) {
            System.gc();
        }
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static double seconds(long start, long end) {
        return (end - start) / 1e9;
    }

    private static double micros(long nanos) {
        return nanos / 1e3;
    }

    private static void delete(Path dir) throws IOException {
        try (Stream<Path> paths = Files.walk(dir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}
