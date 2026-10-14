package io.github.vivekdavara.keystore.bench;

import io.github.vivekdavara.keystore.Bytes;
import io.github.vivekdavara.keystore.Options;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.SyncPolicy;
import io.github.vivekdavara.keystore.memtable.MemTable;
import io.github.vivekdavara.keystore.memtable.SkipList;
import io.github.vivekdavara.keystore.wal.RecoveryMode;
import io.github.vivekdavara.keystore.wal.WalFormat;
import io.github.vivekdavara.keystore.wal.WalReader;
import io.github.vivekdavara.keystore.wal.WalRecord;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentSkipListMap;
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
