package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reads running alongside a writer that flushes and merges all the time. Write i puts key
 * {@code k(i % KEYS)} = {@code "i"} and then publishes i + 1 through a volatile counter, so a reader
 * that sees the counter at n knows the newest write of each key below n, and that every value it
 * reads must be that one or newer, never older (a stale read: a key found in an older table, or
 * missed in the frozen memtable) and never missing. Readers also must never see a closed table:
 * versions keep a table a merge dropped open until the last read in it is done.
 */
class StoreConcurrencyTest {
    private static final int KEYS = 2_000;
    private static final int WRITES = 30_000;
    private static final int READERS = 4;

    @TempDir
    Path dir;

    /** The newest write of key {@code k} among writes 0..n-1, or -1 if there is none. */
    private static long newestWrite(int k, long n) {
        if (n <= k) {
            return -1;
        }
        return n - 1 - Math.floorMod(n - 1 - k, KEYS);
    }

    @Test
    @Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void readsNeverMissOrGoBackWhileFlushesAndMergesRun() throws Exception {
        Path fds = Path.of("/dev/fd");
        long fdsBefore = Files.isDirectory(fds) ? count(fds) : -1;
        Options options = Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(4096).withMergeWidth(3);
        AtomicLong published = new AtomicLong();
        AtomicBoolean done = new AtomicBoolean();
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        AtomicLong reads = new AtomicLong();
        AtomicLong fromTables = new AtomicLong();

        try (Store store = Store.open(dir, options)) {
            List<Thread> readers = new ArrayList<>();
            for (int r = 0; r < READERS; r++) {
                long seed = r;
                Thread reader = new Thread(() -> {
                    SplittableRandom random = new SplittableRandom(seed);
                    try {
                        while (!done.get()) {
                            long n = published.get();
                            if (n == 0) {
                                continue;
                            }
                            int k = random.nextInt((int) Math.min(n, KEYS));
                            long newest = newestWrite(k, n);
                            Optional<byte[]> value = store.get(utf8("k" + k));
                            long after = published.get();
                            if (value.isEmpty()) {
                                throw new AssertionError("k" + k + " missing; write " + newest + " was published");
                            }
                            long i = Long.parseLong(new String(value.get(), StandardCharsets.UTF_8));
                            if (i % KEYS != k || i < newest || i >= after + 1) {
                                throw new AssertionError("k" + k + " = write " + i + "; published " + n
                                        + " before the read (newest write of the key " + newest + "), " + after + " after");
                            }
                            reads.incrementAndGet();
                            if (n - newest > 400) {
                                fromTables.incrementAndGet(); // old enough to have been flushed, roughly
                            }
                        }
                    } catch (Throwable t) {
                        failures.add(t);
                    }
                }, "reader-" + r);
                reader.start();
                readers.add(reader);
            }
            try {
                for (int i = 0; i < WRITES && failures.isEmpty(); i++) {
                    store.put(utf8("k" + (i % KEYS)), utf8(Long.toString(i)));
                    published.set(i + 1);
                }
            } finally {
                done.set(true);
                for (Thread reader : readers) {
                    reader.join();
                }
            }
            if (!failures.isEmpty()) {
                AssertionError error = new AssertionError(failures.size() + " reader failures; the first: " + failures.peek());
                failures.forEach(error::addSuppressed);
                throw error;
            }
            System.out.printf("concurrency: %d writes, %d reads (%d of keys written 400+ writes earlier), %d tables at"
                    + " the end, next file number %d%n", WRITES, reads.get(), fromTables.get(), store.tables().size(),
                    store.manifest().nextFileNumber());
            assertTrue(store.manifest().nextFileNumber() > 150, "enough flushes and merges happened");
            assertTrue(reads.get() > 10_000, "the readers ran: " + reads.get());

            for (int k = 0; k < KEYS; k++) {
                assertEquals(Optional.of(Long.toString(newestWrite(k, WRITES))),
                        store.get(utf8("k" + k)).map(v -> new String(v, StandardCharsets.UTF_8)), "k" + k);
            }
            // Every table a merge dropped was deleted once no read was in it: only live ones remain.
            List<String> live = store.tables().stream().map(t -> Store.tableName(t.number())).sorted().toList();
            assertEquals(live, files(".sst"));
        }
        if (fdsBefore >= 0) { // where /dev/fd exists (Linux, macOS)
            long fdsAfter = count(fds);
            assertTrue(fdsAfter - fdsBefore < 5, "file descriptors went from " + fdsBefore + " to " + fdsAfter
                    + ": a table's channel was never closed");
        }
    }

    private List<String> files(String suffix) throws IOException {
        try (Stream<Path> list = Files.list(dir)) {
            return list.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(suffix)).sorted().toList();
        }
    }

    private static long count(Path fds) throws IOException {
        try (Stream<Path> open = Files.list(fds)) {
            return open.count();
        }
    }
}
