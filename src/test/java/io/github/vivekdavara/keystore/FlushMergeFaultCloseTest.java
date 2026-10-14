package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * From the day-2 review: a fault at each single write or force of a flush and of a merge, then
 * close() (not a crash image), then a reopen without faults, checked against a model of the writes
 * that returned, under both sync policies, with the fault once (later I/O succeeds) or sticky
 * (every later write and force fails too).
 *
 * <p>No force here reaches the device: they are recorded (or skipped, outside the injector), since
 * nothing cuts the power and each real one costs about 3 ms on macOS. What is checked is the
 * store's logic: which forces it calls, what close() says, and what a reopen finds.
 *
 * <p>close() is judged by an oracle of what was forced: under EVERY_WRITE every acknowledged write
 * was forced on its own; under NEVER the writes acknowledged in this session are on the device only
 * if a force of their log's channel succeeded after them. close() must throw StoreFailedException
 * exactly when they may not be.
 */
class FlushMergeFaultCloseTest {
    @TempDir
    Path tmp;

    private static final int KEYS = 40;
    private static final long HUGE = 1L << 40;
    private static final String LATE = "late";

    enum Op { FLUSH, COMPACT_ALL, PUT_THAT_FLUSHES, COMPACT_ALL_TOMBSTONES }

    /** Once armed, every write and force on every channel counts; the failAt-th fails (and every later one, if sticky). */
    private static final class Injector {
        private boolean armed;
        private int count;
        private int failAt = Integer.MAX_VALUE;
        private boolean sticky;
        private String failed;
        private final List<String> trace = new ArrayList<>();
        private final Map<String, Integer> forcesOk = new HashMap<>();

        synchronized void arm(int failAt, boolean sticky) {
            this.armed = true;
            this.count = 0;
            this.failAt = failAt;
            this.sticky = sticky;
            this.failed = null;
            this.trace.clear();
        }

        synchronized void disarm() {
            armed = false;
        }

        synchronized boolean fails(String step) {
            if (!armed) {
                return false;
            }
            count++;
            trace.add(step);
            if (count == failAt) {
                failed = step;
                return true;
            }
            return sticky && count > failAt;
        }

        synchronized void forced(String name) {
            forcesOk.merge(name, 1, Integer::sum);
        }

        synchronized int forcesOf(String name) {
            return forcesOk.getOrDefault(name, 0);
        }

        synchronized int count() {
            return count;
        }

        synchronized String failed() {
            return failed;
        }

        synchronized List<String> trace() {
            return List.copyOf(trace);
        }
    }

    private static final class Channel extends FaultyChannel {
        private final String name;
        private final Injector injector;

        Channel(FileChannel inner, String name, Injector injector) {
            super(inner);
            this.name = name;
            this.injector = injector;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            if (injector.fails(name + " write")) {
                failWritesAfter(src.remaining() / 2); // torn: half of it reaches the file
            }
            return super.write(src);
        }

        /**
         * Recorded, not performed: nothing here cuts the power, so what matters is which forces were
         * called and succeeded, and on macOS each real one costs about 3 ms (F_FULLFSYNC), which made
         * the sweeps take 45 seconds.
         */
        @Override
        public void force(boolean metaData) throws IOException {
            if (injector.fails(name + " force")) {
                throw new IOException("injected: force failed (" + name + ")");
            }
            injector.forced(name);
        }
    }

    /** For stores opened outside the injector: forces skipped, everything else real. */
    private static final BiFunction<Path, FileChannel, FileChannel> NO_FORCE = (path, inner) -> new FaultyChannel(inner) {
        @Override
        public void force(boolean metaData) {
        }
    };

    private static String nameOf(Path path) {
        return Files.isDirectory(path) ? "dir" : String.valueOf(path.getFileName());
    }

    private record Result(int ops, long logSizeAfterPhase3, String line) {}

    /**
     * Found by review: under NEVER, the rotation forces the old log (every acknowledged write is now
     * on the device), then creating the new log fails at its directory force. close() threw
     * StoreFailedException, because closeLog saw the old log closed and took that to mean it was never
     * forced. It must return: nothing acknowledged is at risk.
     */
    @Test
    @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void closeReturnsWhenTheRotationForcedEveryWriteAndTheNewLogFailed() throws IOException {
        Path dir = tmp.resolve("focused");
        Injector injector = new Injector();
        Options options = Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(HUGE);
        Store store = Store.open(dir, options, (path, inner) -> new Channel(inner, nameOf(path), injector));
        store.put(utf8("a"), utf8("1"));
        int before = injector.forcesOf(Store.logName(1));
        injector.arm(2, false); // step 1: the old log's force at rotation; step 2: the new log's directory force
        IOException failed = assertThrows(IOException.class, store::flush);
        assertEquals("injected: force failed (dir)", failed.getMessage());
        assertEquals(List.of(Store.logName(1) + " force", "dir force"), injector.trace());
        assertEquals(before + 1, injector.forcesOf(Store.logName(1)), "write #1 was forced at the rotation");
        assertDoesNotThrow(store::close, "every acknowledged write is on the device");
        try (Store again = Store.open(dir, options, NO_FORCE)) {
            assertEquals(1, again.lastSequence());
        }
    }

    @Test
    @Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aFaultAtEveryStepOfAFlushThenCloseAndReopen() throws IOException {
        sweep(Op.FLUSH);
    }

    @Test
    @Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aFaultAtEveryStepOfCompactAllThenCloseAndReopen() throws IOException {
        sweep(Op.COMPACT_ALL);
    }

    @Test
    @Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aFaultAtEveryStepOfAPutThatFlushesThenCloseAndReopen() throws IOException {
        sweep(Op.PUT_THAT_FLUSHES);
    }

    @Test
    @Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aFaultAtEveryStepOfAMergeWithNoOutputThenCloseAndReopen() throws IOException {
        sweep(Op.COMPACT_ALL_TOMBSTONES);
    }

    private void sweep(Op op) throws IOException {
        List<String> problems = new ArrayList<>();
        TreeSet<String> steps = new TreeSet<>();
        for (SyncPolicy policy : SyncPolicy.values()) {
            long threshold = HUGE;
            if (op == Op.PUT_THAT_FLUSHES) {
                threshold = run(op, policy, Integer.MAX_VALUE, false, HUGE, op + "-" + policy + "-size", problems)
                        .logSizeAfterPhase3();
            }
            int total = run(op, policy, Integer.MAX_VALUE, false, threshold, op + "-" + policy + "-dry", problems).ops();
            assertTrue(total > 2, op + " " + policy + ": only " + total + " I/O steps");
            for (boolean sticky : new boolean[] {false, true}) {
                for (int failAt = 1; failAt <= total; failAt++) {
                    Result r = run(op, policy, failAt, sticky, threshold,
                            op + "-" + policy + "-" + (sticky ? "sticky" : "once") + "-" + failAt, problems);
                    steps.add(r.line());
                }
            }
        }
        System.out.println(op + ": " + steps.size() + " distinct outcomes");
        for (String s : steps) {
            System.out.println("  " + s);
        }
        assertTrue(problems.isEmpty(), op + ": " + problems.size() + " problems:\n  " + String.join("\n  ", problems));
    }

    private Result run(Op op, SyncPolicy policy, int failAt, boolean sticky, long putFlushBytes, String label,
            List<String> problems) throws IOException {
        Path dir = tmp.resolve(label);
        Injector injector = new Injector();
        BiFunction<Path, FileChannel, FileChannel> wrap = (path, inner) -> new Channel(inner, nameOf(path), injector);
        Map<String, String> model = new TreeMap<>();
        long acknowledged = 0;

        // Setup session: tables and log records, NEVER, closed cleanly.
        Options setup = Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(HUGE).withMergeWidth(64)
                .withBlockBytes(256);
        try (Store store = Store.open(dir, setup, NO_FORCE)) {
            for (int i = 0; i < KEYS; i++) {
                put(store, model, "k" + i, "a" + i + "-" + "x".repeat(i % 7));
                acknowledged++;
            }
            store.flush();
            if (op == Op.COMPACT_ALL_TOMBSTONES) {
                for (int i = 0; i < KEYS; i++) {
                    delete(store, model, "k" + i);
                    acknowledged++;
                }
                store.flush();
            } else {
                for (int i = 0; i < KEYS; i++) {
                    if (i % 2 == 0) {
                        put(store, model, "k" + i, "b" + i);
                        acknowledged++;
                    } else if (i % 4 == 1) {
                        delete(store, model, "k" + i);
                        acknowledged++;
                    }
                }
                if (op == Op.COMPACT_ALL) {
                    store.flush();
                }
            }
        }

        boolean flushes = op == Op.FLUSH || op == Op.PUT_THAT_FLUSHES;
        Options options = Options.defaults().withSync(policy)
                .withFlushLogBytes(op == Op.PUT_THAT_FLUSHES ? putFlushBytes : HUGE)
                .withMergeWidth(flushes ? 2 : 64).withBlockBytes(256);
        Store store = Store.open(dir, options, wrap);
        String closeResult;
        String opResult;
        boolean lateUnknown = false;
        String oldLog;
        int forcesBefore;
        long logSize;
        try {
            // Phase 3: writes acknowledged in this session, in its log, not forced under NEVER.
            for (int i = 0; i < 8; i++) {
                put(store, model, "k" + (3 * i), "c" + i);
                delete(store, model, "k" + (3 * i + 1));
                acknowledged += 2;
            }
            logSize = store.logSize();
            oldLog = newestLog(dir);
            forcesBefore = injector.forcesOf(oldLog);

            injector.arm(failAt, sticky);
            IOException failure = null;
            try {
                switch (op) {
                    case FLUSH -> store.flush();
                    case COMPACT_ALL, COMPACT_ALL_TOMBSTONES -> store.compactAll();
                    case PUT_THAT_FLUSHES -> {
                        store.put(utf8(LATE), utf8("L"));
                        model.put(LATE, "L");
                        acknowledged++;
                    }
                }
            } catch (IOException e) {
                failure = e;
            }
            if (failure == null) {
                int ops = injector.count();
                store.close();
                if (failAt != Integer.MAX_VALUE) {
                    problems.add(label + ": no fault reached the operation (" + ops + " steps)");
                }
                return new Result(ops, logSize, op + " " + policy + " no fault");
            }
            String step = injector.failed();
            opResult = failure.getClass().getSimpleName();
            if (op == Op.PUT_THAT_FLUSHES && step != null && step.contains(".log ") && !step.startsWith(oldLog)) {
                lateUnknown = true; // the put's own append or force failed: its record may be in the log
            }

            // Reads still answer from the failed store, as the model says.
            String reads = checkReads(store, model, false, "before close");
            if (reads != null) {
                problems.add(label + " [" + step + "]: " + reads);
            }
            // The manifest on disk names only files that exist; note divergence from the in-memory one.
            Manifest onDisk = Manifest.read(dir);
            String missing = missingTables(dir, onDisk);
            if (missing != null) {
                problems.add(label + " [" + step + "]: before close, " + missing);
            }
            boolean diverged = !Objects.equals(onDisk, store.manifest());

            // Later writes, flushes and compactions refuse cleanly and change nothing.
            Map<String, byte[]> snapshot = snapshot(dir);
            for (String what : new String[] {"put", "delete", "flush", "compactAll"}) {
                try {
                    switch (what) {
                        case "put" -> store.put(utf8("k0"), utf8("zzz"));
                        case "delete" -> store.delete(utf8("k2"));
                        case "flush" -> store.flush();
                        default -> store.compactAll();
                    }
                    problems.add(label + " [" + step + "]: a " + what + " after the failure succeeded");
                } catch (StoreFailedException expected) {
                    // as it should
                } catch (IOException | RuntimeException e) {
                    problems.add(label + " [" + step + "]: a " + what + " after the failure threw " + e);
                }
            }
            if (!sameFiles(snapshot, snapshot(dir))) {
                problems.add(label + " [" + step + "]: refused operations changed the directory");
            }

            // close(), judged by what was forced.
            try {
                store.close();
                closeResult = "returned";
            } catch (StoreFailedException e) {
                closeResult = "StoreFailedException";
            } catch (IOException e) {
                closeResult = "IOException(" + e.getMessage() + ")";
            }
            boolean durable = policy == SyncPolicy.EVERY_WRITE || injector.forcesOf(oldLog) > forcesBefore;
            if (durable && closeResult.equals("StoreFailedException")) {
                problems.add(label + " [" + step + "]: close threw StoreFailedException, but every acknowledged"
                        + " write was forced (" + oldLog + " forced " + (injector.forcesOf(oldLog) - forcesBefore)
                        + "x after the session's writes); trace " + injector.trace());
            }
            if (!durable && closeResult.equals("returned")) {
                problems.add(label + " [" + step + "]: close returned, but no force of " + oldLog
                        + " succeeded after the session's acknowledged writes; trace " + injector.trace());
            }
            injector.disarm();
            String after = missingTables(dir, Manifest.read(dir));
            if (after != null) {
                problems.add(label + " [" + step + "]: after close, " + after);
            }
            String line = op + " " + policy + " " + (sticky ? "sticky" : "once") + " @" + stepKind(step, oldLog)
                    + " -> op " + opResult + ", close " + closeResult.replaceAll("\\(.*", "(...)")
                    + (diverged ? ", manifest in memory != on disk" : "");

            // Reopen without faults: exactly the model.
            reopenAndCheck(dir, options, model, acknowledged, lateUnknown, label + " [" + step + "]", problems);
            return new Result(injector.count(), logSize, line);
        } catch (IOException | RuntimeException | Error e) {
            try {
                store.close();
            } catch (IOException | RuntimeException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /** The step's kind, with the old log told apart from the new one. */
    private static String stepKind(String step, String oldLog) {
        if (step == null) {
            return "none";
        }
        if (step.startsWith(oldLog)) {
            return "old log" + step.substring(oldLog.length());
        }
        if (step.contains(".log")) {
            return "new log" + step.substring(step.indexOf(' '));
        }
        if (step.contains(".sst")) {
            return "table" + step.substring(step.indexOf(' '));
        }
        return step;
    }

    private static void reopenAndCheck(Path dir, Options options, Map<String, String> model, long acknowledged,
            boolean lateUnknown, String label, List<String> problems) throws IOException {
        Store store;
        try {
            store = Store.open(dir, options, NO_FORCE);
        } catch (IOException | RuntimeException e) {
            problems.add(label + ": the reopen failed: " + e);
            return;
        }
        try {
            boolean lateThere = store.get(utf8(LATE)).isPresent();
            Map<String, String> expected = new TreeMap<>(model);
            if (lateUnknown && lateThere) {
                expected.put(LATE, "L");
            }
            long seq = acknowledged + (lateUnknown && lateThere ? 1 : 0);
            if (store.lastSequence() != seq) {
                problems.add(label + ": reopened with lastSequence " + store.lastSequence() + ", expected " + seq);
            }
            String reads = checkReads(store, expected, true, "after reopen");
            if (reads != null) {
                problems.add(label + ": " + reads);
            }
            // The store works on: more writes, a flush and a full merge, and they survive a reopen.
            store.put(utf8("k1"), utf8("after"));
            expected.put("k1", "after");
            store.flush();
            store.compactAll();
            store.close();
            try (Store again = Store.open(dir, options, NO_FORCE)) {
                String readsAgain = checkReads(again, expected, true, "after a flush, compactAll and reopen");
                if (readsAgain != null) {
                    problems.add(label + ": " + readsAgain);
                }
            }
        } catch (IOException | RuntimeException e) {
            problems.add(label + ": after reopen: " + e);
            try {
                store.close();
            } catch (IOException | RuntimeException ignored) {
                // reported above
            }
        }
    }

    private static String checkReads(Store store, Map<String, String> model, boolean includeLate, String when)
            throws IOException {
        List<String> wrong = new ArrayList<>();
        for (int i = 0; i < KEYS; i++) {
            String key = "k" + i;
            Optional<String> got = string(store.get(utf8(key)));
            if (!got.equals(Optional.ofNullable(model.get(key)))) {
                wrong.add(key + "=" + got.orElse("<none>") + " (expected " + model.getOrDefault(key, "<none>") + ")");
            }
        }
        Optional<String> late = string(store.get(utf8(LATE)));
        if (!late.equals(Optional.ofNullable(model.get(LATE))) && (includeLate || late.isPresent())) {
            wrong.add(LATE + "=" + late.orElse("<none>"));
        }
        return wrong.isEmpty() ? null : when + ", reads wrong: " + wrong;
    }

    private static String missingTables(Path dir, Manifest manifest) {
        if (manifest == null) {
            return "no MANIFEST";
        }
        List<Long> missing = new ArrayList<>();
        for (Manifest.TableRef ref : manifest.tables()) {
            if (!Files.exists(dir.resolve(Store.tableName(ref.number())))) {
                missing.add(ref.number());
            }
        }
        return missing.isEmpty() ? null : "the manifest names missing tables " + missing;
    }

    private static String newestLog(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(Store.LOG_SUFFIX))
                    .max(String::compareTo).orElseThrow();
        }
    }

    private static Map<String, byte[]> snapshot(Path dir) throws IOException {
        Map<String, byte[]> files = new TreeMap<>();
        try (Stream<Path> list = Files.list(dir)) {
            for (Path file : (Iterable<Path>) list::iterator) {
                files.put(file.getFileName().toString(), Files.readAllBytes(file));
            }
        }
        return files;
    }

    private static boolean sameFiles(Map<String, byte[]> a, Map<String, byte[]> b) {
        if (!a.keySet().equals(b.keySet())) {
            return false;
        }
        for (Map.Entry<String, byte[]> e : a.entrySet()) {
            if (!Arrays.equals(e.getValue(), b.get(e.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private static void put(Store store, Map<String, String> model, String key, String value) throws IOException {
        store.put(utf8(key), utf8(value));
        model.put(key, value);
    }

    private static void delete(Store store, Map<String, String> model, String key) throws IOException {
        store.delete(utf8(key));
        model.remove(key);
    }

    private static Optional<String> string(Optional<byte[]> value) {
        return value.map(v -> new String(v, StandardCharsets.UTF_8));
    }
}
