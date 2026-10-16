package io.github.vivekdavara.keystore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.wal.Recovery.Outcome;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Crash tests with real process deaths. A child JVM ({@link KillTarget}) writes to a store and dies
 * by SIGKILL, from this test at a random moment or by its own hand at a chosen file operation; then
 * the store is reopened here. Nothing is simulated: the child's JVM is gone, the kernel closed its
 * files and released its lock, and the page cache holds what it wrote, as after any process crash
 * (a power loss can lose more; see DESIGN.md, "Testing").
 *
 * <p>After each death the store must hold exactly the writes up to its last sequence number, which
 * must be the last write the child reported as returned, or the one after it (the write in flight
 * may have reached the log). Writes are {@link Workload}'s, a pure function of the sequence number,
 * so this test knows what the store must hold without having made the writes. The log flushes every
 * 4 KiB (16 KiB for random kills), so deaths land in flushes and merges too.
 */
class KillTest {
    private static final int KEYS = 300;
    /** The JVM running this test, for the children. */
    private static final String JAVA = ProcessHandle.current().info().command().orElseThrow();

    @TempDir
    Path tmp;

    /** A child process running {@link KillTarget}, with its stdout read as it arrives. */
    private static final class Child {
        private final Process process;
        private final Thread reader;
        private final Path err;
        private volatile long opened = -1;
        private volatile long lastAck;
        private volatile long acks;

        private Child(Process process, Path err) {
            this.process = process;
            this.err = err;
            this.reader = Thread.ofPlatform().daemon().start(this::read);
        }

        static Child start(Path dir, SyncPolicy sync, long seed, long flushLogBytes, String... kill) throws IOException {
            List<String> command = new ArrayList<>(List.of(JAVA, "-XX:TieredStopAtLevel=1", "-XX:+UseSerialGC",
                    "-cp", System.getProperty("java.class.path"), KillTarget.class.getName(),
                    dir.toString(), sync.name(), Long.toString(seed), Integer.toString(KEYS),
                    Long.toString(flushLogBytes)));
            command.addAll(List.of(kill));
            Path err = Files.createTempFile(dir.getParent(), "child", ".err");
            return new Child(new ProcessBuilder(command).redirectError(err.toFile()).start(), err);
        }

        /** Lines as they arrive; a last line without its newline (a print cut off) is ignored. */
        private void read() {
            try (InputStream in = process.getInputStream()) {
                StringBuilder line = new StringBuilder();
                int b;
                while ((b = in.read()) >= 0) {
                    if (b != '\n') {
                        line.append((char) b);
                        continue;
                    }
                    String s = line.toString();
                    line.setLength(0);
                    if (s.startsWith("opened ")) {
                        opened = Long.parseLong(s.substring(7));
                    } else if (s.startsWith("ack ")) {
                        lastAck = Long.parseLong(s.substring(4));
                        acks++;
                    } else {
                        throw new IllegalStateException("not a line KillTarget prints: " + s);
                    }
                }
            } catch (IOException ignored) {
                // the child is gone
            }
        }

        /** Waits for the child to die (by its own hand); false if it was still alive at the deadline. */
        boolean awaitDeath(long seconds) throws InterruptedException {
            return process.waitFor(seconds, TimeUnit.SECONDS);
        }

        /**
         * SIGKILLs the child, which must still be running. With kill(1), not destroyForcibly(),
         * which also closes the streams, and the acks still in the pipe would be lost.
         */
        void kill() throws InterruptedException, IOException {
            assertTrue(process.isAlive(), () -> "the child died before it was killed: " + stderr());
            Process kill = new ProcessBuilder("kill", "-KILL", Long.toString(process.pid())).inheritIO().start();
            assertTrue(kill.waitFor(30, TimeUnit.SECONDS) && kill.exitValue() == 0, "kill(1) ran");
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "the child died");
        }

        /** After its death: waits until its output is all read, and checks it died of SIGKILL. */
        void checkKilled() throws InterruptedException {
            reader.join(30_000);
            assertTrue(!reader.isAlive(), "the child's output was read to its end");
            assertEquals(128 + 9, process.exitValue(), () -> "exit status of a SIGKILL; stderr: " + stderr());
        }

        String stderr() {
            try {
                return Files.readString(err);
            } catch (IOException e) {
                return "(stderr unreadable: " + e + ")";
            }
        }
    }

    /**
     * The test kills the child at a random moment, 20 times a sync policy. Most of the time the
     * child is in a force (a flush under NEVER, every write under EVERY_WRITE), so that is where
     * most deaths land.
     */
    @ParameterizedTest(name = "{0}")
    @EnumSource(SyncPolicy.class)
    @Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aWriterKilledAtRandomMomentsLosesNoAcknowledgedWrite(SyncPolicy sync) throws Exception {
        Path dir = tmp.resolve("store");
        Workload workload = new Workload(7, KEYS);
        Workload.Model model = new Workload.Model(workload);
        SplittableRandom random = new SplittableRandom(17 + sync.ordinal());
        Options options = Options.defaults().withSync(sync).withFlushLogBytes(16 << 10);
        Map<String, Integer> found = new TreeMap<>();
        long acknowledged = 0;
        int cycles = 20;
        for (int cycle = 0; cycle < cycles; cycle++) {
            Child child = Child.start(dir, sync, 7, options.flushLogBytes());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (child.opened < 0) { // started and recovered: from here on it is writing
                assertTrue(System.nanoTime() < deadline && child.process.isAlive(), () -> "no open: " + child.stderr());
                Thread.sleep(1);
            }
            assertEquals(model.seq(), child.opened, "the child opened the store where the last check left it");
            Thread.sleep(random.nextInt(50, 400));
            child.kill();
            child.checkKilled();
            long acked = child.acks > 0 ? child.lastAck : model.seq();
            assertTrue(acked >= acknowledged, "acks only go up");
            acknowledged = acked;
            found.merge(where(dir), 1, Integer::sum);
            found.merge(check(dir, options, model, acked), 1, Integer::sum);
        }
        System.out.printf("kill tests, %s, random moments: %d deaths, %d acknowledged writes; %s%n",
                sync, cycles, acknowledged, found);
        assertTrue(acknowledged > cycles, "the children wrote: " + acknowledged);
    }

    /**
     * The child kills itself at its N-th write or force, N random, until deaths have landed on
     * every kind of step: a log's append and force, a table's write and force, the manifest's
     * write and force, a directory's force. Each death's place is printed by the child.
     */
    @ParameterizedTest(name = "{0}")
    @EnumSource(SyncPolicy.class)
    @Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aWriterKilledAtEveryKindOfStepLosesNoAcknowledgedWrite(SyncPolicy sync) throws Exception {
        Path dir = tmp.resolve("store");
        Workload workload = new Workload(11, KEYS);
        Workload.Model model = new Workload.Model(workload);
        SplittableRandom random = new SplittableRandom(23 + sync.ordinal());
        Options options = Options.defaults().withSync(sync).withFlushLogBytes(4 << 10);
        Set<String> steps = Set.of("log write", "log force", "table write", "table force", "manifest write",
                "manifest force", "directory force");
        Map<String, Integer> deaths = new TreeMap<>();
        Map<String, Integer> found = new TreeMap<>();
        long acknowledged = 0;
        int cycle = 0;
        for (; cycle < 20 || !deaths.keySet().containsAll(steps); cycle++) {
            assertTrue(cycle < 200, () -> "200 deaths and still not every step: " + deaths);
            int at = random.nextInt(1, 400);
            Child child = Child.start(dir, sync, 11, options.flushLogBytes(), Integer.toString(at),
                    Long.toString(random.nextLong()));
            if (!child.awaitDeath(60)) {
                child.kill();
                throw new AssertionError("the child never reached its " + at + "th write or force");
            }
            child.checkKilled();
            String stderr = child.stderr().trim();
            assertTrue(stderr.startsWith("kill "), () -> "the child said where it died: " + stderr);
            deaths.merge(stderr.substring(5), 1, Integer::sum);
            long acked = child.acks > 0 ? child.lastAck : model.seq();
            assertTrue(acked >= acknowledged, "acks only go up");
            acknowledged = acked;
            found.merge(where(dir), 1, Integer::sum);
            found.merge(check(dir, options, model, acked), 1, Integer::sum);
        }
        System.out.printf("kill tests, %s, chosen steps: %d deaths at %s, %d acknowledged writes; %s%n",
                sync, cycle, deaths, acknowledged, found);
    }

    /**
     * Reopens the store and checks it holds exactly the writes up to its last sequence number,
     * which must be {@code acked} or the one after; returns what recovery found.
     */
    private static String check(Path dir, Options options, Workload.Model model, long acked) throws IOException {
        try (Store store = Store.open(dir, options)) {
            long last = store.lastSequence();
            assertTrue(last == acked || last == acked + 1, "recovered " + last + " after " + acked + " returned");
            model.advanceTo(last);
            for (int k = 0; k < KEYS; k++) {
                assertEquals(string(model.get(k)), string(store.get(Workload.key(k))), "k" + k + " at " + last);
            }
            Outcome outcome = store.recovery().outcome();
            assertTrue(outcome == Outcome.CLEAN || outcome == Outcome.TORN_TAIL, outcome::toString);
            return "recovery " + outcome + (last > acked ? " with the write in flight" : "");
        }
    }

    /** What the directory shows of where the death came, before recovery tidies it. */
    private static String where(Path dir) throws IOException {
        Manifest manifest = Manifest.read(dir);
        if (manifest == null) {
            return "no manifest yet";
        }
        Set<Long> live = new HashSet<>();
        for (Manifest.TableRef ref : manifest.tables()) {
            live.add(ref.number());
        }
        int logsToReplay = 0;
        boolean coveredLog = false;
        boolean garbageTable = false;
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                String name = file.getFileName().toString();
                long log = Store.fileNumber(name, Store.LOG_SUFFIX);
                long table = Store.fileNumber(name, Store.TABLE_SUFFIX);
                if (log >= manifest.logNumber()) {
                    logsToReplay++;
                } else if (log >= 0) {
                    coveredLog = true;
                } else if (table >= 0 && !live.contains(table)) {
                    garbageTable = true;
                }
            }
        }
        if (Files.exists(dir.resolve(Manifest.TEMP_FILE))) {
            return "died writing the manifest";
        }
        if (garbageTable) {
            return "died with a table the manifest doesn't name";
        }
        if (logsToReplay > 1) {
            return "died in a flush, before the manifest";
        }
        if (coveredLog) {
            return "died in a flush, after the manifest";
        }
        return "died between flushes";
    }

    private static Optional<String> string(Optional<byte[]> value) {
        return value.map(v -> new String(v, StandardCharsets.UTF_8));
    }
}
