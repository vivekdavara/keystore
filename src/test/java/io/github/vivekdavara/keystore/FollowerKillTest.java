package io.github.vivekdavara.keystore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.cli.Main;
import io.github.vivekdavara.keystore.server.Server;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A follower killed with SIGKILL while it catches up and streams, and restarted, again and again.
 * The leader runs here, taking {@link Workload}'s writes the whole time; the follower is the real
 * {@code keystore follow} in a child JVM. After each death the follower's store must hold exactly
 * the leader's writes up to its own last sequence number, and that number must be at least the
 * highest the leader saw it acknowledge: a follower acknowledges only what it has applied, after
 * forcing it (it runs with the default {@link SyncPolicy#EVERY_WRITE}). At the end it catches up
 * and is stopped by SIGTERM, and the two stores must be the same.
 */
class FollowerKillTest {
    private static final int KEYS = 300;
    private static final String JAVA = ProcessHandle.current().info().command().orElseThrow();
    private static final Pattern STARTED = Pattern.compile("keystore: following \\S+ into \\S+ \\(at #(\\d+)\\).*");

    @TempDir
    Path tmp;

    @Test
    @Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aFollowerKilledAgainAndAgainKeepsExactlyALeadersPrefix() throws Exception {
        Workload workload = new Workload(29, KEYS);
        Options leaderOptions = Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(64 << 10)
                .withRetainLogBytes(64L << 20);
        Path followerDir = tmp.resolve("follower");
        SplittableRandom random = new SplittableRandom(31);
        try (Store leader = Store.open(tmp.resolve("leader"), leaderOptions);
                Server server = Server.leader(leader, Server.Config.loopback(0))) {
            AtomicBoolean writing = new AtomicBoolean(true);
            AtomicLong burst = new AtomicLong(); // writes to make at full speed, before going on slowly
            AtomicReference<Throwable> writerFailure = new AtomicReference<>();
            Thread writer = Thread.ofPlatform().start(() -> {
                try {
                    for (long seq = 1; writing.get(); seq++) {
                        assertEquals(seq, workload.apply(leader, seq));
                        if (burst.get() > 0) {
                            burst.decrementAndGet();
                        } else if (seq % 20 == 0) {
                            Thread.sleep(5); // about 4,000 writes a second
                        }
                    }
                } catch (Throwable e) {
                    writerFailure.set(e);
                }
            });

            Workload.Model model = new Workload.Model(workload);
            long acknowledged = 0;
            int deaths = 0;
            int behindAtDeath = 0;
            for (int cycle = 0; cycle < 12; cycle++) {
                // While it is down, the leader sometimes races ahead, so the next start has a lot to
                // catch up on, and a death can land in the middle of it.
                boolean catchUp = cycle % 2 == 0;
                burst.set(catchUp ? random.nextInt(20_000, 60_000) : 0);
                while (burst.get() > 0) {
                    Thread.sleep(1);
                }
                Process child = startFollower(followerDir, server.port());
                long at = awaitStarted(child);
                assertTrue(at >= acknowledged, "restarted at " + at + ", but the leader saw an ack of " + acknowledged);
                // Catching up, it dies once it has acknowledged a random part of what it was missing;
                // otherwise, at a random moment while it streams.
                long target = catchUp ? at + random.nextLong(1, leader.lastSequence() - at) : Long.MAX_VALUE;
                long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(catchUp ? 30_000 : random.nextInt(0, 500));
                while (System.nanoTime() < until && acknowledged < target) { // the leader's view of its acks
                    for (Server.FollowerInfo f : server.followers()) {
                        acknowledged = Math.max(acknowledged, f.acked());
                    }
                    Thread.onSpinWait();
                }
                if (catchUp) {
                    assertTrue(acknowledged >= target, "the follower got to " + target + " within 30 s: " + acknowledged);
                }
                long leaderAtDeath = leader.lastSequence();
                signal(child, "KILL");
                assertEquals(128 + 9, child.exitValue(), "a SIGKILL's exit status");
                deaths++;
                for (Server.FollowerInfo f : server.followers()) { // acks read since: sent before it died
                    acknowledged = Math.max(acknowledged, f.acked());
                }
                long last = check(followerDir, model, leader);
                assertTrue(last >= acknowledged, "recovered " + last + ", but the leader saw an ack of " + acknowledged);
                behindAtDeath += leaderAtDeath - last > 1_000 ? 1 : 0;
            }
            writing.set(false);
            writer.join();
            assertEquals(null, writerFailure.get(), "the leader's writer");
            long end = leader.lastSequence();
            assertTrue(acknowledged > 0 && acknowledged < end, "the follower was killed with work left: "
                    + acknowledged + " of " + end);

            // The last start catches up, and stops cleanly.
            Process child = startFollower(followerDir, server.port());
            awaitStarted(child);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (server.followers().stream().mapToLong(Server.FollowerInfo::acked).max().orElse(0) < end) {
                assertTrue(System.nanoTime() < deadline, "the follower caught up: " + server.followers());
                Thread.sleep(5);
            }
            signal(child, "TERM");
            assertEquals(128 + 15, child.exitValue(), "a SIGTERM's exit status");
            try (Store follower = Store.open(followerDir)) {
                assertEquals(end, follower.lastSequence());
                model.advanceTo(end);
                for (int k = 0; k < KEYS; k++) {
                    byte[] key = Workload.key(k);
                    assertEquals(string(leader.get(key)), string(follower.get(key)), "k" + k);
                    assertEquals(string(model.get(k)), string(follower.get(key)), "k" + k);
                }
            }
            System.out.printf("follower kill test: %d deaths (%d of them more than 1,000 writes behind the leader),"
                    + " %d writes, acks up to %d seen before the last death%n", deaths, behindAtDeath, end, acknowledged);
        }
    }

    private Process startFollower(Path dir, int leaderPort) throws IOException {
        return new ProcessBuilder(JAVA, "-XX:TieredStopAtLevel=1", "-XX:+UseSerialGC",
                "-cp", System.getProperty("java.class.path"), Main.class.getName(),
                "follow", dir.toString(), "0", "127.0.0.1:" + leaderPort)
                .redirectError(Files.createTempFile(tmp, "follower", ".err").toFile())
                .start();
    }

    /** Reads the line {@code keystore follow} prints once its store is open; returns where the store is. */
    private static long awaitStarted(Process child) throws IOException {
        BufferedReader out = new BufferedReader(new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8));
        String line = out.readLine();
        assertTrue(line != null, "the follower started");
        Matcher m = STARTED.matcher(line);
        assertTrue(m.matches(), line);
        return Long.parseLong(m.group(1));
    }

    /** Sends the child a signal with kill(1) and waits for it to exit. */
    private static void signal(Process child, String signal) throws IOException, InterruptedException {
        assertTrue(child.isAlive(), "the follower was running");
        Process kill = new ProcessBuilder("kill", "-" + signal, Long.toString(child.pid())).inheritIO().start();
        assertTrue(kill.waitFor(30, TimeUnit.SECONDS) && kill.exitValue() == 0, "kill(1) ran");
        assertTrue(child.waitFor(30, TimeUnit.SECONDS), "the follower exited");
    }

    /**
     * Opens the follower's store here and checks it holds exactly the leader's writes up to its last
     * sequence number; returns that number.
     */
    private static long check(Path dir, Workload.Model model, Store leader) throws IOException {
        try (Store follower = Store.open(dir)) {
            long last = follower.lastSequence();
            assertTrue(last <= leader.lastSequence(), "the follower is not ahead of its leader");
            model.advanceTo(last);
            for (int k = 0; k < KEYS; k++) {
                assertEquals(string(model.get(k)), string(follower.get(Workload.key(k))), "k" + k + " at " + last);
            }
            return last;
        }
    }

    private static Optional<String> string(Optional<byte[]> value) {
        return value.map(v -> new String(v, StandardCharsets.UTF_8));
    }
}
