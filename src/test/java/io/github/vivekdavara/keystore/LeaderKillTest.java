package io.github.vivekdavara.keystore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.cli.Main;
import io.github.vivekdavara.keystore.server.Client;
import io.github.vivekdavara.keystore.server.Follower;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A leader killed with SIGKILL while a client writes to it and a follower streams from it, then
 * restarted on the same port, again and again. The leader is the real {@code keystore serve} in a
 * child JVM (under {@link SyncPolicy#EVERY_WRITE}, its default); the client and the follower run
 * here. After each restart the leader must hold every write the client got an {@code OK} for, and
 * at most the one in flight besides; the follower, which only ever got acknowledged writes, must
 * never find itself ahead of the restarted leader or on a different history, and must end up with
 * exactly the leader's writes.
 */
class LeaderKillTest {
    private static final int KEYS = 300;
    private static final String JAVA = ProcessHandle.current().info().command().orElseThrow();
    private static final Pattern STARTED = Pattern.compile("keystore: leader for \\S+ \\(at #(\\d+)\\) on \\S+");

    @TempDir
    Path tmp;

    @Test
    @Timeout(value = 300, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aLeaderKilledAgainAndAgainKeepsWhatItAcknowledgedAndItsFollower() throws Exception {
        Workload workload = new Workload(37, KEYS);
        Workload.Model model = new Workload.Model(workload);
        SplittableRandom random = new SplittableRandom(41);
        Path leaderDir = tmp.resolve("leader");
        InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), freePort(random));
        Set<Follower.State> stops = EnumSet.of(Follower.State.AHEAD, Follower.State.DIVERGED, Follower.State.FAILED,
                Follower.State.BEHIND);
        long acked = 0;
        int deaths = 0;
        int inFlightKept = 0;
        try (Store followerStore = Store.open(tmp.resolve("follower"));
                Follower follower = Follower.start(followerStore, Follower.Config.of(address)
                        .withTimeout(Duration.ofSeconds(5)).withMaxBackoff(Duration.ofMillis(50)))) {
            for (int cycle = 0; cycle < 8; cycle++) {
                Process leader = startLeader(leaderDir, address.getPort());
                long at = awaitStarted(leader);
                assertTrue(at == acked || at == acked + 1, "restarted at #" + at + " after the client's last OK, #" + acked);
                inFlightKept += at > acked ? 1 : 0;
                model.advanceTo(at);
                acked = at;
                // A killer thread ends the leader at a random moment while this thread writes to it.
                long delay = random.nextInt(100, 500);
                Thread killer = Thread.ofPlatform().start(() -> {
                    try {
                        Thread.sleep(delay);
                        signal(leader, "KILL");
                    } catch (IOException | InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                });
                try (Client client = Client.connect(address)) {
                    for (long seq = at + 1; ; seq++) {
                        assertTrue(seq < at + 100_000, "the leader was never killed");
                        Workload.Write w = workload.write(seq);
                        long got = w.value() == null ? client.delete(w.key()) : client.put(w.key(), w.value());
                        assertEquals(seq, got, "the leader gave the client's write its number");
                        acked = seq;
                    }
                } catch (IOException expected) {
                    // the leader died: the write in flight may or may not have reached its log
                }
                killer.join();
                assertTrue(leader.waitFor(30, TimeUnit.SECONDS), "the leader died");
                assertEquals(128 + 9, leader.exitValue(), "a SIGKILL's exit status");
                deaths++;
                assertTrue(!stops.contains(follower.status().state()), "the follower goes on: " + follower.status());
            }

            // A last start: the follower catches up with it, then the leader stops on SIGTERM.
            Process leader = startLeader(leaderDir, address.getPort());
            long end = awaitStarted(leader);
            assertTrue(end == acked || end == acked + 1, "restarted at #" + end + " after #" + acked);
            assertTrue(follower.awaitApplied(end, Duration.ofSeconds(60)), follower.status().toString());
            assertTrue(!stops.contains(follower.status().state()), follower.status().toString());
            signal(leader, "TERM");
            assertEquals(128 + 15, leader.exitValue(), "a SIGTERM's exit status");
            model.advanceTo(end);
            assertEquals(end, followerStore.lastSequence());
            try (Store leaderStore = Store.open(leaderDir)) {
                assertEquals(end, leaderStore.lastSequence());
                for (int k = 0; k < KEYS; k++) {
                    byte[] key = Workload.key(k);
                    assertEquals(string(model.get(k)), string(leaderStore.get(key)), "the leader's k" + k);
                    assertEquals(string(model.get(k)), string(followerStore.get(key)), "the follower's k" + k);
                }
            }
            assertTrue(acked > 8, "the client wrote: " + acked);
            System.out.printf("leader kill test: %d deaths, %d writes acknowledged to the client, the write in flight"
                    + " kept by %d restarts, the follower at #%d with %d connects%n", deaths, acked, inFlightKept, end,
                    follower.status().connects());
        }
    }

    /**
     * A port for the leader to keep across restarts: a free one outside the ephemeral range, which
     * another test's outgoing connection could be given while the leader is down.
     */
    private static int freePort(SplittableRandom random) throws IOException {
        for (int attempt = 0; attempt < 100; attempt++) {
            int port = random.nextInt(20_000, 32_000);
            try (ServerSocket probe = new ServerSocket()) {
                probe.setReuseAddress(true);
                probe.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
                return port;
            } catch (IOException taken) {
                // try another
            }
        }
        throw new IOException("no free port in 20000-31999");
    }

    private Process startLeader(Path dir, int port) throws IOException {
        return new ProcessBuilder(JAVA, "-XX:TieredStopAtLevel=1", "-XX:+UseSerialGC",
                "-cp", System.getProperty("java.class.path"), Main.class.getName(),
                "serve", dir.toString(), Integer.toString(port))
                .redirectError(Files.createTempFile(tmp, "leader", ".err").toFile())
                .start();
    }

    /** Reads the line {@code keystore serve} prints once its store is open; returns where the store is. */
    private static long awaitStarted(Process leader) throws IOException {
        BufferedReader out = new BufferedReader(new InputStreamReader(leader.getInputStream(), StandardCharsets.UTF_8));
        String line = out.readLine();
        assertTrue(line != null, "the leader started");
        Matcher m = STARTED.matcher(line);
        assertTrue(m.matches(), line);
        return Long.parseLong(m.group(1));
    }

    /** Sends the child a signal with kill(1) and waits for it to exit. */
    private static void signal(Process child, String signal) throws IOException, InterruptedException {
        Process kill = new ProcessBuilder("kill", "-" + signal, Long.toString(child.pid())).inheritIO().start();
        assertTrue(kill.waitFor(30, TimeUnit.SECONDS) && kill.exitValue() == 0, "kill(1) ran");
        assertTrue(child.waitFor(30, TimeUnit.SECONDS), "the child exited");
    }

    private static Optional<String> string(Optional<byte[]> value) {
        return value.map(v -> new String(v, StandardCharsets.UTF_8));
    }
}
