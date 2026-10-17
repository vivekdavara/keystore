package io.github.vivekdavara.keystore.review5;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.github.vivekdavara.keystore.Bytes;
import io.github.vivekdavara.keystore.LogTail;
import io.github.vivekdavara.keystore.Options;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.cli.Main;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * README "Limitations" and DESIGN.md say a BEHIND follower can start again from "{@code keystore
 * snapshot} while the leader is stopped". But every one-shot command (get, put, delete, flush, compact,
 * snapshot) opens the store with {@code Options.defaults()}, whose {@code retainLogBytes} is 0, so the
 * open deletes every covered log that {@code keystore serve} kept (64 MiB) for its followers. When the
 * leader is started again, a follower that was only a few writes behind gets {@code ERR BEHIND}. Even
 * {@code keystore get}, a read, does it.
 *
 * <p>Runs the real command in a child JVM, as the kill tests do.
 */
class CommandOnAStoppedLeaderKeepsFollowersLogsTest {
    private static final String JAVA = ProcessHandle.current().info().command().orElseThrow();
    /** What {@code keystore serve} opens a leader with (Main.SERVE_RETAIN_LOG_BYTES). */
    private static final Options LEADER = Options.defaults().withRetainLogBytes(64L << 20);

    @TempDir
    Path tmp;

    /** A leader's directory as {@code serve} leaves it: 100 writes in a table, their log kept, 1 more in the newest log. */
    private Path stoppedLeader() throws IOException {
        Path dir = tmp.resolve("leader");
        try (Store store = Store.open(dir, LEADER)) {
            for (int i = 1; i <= 100; i++) {
                store.put(Bytes.utf8("k" + i), Bytes.utf8("v" + i));
            }
            store.flush();
            store.put(Bytes.utf8("city"), Bytes.utf8("Boston"));
        }
        assertEquals(2, logs(dir).size(), "the covered log, kept for followers, and the newest: " + logs(dir));
        return dir;
    }

    private static List<String> logs(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(f -> f.getFileName().toString()).filter(n -> n.endsWith(".log")).sorted().toList();
        }
    }

    private static int keystore(String... args) throws IOException, InterruptedException {
        String[] command = Stream.concat(
                        Stream.of(JAVA, "-cp", System.getProperty("java.class.path"), Main.class.getName()),
                        Stream.of(args))
                .toArray(String[]::new);
        Process child = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!child.waitFor(60, TimeUnit.SECONDS)) {
            child.destroyForcibly();
            throw new AssertionError("keystore " + String.join(" ", args) + " didn't finish: " + output);
        }
        return child.exitValue();
    }

    /** The leader started again; a follower at #49 asks for #50, which was in the kept log. */
    private static void assertAFollowerAt49CanCatchUp(Path dir) throws IOException {
        try (Store store = Store.open(dir, LEADER); LogTail tail = store.tail(50)) {
            byte[] record = assertDoesNotThrow(tail::poll, "the log serve kept for followers is gone: " + logs(dir));
            assertNotNull(record);
        }
    }

    @Test
    void getOnAStoppedLeaderKeepsTheLogsServeKeptForFollowers() throws Exception {
        Path dir = stoppedLeader();
        assertEquals(0, keystore("get", dir.toString(), "city"));
        assertAFollowerAt49CanCatchUp(dir);
    }

    @Test
    void snapshotOfAStoppedLeaderKeepsTheLogsServeKeptForFollowers() throws Exception {
        Path dir = stoppedLeader();
        assertEquals(0, keystore("snapshot", dir.toString(), tmp.resolve("snap").toString()));
        assertAFollowerAt49CanCatchUp(dir);
    }
}
