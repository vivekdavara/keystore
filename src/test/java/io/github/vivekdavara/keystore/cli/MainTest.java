package io.github.vivekdavara.keystore.cli;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.Store;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MainTest {
    @TempDir
    Path dir;

    private String out;
    private String err;

    private int run(String... args) {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        int status = Main.run(args, new PrintStream(outBytes, true, StandardCharsets.UTF_8),
                new PrintStream(errBytes, true, StandardCharsets.UTF_8));
        out = outBytes.toString(StandardCharsets.UTF_8);
        err = errBytes.toString(StandardCharsets.UTF_8);
        return status;
    }

    @Test
    void putGetDelete() {
        String d = dir.toString();
        assertEquals(0, run("put", d, "greeting", "hello world"));
        assertEquals(0, run("get", d, "greeting"));
        assertEquals("hello world\n", out);
        assertEquals(0, run("delete", d, "greeting"));
        assertEquals(1, run("get", d, "greeting"));
        assertEquals("keystore: greeting: not found\n", err);
    }

    @Test
    void getCreatesNothingWhereThereIsNoStore() {
        Path elsewhere = dir.resolve("not-a-store");
        assertEquals(1, run("get", elsewhere.toString(), "k"));
        assertEquals("keystore: no store at " + elsewhere + "\n", err);
        assertTrue(Files.notExists(elsewhere));
        assertEquals(1, run("get", dir.toString(), "k")); // a directory, but not a store
        assertTrue(Files.notExists(dir.resolve("000001.log")));
        assertTrue(Files.notExists(dir.resolve("LOCK")));
    }

    @Test
    void badUsageExitsWithTwo() {
        assertEquals(2, run());
        assertTrue(err.startsWith("usage: keystore put DIR KEY VALUE"), err);
        assertEquals(2, run("put", dir.toString(), "only a key"));
        assertEquals(2, run("frobnicate", dir.toString()));
        assertEquals(2, run("get", dir.toString(), "k", "extra"));
    }

    @Test
    void errorsAreReportedNotThrown() throws IOException {
        assertEquals(1, run("put", dir.toString(), "", "v"));
        assertTrue(err.contains("a key must be 1 to 65536 bytes"), err);
        try (Store store = Store.open(dir)) {
            assertEquals(1, run("put", dir.toString(), "k", "v"));
            assertTrue(err.contains("already open"), err);
            assertEquals(0, store.lastSequence());
        }
        assertEquals(1, run("dump", dir.resolve("nowhere").toString()));
        assertTrue(err.startsWith("keystore: no store at"), err);
        assertEquals(1, run("flush", dir.resolve("nowhere").toString()));
        assertFalse(Files.exists(dir.resolve("nowhere")), "only a write creates a store");
    }

    @Test
    void dumpListsACleanLog() {
        String d = dir.toString();
        run("put", d, "a", "1");
        run("delete", d, "a");
        assertEquals(0, run("dump", d));
        assertTrue(out.contains("         0  #1 put a (1 bytes)\n"), out);
        assertTrue(out.contains("#2 delete a\n"), out);
        assertTrue(out.contains("clean:"), out);
    }

    @Test
    void dumpReportsATornTail() throws IOException {
        String d = dir.toString();
        run("put", d, "a", "1");
        run("put", d, "b", "2");
        Path log = dir.resolve("000001.log");
        try (RandomAccessFile raw = new RandomAccessFile(log.toFile(), "rw")) {
            raw.setLength(Files.size(log) - 2);
        }
        assertEquals(0, run("dump", d));
        assertTrue(out.contains("#1 put a"), out);
        // Each record is 27 bytes: an 8-byte header and a 19-byte payload.
        assertTrue(out.contains("torn tail: recovery drops 25 bytes from offset 27 (TRUNCATED_PAYLOAD, length 19 but 17"
                + " bytes left), and nothing intact follows"), out);
    }

    /** The operator's view of a log the store refuses: what would be dropped. The file is untouched. */
    @Test
    void dumpShowsWhatDamageWouldDropAndChangesNothing() throws IOException {
        String d = dir.toString();
        for (int i = 1; i <= 4; i++) {
            run("put", d, "k" + i, "v" + i);
        }
        Path log = dir.resolve("000001.log");
        byte[] bytes = Files.readAllBytes(log);
        int recordSize = bytes.length / 4;
        bytes[recordSize + 12] ^= 1; // the seq of record #2
        Files.write(log, bytes);

        assertEquals(3, run("dump", d));
        assertTrue(out.contains("#1 put k1"), out);
        assertTrue(out.contains("damage at offset " + recordSize + " (BAD_CHECKSUM"), out);
        assertTrue(out.contains("#3 put k3"), out);
        assertTrue(out.contains("#4 put k4"), out);
        assertTrue(out.contains("POINT_IN_TIME would keep " + recordSize + " bytes and drop " + (3 * recordSize)
                + " bytes, including the 2 intact records above"), out);
        assertArrayEquals(bytes, Files.readAllBytes(log));
    }

    /** flush writes a table that get reads from; compact merges the tables; dump lists them and the live log. */
    @Test
    void flushAndCompactWriteTablesThatDumpLists() throws IOException {
        String d = dir.toString();
        run("put", d, "a", "1");
        assertEquals(0, run("flush", d));
        run("put", d, "a", "2");
        run("put", d, "b", "3");
        assertEquals(0, run("flush", d));
        assertEquals(0, run("get", d, "a"));
        assertEquals("2\n", out, "the newer table's value");
        assertEquals(0, run("dump", d));
        assertTrue(out.contains("tables cover writes up to #3"), out);
        assertTrue(out.contains("000003.sst: ") && out.contains("000005.sst: "), out);
        assertEquals(0, run("compact", d));
        assertEquals(0, run("dump", d));
        assertTrue(out.contains("000006.sst: ") && !out.contains("000003.sst") && !out.contains("000005.sst"), out);
        assertTrue(Files.notExists(dir.resolve("000003.sst")), "the merged inputs are deleted");
        assertEquals(0, run("get", d, "b"));
        assertEquals("3\n", out);
    }

    /**
     * After a crash during a flush there are two logs; dump lists both. An older log that isn't clean
     * can't be a torn write (it was closed before the next one started), so dump says the store would
     * refuse, and exits 3.
     */
    @Test
    void snapshotCopiesAStoreThatOpensAndVerifies() {
        String d = dir.resolve("store").toString();
        String target = dir.resolve("snapshot").toString();
        assertEquals(0, run("put", d, "city", "Boston"));
        assertEquals(0, run("flush", d));
        assertEquals(0, run("put", d, "river", "Charles"));
        assertEquals(0, run("snapshot", d, target));
        assertTrue(out.startsWith("keystore: snapshot of ") && out.contains(" at #2 in ")
                && out.endsWith(": 2 tables, " + tableBytes(dir.resolve("snapshot")) + " bytes, 2 hard-linked\n"), out);
        assertEquals(0, run("get", target, "river"));
        assertEquals("Charles\n", out);
        assertEquals(0, run("verify", target));
        assertEquals("000005.sst: ok, 1 entries in 1 blocks, writes #2 to #2\n"
                + "000003.sst: ok, 1 entries in 1 blocks, writes #1 to #1\n"
                + "000006.log: ok, no writes\nok\n", out, "the get above opened it, which made its first log");
        assertEquals(1, run("snapshot", d, target), "the target exists");
        assertTrue(err.startsWith("keystore: ") && err.contains("must not exist"), err);
        assertEquals(1, run("snapshot", dir.resolve("nothing").toString(), target));
        assertEquals("keystore: no store at " + dir.resolve("nothing") + "\n", err);
    }

    private static long tableBytes(Path dir) {
        try (var files = Files.list(dir)) {
            return files.filter(f -> f.toString().endsWith(".sst")).mapToLong(f -> f.toFile().length()).sum();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** Damage in a data block: open and dump don't read blocks, so they don't see it; verify does. */
    @Test
    void verifyFindsDamageThatDumpAndOpenDont() throws IOException {
        String d = dir.toString();
        assertEquals(0, run("put", d, "city", "Boston"));
        assertEquals(0, run("flush", d));
        assertEquals(0, run("put", d, "river", "Charles"));
        assertEquals(0, run("verify", d));
        assertEquals("000003.sst: ok, 1 entries in 1 blocks, writes #1 to #1\n"
                + "000002.log: ok, writes #2 to #2\nok\n", out);
        try (RandomAccessFile table = new RandomAccessFile(dir.resolve("000003.sst").toFile(), "rw")) {
            table.seek(6);
            int b = table.read();
            table.seek(6);
            table.write(b ^ 0x01);
        }
        assertEquals(0, run("dump", d), "dump reads no data block");
        assertEquals(0, run("get", d, "river"), "nor does a read of a key elsewhere");
        assertEquals(3, run("verify", d));
        assertTrue(out.startsWith("000003.sst: DAMAGED: ") && out.contains("fails its checksum")
                && out.endsWith("000002.log: ok, writes #2 to #2\n1 damaged\n"), out);
        assertEquals(1, run("get", d, "city"));
        assertTrue(err.contains("fails its checksum"), err);
    }

    @Test
    void dumpListsEveryLiveLogAndRefusesATornOlderOne() throws IOException {
        String d = dir.toString();
        run("put", d, "a", "1");
        run("put", d, "b", "2");
        byte[] older = Files.readAllBytes(dir.resolve("000001.log"));
        Files.write(dir.resolve("000004.log"), new byte[0]); // the new log a flush starts first
        assertEquals(0, run("dump", d));
        assertTrue(out.contains("000001.log: ") && out.contains("000004.log: 0 bytes"), out);

        Files.write(dir.resolve("000001.log"), java.util.Arrays.copyOf(older, older.length - 3));
        assertEquals(3, run("dump", d));
        assertTrue(out.contains("refused: an older log must be clean"), out);
        assertEquals(1, run("get", d, "a"));
        assertTrue(err.contains("older log"), err);
    }

    @Test
    void serveFollowAndClientReplicateEndToEnd() throws Exception {
        ByteArrayOutputStream leaderOut = new ByteArrayOutputStream();
        Closeable leader = Main.start(new String[] {"serve", dir.resolve("a").toString(), "0"},
                new PrintStream(leaderOut, true, StandardCharsets.UTF_8), System.err);
        try {
            String leaderLine = leaderOut.toString(StandardCharsets.UTF_8);
            assertTrue(leaderLine.startsWith("keystore: leader for "), leaderLine);
            String leaderAddress = leaderLine.substring(leaderLine.lastIndexOf(' ') + 1).trim();
            assertEquals(0, run("client", leaderAddress, "put", "city", "Boston"));
            assertEquals("1\n", out);

            ByteArrayOutputStream followerOut = new ByteArrayOutputStream();
            Closeable follower = Main.start(new String[] {"follow", dir.resolve("b").toString(), "0", leaderAddress},
                    new PrintStream(followerOut, true, StandardCharsets.UTF_8), System.err);
            try {
                String followerLine = followerOut.toString(StandardCharsets.UTF_8);
                String followerAddress = followerLine.substring(followerLine.lastIndexOf(' ') + 1).trim();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (run("client", followerAddress, "get", "city") != 0) {
                    assertTrue(System.nanoTime() < deadline, "the follower never got the write: " + err);
                    Thread.sleep(10);
                }
                assertEquals("Boston\n", out);
                assertEquals(1, run("client", followerAddress, "put", "city", "Paris"));
                assertTrue(err.contains("READONLY"), err);
                assertEquals(0, run("client", followerAddress, "info"));
                assertTrue(out.startsWith("role=follower seq=1 "), out);
                assertTrue(out.contains("state=STREAMING"), out);
                assertEquals(0, run("client", leaderAddress, "info"));
                assertTrue(out.contains("followers=1"), out);
            } finally {
                follower.close();
            }
        } finally {
            leader.close();
        }
        // Both stores are closed, and the follower's has the write.
        assertEquals(0, run("get", dir.resolve("b").toString(), "city"));
        assertEquals("Boston\n", out);
    }

    @Test
    void serverCommandsCheckTheirArguments() throws Exception {
        assertEquals(2, run("client", "nowhere", "info"));
        assertEquals(2, run("client", "127.0.0.1:70000", "info"));
        assertEquals(2, run("client", "127.0.0.1:1", "frob"));
        assertEquals(2, run("client", "127.0.0.1:1", "get"));
        assertThrows(Main.UsageException.class,
                () -> Main.start(new String[] {"serve", dir.toString(), "x"}, System.out, System.err));
        assertThrows(Main.UsageException.class,
                () -> Main.start(new String[] {"follow", dir.toString(), "0", "nohostport"}, System.out, System.err));
        assertFalse(Files.exists(dir.resolve("MANIFEST")), "a refused command creates no store");
    }

    @Test
    void aClientWithNoServerFails() throws IOException {
        int port;
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort(); // free once closed
        }
        assertEquals(1, run("client", "127.0.0.1:" + port, "info"));
        assertTrue(err.startsWith("keystore: "), err);
    }
}
