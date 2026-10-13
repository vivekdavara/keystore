package io.github.vivekdavara.keystore.cli;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.Store;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
        assertTrue(err.startsWith("keystore: no log at"), err);
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
        Path log = dir.resolve("wal.log");
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
        Path log = dir.resolve("wal.log");
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
}
