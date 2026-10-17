package io.github.vivekdavara.keystore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.sstable.TableSurgery;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A store that a later build wrote, opened by this one: a manifest or a table in a format this build
 * doesn't know is refused by name, and the refused open changes no file. A downgrade must neither
 * read a newer layout as its own nor delete what it can't account for.
 */
class LaterFormatTest {
    @TempDir
    Path dir;

    /** A table of 100 writes and a log of 50 more. */
    private void makeStore() throws IOException {
        try (Store store = Store.open(dir)) {
            for (int i = 0; i < 150; i++) {
                if (i == 100) {
                    store.flush();
                }
                store.put(Bytes.utf8("k" + i), Bytes.utf8("v" + i));
            }
        }
    }

    /** Every file but the lock, by name. */
    private Map<String, byte[]> files() throws IOException {
        Map<String, byte[]> files = new TreeMap<>();
        try (Stream<Path> list = Files.list(dir)) {
            for (Path file : (Iterable<Path>) list::iterator) {
                String name = file.getFileName().toString();
                if (!name.equals("LOCK")) {
                    files.put(name, Files.readAllBytes(file));
                }
            }
        }
        return files;
    }

    private static void assertUnchanged(Map<String, byte[]> before, Map<String, byte[]> after) {
        assertEquals(before.keySet(), after.keySet());
        for (String name : before.keySet()) {
            assertArrayEquals(before.get(name), after.get(name), name);
        }
    }

    @Test
    void aManifestInALaterFormatIsRefusedByNameAndNothingChanges() throws IOException {
        makeStore();
        Path manifest = dir.resolve(Manifest.FILE);
        byte[] bytes = Files.readAllBytes(manifest);
        bytes[7] = 3; // "KSMANIF" and a 3, its checksum recomputed as the later build would
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer.wrap(bytes).putInt(bytes.length - 4, (int) crc.getValue());
        Files.write(manifest, bytes);
        Map<String, byte[]> before = files();
        assertEquals(3, before.size(), "the manifest, a table and a log: " + before.keySet());

        IOException refused = assertThrows(IOException.class, () -> Store.open(dir));
        assertTrue(refused.getMessage().endsWith("MANIFEST: manifest format 3, which this build doesn't read (it reads 1"
                + " to 2): written by a later version of keystore, or damaged"), refused.getMessage());
        assertUnchanged(before, files());
    }

    @Test
    void aTableInALaterFormatIsRefusedByNameAndNothingChanges() throws IOException {
        makeStore();
        Path table;
        try (Stream<Path> list = Files.list(dir)) {
            table = list.filter(f -> f.toString().endsWith(".sst")).findFirst().orElseThrow();
        }
        TableSurgery.setFormatVersion(table, 2);
        Map<String, byte[]> before = files();

        IOException refused = assertThrows(IOException.class, () -> Store.open(dir));
        assertTrue(refused.getMessage().contains("table format 2, which this build doesn't read (it reads 1)"),
                refused.getMessage());
        assertUnchanged(before, files());
    }
}
