package io.github.vivekdavara.keystore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.sstable.TableSurgery;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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

    /** Every file in {@code d} but the lock, by name. */
    private static Map<String, byte[]> files(Path d) throws IOException {
        Map<String, byte[]> files = new TreeMap<>();
        try (Stream<Path> list = Files.list(d)) {
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
        Map<String, byte[]> before = files(dir);
        assertEquals(3, before.size(), "the manifest, a table and a log: " + before.keySet());

        IOException refused = assertThrows(IOException.class, () -> Store.open(dir));
        assertTrue(refused.getMessage().endsWith("MANIFEST: manifest format 3, which this build doesn't read (it reads 1"
                + " to 2): written by a later version of keystore, or damaged"), refused.getMessage());
        assertUnchanged(before, files(dir));
    }

    /**
     * A directory without a lock file (a snapshot, a copy, tables without a manifest) gains the empty
     * {@code LOCK} even from an open that is refused: the open locks before it reads anything, and
     * the file is never deleted (DESIGN.md, "Recovery"). Nothing else appears. (The release review's
     * two cases; it asked that nothing appear at all.)
     */
    @Test
    void aRefusedOpenAddsAtMostTheEmptyLockFile() throws IOException {
        Path snapshot = dir.resolve("snapshot");
        try (Store store = Store.open(dir.resolve("store"))) {
            store.put(Bytes.utf8("city"), Bytes.utf8("Boston"));
            store.flush();
            store.snapshot(snapshot);
        }
        Path manifest = snapshot.resolve(Manifest.FILE);
        byte[] bytes = Files.readAllBytes(manifest);
        bytes[7] = 3;
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer.wrap(bytes).putInt(bytes.length - 4, (int) crc.getValue());
        Files.write(manifest, bytes);

        Path tables = Files.createDirectory(dir.resolve("tables"));
        try (Stream<Path> files = Files.list(dir.resolve("store"))) {
            for (Path f : files.filter(f -> f.toString().endsWith(".sst")).toList()) {
                Files.copy(f, tables.resolve(f.getFileName()));
            }
        }

        for (Path refused : List.of(snapshot, tables)) {
            assertTrue(Files.notExists(refused.resolve(Store.LOCK_FILE)), refused + " starts without a lock file");
            Map<String, byte[]> before = files(refused);
            assertThrows(IOException.class, () -> Store.open(refused));
            assertUnchanged(before, files(refused));
            assertEquals(0, Files.size(refused.resolve(Store.LOCK_FILE)), "the lock file is empty");
        }
    }

    /**
     * A store with what a successful open changes: a torn tail in its newest log, a table the
     * manifest doesn't name, and a covered log the open's options don't keep. The control, a copy
     * with every table in this build's format, opens and truncates, deletes and deletes; the store
     * itself, its live table in a later format, is refused with every file as it was. So the tables'
     * format is checked before recovery truncates or deletes anything. (The release review moved the
     * tables' check after the newest log's recovery, and the deletions before the tables' check; no
     * test noticed either.)
     */
    @Test
    void aRefusedOpenTruncatesAndDeletesNothingASuccessfulOneWould() throws IOException {
        Path store = dir.resolve("store");
        try (Store s = Store.open(store, Options.defaults().withRetainLogBytes(1 << 20))) {
            for (int i = 0; i < 60; i++) {
                if (i == 50) {
                    s.flush(); // a table, and the covered log kept
                }
                s.put(Bytes.utf8("k" + i), Bytes.utf8("v" + i));
            }
        }
        List<Path> logs;
        Path live;
        try (Stream<Path> list = Files.list(store)) {
            List<Path> all = list.sorted().toList();
            logs = all.stream().filter(f -> f.toString().endsWith(".log")).toList();
            live = all.stream().filter(f -> f.toString().endsWith(".sst")).findFirst().orElseThrow();
        }
        assertEquals(2, logs.size(), "a covered log and the newest: " + logs);
        Path newest = logs.get(1);
        try (RandomAccessFile raw = new RandomAccessFile(newest.toFile(), "rw")) {
            raw.setLength(raw.length() - 3); // the last write torn
        }
        Files.copy(live, store.resolve("000099.sst")); // a table the manifest doesn't name

        Path control = Files.createDirectory(dir.resolve("control"));
        for (Map.Entry<String, byte[]> f : files(store).entrySet()) {
            Files.write(control.resolve(f.getKey()), f.getValue());
        }
        try (Store s = Store.open(control)) {
            assertEquals(59, s.lastSequence(), "the torn write dropped");
        }
        Map<String, byte[]> opened = files(control);
        assertTrue(Files.size(control.resolve(newest.getFileName())) < Files.size(newest), "the control's tail truncated");
        assertTrue(!opened.containsKey("000099.sst") && !opened.containsKey(logs.get(0).getFileName().toString()),
                "the control's orphan table and covered log deleted: " + opened.keySet());

        TableSurgery.setFormatVersion(live, 2);
        Map<String, byte[]> before = files(store);
        assertThrows(IOException.class, () -> Store.open(store));
        assertUnchanged(before, files(store));
    }

    @Test
    void aTableInALaterFormatIsRefusedByNameAndNothingChanges() throws IOException {
        makeStore();
        Path table;
        try (Stream<Path> list = Files.list(dir)) {
            table = list.filter(f -> f.toString().endsWith(".sst")).findFirst().orElseThrow();
        }
        TableSurgery.setFormatVersion(table, 2);
        Map<String, byte[]> before = files(dir);

        IOException refused = assertThrows(IOException.class, () -> Store.open(dir));
        assertTrue(refused.getMessage().contains("table format 2, which this build doesn't read (it reads 1)"),
                refused.getMessage());
        assertUnchanged(before, files(dir));
    }
}
