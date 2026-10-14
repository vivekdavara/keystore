package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.sstable.CorruptTableException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Flushes to tables, reads across the memtable and tables, compaction, and recovery with tables. */
class StoreTablesTest {
    @TempDir
    Path dir;

    /** A flush every 2 KiB of log, NEVER sync to keep the tests fast. */
    private static final Options SMALL = Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(2048);

    private static Optional<String> string(Optional<byte[]> value) {
        return value.map(v -> new String(v, StandardCharsets.UTF_8));
    }

    private List<String> files(String suffix) throws IOException {
        try (Stream<Path> list = Files.list(dir)) {
            return list.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(suffix)).sorted().toList();
        }
    }

    @Test
    void writesPastTheThresholdGoToTablesAndReadBackAfterReopen() throws IOException {
        try (Store store = Store.open(dir, SMALL)) {
            for (int i = 0; i < 500; i++) {
                store.put(utf8("key-" + i), utf8("value-" + i));
            }
            assertFalse(store.tables().isEmpty(), "the log passed 2 KiB many times");
            assertTrue(store.memtableSize() < 500);
            for (int i = 0; i < 500; i++) {
                assertEquals(Optional.of("value-" + i), string(store.get(utf8("key-" + i))), "key-" + i);
            }
            assertEquals(Optional.empty(), string(store.get(utf8("key-500"))));
        }
        assertEquals(1, files(".log").size(), "logs the tables cover are deleted: " + files(".log"));
        try (Store store = Store.open(dir, SMALL)) {
            assertEquals(500, store.lastSequence());
            for (int i = 0; i < 500; i++) {
                assertEquals(Optional.of("value-" + i), string(store.get(utf8("key-" + i))), "key-" + i);
            }
        }
    }

    @Test
    void theNewestVersionWinsAcrossTheMemtableAndTables() throws IOException {
        try (Store store = Store.open(dir, SMALL)) {
            store.put(utf8("k"), utf8("1"));
            store.flush();
            store.put(utf8("k"), utf8("2"));
            store.flush();
            store.put(utf8("k"), utf8("3"));
            assertEquals(2, store.tables().size());
            assertEquals(Optional.of("3"), string(store.get(utf8("k"))), "the memtable's");
            store.flush();
            assertEquals(Optional.of("3"), string(store.get(utf8("k"))), "the newest table's");
            store.delete(utf8("k"));
            assertEquals(Optional.empty(), string(store.get(utf8("k"))), "a tombstone in the memtable hides the tables");
            store.flush();
            assertEquals(Optional.empty(), string(store.get(utf8("k"))), "and in a table");
        }
        try (Store store = Store.open(dir, SMALL)) {
            assertEquals(Optional.empty(), string(store.get(utf8("k"))));
            store.compactAll();
            assertEquals(Optional.empty(), string(store.get(utf8("k"))));
            assertTrue(store.tables().isEmpty(), "everything was a tombstone or older: " + store.tables());
            assertTrue(files(".sst").isEmpty(), files(".sst").toString());
        }
    }

    @Test
    void compactAllKeepsTheNewestValueAndDropsTombstones() throws IOException {
        try (Store store = Store.open(dir, SMALL)) {
            for (int round = 0; round < 3; round++) {
                for (int i = 0; i < 40; i++) {
                    store.put(utf8("k" + i), utf8("r" + round));
                }
                store.flush();
            }
            for (int i = 0; i < 40; i += 2) {
                store.delete(utf8("k" + i));
            }
            store.flush();
            store.compactAll();
            List<TableInfo> tables = store.tables();
            assertEquals(1, tables.size());
            assertEquals(20, tables.get(0).entries(), "the odd keys");
            assertEquals(0, tables.get(0).tombstones());
            for (int i = 0; i < 40; i++) {
                assertEquals(i % 2 == 0 ? Optional.empty() : Optional.of("r2"), string(store.get(utf8("k" + i))), "k" + i);
            }
            assertEquals(List.of(String.format("%06d.sst", tables.get(0).number())), files(".sst"),
                    "the inputs are deleted once nothing reads them");
        }
    }

    /**
     * Size-tiered: a tier merges when it reaches mergeWidth tables, so there are at most
     * mergeWidth - 1 tables per tier, and few tiers.
     */
    @Test
    void flushesTriggerMergesThatKeepTheTableCountDown() throws IOException {
        try (Store store = Store.open(dir, SMALL.withMergeWidth(4))) {
            int most = 0;
            long flushes = 0;
            for (int i = 0; i < 3_000; i++) {
                store.put(utf8("key-" + (i % 700)), utf8("value-" + i));
                most = Math.max(most, store.tables().size());
            }
            flushes = store.manifest().nextFileNumber(); // logs and tables: about two numbers a flush
            System.out.printf("compaction: 3000 writes, about %d flushes, at most %d tables; at the end: %s%n",
                    flushes / 2, most, store.tables().stream().map(t -> t.fileBytes() + " bytes").toList());
            // Three tiers here: flushes (about 2 KB), merges of four (about 8 KB), and merges of those,
            // which stop growing at the 700 distinct keys (about 20 KB). At most three of each.
            assertTrue(most <= 9, "at most " + most + " tables");
            assertTrue(flushes / 2 > 20, "the test made enough flushes to need merges");
            for (int k = 0; k < 700; k++) {
                int last = k + 700 * ((2_999 - k) / 700); // the last write of key k
                assertEquals(Optional.of("value-" + last), string(store.get(utf8("key-" + k))), "key-" + k);
            }
        }
    }

    /**
     * A flush that fails at the manifest leaves a table the manifest doesn't name and two logs. The
     * store stops writing but still reads; a reopen replays both logs and deletes the table.
     */
    @Test
    void aFlushThatFailsAtTheManifestLosesNothing() throws IOException {
        Options options = SMALL.withFlushLogBytes(1 << 20);
        FaultyChannel[] manifest = new FaultyChannel[1];
        boolean[] failManifest = {false};
        try (Store store = Store.open(dir, options, (path, inner) -> {
            if (!path.endsWith(Manifest.TEMP_FILE) || !failManifest[0]) {
                return inner;
            }
            FaultyChannel faulty = new FaultyChannel(inner);
            faulty.failWritesAfter(5);
            manifest[0] = faulty;
            return faulty;
        })) {
            for (int i = 0; i < 100; i++) {
                store.put(utf8("k" + i), utf8("v" + i));
            }
            failManifest[0] = true;
            assertThrows(IOException.class, store::flush);
            assertTrue(manifest[0] != null, "the manifest write was attempted");
            assertThrows(StoreFailedException.class, () -> store.put(utf8("x"), utf8("y")));
            assertEquals(Optional.of("v7"), string(store.get(utf8("k7"))), "reads go on, from the frozen memtable");
            assertEquals(1, files(".sst").size(), "the table was written");
            assertEquals(2, files(".log").size(), "the old log and the new one");
        }
        try (Store store = Store.open(dir, options)) {
            assertEquals(100, store.recovery().records(), "both logs replayed");
            assertEquals(100, store.lastSequence());
            for (int i = 0; i < 100; i++) {
                assertEquals(Optional.of("v" + i), string(store.get(utf8("k" + i))), "k" + i);
            }
            assertTrue(store.tables().isEmpty());
            assertTrue(files(".sst").isEmpty(), "the table the manifest never named is gone: " + files(".sst"));
            store.flush();
            assertEquals(1, store.tables().size());
        }
        assertEquals(1, files(".log").size(), files(".log").toString());
    }

    @Test
    void aStoreWithTablesButNoManifestRefusesToOpen() throws IOException {
        try (Store store = Store.open(dir, SMALL)) {
            store.put(utf8("k"), utf8("v"));
            store.flush();
        }
        List<String> tables = files(".sst");
        Files.delete(dir.resolve(Manifest.FILE));
        IOException refused = assertThrows(IOException.class, () -> Store.open(dir, SMALL));
        assertTrue(refused.getMessage().contains("no MANIFEST"), refused.getMessage());
        assertEquals(tables, files(".sst"), "nothing was deleted");
    }

    @Test
    void aDamagedTableRefusesToOpenTheStore() throws IOException {
        try (Store store = Store.open(dir, SMALL)) {
            for (int i = 0; i < 50; i++) {
                store.put(utf8("k" + i), utf8("v" + i));
            }
            store.flush();
        }
        Path table = dir.resolve(files(".sst").get(0));
        byte[] bytes = Files.readAllBytes(table);
        bytes[bytes.length - 20] ^= 1; // in the footer
        Files.write(table, bytes);
        assertThrows(CorruptTableException.class, () -> Store.open(dir, SMALL));

        bytes[bytes.length - 20] ^= 1;
        Files.write(table, java.util.Arrays.copyOf(bytes, bytes.length + 1)); // a byte too many
        CorruptTableException mismatch = assertThrows(CorruptTableException.class, () -> Store.open(dir, SMALL));
        assertTrue(mismatch.getMessage().contains("manifest") || mismatch.getMessage().contains("magic"), mismatch.getMessage());
    }

    @Test
    void anEmptyMemtableFlushesNothing() throws IOException {
        try (Store store = Store.open(dir, SMALL)) {
            store.flush();
            assertTrue(store.tables().isEmpty());
            store.compactAll();
            assertEquals(1, files(".log").size());
        }
    }
}
