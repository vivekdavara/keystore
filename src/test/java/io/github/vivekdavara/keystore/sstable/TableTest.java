package io.github.vivekdavara.keystore.sstable;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.Bytes;
import io.github.vivekdavara.keystore.memtable.Entry;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TableTest {
    private static final BiFunction<Path, FileChannel, FileChannel> PLAIN = (path, channel) -> channel;

    @TempDir
    Path dir;

    /** Keys k00000, k00002, ... (even numbers), values of varied sizes, every fifth a tombstone. */
    private static TreeMap<byte[], Entry> sample(int count) {
        TreeMap<byte[], Entry> entries = new TreeMap<>(Bytes.ORDER);
        for (int i = 0; i < count; i++) {
            byte[] key = utf8(String.format("k%05d", 2 * i));
            entries.put(key, i % 5 == 4 ? Entry.tombstone(i + 1) : Entry.value(i + 1, utf8("v" + i + "-" + "x".repeat(i % 37))));
        }
        return entries;
    }

    private Path write(String name, Map<byte[], Entry> entries, int blockBytes) throws IOException {
        Path file = dir.resolve(name);
        TableWriter writer = TableWriter.create(file, blockBytes, 10, PLAIN);
        for (Map.Entry<byte[], Entry> e : entries.entrySet()) {
            writer.add(e.getKey(), e.getValue());
        }
        TableWriter.Written written = writer.finish();
        assertEquals(Files.size(file), written.fileBytes());
        return file;
    }

    private static long hash(byte[] key) {
        return BloomFilter.hash(key);
    }

    @Test
    void everyEntryReadsBackAndAbsentKeysAreNotFound() throws IOException {
        TreeMap<byte[], Entry> entries = sample(500);
        Table table = Table.open(write("1.sst", entries, 128), 1, PLAIN);
        try {
            assertTrue(table.blocks() > 20, "small blocks: " + table.blocks());
            for (Map.Entry<byte[], Entry> e : entries.entrySet()) {
                assertEquals(e.getValue(), table.get(e.getKey(), hash(e.getKey())), Bytes.show(e.getKey()));
            }
            for (int i = 0; i < 500; i++) {
                byte[] odd = utf8(String.format("k%05d", 2 * i + 1)); // between two keys
                assertNull(table.get(odd, hash(odd)));
            }
            for (String outside : List.of("a", "k", "k99999", "z")) {
                assertNull(table.get(utf8(outside), hash(utf8(outside))), outside);
            }
            assertEquals(500, table.entries());
            assertEquals(100, table.tombstones());
            assertEquals(1, table.smallestSeq());
            assertEquals(500, table.largestSeq());
            assertArrayEquals(utf8("k00000"), table.smallestKey());
            assertArrayEquals(utf8("k00998"), table.largestKey());
        } finally {
            table.release();
        }
    }

    @Test
    void aCursorReadsEveryEntryInOrder() throws IOException {
        TreeMap<byte[], Entry> entries = sample(300);
        Table table = Table.open(write("1.sst", entries, 200), 1, PLAIN);
        try {
            List<byte[]> keys = new ArrayList<>();
            Table.Cursor cursor = table.cursor();
            while (cursor.next()) {
                assertEquals(entries.get(cursor.key()), cursor.entry());
                keys.add(cursor.key());
            }
            assertFalse(cursor.next());
            assertEquals(new ArrayList<>(entries.keySet()).size(), keys.size());
            for (int i = 0; i < keys.size(); i++) {
                assertArrayEquals(new ArrayList<>(entries.keySet()).get(i), keys.get(i));
            }
        } finally {
            table.release();
        }
    }

    @Test
    void aValueLargerThanABlockReadsBack() throws IOException {
        TreeMap<byte[], Entry> entries = new TreeMap<>(Bytes.ORDER);
        byte[] big = new byte[1 << 20];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i * 31);
        }
        entries.put(utf8("a"), Entry.value(1, utf8("small")));
        entries.put(utf8("b"), Entry.value(2, big));
        entries.put(utf8("c"), Entry.value(3, new byte[0]));
        Table table = Table.open(write("1.sst", entries, 4096), 1, PLAIN);
        try {
            assertArrayEquals(big, table.get(utf8("b"), hash(utf8("b"))).value());
            assertArrayEquals(new byte[0], table.get(utf8("c"), hash(utf8("c"))).value());
            assertEquals(2, table.blocks(), "a and b fill the first block; c starts the second");
        } finally {
            table.release();
        }
    }

    /** The Bloom filter's point: a key a table doesn't have usually costs no block read. */
    @Test
    void absentKeysInRangeRarelyReadABlock() throws IOException {
        Table table = Table.open(write("1.sst", sample(2_000), 4096), 1, PLAIN);
        try {
            for (int i = 0; i < 2_000; i++) {
                byte[] odd = utf8(String.format("k%05d", 2 * i + 1));
                assertNull(table.get(odd, hash(odd)));
            }
            System.out.println("table: 2000 absent keys in range read " + table.blocksRead() + " blocks");
            assertTrue(table.blocksRead() < 60, "blocks read: " + table.blocksRead()); // about 1% expected
        } finally {
            table.release();
        }
    }

    @Test
    void theWriterRefusesKeysOutOfOrderAnEmptyTableAndAnExistingFile() throws IOException {
        TableWriter writer = TableWriter.create(dir.resolve("1.sst"), 4096, 10, PLAIN);
        writer.add(utf8("b"), Entry.value(1, utf8("1")));
        assertThrows(IllegalArgumentException.class, () -> writer.add(utf8("b"), Entry.value(2, utf8("2"))));
        assertThrows(IllegalArgumentException.class, () -> writer.add(utf8("a"), Entry.value(3, utf8("3"))));
        writer.abandon();
        assertFalse(Files.exists(dir.resolve("1.sst")), "abandon deletes the file");

        TableWriter empty = TableWriter.create(dir.resolve("2.sst"), 4096, 10, PLAIN);
        assertThrows(IllegalStateException.class, empty::finish);
        empty.abandon();

        Files.write(dir.resolve("3.sst"), new byte[0]);
        assertThrows(FileAlreadyExistsException.class, () -> TableWriter.create(dir.resolve("3.sst"), 4096, 10, PLAIN));
    }

    /**
     * Every single-bit flip anywhere in a table is caught: the footer, filter and index when it opens,
     * a data block when it is read. None gives back different data.
     */
    @Test
    void everyBitFlipIsCaughtAtOpenOrRead() throws IOException {
        TreeMap<byte[], Entry> entries = sample(12);
        Path file = write("1.sst", entries, 64);
        byte[] good = Files.readAllBytes(file);
        Path flipped = dir.resolve("flipped.sst");
        int atOpen = 0;
        int atRead = 0;
        for (int bit = 0; bit < good.length * 8; bit++) {
            byte[] bytes = good.clone();
            bytes[bit / 8] ^= (byte) (1 << (bit % 8));
            Files.write(flipped, bytes);
            Table table;
            try {
                table = Table.open(flipped, 9, PLAIN);
            } catch (CorruptTableException expected) {
                atOpen++;
                continue;
            }
            try {
                Table.Cursor cursor = table.cursor();
                assertThrows(CorruptTableException.class, () -> {
                    while (cursor.next()) {
                        assertEquals(entries.get(cursor.key()), cursor.entry());
                    }
                }, "bit " + bit);
                atRead++;
            } finally {
                table.release();
            }
        }
        assertEquals(good.length * 8, atOpen + atRead);
        assertTrue(atOpen > 0 && atRead > 0, atOpen + " at open, " + atRead + " at read");
    }

    /** A table cut short anywhere (a copy that didn't finish, say) doesn't open. */
    @Test
    void aTableCutAtAnyByteDoesNotOpen() throws IOException {
        Path file = write("1.sst", sample(12), 64);
        byte[] good = Files.readAllBytes(file);
        Path cut = dir.resolve("cut.sst");
        for (int length = 0; length < good.length; length++) {
            Files.write(cut, java.util.Arrays.copyOf(good, length));
            int at = length;
            assertThrows(CorruptTableException.class, () -> Table.open(cut, 9, PLAIN), "cut at " + at);
        }
    }

    @Test
    void theLastReleaseClosesTheFileAndDeletesItIfObsolete() throws IOException {
        Path kept = write("1.sst", sample(10), 4096);
        Table table = Table.open(kept, 1, PLAIN);
        assertTrue(table.tryRetain());
        table.release();
        assertEquals(1, table.references());
        table.release();
        assertFalse(table.tryRetain(), "closed");
        assertTrue(Files.exists(kept), "not obsolete: kept");
        byte[] key = utf8("k00000");
        assertThrows(IOException.class, () -> table.get(key, hash(key)), "the file is closed");

        Path dropped = write("2.sst", sample(10), 4096);
        Table obsolete = Table.open(dropped, 2, PLAIN);
        assertTrue(obsolete.tryRetain()); // a reader
        obsolete.markObsolete();
        obsolete.release(); // the owner's
        assertTrue(Files.exists(dropped), "a reader still has it");
        assertEquals(Entry.value(1, utf8("v0-")), obsolete.get(key, hash(key)), "and can read it");
        obsolete.release(); // the reader's
        assertFalse(Files.exists(dropped));
        assertThrows(IllegalStateException.class, obsolete::release);
    }
}
