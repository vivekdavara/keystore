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
import io.github.vivekdavara.keystore.wal.WalFormat;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TableTest {
    private static final BiFunction<Path, FileChannel, FileChannel> PLAIN = (path, channel) -> channel;
    /** Bytes on both sides of 0x80, so that unsigned order matters and many keys are prefixes of others. */
    private static final byte[] ALPHABET = {0x00, 0x01, 0x41, 0x7f, (byte) 0x80, (byte) 0x81, (byte) 0xfe, (byte) 0xff};

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
            Files.write(cut, Arrays.copyOf(good, length));
            int at = length;
            assertThrows(CorruptTableException.class, () -> Table.open(cut, 9, PLAIN), "cut at " + at);
        }
    }

    /**
     * FileChannel is interruptible: an interrupted read closes the channel for every thread. Found
     * while building day 2 (a probe: after one interrupted get, every later read of the table failed
     * with ClosedChannelException until a reopen). The interrupted read still fails; the next reads,
     * on any thread, work.
     */
    @Test
    void anInterruptedReadDoesNotBreakTheTableForOthers() throws Exception {
        Table table = Table.open(write("1.sst", sample(200), 128), 1, PLAIN);
        try {
            byte[] key = utf8("k00010");
            Thread.currentThread().interrupt();
            try {
                assertThrows(java.nio.channels.ClosedByInterruptException.class, () -> table.get(key, hash(key)));
            } finally {
                Thread.interrupted(); // clear it
            }
            assertEquals(Entry.value(6, utf8("v5-xxxxx")), table.get(key, hash(key)), "this thread, after the interrupt");
            Entry[] fromOther = new Entry[1];
            Thread other = new Thread(() -> {
                try {
                    fromOther[0] = table.get(key, hash(key));
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
            other.start();
            other.join();
            assertEquals(Entry.value(6, utf8("v5-xxxxx")), fromOther[0], "another thread");
            Table.Cursor cursor = table.cursor();
            int entries = 0;
            while (cursor.next()) {
                entries++;
            }
            assertEquals(200, entries);
        } finally {
            table.release();
        }
        byte[] key = utf8("k00010");
        assertThrows(java.nio.channels.ClosedChannelException.class, () -> table.get(key, hash(key)),
                "released for good: not reopened");
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

    private static void assertSameEntry(Entry expected, Entry actual, String what) {
        if (expected == null) {
            assertNull(actual, what);
            return;
        }
        assertTrue(actual != null, what + ": missing");
        assertEquals(expected.seq(), actual.seq(), what);
        assertEquals(expected.isTombstone(), actual.isTombstone(), what);
        if (!expected.isTombstone()) {
            assertArrayEquals(expected.value(), actual.value(), what);
        }
    }

    /**
     * From the day-2 review: random tables over keys of 1-5 bytes drawn from bytes on both sides of 0x80 (so many keys are
     * prefixes of others), with empty values, values bigger than a block, and tombstones: every key,
     * every prefix and one-byte extension of a key, and random probes answer as a TreeMap would.
     */
    @Test
    void randomTablesAnswerEveryLookupExactly() throws IOException {
        int tables = 0;
        for (int blockBytes : new int[] {64, 100, 333, 4096}) {
            for (long seed = 1; seed <= 25; seed++) {
                Random random = new Random(seed * 31 + blockBytes);
                TreeMap<byte[], Entry> entries = new TreeMap<>(Bytes.ORDER);
                int count = 1 + random.nextInt(seed % 5 == 0 ? 3 : 300);
                long seq = 1;
                for (int i = 0; i < count; i++) {
                    byte[] key = new byte[1 + random.nextInt(5)];
                    for (int j = 0; j < key.length; j++) {
                        key[j] = ALPHABET[random.nextInt(ALPHABET.length)];
                    }
                    int kind = random.nextInt(10);
                    Entry entry;
                    if (kind == 0) {
                        entry = Entry.tombstone(seq++);
                    } else if (kind == 1) {
                        entry = Entry.value(seq++, new byte[0]);
                    } else if (kind == 2) {
                        byte[] big = new byte[blockBytes * 2 + random.nextInt(50)];
                        random.nextBytes(big);
                        entry = Entry.value(seq++, big);
                    } else {
                        byte[] value = new byte[random.nextInt(20)];
                        random.nextBytes(value);
                        entry = Entry.value(seq++, value);
                    }
                    entries.put(key, entry);
                }
                Path file = write("t" + (tables++) + ".sst", entries, blockBytes);
                Table table = Table.open(file, 1, PLAIN);
                try {
                    TreeSet<byte[]> probes = new TreeSet<>(Bytes.ORDER);
                    for (byte[] key : entries.keySet()) {
                        probes.add(key);
                        for (int cut = 1; cut < key.length; cut++) {
                            probes.add(Arrays.copyOf(key, cut));
                        }
                        for (byte b : ALPHABET) {
                            byte[] longer = Arrays.copyOf(key, key.length + 1);
                            longer[key.length] = b;
                            probes.add(longer);
                        }
                    }
                    for (int i = 0; i < 500; i++) {
                        byte[] probe = new byte[1 + random.nextInt(6)];
                        random.nextBytes(probe);
                        probes.add(probe);
                    }
                    for (byte[] probe : probes) {
                        assertSameEntry(entries.get(probe), table.get(probe, BloomFilter.hash(probe)),
                                "seed " + seed + ", blocks of " + blockBytes + ", key " + Bytes.show(probe));
                    }
                    Table.Cursor cursor = table.cursor();
                    int n = 0;
                    for (Map.Entry<byte[], Entry> e : entries.entrySet()) {
                        assertTrue(cursor.next(), "cursor ended early at " + n);
                        assertArrayEquals(e.getKey(), cursor.key());
                        assertSameEntry(e.getValue(), cursor.entry(), "cursor entry " + n);
                        n++;
                    }
                    assertFalse(cursor.next());
                    assertFalse(cursor.next(), "stays at the end");
                } finally {
                    table.release();
                }
            }
        }
    }

    /** Keys of the maximum length, differing only in their last byte across 0x80, and a single-entry table. */
    @Test
    void maximumLengthKeysAndASingleEntryTable() throws IOException {
        byte[] prefix = new byte[WalFormat.MAX_KEY_BYTES - 1];
        Arrays.fill(prefix, (byte) 0x90);
        byte[] low = Arrays.copyOf(prefix, WalFormat.MAX_KEY_BYTES);
        low[low.length - 1] = 0x7f;
        byte[] high = Arrays.copyOf(prefix, WalFormat.MAX_KEY_BYTES);
        high[high.length - 1] = (byte) 0x80;
        TreeMap<byte[], Entry> entries = new TreeMap<>(Bytes.ORDER);
        entries.put(prefix, Entry.value(1, new byte[0]));
        entries.put(low, Entry.tombstone(2));
        entries.put(high, Entry.value(3, new byte[] {7}));
        Table table = Table.open(write("max.sst", entries, 64), 1, PLAIN);
        try {
            assertEquals(3, table.blocks());
            for (Map.Entry<byte[], Entry> e : entries.entrySet()) {
                assertSameEntry(e.getValue(), table.get(e.getKey(), BloomFilter.hash(e.getKey())), "max-length key");
            }
            byte[] between = Arrays.copyOf(prefix, WalFormat.MAX_KEY_BYTES);
            between[between.length - 1] = 0x00;
            assertNull(table.get(between, BloomFilter.hash(between)));
        } finally {
            table.release();
        }

        TreeMap<byte[], Entry> one = new TreeMap<>(Bytes.ORDER);
        one.put(high, Entry.value(9, new byte[100_000]));
        Table single = Table.open(write("one.sst", one, 64), 2, PLAIN);
        try {
            assertSameEntry(one.get(high), single.get(high, BloomFilter.hash(high)), "single entry");
            assertNull(single.get(low, BloomFilter.hash(low)));
            assertNull(single.get(prefix, BloomFilter.hash(prefix)));
            Table.Cursor cursor = single.cursor();
            assertTrue(cursor.next());
            assertFalse(cursor.next());
        } finally {
            single.release();
        }
    }

    /**
     * Found by review: a block's checksum covered its bytes but not where they were. Two data blocks
     * of the same length swapped (or one written over the other) passed every check at open, and a
     * lookup in the block answered "not here" for a key the table has, so a store read fell through
     * to an older table and returned an overwritten value. The trailer now covers the offset too.
     */
    @Test
    void swappedBlocksOfEqualLengthFailTheirChecksums() throws IOException {
        TreeMap<byte[], Entry> entries = new TreeMap<>(Bytes.ORDER);
        for (int i = 0; i < 40; i++) {
            entries.put(Bytes.utf8(String.format("key%04d", i)), Entry.value(100 + i, Bytes.utf8(String.format("val%04d", i))));
        }
        Path file = write("swap.sst", entries, 64);
        Table good = Table.open(file, 1, PLAIN);
        int blocks = good.blocks();
        good.release();
        assertTrue(blocks >= 3, blocks + " blocks");
        byte[] bytes = Files.readAllBytes(file);
        // Every entry is 4 + 7 + 8 + 1 + 4 + 7 = 31 bytes, so with 64-byte blocks each block holds 3 entries: 93 bytes.
        int blockAndTrailer = 93 + TableFormat.TRAILER_BYTES;
        byte[] first = Arrays.copyOfRange(bytes, 0, blockAndTrailer);
        System.arraycopy(bytes, blockAndTrailer, bytes, 0, blockAndTrailer);
        System.arraycopy(first, 0, bytes, blockAndTrailer, blockAndTrailer);
        Path damaged = dir.resolve("damaged.sst");
        Files.write(damaged, bytes);

        Table table = Table.open(damaged, 1, PLAIN); // the filter and index are where they belong
        try {
            for (String k : new String[] {"key0001", "key0004"}) { // one key in each swapped block
                byte[] key = Bytes.utf8(k);
                CorruptTableException caught = assertThrows(CorruptTableException.class,
                        () -> table.get(key, BloomFilter.hash(key)), k);
                assertTrue(caught.getMessage().contains("fails its checksum"), caught.getMessage());
            }
            byte[] third = Bytes.utf8("key0007");
            assertSameEntry(entries.get(third), table.get(third, BloomFilter.hash(third)), "an untouched block still reads");
            Table.Cursor cursor = table.cursor();
            assertThrows(CorruptTableException.class, cursor::next);
        } finally {
            table.release();
        }
    }

    /**
     * Found by review, with a crafted file whose checksums are all valid: the index's block count was
     * bounded only by the footer's entry count, so the reader allocated three arrays of that many
     * elements before finding the index far too short for them, and died of an OutOfMemoryError.
     * The count is now checked against the index's length first.
     */
    @Test
    void aTableInALaterFormatIsRefusedByName() throws IOException {
        TreeMap<byte[], Entry> entries = new TreeMap<>(Bytes.ORDER);
        entries.put(Bytes.utf8("a"), Entry.value(1, Bytes.utf8("x")));
        Path labelled = write("labelled.sst", entries, 64);
        TableSurgery.setFormatVersion(labelled, 2); // the footer's version field, its checksum recomputed
        Path magic = write("magic.sst", entries, 64);
        byte[] bytes = Files.readAllBytes(magic);
        bytes[bytes.length - 1] = 2; // "KSTABLE" and a 2
        Files.write(magic, bytes);
        for (Path later : List.of(labelled, magic)) {
            CorruptTableException refused = assertThrows(CorruptTableException.class, () -> Table.open(later, 1, PLAIN));
            assertTrue(refused.getMessage().endsWith(": table format 2, which this build doesn't read (it reads 1): written"
                    + " by a later version of keystore, or damaged"), refused.getMessage());
        }
    }

    /** No version wrote format 0, by the version field or the magic: damage, not a later version (the release review). */
    @Test
    void aTableBelowTheOldestFormatIsCalledDamaged() throws IOException {
        TreeMap<byte[], Entry> entries = new TreeMap<>(Bytes.ORDER);
        entries.put(Bytes.utf8("a"), Entry.value(1, Bytes.utf8("x")));
        Path labelled = write("labelled.sst", entries, 64);
        TableSurgery.setFormatVersion(labelled, 0);
        Path magic = write("magic.sst", entries, 64);
        byte[] bytes = Files.readAllBytes(magic);
        bytes[bytes.length - 1] = 0;
        Files.write(magic, bytes);
        for (Path damaged : List.of(labelled, magic)) {
            CorruptTableException refused = assertThrows(CorruptTableException.class, () -> Table.open(damaged, 1, PLAIN));
            assertTrue(refused.getMessage().endsWith(": table format 0, which this build doesn't read (it reads 1): no"
                    + " version of keystore writes it, so the file is damaged"), refused.getMessage());
        }
    }

    @Test
    void anIndexClaimingAHugeBlockCountIsRefusedBeforeAllocating() throws IOException {
        TreeMap<byte[], Entry> entries = new TreeMap<>(Bytes.ORDER);
        entries.put(Bytes.utf8("a"), Entry.value(1, Bytes.utf8("x")));
        Path file = write("crafted.sst", entries, 64);
        byte[] bytes = Files.readAllBytes(file);
        int footerAt = bytes.length - TableFormat.FOOTER_BYTES;
        ByteBuffer footer = ByteBuffer.wrap(bytes, footerAt, TableFormat.FOOTER_BYTES).slice();
        int indexOffset = (int) footer.getLong(12);
        int indexLength = footer.getInt(20);
        ByteBuffer index = ByteBuffer.wrap(bytes, indexOffset, indexLength).slice();
        int smallestLength = index.getInt(0);
        index.putInt(4 + smallestLength, Integer.MAX_VALUE - 8); // the block count
        byte[] indexBytes = Arrays.copyOfRange(bytes, indexOffset, indexOffset + indexLength);
        ByteBuffer.wrap(bytes).putInt(indexOffset + indexLength, TableFormat.blockCrc(indexOffset, indexBytes, indexLength));
        footer.putLong(24, Long.MAX_VALUE / 2); // entries, so the count passes "blocks <= entries"
        footer.putInt(TableFormat.FOOTER_BYTES - 12, TableFormat.crc(bytes, footerAt, TableFormat.FOOTER_BYTES - 12));
        Path crafted = dir.resolve("crafted2.sst");
        Files.write(crafted, bytes);
        CorruptTableException refused = assertThrows(CorruptTableException.class, () -> Table.open(crafted, 1, PLAIN));
        assertTrue(refused.getMessage().contains("blocks for"), "refused by the count check: " + refused.getMessage());
    }

}
