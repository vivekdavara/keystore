package io.github.vivekdavara.keystore.sstable;

import io.github.vivekdavara.keystore.Bytes;
import io.github.vivekdavara.keystore.memtable.Entry;
import io.github.vivekdavara.keystore.wal.WalFormat;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;

/**
 * An open table file ({@link TableFormat}): its footer, Bloom filter and index are read and checked
 * when it opens and kept in memory; a lookup reads at most one data block, with a positional read,
 * so any number of threads can read at once.
 *
 * <p>The table counts references, so that a compaction can drop it while reads are still in it: it
 * opens with one, the last {@link #release} closes the file, and deletes it if it was
 * {@link #markObsolete marked obsolete}.
 *
 * <p>A {@link FileChannel} is interruptible: a thread interrupted during a read closes the channel,
 * for every thread. So a read that finds the channel closed while the table still has references
 * reopens it ({@link #reopen}); the interrupted read itself still fails.
 */
public final class Table {
    private final Path file;
    private final long number;
    private final BiFunction<Path, FileChannel, FileChannel> wrap;
    private volatile FileChannel channel; // replaced only by reopen, under reopenLock
    private final Object reopenLock = new Object();
    private boolean closedForGood; // guarded by reopenLock: the last reference is gone
    private final long fileBytes;
    private final TableFormat.Footer footer;
    private final BloomFilter filter;
    private final byte[] smallest;
    private final byte[][] lastKeys;
    private final long[] blockOffsets;
    private final int[] blockLengths;

    private final AtomicInteger refs = new AtomicInteger(1);
    private volatile boolean obsolete;
    private final LongAdder blocksRead = new LongAdder();

    private Table(Path file, long number, BiFunction<Path, FileChannel, FileChannel> wrap, FileChannel channel,
            long fileBytes, TableFormat.Footer footer, BloomFilter filter, byte[] smallest, byte[][] lastKeys,
            long[] blockOffsets, int[] blockLengths) {
        this.file = file;
        this.number = number;
        this.wrap = wrap;
        this.channel = channel;
        this.fileBytes = fileBytes;
        this.footer = footer;
        this.filter = filter;
        this.smallest = smallest;
        this.lastKeys = lastKeys;
        this.blockOffsets = blockOffsets;
        this.blockLengths = blockLengths;
    }

    /**
     * Opens a table and checks its footer, filter and index (data blocks are checked when read).
     *
     * @param number the table's file number, for messages and the manifest
     * @throws CorruptTableException if any check fails
     */
    public static Table open(Path file, long number, BiFunction<Path, FileChannel, FileChannel> wrap)
            throws IOException {
        FileChannel channel = wrap.apply(file, FileChannel.open(file, StandardOpenOption.READ));
        try {
            return read(file, number, wrap, channel);
        } catch (IOException | RuntimeException | Error e) {
            try {
                channel.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private static Table read(Path file, long number, BiFunction<Path, FileChannel, FileChannel> wrap,
            FileChannel channel) throws IOException {
        long size = channel.size();
        if (size < TableFormat.FOOTER_BYTES) {
            throw new CorruptTableException(file, size + " bytes is too short for a table");
        }
        byte[] footerBytes = readAt(file, channel, size - TableFormat.FOOTER_BYTES, TableFormat.FOOTER_BYTES);
        String[] problem = new String[1];
        TableFormat.Footer footer = TableFormat.decodeFooter(footerBytes, problem);
        if (footer == null) {
            throw new CorruptTableException(file, problem[0]);
        }
        long footerAt = size - TableFormat.FOOTER_BYTES;
        if (footer.filterLength() < 0 || footer.indexLength() < 0
                || footer.indexOffset() + footer.indexLength() + TableFormat.TRAILER_BYTES != footerAt
                || footer.filterOffset() + footer.filterLength() + TableFormat.TRAILER_BYTES != footer.indexOffset()
                || footer.filterOffset() < 0 || footer.entries() < 1 || footer.tombstones() < 0
                || footer.tombstones() > footer.entries() || footer.smallestSeq() < 1
                || footer.smallestSeq() > footer.largestSeq()) {
            throw new CorruptTableException(file, "the footer's layout doesn't fit the file: " + footer);
        }
        BloomFilter filter;
        try {
            filter = BloomFilter.fromBytes(readBlock(file, channel, footer.filterOffset(), footer.filterLength(), "the filter"));
        } catch (IllegalArgumentException e) {
            throw new CorruptTableException(file, "the filter: " + e.getMessage());
        }
        byte[] index = readBlock(file, channel, footer.indexOffset(), footer.indexLength(), "the index");
        ByteBuffer in = ByteBuffer.wrap(index);
        try {
            byte[] smallest = key(in);
            int blocks = in.getInt();
            // Checked against what the index can hold before anything is allocated for them: the
            // checksums say the bytes are as written, not that a crafted file's counts are sane.
            if (blocks < 1 || blocks > footer.entries() || blocks > in.remaining() / TableFormat.MIN_INDEX_ENTRY) {
                throw new CorruptTableException(file, "the index lists " + blocks + " blocks for " + footer.entries()
                        + " entries in " + index.length + " bytes");
            }
            byte[][] lastKeys = new byte[blocks][];
            long[] offsets = new long[blocks];
            int[] lengths = new int[blocks];
            long expectedOffset = 0;
            for (int i = 0; i < blocks; i++) {
                lastKeys[i] = key(in);
                offsets[i] = in.getLong();
                lengths[i] = in.getInt();
                if (offsets[i] != expectedOffset || lengths[i] < 1
                        || lengths[i] > footer.filterOffset() - expectedOffset - TableFormat.TRAILER_BYTES) {
                    throw new CorruptTableException(file, "index entry " + i + " doesn't follow the block before it");
                }
                if (Bytes.compare(i == 0 ? smallest : lastKeys[i - 1], lastKeys[i]) > 0
                        || (i > 0 && Bytes.compare(lastKeys[i - 1], lastKeys[i]) == 0)) {
                    throw new CorruptTableException(file, "the index's keys are out of order at block " + i);
                }
                expectedOffset += (long) lengths[i] + TableFormat.TRAILER_BYTES;
            }
            if (expectedOffset != footer.filterOffset() || in.hasRemaining()) {
                throw new CorruptTableException(file, "the index doesn't cover the data blocks exactly");
            }
            return new Table(file, number, wrap, channel, size, footer, filter, smallest, lastKeys, offsets, lengths);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new CorruptTableException(file, "the index ends early or holds a bad length");
        }
    }

    private static byte[] key(ByteBuffer in) {
        int length = in.getInt();
        if (length < 1 || length > WalFormat.MAX_KEY_BYTES || length > in.remaining()) {
            throw new IllegalArgumentException("key length " + length);
        }
        byte[] key = new byte[length];
        in.get(key);
        return key;
    }

    /** The bytes of a block at {@code offset}, checked against its trailer. */
    private static byte[] readBlock(Path file, FileChannel channel, long offset, int length, String what)
            throws IOException {
        byte[] bytes = readAt(file, channel, offset, length + TableFormat.TRAILER_BYTES);
        if (TableFormat.intAt(bytes, length) != TableFormat.blockCrc(offset, bytes, length)) {
            throw new CorruptTableException(file, what + " at offset " + offset + " fails its checksum");
        }
        return Arrays.copyOf(bytes, length);
    }

    private static byte[] readAt(Path file, FileChannel channel, long position, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position + buffer.position());
            if (read < 0) {
                throw new CorruptTableException(file, "the file ends inside a block at offset " + position);
            }
        }
        return buffer.array();
    }

    /** Whether the key is within this table's key range and passes its Bloom filter. Reads nothing. */
    public boolean mightContain(byte[] key, long hash) {
        return Bytes.compare(key, smallest) >= 0
                && Bytes.compare(key, lastKeys[lastKeys.length - 1]) <= 0
                && filter.mightContain(hash);
    }

    /**
     * The key's entry here (a value or a tombstone), or null if the table doesn't have it.
     * {@code hash} is {@link BloomFilter#hash}{@code (key)}, computed once by the caller for every
     * table it asks.
     *
     * @throws CorruptTableException if the block it reads fails a check
     */
    public Entry get(byte[] key, long hash) throws IOException {
        if (!mightContain(key, hash)) {
            return null;
        }
        int b = firstBlockEndingAtOrAfter(key);
        byte[] block = dataBlock(b);
        int at = 0;
        while (at < block.length) {
            int keyLength = checkedKeyLength(block, at, b);
            int keyAt = at + 4;
            int cmp = Arrays.compareUnsigned(block, keyAt, keyAt + keyLength, key, 0, key.length);
            int next = entryEnd(block, keyAt + keyLength, b);
            if (cmp == 0) {
                return entryAt(block, keyAt + keyLength);
            }
            if (cmp > 0) {
                return null; // keys are sorted: it isn't here
            }
            at = next;
        }
        return null;
    }

    /** The index of the first block whose last key is at least {@code key}; the key is known to be in range. */
    private int firstBlockEndingAtOrAfter(byte[] key) {
        int lo = 0;
        int hi = lastKeys.length - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (Bytes.compare(lastKeys[mid], key) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    private byte[] dataBlock(int b) throws IOException {
        blocksRead.increment();
        for (boolean retried = false; ; retried = true) {
            FileChannel current = channel;
            try {
                return readBlock(file, current, blockOffsets[b], blockLengths[b], "data block " + b);
            } catch (ClosedByInterruptException e) {
                reopen(current); // this thread was interrupted and closed it for everyone: reopen it for them
                throw e;
            } catch (ClosedChannelException e) { // includes AsynchronousCloseException: another thread's interrupt
                if (retried || !reopen(current)) {
                    throw e;
                }
            }
        }
    }

    /**
     * Replaces a channel an interrupt closed, unless another read already has or the table has
     * been released for good; returns whether there is a usable channel to retry with.
     */
    private boolean reopen(FileChannel closed) throws IOException {
        synchronized (reopenLock) {
            if (closedForGood) {
                return false;
            }
            if (channel == closed && !closed.isOpen()) {
                channel = wrap.apply(file, FileChannel.open(file, StandardOpenOption.READ));
            }
            return true;
        }
    }

    private int checkedKeyLength(byte[] block, int at, int b) throws CorruptTableException {
        if (block.length - at < 4) {
            throw new CorruptTableException(file, "data block " + b + " ends inside an entry");
        }
        int keyLength = TableFormat.intAt(block, at);
        if (keyLength < 1 || keyLength > WalFormat.MAX_KEY_BYTES || keyLength > block.length - at - 4) {
            throw new CorruptTableException(file, "data block " + b + " holds a key length of " + keyLength);
        }
        return keyLength;
    }

    /** Where the entry whose key ends at {@code afterKey} ends, after checking its fields fit. */
    private int entryEnd(byte[] block, int afterKey, int b) throws CorruptTableException {
        if (block.length - afterKey < 9) {
            throw new CorruptTableException(file, "data block " + b + " ends inside an entry");
        }
        long seq = TableFormat.longAt(block, afterKey);
        byte kind = block[afterKey + 8];
        if (seq < 1 || (kind != TableFormat.VALUE && kind != TableFormat.TOMBSTONE)) {
            throw new CorruptTableException(file, "data block " + b + " holds an entry with seq " + seq + ", kind " + kind);
        }
        if (kind == TableFormat.TOMBSTONE) {
            return afterKey + 9;
        }
        if (block.length - afterKey - 9 < 4) {
            throw new CorruptTableException(file, "data block " + b + " ends inside an entry");
        }
        int valueLength = TableFormat.intAt(block, afterKey + 9);
        if (valueLength < 0 || valueLength > WalFormat.MAX_VALUE_BYTES || valueLength > block.length - afterKey - 13) {
            throw new CorruptTableException(file, "data block " + b + " holds a value length of " + valueLength);
        }
        return afterKey + 13 + valueLength;
    }

    /** The entry whose key ends at {@code afterKey}; its fields are known to fit. */
    private static Entry entryAt(byte[] block, int afterKey) {
        long seq = TableFormat.longAt(block, afterKey);
        if (block[afterKey + 8] == TableFormat.TOMBSTONE) {
            return Entry.tombstone(seq);
        }
        int valueLength = TableFormat.intAt(block, afterKey + 9);
        return Entry.value(seq, Arrays.copyOfRange(block, afterKey + 13, afterKey + 13 + valueLength));
    }

    /** Every entry in key order, for compaction and tools. */
    public Cursor cursor() {
        return new Cursor();
    }

    /**
     * Reads a table's entries in key order, one data block at a time, checking that keys strictly
     * increase, that each block ends with the key the index has for it, and that the count matches
     * the footer's.
     */
    public final class Cursor {
        private int nextBlock;
        private byte[] block;
        private int at;
        private int blockIndex = -1;
        private byte[] key;
        private Entry entry;
        private long seen;

        private Cursor() {}

        /** Moves to the next entry; false at the end. */
        public boolean next() throws IOException {
            while (block == null || at == block.length) {
                if (block != null && !Arrays.equals(key, lastKeys[blockIndex])) {
                    throw new CorruptTableException(file, "data block " + blockIndex + " doesn't end with the index's key");
                }
                if (nextBlock == lastKeys.length) {
                    if (seen != footer.entries()) {
                        throw new CorruptTableException(file, seen + " entries, but the footer says " + footer.entries());
                    }
                    block = null;
                    key = null;
                    entry = null;
                    return false;
                }
                blockIndex = nextBlock++;
                block = dataBlock(blockIndex);
                at = 0;
            }
            int keyLength = checkedKeyLength(block, at, blockIndex);
            byte[] next = Arrays.copyOfRange(block, at + 4, at + 4 + keyLength);
            if (key != null && Bytes.compare(key, next) >= 0) {
                throw new CorruptTableException(file, "keys out of order in data block " + blockIndex);
            }
            int end = entryEnd(block, at + 4 + keyLength, blockIndex);
            entry = entryAt(block, at + 4 + keyLength);
            key = next;
            at = end;
            seen++;
            return true;
        }

        /** The current entry's key (a copy the caller may keep). */
        public byte[] key() {
            return key;
        }

        public Entry entry() {
            return entry;
        }
    }

    /** Takes a reference; false if the table has already been closed by its last release. */
    public boolean tryRetain() {
        for (;;) {
            int r = refs.get();
            if (r == 0) {
                return false;
            }
            if (refs.compareAndSet(r, r + 1)) {
                return true;
            }
        }
    }

    /**
     * Drops a reference. The last one closes the file, and deletes it if the table is obsolete. A
     * failure to close a read-only file or to delete it loses nothing: a file the manifest doesn't
     * name is removed when the store next opens.
     */
    public void release() {
        int r = refs.decrementAndGet();
        if (r < 0) {
            throw new IllegalStateException("table " + number + " released more often than retained");
        }
        if (r == 0) {
            synchronized (reopenLock) {
                closedForGood = true;
                try {
                    channel.close();
                } catch (IOException ignored) {
                    // Read-only: nothing to lose.
                }
            }
            if (obsolete) {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException ignored) {
                    // Removed by the next open.
                }
            }
        }
    }

    /** Marks the table for deletion when its last reference goes: a compaction replaced it. */
    public void markObsolete() {
        obsolete = true;
    }

    /** References held now: for tests. */
    public int references() {
        return refs.get();
    }

    public long number() {
        return number;
    }

    public Path file() {
        return file;
    }

    public long fileBytes() {
        return fileBytes;
    }

    public long entries() {
        return footer.entries();
    }

    public long tombstones() {
        return footer.tombstones();
    }

    public long smallestSeq() {
        return footer.smallestSeq();
    }

    public long largestSeq() {
        return footer.largestSeq();
    }

    /** The smallest key (not a copy). */
    public byte[] smallestKey() {
        return smallest;
    }

    /** The largest key (not a copy). */
    public byte[] largestKey() {
        return lastKeys[lastKeys.length - 1];
    }

    public int blocks() {
        return lastKeys.length;
    }

    /** Data blocks read so far, by lookups and cursors. */
    public long blocksRead() {
        return blocksRead.sum();
    }

    public int filterBytes() {
        return filter.sizeInBytes();
    }

    @Override
    public String toString() {
        return String.format("table %d (%d bytes, %d entries, seq %d-%d)",
                number, fileBytes, footer.entries(), footer.smallestSeq(), footer.largestSeq());
    }
}
