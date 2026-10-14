package io.github.vivekdavara.keystore.sstable;

import io.github.vivekdavara.keystore.Bytes;
import io.github.vivekdavara.keystore.memtable.Entry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Writes one table file ({@link TableFormat}): add entries in key order, then {@link #finish}. A
 * file is written once and never changed. Not thread-safe.
 */
public final class TableWriter {
    /** What {@link #finish} wrote. */
    public record Written(long fileBytes, long entries, long tombstones, long smallestSeq, long largestSeq) {}

    private final Path file;
    private final FileChannel channel;
    private final int blockBytes;
    private final int bitsPerKey;

    private byte[] block = new byte[256];
    private int blockSize;
    private long offset; // where the current block will start
    private final List<byte[]> lastKeys = new ArrayList<>();
    private final List<long[]> blockOffsetsAndLengths = new ArrayList<>();
    private long[] hashes = new long[64];
    private long entries;
    private long tombstones;
    private long smallestSeq = Long.MAX_VALUE;
    private long largestSeq;
    private byte[] smallestKey;
    private byte[] lastKey;
    private boolean done;

    private TableWriter(Path file, FileChannel channel, int blockBytes, int bitsPerKey) {
        this.file = file;
        this.channel = channel;
        this.blockBytes = blockBytes;
        this.bitsPerKey = bitsPerKey;
    }

    /**
     * Creates {@code file}, which must not exist, for a table whose data blocks are about
     * {@code blockBytes} each (an entry bigger than that gets a block of its own), with a Bloom
     * filter of {@code bitsPerKey} bits a key. Every channel goes through {@code wrap}.
     */
    public static TableWriter create(
            Path file, int blockBytes, int bitsPerKey, BiFunction<Path, FileChannel, FileChannel> wrap)
            throws IOException {
        if (blockBytes < 64 || blockBytes > (1 << 24)) {
            throw new IllegalArgumentException("blocks must be 64 bytes to 16 MiB, not " + blockBytes);
        }
        if (bitsPerKey < 1 || bitsPerKey > 64) {
            throw new IllegalArgumentException("bits per key must be 1 to 64, not " + bitsPerKey);
        }
        FileChannel channel = wrap.apply(file, FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
        return new TableWriter(file, channel, blockBytes, bitsPerKey);
    }

    /**
     * Adds a key's entry (a value or a tombstone). Keys must come in strictly increasing order.
     *
     * @throws IllegalArgumentException if {@code key} isn't after the previous key
     */
    public void add(byte[] key, Entry entry) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(entry, "entry");
        if (done) {
            throw new IllegalStateException("the table is finished");
        }
        if (lastKey != null && Bytes.compare(lastKey, key) >= 0) {
            throw new IllegalArgumentException(
                    "keys must be added in increasing order: " + Bytes.show(key) + " after " + Bytes.show(lastKey));
        }
        int size = 4 + key.length + 8 + 1 + (entry.isTombstone() ? 0 : 4 + entry.value().length);
        ensureBlockRoom(size);
        ByteBuffer out = ByteBuffer.wrap(block, blockSize, size);
        out.putInt(key.length).put(key).putLong(entry.seq());
        if (entry.isTombstone()) {
            out.put(TableFormat.TOMBSTONE);
            tombstones++;
        } else {
            out.put(TableFormat.VALUE).putInt(entry.value().length).put(entry.value());
        }
        blockSize += size;

        if (entries == hashes.length) {
            hashes = Arrays.copyOf(hashes, hashes.length * 2);
        }
        hashes[(int) entries] = BloomFilter.hash(key);
        entries++;
        smallestSeq = Math.min(smallestSeq, entry.seq());
        largestSeq = Math.max(largestSeq, entry.seq());
        if (smallestKey == null) {
            smallestKey = key;
        }
        lastKey = key;
        if (blockSize >= blockBytes) {
            writeBlock();
        }
    }

    private void ensureBlockRoom(int size) {
        if (blockSize + size > block.length) {
            block = Arrays.copyOf(block, Math.max(block.length * 2, blockSize + size));
        }
    }

    private void writeBlock() throws IOException {
        lastKeys.add(lastKey);
        blockOffsetsAndLengths.add(new long[] {offset, blockSize});
        writeWithTrailer(block, blockSize);
        blockSize = 0;
        if (block.length > 4 * blockBytes) {
            block = new byte[256]; // don't keep a huge value's buffer around
        }
    }

    /** Writes {@code bytes[0, length)} and its checksum; returns where it started. */
    private long writeWithTrailer(byte[] bytes, int length) throws IOException {
        long start = offset;
        writeFully(ByteBuffer.wrap(bytes, 0, length));
        writeFully(ByteBuffer.allocate(TableFormat.TRAILER_BYTES).putInt(0, TableFormat.blockCrc(start, bytes, length)));
        offset += length + TableFormat.TRAILER_BYTES;
        return start;
    }

    private void writeFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    /**
     * Writes the last data block, the filter, the index and the footer, forces the file to the
     * device and closes it. The file's directory entry isn't forced: the caller does that before
     * anything refers to the file.
     *
     * @throws IllegalStateException if no entry was added: a table is never empty
     */
    public Written finish() throws IOException {
        if (done) {
            throw new IllegalStateException("the table is finished");
        }
        if (entries == 0) {
            throw new IllegalStateException("a table needs at least one entry");
        }
        if (blockSize > 0) {
            writeBlock();
        }
        byte[] filter = BloomFilter.build(hashes, (int) entries, bitsPerKey).toBytes();
        hashes = null;
        long filterOffset = writeWithTrailer(filter, filter.length);

        int indexSize = 4 + smallestKey.length + 4;
        for (byte[] key : lastKeys) {
            indexSize += 4 + key.length + 8 + 4;
        }
        ByteBuffer index = ByteBuffer.allocate(indexSize);
        index.putInt(smallestKey.length).put(smallestKey).putInt(lastKeys.size());
        for (int i = 0; i < lastKeys.size(); i++) {
            long[] at = blockOffsetsAndLengths.get(i);
            index.putInt(lastKeys.get(i).length).put(lastKeys.get(i)).putLong(at[0]).putInt((int) at[1]);
        }
        long indexOffset = writeWithTrailer(index.array(), indexSize);

        writeFully(TableFormat.encodeFooter(new TableFormat.Footer(
                filterOffset, filter.length, indexOffset, indexSize, entries, tombstones, smallestSeq, largestSeq)));
        offset += TableFormat.FOOTER_BYTES;
        channel.force(false);
        channel.close();
        done = true;
        return new Written(offset, entries, tombstones, smallestSeq, largestSeq);
    }

    /** Closes and deletes an unfinished file, after a failure. A file left behind is garbage the store removes on open. */
    public void abandon() {
        done = true;
        try {
            channel.close();
        } catch (IOException ignored) {
            // The file is garbage either way.
        }
        try {
            Files.deleteIfExists(file);
        } catch (IOException ignored) {
            // Removed by the next open.
        }
    }
}
