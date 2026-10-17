package io.github.vivekdavara.keystore.sstable;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * The table file format. All integers are big-endian.
 *
 * <pre>
 * file         = data block, data block, ..., filter block, index block, footer
 * every block is followed by a 4-byte trailer: the CRC32C of the block's offset in the file
 * (8 bytes), then the block's bytes, so a block found in the wrong place fails it as well
 *
 * data block   = entry, entry, ...  (keys strictly increasing, across blocks too)
 * entry        = key length (4) | key | seq (8) | kind (1: 1 = value, 2 = tombstone)
 *                | value length (4) | value          (the last two for a value only)
 * filter block = the Bloom filter ({@link BloomFilter#toBytes})
 * index block  = smallest key length (4) | smallest key | block count (4)
 *                | per data block: last key length (4) | last key | offset (8) | length (4)
 * footer       = filter offset (8) | filter length (4) | index offset (8) | index length (4)
 *                | entries (8) | tombstones (8) | smallest seq (8) | largest seq (8)
 *                | format version (4) | CRC32C of the footer's bytes before it (4) | magic (8)
 * </pre>
 *
 * Lengths exclude the trailer. The index is sparse: one key per data block, its last, so a read
 * binary-searches the index for the first block whose last key is at least the key it wants, and
 * reads that one block.
 */
final class TableFormat {
    private TableFormat() {}

    static final int TRAILER_BYTES = 4;
    static final int FOOTER_BYTES = 8 + 4 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 4 + 8;
    static final int VERSION = 1;
    /** "KSTABLE" and a 1: the magic's last byte is the format version too. */
    static final long MAGIC = 0x4b535441424c4501L;

    static final byte VALUE = 1;
    static final byte TOMBSTONE = 2;

    /** What the footer says. */
    record Footer(
            long filterOffset, int filterLength, long indexOffset, int indexLength,
            long entries, long tombstones, long smallestSeq, long largestSeq) {}

    static ByteBuffer encodeFooter(Footer f) {
        ByteBuffer out = ByteBuffer.allocate(FOOTER_BYTES);
        out.putLong(f.filterOffset()).putInt(f.filterLength());
        out.putLong(f.indexOffset()).putInt(f.indexLength());
        out.putLong(f.entries()).putLong(f.tombstones());
        out.putLong(f.smallestSeq()).putLong(f.largestSeq());
        out.putInt(VERSION);
        out.putInt(crc(out.array(), 0, out.position()));
        out.putLong(MAGIC);
        return out.flip();
    }

    /** The footer in {@code bytes}, or null with {@code problem[0]} set. */
    static Footer decodeFooter(byte[] bytes, String[] problem) {
        ByteBuffer in = ByteBuffer.wrap(bytes);
        long magic = in.getLong(FOOTER_BYTES - 8);
        if (magic >>> 8 != MAGIC >>> 8) {
            problem[0] = "no table magic at the end of the file";
            return null;
        }
        if (magic != MAGIC) {
            // A later format may lay the footer out differently: refuse before reading it.
            problem[0] = unreadable(magic & 0xff);
            return null;
        }
        int checked = FOOTER_BYTES - 12;
        if (in.getInt(checked) != crc(bytes, 0, checked)) {
            problem[0] = "the footer's checksum doesn't match";
            return null;
        }
        Footer footer = new Footer(
                in.getLong(), in.getInt(), in.getLong(), in.getInt(),
                in.getLong(), in.getLong(), in.getLong(), in.getLong());
        int version = in.getInt();
        if (version != VERSION) {
            problem[0] = unreadable(version);
            return null;
        }
        return footer;
    }

    /** Below this build's format (0, or a negative version field), no version wrote it. */
    private static String unreadable(long format) {
        return "table format " + format + ", which this build doesn't read (it reads " + VERSION + "): "
                + (format > VERSION
                        ? "written by a later version of keystore, or damaged"
                        : "no version of keystore writes it, so the file is damaged");
    }

    /**
     * A block's trailer: the CRC32C of its offset in the file, then its bytes. A checksum of the
     * bytes alone would pass two blocks of one length swapped, or a block written over another,
     * and a lookup in such a block finds no key and says so.
     */
    static int blockCrc(long position, byte[] bytes, int length) {
        CRC32C crc = new CRC32C();
        crc.update(ByteBuffer.allocate(8).putLong(0, position));
        crc.update(bytes, 0, length);
        return (int) crc.getValue();
    }

    /** The smallest index entry: a 1-byte last key with its length, the block's offset and length. */
    static final int MIN_INDEX_ENTRY = 4 + 1 + 8 + 4;

    static int crc(byte[] bytes, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }

    static int intAt(byte[] b, int at) {
        return ((b[at] & 0xff) << 24) | ((b[at + 1] & 0xff) << 16) | ((b[at + 2] & 0xff) << 8) | (b[at + 3] & 0xff);
    }

    static long longAt(byte[] b, int at) {
        return ((long) intAt(b, at) << 32) | (intAt(b, at + 4) & 0xffffffffL);
    }
}
