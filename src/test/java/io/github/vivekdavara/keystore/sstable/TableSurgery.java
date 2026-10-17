package io.github.vivekdavara.keystore.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Rewrites a table file so that it still passes every checksum but says something false, for the
 * checks that look past checksums (as `keystore verify` does). Each recomputes what it must: the
 * footer's checksum, or a block's trailer.
 */
public final class TableSurgery {
    private TableSurgery() {}

    /** Rewrites the footer's tombstone count and sequence range; everything else stays. */
    public static void rewriteFooter(Path file, long tombstones, long smallestSeq, long largestSeq) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        int at = bytes.length - TableFormat.FOOTER_BYTES;
        TableFormat.Footer f = footer(bytes);
        TableFormat.Footer lie = new TableFormat.Footer(f.filterOffset(), f.filterLength(), f.indexOffset(),
                f.indexLength(), f.entries(), tombstones, smallestSeq, largestSeq);
        TableFormat.encodeFooter(lie).get(bytes, at, TableFormat.FOOTER_BYTES);
        Files.write(file, bytes);
    }

    /** Replaces the Bloom filter with one of the same size that holds no key, and its trailer to match. */
    public static void emptyTheFilter(Path file) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        TableFormat.Footer f = footer(bytes);
        int from = Math.toIntExact(f.filterOffset());
        Arrays.fill(bytes, from + 1, from + f.filterLength(), (byte) 0); // keep the probe count, clear the bits
        byte[] filter = Arrays.copyOfRange(bytes, from, from + f.filterLength());
        ByteBuffer.wrap(bytes).putInt(from + f.filterLength(), TableFormat.blockCrc(f.filterOffset(), filter, filter.length));
        Files.write(file, bytes);
    }

    /**
     * Writes {@code version} into the footer's format version and recomputes the footer's checksum:
     * the table as a later build would label it.
     */
    public static void setFormatVersion(Path file, int version) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        int at = bytes.length - TableFormat.FOOTER_BYTES;
        ByteBuffer footer = ByteBuffer.wrap(bytes, at, TableFormat.FOOTER_BYTES).slice();
        footer.putInt(TableFormat.FOOTER_BYTES - 16, version);
        footer.putInt(TableFormat.FOOTER_BYTES - 12, TableFormat.crc(bytes, at, TableFormat.FOOTER_BYTES - 12));
        Files.write(file, bytes);
    }

    private static TableFormat.Footer footer(byte[] bytes) throws IOException {
        String[] problem = new String[1];
        TableFormat.Footer f = TableFormat.decodeFooter(
                Arrays.copyOfRange(bytes, bytes.length - TableFormat.FOOTER_BYTES, bytes.length), problem);
        if (f == null) {
            throw new IOException(problem[0]);
        }
        return f;
    }
}
