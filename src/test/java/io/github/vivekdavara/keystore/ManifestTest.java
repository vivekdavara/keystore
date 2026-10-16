package io.github.vivekdavara.keystore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.vivekdavara.keystore.Manifest.TableRef;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ManifestTest {
    private static final BiFunction<Path, FileChannel, FileChannel> PLAIN = (path, channel) -> channel;

    @TempDir
    Path dir;

    private static Manifest sample() {
        return new Manifest(12, 11, 900, 0xfedcba98L, List.of(
                new TableRef(10, 5_000, 701, 890),
                new TableRef(7, 20_000, 1, 700)));
    }

    /** {@code manifest} in day 2's format, version 1, which has no last checksum. */
    static byte[] encodeVersion1(Manifest manifest) {
        ByteBuffer out = ByteBuffer.allocate(8 + 8 + 8 + 8 + 4 + 32 * manifest.tables().size() + 4);
        out.putLong(Manifest.MAGIC_V1).putLong(manifest.nextFileNumber()).putLong(manifest.logNumber())
                .putLong(manifest.lastSequence()).putInt(manifest.tables().size());
        for (TableRef t : manifest.tables()) {
            out.putLong(t.number()).putLong(t.fileBytes()).putLong(t.smallestSeq()).putLong(t.largestSeq());
        }
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.position());
        out.putInt((int) crc.getValue());
        return out.array();
    }

    @Test
    void aDayTwoManifestReadsWithoutItsLastChecksum() throws IOException {
        Manifest read = Manifest.decode(encodeVersion1(sample()));
        assertEquals(new Manifest(12, 11, 900, -1, sample().tables()), read);
        assertEquals(Manifest.empty(), Manifest.decode(encodeVersion1(Manifest.empty())));
        // Written again, it is version 2.
        read.write(dir, PLAIN);
        assertEquals(Manifest.MAGIC, ByteBuffer.wrap(Files.readAllBytes(dir.resolve(Manifest.FILE))).getLong());
    }

    @Test
    void theLastChecksumIsUnsigned32BitsOrUnknown() throws IOException {
        for (long checksum : new long[] {-1, 0, 1, 0x7fffffffL, 0x80000000L, 0xffffffffL}) {
            Manifest m = new Manifest(12, 11, 900, checksum, List.of());
            assertEquals(m, Manifest.decode(m.encode()));
        }
        assertThrows(IllegalArgumentException.class, () -> new Manifest(12, 11, 900, -2, List.of()));
        assertThrows(IllegalArgumentException.class, () -> new Manifest(12, 11, 900, 0x100000000L, List.of()));
        assertThrows(IllegalArgumentException.class, () -> new Manifest(12, 11, 0, 7, List.of()), "no writes, no checksum");
    }

    @Test
    void aManifestReadsBackAsWritten() throws IOException {
        assertNull(Manifest.read(dir));
        sample().write(dir, PLAIN);
        assertEquals(sample(), Manifest.read(dir));
        Manifest.empty().write(dir, PLAIN);
        assertEquals(Manifest.empty(), Manifest.read(dir), "the second write replaces the first");
        assertFalse(Files.exists(dir.resolve(Manifest.TEMP_FILE)), "renamed, not copied");
    }

    @Test
    void everyBitFlipIsCaught() {
        byte[] good = sample().encode();
        for (int bit = 0; bit < good.length * 8; bit++) {
            byte[] bytes = good.clone();
            bytes[bit / 8] ^= (byte) (1 << (bit % 8));
            int at = bit;
            assertThrows(IOException.class, () -> Manifest.decode(bytes), "bit " + at);
        }
    }

    @Test
    void aManifestCutShortIsCaught() {
        byte[] good = sample().encode();
        for (int length = 0; length < good.length; length++) {
            byte[] cut = Arrays.copyOf(good, length);
            int at = length;
            assertThrows(IOException.class, () -> Manifest.decode(cut), "cut at " + at);
        }
    }

    /** A crash while writing the new manifest leaves a temp file and the old manifest, which is what's read. */
    @Test
    void aTornTempFileLeavesTheOldManifest() throws IOException {
        sample().write(dir, PLAIN);
        Files.write(dir.resolve(Manifest.TEMP_FILE), Arrays.copyOf(Manifest.empty().encode(), 10));
        assertEquals(sample(), Manifest.read(dir));
    }

    @Test
    void inconsistentFieldsAreRefused() {
        // Tables must be newest first, with disjoint sequence ranges below the last sequence.
        assertThrows(IllegalArgumentException.class, () -> new Manifest(12, 11, 900, List.of(
                new TableRef(7, 20_000, 1, 700), new TableRef(10, 5_000, 701, 890))));
        assertThrows(IllegalArgumentException.class, () -> new Manifest(12, 11, 900, List.of(
                new TableRef(10, 5_000, 600, 890), new TableRef(7, 20_000, 1, 700))));
        assertThrows(IllegalArgumentException.class, () -> new Manifest(12, 11, 800, List.of(
                new TableRef(10, 5_000, 701, 890))));
        // File numbers: unique, below the next one, and the log below it too.
        assertThrows(IllegalArgumentException.class, () -> new Manifest(12, 11, 900, List.of(
                new TableRef(12, 5_000, 701, 890))));
        assertThrows(IllegalArgumentException.class, () -> new Manifest(12, 11, 900, List.of(
                new TableRef(7, 5_000, 701, 890), new TableRef(7, 5_000, 1, 700))));
        assertThrows(IllegalArgumentException.class, () -> new Manifest(11, 11, 0, List.of()));
        assertThrows(IllegalArgumentException.class, () -> new Manifest(2, 0, 0, List.of()));
    }
}
