package io.github.vivekdavara.keystore.review5;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.vivekdavara.keystore.Bytes;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.sstable.TableSurgery;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Manifest.decode and TableFormat.decodeFooter call every format they don't read "written by a later
 * version of keystore, or damaged", including formats below the oldest this build reads: a manifest
 * whose magic ends in 0 (this build reads 1 to 2), or a table whose footer says format 0 (it reads
 * 1). No version wrote format 0, so "a later version" points an operator at the wrong cause: it can
 * only be damage. (Found by setting the version byte to 0 with the checksum recomputed.)
 */
class AnOlderFormatIsNotALaterVersionsTest {
    @TempDir
    Path dir;

    private void makeStore() throws IOException {
        try (Store store = Store.open(dir)) {
            store.put(Bytes.utf8("city"), Bytes.utf8("Boston"));
            store.flush();
        }
    }

    @Test
    void aManifestInFormat0IsNotCalledALaterVersions() throws IOException {
        makeStore();
        Path manifest = dir.resolve("MANIFEST");
        byte[] bytes = Files.readAllBytes(manifest);
        bytes[7] = 0;
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        ByteBuffer.wrap(bytes).putInt(bytes.length - 4, (int) crc.getValue());
        Files.write(manifest, bytes);

        String message = assertThrows(IOException.class, () -> Store.open(dir)).getMessage();
        assertFalse(message.contains("later version"), message);
    }

    @Test
    void aTableInFormat0IsNotCalledALaterVersions() throws IOException {
        makeStore();
        Path table;
        try (Stream<Path> list = Files.list(dir)) {
            table = list.filter(f -> f.toString().endsWith(".sst")).findFirst().orElseThrow();
        }
        TableSurgery.setFormatVersion(table, 0);

        String message = assertThrows(IOException.class, () -> Store.open(dir)).getMessage();
        assertFalse(message.contains("later version"), message);
    }
}
