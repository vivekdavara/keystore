package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Review 5: since db858a8 a snapshot is built in {@code .TARGET.snapshot-N} beside the target, a
 * name about 30 bytes longer than the target's. A target whose name the file system takes (up to
 * 255 bytes on APFS and ext4) but whose building directory's it doesn't now fails, with "File name
 * too long" for a name the caller never gave; before the rewrite the target was made directly.
 */
class SnapshotTargetWithALongNameTest {
    @TempDir
    Path tmp;

    @Test
    void aTargetNameTheFileSystemTakesWorks() throws IOException {
        Path target = tmp.resolve("s".repeat(240));
        Files.createDirectory(target); // the file system takes the name...
        Files.delete(target);
        try (Store store = Store.open(tmp.resolve("store"), Options.defaults().withSync(SyncPolicy.NEVER))) {
            store.put(utf8("k"), utf8("v"));
            assertEquals(1, store.snapshot(target).lastSequence()); // ...and a snapshot there should too
        }
    }

    /**
     * The build directory's name keeps at most 200 bytes of the target's, not 200 characters: 80
     * three-byte characters are 240 bytes, which ext4 (255 bytes) takes, and with the build
     * directory's 30 more bytes it wouldn't. APFS counts characters (255), so on macOS this passes
     * either way; it is Linux CI that tells a cut by characters from a cut by bytes.
     */
    @Test
    void aLongNameOfManyByteCharactersIsCutBetweenThem() throws IOException {
        Path target = tmp.resolve("\u5feb".repeat(80)); // 快, 3 bytes in UTF-8
        Files.createDirectory(target);
        Files.delete(target);
        try (Store store = Store.open(tmp.resolve("store"), Options.defaults().withSync(SyncPolicy.NEVER))) {
            store.put(utf8("k"), utf8("v"));
            assertEquals(1, store.snapshot(target).lastSequence());
        }
        try (Store copy = Store.open(target, Options.defaults())) {
            assertEquals("v", new String(copy.get(utf8("k")).orElseThrow(), java.nio.charset.StandardCharsets.UTF_8));
        }
        try (var files = Files.list(tmp)) {
            assertEquals(2, files.count(), "the store and the snapshot, nothing else");
        }
    }
}
