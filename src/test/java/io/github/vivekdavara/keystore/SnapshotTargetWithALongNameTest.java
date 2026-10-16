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
}
