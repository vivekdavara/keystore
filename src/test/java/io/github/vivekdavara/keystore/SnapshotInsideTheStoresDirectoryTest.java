package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Review 5: {@code Store.snapshot} refuses a target directly in the store's own directory (its
 * name could be one the store writes next, like MANIFEST.tmp, "and the store's next write of that
 * file would fail"), but it only compares the real path of the target's parent with the store's.
 * Two ways past that check, each leaving a directory named MANIFEST.tmp in the store's directory:
 *
 * <ul>
 *   <li>the store's directory by another path that toRealPath keeps: a macOS firmlink
 *       (/System/Volumes/Data/private/... is /private/...), the case Store.open's own check uses
 *       the file key for. The snapshot succeeds, and becomes STORE/MANIFEST.tmp; every flush
 *       after it fails, and goes on failing after a reopen, since recovery can't delete a
 *       non-empty directory.
 *   <li>a target two levels down, {@code STORE/MANIFEST.tmp/snap}: the snapshot creates its
 *       missing parent, STORE/MANIFEST.tmp, before any check, so the snapshot's own flush fails,
 *       and the store takes no more writes until it's reopened.
 * </ul>
 */
class SnapshotInsideTheStoresDirectoryTest {
    @TempDir
    Path tmp;

    private static Options options() {
        return Options.defaults().withSync(SyncPolicy.NEVER);
    }

    @Test
    void theStoresDirectoryByAFirmlinkedPathIsRefused() throws IOException {
        Path dir = tmp.resolve("store");
        Exception thrown;
        try (Store store = Store.open(dir, options())) {
            Path other = Path.of("/System/Volumes/Data" + dir.toRealPath());
            assumeTrue(Files.isDirectory(other) && Files.isSameFile(other, dir), "a macOS firmlink to the store");
            store.put(utf8("k"), utf8("v"));
            thrown = snapshot(store, other.resolve("MANIFEST.tmp"));
            assertNull(writeAndFlush(store, "k2"), "a write and a flush after the snapshot (which threw " + thrown + ")");
        }
        try (Store store = Store.open(dir, options())) {
            assertNull(writeAndFlush(store, "k3"), "a write and a flush after a reopen");
        }
        assertInstanceOf(IllegalArgumentException.class, thrown, "a snapshot into the store's own directory");
    }

    @Test
    void aTargetWhoseMissingParentIsAStoreFileNameIsRefused() throws IOException {
        Path dir = tmp.resolve("store");
        try (Store store = Store.open(dir, options())) {
            store.put(utf8("k"), utf8("v"));
            Exception thrown = snapshot(store, dir.resolve("MANIFEST.tmp").resolve("snap"));
            assertNull(writeAndFlush(store, "k2"), "a write and a flush after the snapshot (which threw " + thrown + ")");
            assertInstanceOf(IllegalArgumentException.class, thrown, "a snapshot that makes STORE/MANIFEST.tmp");
        }
    }

    private static Exception snapshot(Store store, Path target) {
        try {
            store.snapshot(target);
            return null;
        } catch (IOException | RuntimeException e) {
            return e;
        }
    }

    private static IOException writeAndFlush(Store store, String key) {
        try {
            store.put(utf8(key), utf8("v"));
            store.flush();
            return null;
        } catch (IOException e) {
            return e;
        }
    }
}
