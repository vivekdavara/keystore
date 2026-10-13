package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StoreLockTest {
    @TempDir
    Path dir;

    @Test
    void aSecondOpenInThisProcessIsRefused() throws IOException {
        try (Store store = Store.open(dir)) {
            StoreLockedException refused = assertThrows(StoreLockedException.class, () -> Store.open(dir));
            assertTrue(refused.getMessage().contains("already open in this process"), refused.getMessage());
            store.put(utf8("k"), utf8("still works"));
        }
        try (Store again = Store.open(dir)) {
            assertEquals(1, again.lastSequence());
        }
    }

    @Test
    void anotherPathToTheSameDirectoryIsTheSameStore() throws IOException {
        Path link = Files.createSymbolicLink(dir.resolveSibling(dir.getFileName() + "-link"), dir);
        Files.createDirectory(dir.resolve("sub"));
        try (Store store = Store.open(dir)) {
            assertThrows(StoreLockedException.class, () -> Store.open(link));
            assertThrows(StoreLockedException.class, () -> Store.open(dir.resolve("sub").resolve("..")));
            assertEquals(dir.toRealPath(), store.directory());
        } finally {
            Files.delete(link);
        }
    }

    @Test
    void anotherDirectoryOpensAlongside() throws IOException {
        try (Store one = Store.open(dir.resolve("one")); Store two = Store.open(dir.resolve("two"))) {
            one.put(utf8("k"), utf8("1"));
            two.put(utf8("k"), utf8("2"));
            assertEquals("1", StoreTest.string(one.get(utf8("k"))));
            assertEquals("2", StoreTest.string(two.get(utf8("k"))));
        }
    }

    @Test
    void anotherProcessCannotOpenItUntilItIsClosed() throws Exception {
        try (Store store = Store.open(dir)) {
            store.put(utf8("k"), utf8("v"));
            assertEquals("locked", probe(dir));
        }
        assertEquals("opened 1", probe(dir));
    }

    /**
     * POSIX record locks belong to the process, and closing any descriptor for the file releases
     * them all. So if a refused second open in this JVM opened the lock file and closed it again,
     * the first store would silently lose its lock, and another process could open the directory.
     */
    @Test
    void aRefusedSecondOpenDoesNotReleaseTheFirstStoresLock() throws Exception {
        try (Store store = Store.open(dir)) {
            assertThrows(StoreLockedException.class, () -> Store.open(dir));
            assertThrows(StoreLockedException.class, () -> Store.open(dir));
            assertEquals("locked", probe(dir));
            store.put(utf8("k"), utf8("v"));
        }
    }

    /** Runs {@link LockProbe} in a child JVM against {@code dir} and returns what it printed. */
    private static String probe(Path dir) throws Exception {
        String java = ProcessHandle.current().info().command().orElseThrow();
        Process child = new ProcessBuilder(
                        java, "-cp", System.getProperty("java.class.path"), LockProbe.class.getName(), dir.toString())
                .redirectErrorStream(true)
                .start();
        String output = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        assertTrue(child.waitFor(60, TimeUnit.SECONDS), "the child finished");
        assertEquals(0, child.exitValue(), output);
        return output;
    }
}
