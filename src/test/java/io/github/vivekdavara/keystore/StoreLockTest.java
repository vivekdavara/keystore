package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
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

    /**
     * One directory can have two real paths: macOS's firmlinks make /private/var/... also
     * /System/Volumes/Data/private/var/..., and toRealPath keeps them apart (a Linux bind mount is
     * the same). Found by review: keyed by real path, the second open got past the set, its failed
     * lock attempt closed a channel on LOCK, and that released the first store's lock.
     */
    @Test
    void aSecondRealPathToTheSameDirectoryIsTheSameStore() throws Exception {
        Path real = dir.toRealPath();
        Path alias = Path.of("/System/Volumes/Data" + real);
        assumeTrue(Files.isDirectory(alias), "needs a second path to the directory (a macOS firmlink)");
        assumeTrue(!alias.toRealPath().equals(real) && Files.isSameFile(alias, real));
        try (Store store = Store.open(dir)) {
            StoreLockedException refused = assertThrows(StoreLockedException.class, () -> Store.open(alias));
            // Refused by the set (keyed by file key), before the lock file was touched.
            assertTrue(refused.getMessage().endsWith("is already open in this process"), refused.getMessage());
            assertEquals("locked", probe(dir), "the first store's lock still holds");
            store.put(utf8("k"), utf8("v"));
        }
    }

    /** Found by review: an open store's directory, renamed, has a new real path but is the same store. */
    @Test
    void aRenamedOpenDirectoryIsStillTheSameStore() throws Exception {
        Path before = dir.resolve("before");
        Path after = dir.resolve("after");
        try (Store store = Store.open(before)) {
            store.put(utf8("k"), utf8("v"));
            Files.move(before, after); // the store keeps writing through its open channels
            StoreLockedException refused = assertThrows(StoreLockedException.class, () -> Store.open(after));
            assertTrue(refused.getMessage().endsWith("is already open in this process"), refused.getMessage());
            assertEquals("locked", probe(after));
        }
        assertEquals("opened 1", probe(after));
    }

    /**
     * Found by review: Store loaded by a second class loader has a set of its own, which can't know
     * the first store. Its open reaches tryLock, which the JDK refuses (its lock table is per JVM), and
     * the channel must then stay open: closing it would release the first store's lock. This is the
     * one portable way to reach that path.
     */
    @Test
    void aSecondClassLoadersOpenLeavesTheLockHeld() throws Exception {
        URL classes = Store.class.getProtectionDomain().getCodeSource().getLocation();
        try (Store store = Store.open(dir);
                URLClassLoader other = new URLClassLoader(new URL[] {classes}, null)) {
            Class<?> otherStore = Class.forName(Store.class.getName(), true, other);
            Method open = otherStore.getMethod("open", Path.class);
            InvocationTargetException refused = assertThrows(InvocationTargetException.class, () -> open.invoke(null, dir));
            assertEquals(StoreLockedException.class.getName(), refused.getCause().getClass().getName());
            assertTrue(refused.getCause().getMessage().endsWith("by another path"), refused.getCause().getMessage());
            assertEquals("locked", probe(dir));
            store.put(utf8("k"), utf8("v"));
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
