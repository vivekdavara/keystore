package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.vivekdavara.keystore.wal.WalCorruptionException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The durability steps that a crash test can't see on a machine that doesn't crash: which channels
 * get forced, counted per path. Found by review: removing any of these forces failed no test.
 */
class DurabilityTest {
    @TempDir
    Path dir;

    private final Map<Path, Integer> forces = new HashMap<>();

    /** Wraps every channel the store opens so that its forces are counted by path. */
    private final BiFunction<Path, FileChannel, FileChannel> counting = (path, inner) -> new FaultyChannel(inner) {
        @Override
        public void force(boolean metaData) throws IOException {
            super.force(metaData);
            synchronized (forces) {
                forces.merge(path.toAbsolutePath().normalize(), 1, Integer::sum);
            }
        }
    };

    private int forcesOf(Path path) {
        synchronized (forces) {
            return forces.getOrDefault(path.toAbsolutePath().normalize(), 0);
        }
    }

    /** Each new directory's entry is in its parent, so every parent of a new directory is forced. */
    @Test
    void creatingAStoreForcesTheParentOfEveryNewDirectory() throws IOException {
        Path store = dir.resolve("a").resolve("b").resolve("c");
        try (Store opened = Store.open(store, Options.defaults(), counting)) {
            opened.put(utf8("k"), utf8("v"));
        }
        assertEquals(1, forcesOf(dir), "dir holds the new entry a");
        assertEquals(1, forcesOf(dir.resolve("a")), "a holds b");
        assertEquals(1, forcesOf(dir.resolve("a").resolve("b")), "b holds c");
        assertTrue(forcesOf(store.toRealPath()) >= 1, "c holds the new log");
    }

    /**
     * The log's directory is forced on every open, not only when the log is new: a crash between
     * creating the file and forcing the directory would otherwise leave the entry unforced for good.
     */
    @Test
    void everyOpenForcesTheLogsDirectory() throws IOException {
        Files.createDirectories(dir.resolve("s"));
        Path real = dir.resolve("s").toRealPath();
        try (Store created = Store.open(real, Options.defaults(), counting)) {
            assertEquals(2, forcesOf(real), "the manifest's rename, then the new log");
            assertEquals(0, created.lastSequence());
        }
        // Now an empty log and a manifest, as a crash right after creating the log leaves them.
        try (Store opened = Store.open(real, Options.defaults(), counting)) {
            assertEquals(3, forcesOf(real), "the existing log's directory, again");
            assertEquals(0, opened.lastSequence());
        }
        try (Store opened = Store.open(real, Options.defaults(), counting)) {
            assertEquals(4, forcesOf(real));
            assertEquals(0, opened.lastSequence());
        }
    }

    /** A truncation is forced before anything is appended after it. */
    @Test
    void aTruncatedTailIsForcedBeforeTheStoreOpens() throws IOException {
        try (Store store = Store.open(dir)) {
            store.put(utf8("a"), utf8("1"));
            store.put(utf8("b"), utf8("2"));
        }
        Path log = dir.resolve(Store.logName(1)).toRealPath();
        try (RandomAccessFile raw = new RandomAccessFile(log.toFile(), "rw")) {
            raw.setLength(Files.size(log) - 1);
        }
        try (Store opened = Store.open(dir, Options.defaults(), counting)) {
            assertEquals(1, forcesOf(log), "the truncation, by the time open returned");
            assertEquals(1, opened.lastSequence());
        }
        try (Store opened = Store.open(dir, Options.defaults(), counting)) {
            assertEquals(2, forcesOf(log), "the first close forced it once more; a clean open adds none");
            assertEquals(1, opened.lastSequence());
        }
    }

    /** Found by review: a refused open could leak its log channel and no test would notice. */
    @Test
    void refusedOpensLeakNoFileDescriptors() throws Exception {
        Path fds = Path.of("/dev/fd");
        assumeTrue(Files.isDirectory(fds), "needs /dev/fd");
        try (Store store = Store.open(dir)) {
            for (int i = 0; i < 3; i++) {
                store.put(utf8("k" + i), utf8("v"));
            }
        }
        Path log = dir.resolve(Store.logName(1));
        byte[] bytes = Files.readAllBytes(log);
        bytes[10] ^= 1; // damage record #1, so the default refuses
        Files.write(log, bytes);
        assertThrows(WalCorruptionException.class, () -> Store.open(dir)); // warm up the code paths
        long before = count(fds);
        for (int i = 0; i < 50; i++) {
            assertThrows(WalCorruptionException.class, () -> Store.open(dir));
        }
        long after = count(fds);
        assertTrue(after - before < 10, "file descriptors went from " + before + " to " + after);
    }

    private static long count(Path fds) throws IOException {
        try (Stream<Path> open = Files.list(fds)) {
            return open.count();
        }
    }
}
