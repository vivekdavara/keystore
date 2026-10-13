package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.wal.Recovery.Outcome;
import io.github.vivekdavara.keystore.wal.RecoveryMode;
import io.github.vivekdavara.keystore.wal.WalCorruptionException;
import io.github.vivekdavara.keystore.wal.WalFormat;
import io.github.vivekdavara.keystore.wal.WalRecord;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class StoreTest {
    @TempDir
    Path dir;

    @Test
    void putGetDelete() throws IOException {
        try (Store store = Store.open(dir)) {
            assertEquals(Optional.empty(), store.get(utf8("k")));
            store.put(utf8("k"), utf8("v1"));
            assertEquals("v1", string(store.get(utf8("k"))));
            store.put(utf8("k"), utf8("v2"));
            assertEquals("v2", string(store.get(utf8("k"))));
            store.delete(utf8("k"));
            assertEquals(Optional.empty(), store.get(utf8("k")));
            store.delete(utf8("never written")); // still a write
            assertEquals(4, store.lastSequence());
        }
    }

    @Test
    void anEmptyValueIsAValueNotADelete() throws IOException {
        try (Store store = Store.open(dir)) {
            store.put(utf8("k"), new byte[0]);
            assertArrayEquals(new byte[0], store.get(utf8("k")).orElseThrow());
        }
        try (Store store = Store.open(dir)) {
            assertArrayEquals(new byte[0], store.get(utf8("k")).orElseThrow());
        }
    }

    @Test
    void valuesAreCopiedInAndOut() throws IOException {
        try (Store store = Store.open(dir)) {
            byte[] key = utf8("k");
            byte[] value = utf8("value");
            store.put(key, value);
            key[0] = 'x';
            value[0] = 'X';
            byte[] got = store.get(utf8("k")).orElseThrow();
            assertEquals("value", new String(got, StandardCharsets.UTF_8));
            got[0] = 'Y';
            assertEquals("value", string(store.get(utf8("k"))));
            assertEquals(Optional.empty(), store.get(utf8("x")));
        }
    }

    @Test
    void writesOutsideTheLimitsAreRefusedAndWriteNothing() throws IOException {
        try (Store store = Store.open(dir)) {
            long size = store.logSize();
            assertThrows(IllegalArgumentException.class, () -> store.put(new byte[0], utf8("v")));
            assertThrows(IllegalArgumentException.class, () -> store.put(new byte[65_537], utf8("v")));
            assertThrows(IllegalArgumentException.class, () -> store.put(utf8("k"), new byte[16 * 1024 * 1024 + 1]));
            assertThrows(IllegalArgumentException.class, () -> store.delete(new byte[0]));
            assertThrows(NullPointerException.class, () -> store.put(null, utf8("v")));
            assertThrows(NullPointerException.class, () -> store.put(utf8("k"), null));
            assertEquals(0, store.lastSequence());
            assertEquals(size, store.logSize());
            store.put(new byte[65_536], new byte[16 * 1024 * 1024]); // the limits themselves are fine
            assertEquals(1, store.lastSequence());
        }
    }

    @ParameterizedTest
    @EnumSource(SyncPolicy.class)
    void everythingSurvivesAReopen(SyncPolicy sync) throws IOException {
        Options options = Options.defaults().withSync(sync);
        try (Store store = Store.open(dir, options)) {
            store.put(utf8("a"), utf8("1"));
            store.put(utf8("b"), utf8("2"));
            store.delete(utf8("a"));
            store.put(utf8("c"), utf8("3"));
        }
        try (Store store = Store.open(dir, options)) {
            assertEquals(Outcome.CLEAN, store.recovery().outcome());
            assertEquals(4, store.recovery().records());
            assertEquals(4, store.lastSequence());
            assertEquals(Optional.empty(), store.get(utf8("a")));
            assertEquals("2", string(store.get(utf8("b"))));
            assertEquals("3", string(store.get(utf8("c"))));
            store.put(utf8("d"), utf8("4"));
            assertEquals(5, store.lastSequence(), "sequence numbers continue after a reopen");
        }
        try (Store store = Store.open(dir, options)) {
            assertEquals("4", string(store.get(utf8("d"))));
        }
    }

    /** Random puts and deletes against a HashMap, with a reopen every so often. */
    @Test
    void behavesLikeAMapAcrossReopens() throws IOException {
        SplittableRandom random = new SplittableRandom(21);
        Map<String, String> model = new HashMap<>();
        Options options = Options.defaults().withSync(SyncPolicy.NEVER);
        Store store = Store.open(dir, options);
        try {
            for (int step = 0; step < 5_000; step++) {
                String key = "key-" + random.nextInt(300);
                switch (random.nextInt(10)) {
                    case 0, 1 -> {
                        store.delete(utf8(key));
                        model.remove(key);
                    }
                    case 2 -> {
                        store.close();
                        store = Store.open(dir, options);
                        assertEquals(Outcome.CLEAN, store.recovery().outcome());
                    }
                    default -> {
                        String value = "value-" + step;
                        store.put(utf8(key), utf8(value));
                        model.put(key, value);
                    }
                }
                String probe = "key-" + random.nextInt(300);
                assertEquals(Optional.ofNullable(model.get(probe)), store.get(utf8(probe)).map(StoreTest::string), "step " + step);
            }
            for (int k = 0; k < 300; k++) {
                String key = "key-" + k;
                assertEquals(Optional.ofNullable(model.get(key)), store.get(utf8(key)).map(StoreTest::string));
            }
        } finally {
            store.close();
        }
    }

    @Test
    void aTornTailIsDroppedAndWritingContinues() throws IOException {
        try (Store store = Store.open(dir)) {
            store.put(utf8("a"), utf8("1"));
            store.put(utf8("b"), utf8("2"));
        }
        Path log = dir.resolve(Store.LOG_FILE);
        try (RandomAccessFile raw = new RandomAccessFile(log.toFile(), "rw")) {
            raw.setLength(Files.size(log) - 1); // the crash came one byte short of finishing #2
        }
        try (Store store = Store.open(dir)) {
            assertEquals(Outcome.TORN_TAIL, store.recovery().outcome());
            assertEquals(1, store.lastSequence());
            assertEquals("1", string(store.get(utf8("a"))));
            assertEquals(Optional.empty(), store.get(utf8("b")));
            store.put(utf8("b"), utf8("2 again"));
        }
        try (Store store = Store.open(dir)) {
            assertEquals(Outcome.CLEAN, store.recovery().outcome());
            assertEquals("2 again", string(store.get(utf8("b"))));
        }
    }

    /** A refused open must not keep the directory locked, or the operator couldn't choose to drop. */
    @Test
    void damageInTheMiddleIsRefusedThenDroppedOnRequest() throws IOException {
        try (Store store = Store.open(dir)) {
            for (int i = 1; i <= 5; i++) {
                store.put(utf8("k" + i), utf8("v" + i));
            }
        }
        Path log = dir.resolve(Store.LOG_FILE);
        byte[] bytes = Files.readAllBytes(log);
        int third = 2 * (bytes.length / 5); // the records are the same size
        bytes[third + 20] ^= 0x40;
        Files.write(log, bytes);

        WalCorruptionException refused = assertThrows(WalCorruptionException.class, () -> Store.open(dir));
        assertEquals(third, refused.offset());
        try (Store store = Store.open(dir, Options.defaults().withRecovery(RecoveryMode.POINT_IN_TIME))) {
            assertEquals(Outcome.DAMAGE_DROPPED, store.recovery().outcome());
            assertEquals(2, store.lastSequence());
            assertEquals("v2", string(store.get(utf8("k2"))));
            assertEquals(Optional.empty(), store.get(utf8("k3")));
            assertEquals(Optional.empty(), store.get(utf8("k5")));
        }
        try (Store store = Store.open(dir)) {
            assertEquals(Outcome.CLEAN, store.recovery().outcome(), "the drop was made permanent");
        }
    }

    /**
     * Overwrites replace the memtable's entry but add to the log, so the log, not the memtable, is
     * what the flush (day 2) must bound. Found by review: the memtable's own count of bytes written
     * left out each record's 21 to 25 bytes of framing, 22 times too small for 1-byte deletes, so
     * it is gone, and the log's size is the measure.
     */
    @Test
    void overwritesGrowTheLogNotTheMemtable() throws IOException {
        try (Store store = Store.open(dir, Options.defaults().withSync(SyncPolicy.NEVER))) {
            byte[] value = new byte[100];
            for (int i = 0; i < 10_000; i++) {
                store.put(utf8("hot"), value);
            }
            assertEquals(1, store.memtableSize());
            assertEquals(10_000L * WalFormat.encodedSize(WalRecord.put(1, utf8("hot"), value)), store.logSize());
        }
    }

    @Test
    void aClosedStoreRefusesEverythingAndClosingAgainDoesNothing() throws IOException {
        Store store = Store.open(dir);
        store.put(utf8("k"), utf8("v"));
        store.close();
        store.close();
        assertThrows(IllegalStateException.class, () -> store.put(utf8("k"), utf8("v")));
        assertThrows(IllegalStateException.class, () -> store.delete(utf8("k")));
        assertThrows(IllegalStateException.class, () -> store.get(utf8("k")));
    }

    @Test
    void aMissingDirectoryIsCreated() throws IOException {
        Path nested = dir.resolve("a").resolve("b");
        try (Store store = Store.open(nested)) {
            store.put(utf8("k"), utf8("v"));
            assertEquals(nested.toRealPath(), store.directory());
        }
        assertTrue(Files.exists(nested.resolve(Store.LOG_FILE)));
        assertTrue(Files.exists(nested.resolve(Store.LOCK_FILE)));
    }

    /** Several writers at once: each write gets its own sequence number, and nothing is lost. */
    @Test
    void concurrentWritersAndReadersAgreeAfterAReopen() throws Exception {
        int writers = 4;
        int perWriter = 2_000;
        Options options = Options.defaults().withSync(SyncPolicy.NEVER);
        ExecutorService pool = Executors.newFixedThreadPool(writers + 2);
        try (Store store = Store.open(dir, options)) {
            List<Future<?>> tasks = new ArrayList<>();
            for (int w = 0; w < writers; w++) {
                int writer = w;
                tasks.add(pool.submit(() -> {
                    for (int i = 0; i < perWriter; i++) {
                        store.put(utf8("w" + writer + "-" + i), utf8("v" + i));
                        if (i % 10 == 9) {
                            store.delete(utf8("w" + writer + "-" + (i - 5)));
                        }
                    }
                    return null;
                }));
            }
            for (int r = 0; r < 2; r++) {
                tasks.add(pool.submit(() -> {
                    for (int i = 0; i < 20_000; i++) {
                        Optional<byte[]> value = store.get(utf8("w0-" + (i % perWriter)));
                        value.ifPresent(v -> assertTrue(string(Optional.of(v)).startsWith("v")));
                    }
                    return null;
                }));
            }
            for (Future<?> task : tasks) {
                task.get(60, TimeUnit.SECONDS);
            }
            assertEquals(writers * (perWriter + perWriter / 10), store.lastSequence());
        } finally {
            pool.shutdownNow();
        }
        try (Store store = Store.open(dir, options)) {
            assertEquals(writers * (perWriter + perWriter / 10), store.recovery().records());
            for (int w = 0; w < writers; w++) {
                for (int i = 0; i < perWriter; i++) {
                    boolean deleted = i % 10 == 4;
                    Optional<byte[]> value = store.get(utf8("w" + w + "-" + i));
                    assertEquals(!deleted, value.isPresent(), "w" + w + "-" + i);
                }
            }
            assertFalse(store.get(utf8("w0-4")).isPresent());
        }
    }

    static String string(Optional<byte[]> value) {
        return string(value.orElseThrow());
    }

    private static String string(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
