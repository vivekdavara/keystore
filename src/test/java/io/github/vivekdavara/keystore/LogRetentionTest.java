package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogRetentionTest {
    @TempDir
    Path dir;

    private static final long FLUSH = 16 << 10;

    private static Options options(long retain) {
        return Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(FLUSH).withRetainLogBytes(retain);
    }

    @Test
    void retentionKeepsTheNewestCoveredLogsUntilTheyTotalTheBudget() throws IOException {
        Files.write(dir.resolve(Store.logName(1)), new byte[10]);
        Files.write(dir.resolve(Store.logName(2)), new byte[10]);
        Files.write(dir.resolve(Store.logName(5)), new byte[10]);
        Files.write(dir.resolve(Store.logName(9)), new byte[10]);
        List<Long> covered = List.of(5L, 1L, 9L, 2L, 7L); // in any order; 7 doesn't exist (it counts 0 bytes)
        assertEquals(List.of(1L, 2L, 5L, 7L, 9L), Store.beyondRetention(dir, covered, 0), "oldest first");
        assertEquals(List.of(1L, 2L, 5L, 7L), Store.beyondRetention(dir, covered, 10)); // 9 alone makes 10
        assertEquals(List.of(1L, 2L), Store.beyondRetention(dir, covered, 11)); // 9, then 7 (0 bytes) and 5
        assertEquals(List.of(), Store.beyondRetention(dir, covered, 1_000));
    }

    @Test
    void withoutRetentionOnlyTheLogsTheTablesDontCoverRemain() throws IOException {
        try (Store store = Store.open(dir, options(0))) {
            fill(store, 2_000);
            assertTrue(store.stats().flushes() >= 5, "flushes: " + store.stats().flushes());
            assertEquals(List.of(store.manifest().logNumber()), logs());
        }
    }

    @Test
    void flushesKeepAnUnbrokenRunOfTheNewestLogs() throws IOException {
        long retain = 3 * FLUSH;
        try (Store store = Store.open(dir, options(retain))) {
            fill(store, 4_000);
            assertTrue(store.stats().flushes() >= 10, "flushes: " + store.stats().flushes());
            long live = store.manifest().logNumber();
            List<Long> logs = logs();
            List<Long> covered = logs.subList(0, logs.size() - 1);
            assertEquals(live, logs.get(logs.size() - 1));
            long kept = 0;
            for (long n : covered) {
                kept += Files.size(dir.resolve(Store.logName(n)));
            }
            long newestCovered = Files.size(dir.resolve(Store.logName(covered.get(covered.size() - 1))));
            assertTrue(kept >= retain, "kept " + kept);
            assertTrue(kept - Files.size(dir.resolve(Store.logName(covered.get(0)))) < retain,
                    "one log fewer would be under the budget: " + kept);
            assertTrue(newestCovered >= FLUSH);
        }
    }

    @Test
    void anOpenKeepsRetainedLogsAndTrimsToItsOwnBudget() throws IOException {
        List<Long> before;
        try (Store store = Store.open(dir, options(4 * FLUSH))) {
            fill(store, 4_000);
            before = logs();
        }
        try (Store store = Store.open(dir, options(4 * FLUSH))) {
            assertEquals(before, logs(), "recovery deletes no log retention keeps");
            assertArrayEquals(utf8("value-3999"), store.get(utf8("key-3999")).orElseThrow());
        }
        try (Store store = Store.open(dir, options(FLUSH))) {
            List<Long> after = logs();
            assertTrue(after.size() < before.size(), before + " then " + after);
            assertEquals(before.subList(before.size() - after.size(), before.size()), after, "the oldest went first");
            assertArrayEquals(utf8("value-0"), store.get(utf8("key-0")).orElseThrow());
        }
    }

    private static void fill(Store store, int n) throws IOException {
        for (int i = 0; i < n; i++) {
            store.put(utf8("key-" + i), utf8("value-" + i));
        }
    }

    private List<Long> logs() throws IOException {
        List<Long> logs = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*" + Store.LOG_SUFFIX)) {
            for (Path file : files) {
                logs.add(Store.fileNumber(file.getFileName().toString(), Store.LOG_SUFFIX));
            }
        }
        Collections.sort(logs);
        return logs;
    }
}
