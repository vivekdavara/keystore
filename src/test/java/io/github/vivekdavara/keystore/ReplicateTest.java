package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.wal.LogFullException;
import io.github.vivekdavara.keystore.wal.WalRecord;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** {@link Store#replicate}, how a follower applies its leader's writes, and {@link Store#awaitSequence}. */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ReplicateTest {
    @TempDir
    Path dir;

    private static List<WalRecord> records(long from, int n) {
        List<WalRecord> out = new ArrayList<>();
        for (long seq = from; seq < from + n; seq++) {
            out.add(seq % 5 == 0 ? WalRecord.delete(seq, utf8("k" + (seq - 1))) : WalRecord.put(seq, utf8("k" + seq), utf8("v" + seq)));
        }
        return out;
    }

    @Test
    void appliesRecordsWithTheLeadersSequenceNumbers() throws IOException {
        try (Store store = Store.open(dir)) {
            store.replicate(records(1, 9));
            store.replicate(records(10, 1));
            assertEquals(10, store.lastSequence());
            assertArrayEquals(utf8("v8"), store.get(utf8("k8")).orElseThrow());
            assertEquals(Optional.empty(), store.get(utf8("k4")), "deleted by #5");
            assertEquals(Optional.empty(), store.get(utf8("k9")), "deleted by #10");
            assertEquals(11, store.put(utf8("own"), utf8("write")), "a store's own writes follow on");
        }
        try (Store store = Store.open(dir)) {
            assertEquals(11, store.lastSequence(), "replicated writes are in the log like any other");
            assertArrayEquals(utf8("v3"), store.get(utf8("k3")).orElseThrow());
        }
    }

    @Test
    void recordsThatDontFollowOnWriteNothing() throws IOException {
        try (Store store = Store.open(dir)) {
            store.replicate(records(1, 3));
            long size = store.logSize();
            assertThrows(IllegalArgumentException.class, () -> store.replicate(records(5, 2)), "a gap before");
            assertThrows(IllegalArgumentException.class, () -> store.replicate(records(3, 2)), "a repeat");
            List<WalRecord> gap = new ArrayList<>(records(4, 2));
            gap.add(WalRecord.put(7, utf8("x"), utf8("y")));
            assertThrows(IllegalArgumentException.class, () -> store.replicate(gap), "a gap inside");
            assertThrows(IllegalArgumentException.class,
                    () -> store.replicate(List.of(WalRecord.put(4, new byte[65_537], utf8("y")))), "a key past the limit");
            assertEquals(3, store.lastSequence());
            assertEquals(size, store.logSize());
            store.replicate(records(4, 1)); // the store goes on
            assertEquals(4, store.lastSequence());
            store.replicate(List.of()); // nothing to do
        }
    }

    @Test
    void aBatchIsForcedOnce() throws IOException {
        AtomicInteger forces = new AtomicInteger();
        BiFunction<Path, FileChannel, FileChannel> counting = (path, inner) -> new FaultyChannel(inner) {
            @Override
            public void force(boolean metaData) throws IOException {
                super.force(metaData);
                if (path.getFileName().toString().endsWith(Store.LOG_SUFFIX)) {
                    forces.incrementAndGet();
                }
            }
        };
        try (Store store = Store.open(dir, Options.defaults(), counting)) {
            int before = forces.get();
            store.replicate(records(1, 100));
            assertEquals(before + 1, forces.get(), "one force for 100 records under EVERY_WRITE");
            store.put(utf8("a"), utf8("b"));
            assertEquals(before + 2, forces.get());
        }
    }

    @Test
    void aBatchThatFindsTheLogFullEnoughFlushesFirst() throws IOException {
        try (Store store = Store.open(dir, Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(1024))) {
            long seq = 1;
            for (int i = 0; i < 50; i++) {
                store.replicate(records(seq, 20));
                seq += 20;
            }
            assertTrue(store.stats().flushes() >= 10, "flushes: " + store.stats().flushes());
            assertEquals(1000, store.lastSequence());
            assertArrayEquals(utf8("v998"), store.get(utf8("k998")).orElseThrow());
            assertEquals(Optional.empty(), store.get(utf8("k999")), "deleted by #1000");
        }
    }

    @Test
    void aBatchPastTheLogsLimitWritesNothing() throws IOException {
        try (Store store = Store.open(dir, Options.defaults().withSync(SyncPolicy.NEVER).withMaxLogBytes(1000))) {
            assertThrows(LogFullException.class, () -> store.replicate(records(1, 100)));
            assertEquals(0, store.lastSequence());
            assertEquals(0, store.logSize());
            store.replicate(records(1, 10));
            assertEquals(10, store.lastSequence());
        }
    }

    @Test
    void awaitSequenceWaitsForTheWrite() throws Exception {
        try (Store store = Store.open(dir, Options.defaults().withSync(SyncPolicy.NEVER))) {
            assertTrue(store.awaitSequence(0, 0, TimeUnit.MILLISECONDS), "already there");
            long start = System.nanoTime();
            assertFalse(store.awaitSequence(1, 50, TimeUnit.MILLISECONDS));
            assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(50), "waited out the timeout");

            CompletableFuture<Boolean> waiter = CompletableFuture.supplyAsync(() -> {
                try {
                    return store.awaitSequence(3, 30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            store.put(utf8("a"), utf8("1"));
            store.put(utf8("b"), utf8("2"));
            assertFalse(waiter.isDone() && waiter.get(), "two writes aren't three");
            store.put(utf8("c"), utf8("3"));
            assertTrue(waiter.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void closeWakesWaiters() throws Exception {
        Store store = Store.open(dir, Options.defaults().withSync(SyncPolicy.NEVER));
        CompletableFuture<Boolean> waiter = CompletableFuture.supplyAsync(() -> {
            try {
                return store.awaitSequence(1, 60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!waiterIsWaiting(store) && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        store.close();
        assertFalse(waiter.get(10, TimeUnit.SECONDS), "woken by close, not by the 60-second timeout");
    }

    private static boolean waiterIsWaiting(Store store) {
        return store.sequenceWaiters() > 0;
    }
}
