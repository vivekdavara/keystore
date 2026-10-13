package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.wal.LogFullException;
import io.github.vivekdavara.keystore.wal.Recovery.Outcome;
import io.github.vivekdavara.keystore.wal.WalFormat;
import io.github.vivekdavara.keystore.wal.WalRecord;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Failed appends and flushes, injected through a {@link FaultyChannel} under the log. */
class StoreFaultTest {
    @TempDir
    Path dir;

    private final AtomicReference<FaultyChannel> channel = new AtomicReference<>();

    private Store openFaulty() throws IOException {
        return openFaulty(Options.defaults());
    }

    /** Opens the store with its log's channel (only that one) wrapped in a {@link FaultyChannel}. */
    private Store openFaulty(Options options) throws IOException {
        return Store.open(dir, options, (path, inner) -> {
            if (!path.endsWith(Store.LOG_FILE)) {
                return inner;
            }
            FaultyChannel faulty = new FaultyChannel(inner);
            channel.set(faulty);
            return faulty;
        });
    }

    /**
     * A write that dies partway leaves part of a record at the end of the log. The store must not
     * append after it; reopening drops the partial record and keeps everything before it.
     */
    @Test
    void aWriteThatFailsPartwayStopsWritesUntilRecovery() throws IOException {
        try (Store store = openFaulty()) {
            store.put(utf8("a"), utf8("1"));
            store.put(utf8("b"), utf8("2"));
            channel.get().failWritesAfter(10); // the next record gets 10 bytes in, then fails

            IOException failed = assertThrows(IOException.class, () -> store.put(utf8("c"), utf8("3")));
            assertTrue(failed.getMessage().contains("injected"), failed.getMessage());
            assertEquals(Optional.empty(), store.get(utf8("c")), "a failed write is not visible");

            StoreFailedException refused = assertThrows(StoreFailedException.class, () -> store.put(utf8("d"), utf8("4")));
            assertSame(failed, refused.getCause());
            assertThrows(StoreFailedException.class, () -> store.delete(utf8("a")));
            assertEquals("1", StoreTest.string(store.get(utf8("a"))), "reads still work");
            assertEquals(2, store.lastSequence());
        }
        try (Store store = Store.open(dir)) {
            assertEquals(Outcome.TORN_TAIL, store.recovery().outcome());
            assertEquals(10, store.recovery().droppedBytes());
            assertEquals(2, store.lastSequence());
            assertEquals("2", StoreTest.string(store.get(utf8("b"))));
            assertEquals(Optional.empty(), store.get(utf8("c")));
            store.put(utf8("c"), utf8("3 again"));
            assertEquals(3, store.lastSequence());
        }
    }

    @Test
    void aWriteThatFailsBeforeAnyByteLeavesACleanLog() throws IOException {
        try (Store store = openFaulty()) {
            store.put(utf8("a"), utf8("1"));
            channel.get().failWritesAfter(0);
            assertThrows(IOException.class, () -> store.put(utf8("b"), utf8("2")));
            assertThrows(StoreFailedException.class, () -> store.put(utf8("b"), utf8("2")));
        }
        try (Store store = Store.open(dir)) {
            assertEquals(Outcome.CLEAN, store.recovery().outcome());
            assertEquals(1, store.lastSequence());
        }
    }

    /**
     * A failed flush: the caller is told the write failed, yet the record was written in full and
     * may well be on disk. Here it is, after a reopen. That's what "the outcome is unknown" means.
     */
    @Test
    void aFailedFlushLeavesTheOutcomeUnknown() throws IOException {
        try (Store store = openFaulty()) {
            store.put(utf8("a"), utf8("1"));
            channel.get().failForces();
            IOException failed = assertThrows(IOException.class, () -> store.put(utf8("b"), utf8("2")));
            assertEquals("injected: force failed", failed.getMessage());
            assertEquals(Optional.empty(), store.get(utf8("b")), "not applied: it was never acknowledged");
            assertEquals(1, store.lastSequence(), "lastSequence names only writes that reads can see");
            assertThrows(StoreFailedException.class, () -> store.put(utf8("c"), utf8("3")));
        } // after a failed flush close doesn't force again, and doesn't throw: #1 was forced on its own
        try (Store store = Store.open(dir)) {
            assertEquals(Outcome.CLEAN, store.recovery().outcome());
            assertEquals("2", StoreTest.string(store.get(utf8("b"))), "the 'failed' write is there after all");
            assertEquals(2, store.lastSequence());
        }
    }

    /**
     * FileChannel is interruptible: interrupting a thread in a channel operation closes the channel
     * for everyone. A writer whose thread is interrupted therefore stops the store's writes, which
     * stay stopped until a reopen. Reads go on.
     */
    @Test
    void anInterruptedWriterClosesTheLogAndStopsWrites() throws IOException {
        try (Store store = Store.open(dir)) {
            store.put(utf8("a"), utf8("1"));
            Thread.currentThread().interrupt();
            IOException failed;
            try {
                failed = assertThrows(IOException.class, () -> store.put(utf8("b"), utf8("2")));
            } finally {
                Thread.interrupted(); // clear it before anything else
            }
            assertInstanceOf(ClosedByInterruptException.class, failed);
            StoreFailedException refused = assertThrows(StoreFailedException.class, () -> store.put(utf8("c"), utf8("3")));
            assertSame(failed, refused.getCause());
            assertEquals("1", StoreTest.string(store.get(utf8("a"))));
        }
        try (Store store = Store.open(dir)) {
            assertEquals(1, store.lastSequence());
            assertTrue(Files.size(dir.resolve(Store.LOG_FILE)) > 0);
        }
    }

    /**
     * Found by review: lastSequence() counted a write as soon as it was appended, while it was still
     * being forced and before reads could see it. Held mid-flush, the write isn't counted yet.
     */
    @Test
    void lastSequenceNamesOnlyWritesReadsCanSee() throws Exception {
        try (Store store = openFaulty()) {
            store.put(utf8("a"), utf8("1"));
            FaultyChannel.Gate gate = channel.get().holdNextForce();
            CompletableFuture<Void> put = CompletableFuture.runAsync(() -> unchecked(() -> store.put(utf8("b"), utf8("2"))));
            try {
                gate.awaitEntered(); // appended, being forced
                assertEquals(1, store.lastSequence());
                assertEquals(Optional.empty(), store.get(utf8("b")));
            } finally {
                gate.open();
            }
            put.get(30, TimeUnit.SECONDS);
            assertEquals(2, store.lastSequence());
            assertEquals("2", StoreTest.string(store.get(utf8("b"))));
        }
    }

    /**
     * Found by review: nothing raced close with a write. Close waits for the write lock, so a write
     * in progress finishes (and is in the log) before the log is closed.
     */
    @Test
    void closeWaitsForAWriteInProgress() throws Exception {
        Store store = openFaulty();
        store.put(utf8("a"), utf8("1"));
        FaultyChannel.Gate gate = channel.get().holdNextWrite();
        CompletableFuture<Void> put = CompletableFuture.runAsync(() -> unchecked(() -> store.put(utf8("b"), utf8("2"))));
        CompletableFuture<Void> close;
        try {
            gate.awaitEntered(); // the put holds the write lock, about to append
            close = CompletableFuture.runAsync(() -> unchecked(store::close));
            Thread.sleep(200);
            assertFalse(close.isDone(), "close waits for the write");
        } finally {
            gate.open();
        }
        put.get(30, TimeUnit.SECONDS); // without the lock, the closed channel would fail this put
        close.get(30, TimeUnit.SECONDS);
        try (Store again = Store.open(dir)) {
            assertEquals(Outcome.CLEAN, again.recovery().outcome());
            assertEquals("2", StoreTest.string(again.get(utf8("b"))));
        }
    }

    /**
     * Found by review: under NEVER, close is the moment acknowledged writes are forced, but a store
     * whose append had failed skipped the force and still returned normally. Now it forces them.
     */
    @Test
    void underNeverCloseStillForcesTheWritesBeforeAFailedAppend() throws IOException {
        Store store = openFaulty(Options.defaults().withSync(SyncPolicy.NEVER));
        for (int i = 0; i < 100; i++) {
            store.put(utf8("k" + i), utf8("v" + i));
        }
        channel.get().failWritesAfter(5);
        assertThrows(IOException.class, () -> store.put(utf8("late"), utf8("x")));
        assertEquals(0, channel.get().forces());
        store.close();
        assertEquals(1, channel.get().forces(), "the 100 acknowledged writes were forced at close");
        try (Store again = Store.open(dir)) {
            assertEquals(Outcome.TORN_TAIL, again.recovery().outcome());
            assertEquals(100, again.lastSequence());
        }
    }

    @Test
    void underNeverAFailedForceAtCloseIsReported() throws IOException {
        Store store = openFaulty(Options.defaults().withSync(SyncPolicy.NEVER));
        store.put(utf8("a"), utf8("1"));
        channel.get().failWritesAfter(5);
        assertThrows(IOException.class, () -> store.put(utf8("b"), utf8("2")));
        channel.get().failForces();
        IOException failed = assertThrows(IOException.class, store::close);
        assertEquals("injected: force failed", failed.getMessage());
        try (Store again = Store.open(dir)) { // the directory was released all the same
            assertEquals(1, again.lastSequence());
        }
    }

    /** An interrupt closed the log, so under NEVER nothing can force the acknowledged writes: say so. */
    @Test
    void underNeverAnInterruptedLogMakesCloseThrow() throws IOException {
        Store store = Store.open(dir, Options.defaults().withSync(SyncPolicy.NEVER));
        store.put(utf8("a"), utf8("1"));
        Thread.currentThread().interrupt();
        try {
            assertThrows(ClosedByInterruptException.class, () -> store.put(utf8("b"), utf8("2")));
        } finally {
            Thread.interrupted();
        }
        StoreFailedException failed = assertThrows(StoreFailedException.class, store::close);
        assertTrue(failed.getMessage().contains("may not be on the device"), failed.getMessage());
        try (Store again = Store.open(dir)) {
            assertEquals(1, again.lastSequence());
        }
    }

    /**
     * Found by review: the store acknowledged writes past the size recovery can read back, and then
     * couldn't reopen. A write that doesn't fit is now refused before anything is written, and the
     * store goes on. (The limit is 2 GiB by default; here it is a few records.)
     */
    @Test
    void aWritePastTheLogsLimitIsRefusedAndTheStoreGoesOn() throws IOException {
        int put = WalFormat.encodedSize(WalRecord.put(1, utf8("a"), utf8("1")));
        int delete = WalFormat.encodedSize(WalRecord.delete(1, utf8("a")));
        Options options = Options.defaults().withMaxLogBytes(2L * put + delete);
        try (Store store = Store.open(dir, options)) {
            store.put(utf8("a"), utf8("1"));
            store.put(utf8("b"), utf8("2"));
            LogFullException full = assertThrows(LogFullException.class, () -> store.put(utf8("c"), utf8("3")));
            assertTrue(full.getMessage().contains("past its limit of " + (2L * put + delete)), full.getMessage());
            assertEquals(2 * put, store.logSize(), "nothing was written");
            store.delete(utf8("a")); // a smaller write still fits: the store didn't fail
            assertEquals(3, store.lastSequence());
        }
        try (Store store = Store.open(dir, options)) {
            assertEquals(Outcome.CLEAN, store.recovery().outcome());
            assertEquals(3, store.recovery().records());
        }
    }

    @Test
    void theLogsLimitMustBeRecoverable() {
        assertThrows(IllegalArgumentException.class, () -> Options.defaults().withMaxLogBytes(0));
        assertThrows(IllegalArgumentException.class, () -> Options.defaults().withMaxLogBytes(Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE - 8, Options.defaults().maxLogBytes());
    }

    private interface IoAction {
        void run() throws Exception;
    }

    private static void unchecked(IoAction action) {
        try {
            action.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void aFailedStoreClosesAndReleasesItsDirectory() throws IOException {
        Store store = openFaulty();
        store.put(utf8("a"), utf8("1"));
        channel.get().failForces();
        assertThrows(IOException.class, () -> store.put(utf8("b"), utf8("2")));
        store.close();
        try (Store again = Store.open(dir)) {
            assertEquals(2, again.lastSequence());
        }
    }
}
