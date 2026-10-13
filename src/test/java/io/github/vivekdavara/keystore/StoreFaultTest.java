package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.wal.Recovery.Outcome;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Failed appends and flushes, injected through a {@link FaultyChannel} under the log. */
class StoreFaultTest {
    @TempDir
    Path dir;

    private final AtomicReference<FaultyChannel> channel = new AtomicReference<>();

    private Store openFaulty() throws IOException {
        UnaryOperator<java.nio.channels.FileChannel> wrap = inner -> {
            FaultyChannel faulty = new FaultyChannel(inner);
            channel.set(faulty);
            return faulty;
        };
        return Store.open(dir, Options.defaults(), wrap);
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
            assertEquals(2, store.lastSequence(), "but it is in the log, so its number is used");
            assertThrows(StoreFailedException.class, () -> store.put(utf8("c"), utf8("3")));
        } // close doesn't force a failed log, so this doesn't throw
        try (Store store = Store.open(dir)) {
            assertEquals(Outcome.CLEAN, store.recovery().outcome());
            assertEquals("2", StoreTest.string(store.get(utf8("b"))), "the 'failed' write is there after all");
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
