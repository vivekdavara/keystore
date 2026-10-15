package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.wal.WalFormat;
import io.github.vivekdavara.keystore.wal.WalRecord;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LogTailTest {
    @TempDir
    Path dir;

    private static final long FLUSH = 8 << 10;

    private static Options options(long retain) {
        return Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(FLUSH).withRetainLogBytes(retain);
    }

    private static WalRecord decode(byte[] bytes) {
        WalFormat.Decoded decoded = WalFormat.decode(bytes, 0, bytes.length);
        assertTrue(decoded instanceof WalFormat.Ok, decoded.toString());
        WalFormat.Ok ok = (WalFormat.Ok) decoded;
        assertEquals(bytes.length, ok.next(), "exactly one record");
        return ok.record();
    }

    private static WalRecord expected(long seq) {
        return seq % 7 == 0 ? WalRecord.delete(seq, utf8("key-" + (seq - 1))) : WalRecord.put(seq, utf8("key-" + seq), utf8("value-" + seq));
    }

    private static void write(Store store, long from, long to) throws IOException {
        for (long seq = from; seq <= to; seq++) {
            WalRecord r = expected(seq);
            long got = r.op() == WalRecord.Op.PUT ? store.put(r.key(), r.value()) : store.delete(r.key());
            assertEquals(seq, got);
        }
    }

    @Test
    void readsEveryRecordInOrderAcrossLogsThenWaits() throws Exception {
        try (Store store = Store.open(dir, options(1L << 30)); LogTail tail = store.tail(1)) {
            write(store, 1, 3_000);
            assertTrue(logCount() > 10, "logs: " + logCount());
            for (long seq = 1; seq <= 3_000; seq++) {
                assertEquals(expected(seq), decode(tail.poll()));
            }
            assertNull(tail.poll(), "caught up");
            assertNull(tail.next(20, TimeUnit.MILLISECONDS));
            assertEquals(3_001, tail.nextSequence());
            write(store, 3_001, 3_001);
            assertEquals(expected(3_001), decode(tail.next(1, TimeUnit.SECONDS)));
        }
    }

    @Test
    void startsAnywhere() throws IOException {
        try (Store store = Store.open(dir, options(1L << 30))) {
            write(store, 1, 2_000);
            for (long from : new long[] {1, 2, 999, 1_000, 1_001, 1_999, 2_000}) {
                try (LogTail tail = store.tail(from)) {
                    assertEquals(expected(from), decode(tail.poll()), "from " + from);
                }
            }
            try (LogTail tail = store.tail(2_001)) {
                assertNull(tail.poll(), "nothing yet");
            }
        }
    }

    @Test
    void followsAWriterThroughFlushesAndMerges() throws Exception {
        try (Store store = Store.open(dir, options(1L << 30)); LogTail tail = store.tail(1)) {
            int n = 20_000;
            CompletableFuture<Void> writer = CompletableFuture.runAsync(() -> {
                try {
                    write(store, 1, n);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            for (long seq = 1; seq <= n; seq++) {
                byte[] bytes = tail.next(30, TimeUnit.SECONDS);
                assertEquals(expected(seq), decode(bytes), "record " + seq);
            }
            writer.get(30, TimeUnit.SECONDS);
            assertTrue(store.stats().flushes() > 50 && store.stats().merges() > 5, store.stats().toString());
        }
    }

    @Test
    void aRecordWhoseLogIsGoneIsBehind() throws IOException {
        try (Store store = Store.open(dir, options(0))) {
            write(store, 1, 1_000);
            assertTrue(store.stats().flushes() > 1);
            LogTail.BehindException behind = assertThrows(LogTail.BehindException.class, () -> store.tail(1).poll());
            long oldest = behind.oldest();
            assertTrue(oldest > 1 && oldest <= 1_000, "oldest " + oldest);
            try (LogTail tail = store.tail(oldest)) {
                assertEquals(expected(oldest), decode(tail.poll()), "the oldest left is readable");
            }
        }
    }

    @Test
    void aTailKeepsReadingALogTheStoreDeleted() throws IOException {
        try (Store store = Store.open(dir, options(0))) {
            write(store, 1, 10);
            long firstLog = logs().get(0);
            try (LogTail tail = store.tail(1)) {
                assertEquals(expected(1), decode(tail.poll())); // the tail has the first log open
                write(store, 11, 2_000); // flushes delete it
                assertTrue(Files.notExists(dir.resolve(Store.logName(firstLog))), "the first log is deleted");
                long seq = 2;
                while (true) {
                    byte[] bytes;
                    try {
                        bytes = tail.poll();
                    } catch (LogTail.BehindException e) {
                        break; // at the end of the deleted log, the next one is gone too
                    }
                    assertEquals(expected(seq), decode(bytes));
                    seq++;
                }
                assertTrue(seq > 11, "read on past the writes made before the deletion: " + seq);
            }
        }
    }

    @Test
    void recordsFromBeforeAReopenAreThere() throws IOException {
        try (Store store = Store.open(dir, options(1L << 30))) {
            write(store, 1, 1_500);
        }
        try (Store store = Store.open(dir, options(1L << 30)); LogTail tail = store.tail(1)) {
            write(store, 1_501, 1_600);
            for (long seq = 1; seq <= 1_600; seq++) {
                assertEquals(expected(seq), decode(tail.poll()));
            }
        }
    }

    @Test
    void aDamagedRecordIsAnErrorNotData() throws IOException {
        try (Store store = Store.open(dir, options(1L << 30))) {
            write(store, 1, 1_000);
        }
        long first = logs().get(0);
        try (RandomAccessFile file = new RandomAccessFile(dir.resolve(Store.logName(first)).toFile(), "rw")) {
            long at = file.length() / 2;
            file.seek(at);
            int b = file.read();
            file.seek(at);
            file.write(b ^ 0x10); // in a covered log, which recovery doesn't read
        }
        try (Store store = Store.open(dir, options(1L << 30)); LogTail tail = store.tail(1)) {
            IOException e = assertThrows(IOException.class, () -> {
                for (int i = 0; i < 1_000; i++) {
                    decode(tail.poll());
                }
            });
            assertTrue(e.getMessage().contains("damaged"), e.getMessage());
        }
    }

    @Test
    void aSkipThatRunsPastTheEndIsDamageNotBehind() throws IOException {
        try (Store store = Store.open(dir, options(1L << 30))) {
            write(store, 1, 1_000);
        }
        Path first = dir.resolve(Store.logName(logs().get(0))); // covered by tables: recovery doesn't read it
        try (RandomAccessFile file = new RandomAccessFile(first.toFile(), "rw")) {
            file.seek(offsetOf(first, 3));
            file.writeInt(WalFormat.MAX_PAYLOAD - 10); // a plausible length, far past the end of the log
        }
        try (Store store = Store.open(dir, options(1L << 30)); LogTail tail = store.tail(10)) {
            IOException e = assertThrows(IOException.class, tail::poll); // skips 1, 2 and 3 by their headers
            assertFalse(e instanceof LogTail.BehindException, "from the review: it said BEHIND: " + e);
            assertTrue(e.getMessage().contains("past the end"), e.getMessage());
        }
    }

    @Test
    void aRecordMissingFromAKeptLogIsDamage() throws IOException {
        try (Store store = Store.open(dir, options(1L << 30))) {
            write(store, 1, 1_000);
        }
        Path first = dir.resolve(Store.logName(logs().get(0)));
        byte[] log = Files.readAllBytes(first);
        int from = (int) offsetOf(first, 5);
        int to = (int) offsetOf(first, 6);
        byte[] cut = new byte[log.length - (to - from)];
        System.arraycopy(log, 0, cut, 0, from);
        System.arraycopy(log, to, cut, from, log.length - to); // every record left has a good checksum
        Files.write(first, cut);
        try (Store store = Store.open(dir, options(1L << 30)); LogTail tail = store.tail(1)) {
            for (long seq = 1; seq <= 4; seq++) {
                assertEquals(expected(seq), decode(tail.poll()));
            }
            IOException e = assertThrows(IOException.class, tail::poll);
            assertTrue(e.getMessage().contains("record 6 where 5 should be"), e.getMessage());
        }
    }

    /** Where the record with sequence number {@code seq} starts in a log, found by walking the headers. */
    private static long offsetOf(Path log, long seq) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(log.toFile(), "r")) {
            long offset = 0;
            while (offset < file.length()) {
                file.seek(offset);
                int length = file.readInt();
                file.readInt(); // the checksum
                if (file.readLong() == seq) {
                    return offset;
                }
                offset += WalFormat.HEADER_BYTES + length;
            }
        }
        throw new AssertionError("no record " + seq + " in " + log);
    }

    @Test
    void aClosedStoresTailFailsInsteadOfWaiting() throws Exception {
        Store store = Store.open(dir, options(0));
        LogTail tail = store.tail(1);
        CompletableFuture<byte[]> waiting = CompletableFuture.supplyAsync(() -> {
            try {
                return tail.next(60, TimeUnit.SECONDS);
            } catch (IOException e) {
                return null;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        while (store.sequenceWaiters() == 0) {
            Thread.onSpinWait();
        }
        store.close();
        assertNull(waiting.get(10, TimeUnit.SECONDS));
        assertThrows(IOException.class, () -> tail.next(1, TimeUnit.SECONDS));
        tail.close();
        assertThrows(IllegalStateException.class, tail::poll);
    }

    @Test
    void sequenceNumbersStartAtOne() throws IOException {
        try (Store store = Store.open(dir, options(0))) {
            assertThrows(IllegalArgumentException.class, () -> store.tail(0));
        }
    }

    @Test
    void keysAndValuesComeBackByteForByte() throws IOException {
        try (Store store = Store.open(dir, options(1L << 30)); LogTail tail = store.tail(1)) {
            byte[] big = new byte[1 << 20];
            new Random(3).nextBytes(big);
            store.put(utf8("big"), big);
            store.put(new byte[] {0, '\n', (byte) 0xff}, new byte[0]);
            WalRecord one = decode(tail.poll());
            assertArrayEquals(big, one.value());
            WalRecord two = decode(tail.poll());
            assertArrayEquals(new byte[] {0, '\n', (byte) 0xff}, two.key());
            assertArrayEquals(new byte[0], two.value());
        }
    }

    private long logCount() throws IOException {
        return logs().size();
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
