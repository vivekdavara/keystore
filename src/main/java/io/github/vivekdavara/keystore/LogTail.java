package io.github.vivekdavara.keystore;

import io.github.vivekdavara.keystore.wal.WalFormat;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Reads a store's logs in sequence order from one sequence number on, then follows new writes:
 * what a leader ships to a follower. Each record comes back as the log stores it (length,
 * checksum, payload), so whoever receives it can check the checksum again.
 *
 * <p>It returns only records the store has acknowledged, {@link Store#lastSequence()} or below.
 * Those are whole in the log, so it never reads a write in progress; and under
 * {@link SyncPolicy#EVERY_WRITE} they are on the device, so a follower never gets a write that a
 * crash of the leader could take back.
 *
 * <p>The tail lists the directory when it starts and each time it reaches the end of a log, and
 * reads through a channel of its own, which stays readable after the store deletes the file. A
 * record whose log is gone (the tables cover it and {@link Options#retainLogBytes} didn't keep it)
 * can't be read: {@link BehindException}, and the follower needs a copy of the tables instead.
 *
 * <p>Not thread-safe: one reader a tail.
 */
public final class LogTail implements Closeable {
    /** The record the tail needs is in a log that's gone. */
    public static final class BehindException extends IOException {
        private static final long serialVersionUID = 1L;

        private final long oldest;

        BehindException(long needed, long oldest) {
            super("record " + needed + " is in a log that's gone; "
                    + (oldest > 0 ? "the oldest log left starts at " + oldest : "no log left holds a record"));
            this.oldest = oldest;
        }

        /** The first sequence number of the oldest log left, or 0 if none holds a record. */
        public long oldest() {
            return oldest;
        }
    }

    /** The header and the sequence number: enough to place a record without reading it. */
    private static final int PEEK_BYTES = WalFormat.HEADER_BYTES + 8;

    private final Store store;
    private final Path dir;
    private final ByteBuffer peek = ByteBuffer.allocate(PEEK_BYTES);
    private long next;
    private long logNumber = -1;
    private FileChannel channel; // null until the first read, and after close
    private long position;
    private boolean closed;

    LogTail(Store store, long from) {
        if (from < 1) {
            throw new IllegalArgumentException("sequence numbers start at 1, not " + from);
        }
        this.store = store;
        this.dir = store.directory();
        this.next = from;
    }

    /** The sequence number of the record the next read returns. */
    public long nextSequence() {
        return next;
    }

    /** The next record if the store has acknowledged it, else null. Doesn't wait. */
    public byte[] poll() throws IOException {
        ensureOpen();
        return next <= store.lastSequence() ? read() : null;
    }

    /**
     * The next record, waiting up to {@code timeout} for the store to acknowledge it; null if it
     * didn't.
     *
     * @throws IOException if the store closed while waiting
     */
    public byte[] next(long timeout, TimeUnit unit) throws IOException, InterruptedException {
        ensureOpen();
        if (store.awaitSequence(next, timeout, unit)) {
            return read();
        }
        if (store.isClosed()) {
            throw new IOException("the store at " + dir + " is closed"); // or a follow loop would spin
        }
        return null;
    }

    /** Reads record {@link #next}, which the store has acknowledged: it is whole, in some log. */
    private byte[] read() throws IOException {
        if (channel == null) {
            open(-1);
        }
        for (;;) {
            peek.clear();
            int n = readFully(peek, position);
            if (n == 0) {
                // The end of this log, and the record isn't in it: this log is finished (only the
                // store's newest log is written to, and the record is acknowledged), so the record
                // is in a newer one.
                open(logNumber);
                continue;
            }
            if (n < PEEK_BYTES) {
                throw damaged(n + " bytes, not a whole header");
            }
            int length = peek.getInt(0);
            long seq = peek.getLong(WalFormat.HEADER_BYTES);
            if (length < WalFormat.MIN_PAYLOAD || length > WalFormat.MAX_PAYLOAD) {
                throw damaged("a record length of " + Integer.toUnsignedString(length));
            }
            if (seq < next) {
                position += WalFormat.HEADER_BYTES + length; // before the one wanted: skip it unread
                continue;
            }
            if (seq > next) {
                throw damaged("record " + seq + " where " + next + " should be");
            }
            ByteBuffer record = ByteBuffer.allocate(WalFormat.HEADER_BYTES + length);
            if (readFully(record, position) != record.capacity()) {
                throw damaged("record " + seq + " cut short");
            }
            if (!(WalFormat.decode(record.array(), 0, record.capacity()) instanceof WalFormat.Ok)) {
                throw damaged("record " + seq + " fails its checks");
            }
            position += record.capacity();
            next++;
            return record.array();
        }
    }

    /**
     * Moves to the log holding {@link #next}: of the logs numbered above {@code after} that hold a
     * record, the last whose first record is at or before it. Logs are in sequence order, so that
     * one holds it unless it is missing.
     */
    private void open(long after) throws IOException {
        closeChannel();
        long oldest = 0;
        FileChannel chosen = null;
        long chosenNumber = -1;
        try {
            for (long number : logsAbove(after)) {
                FileChannel c;
                try {
                    c = FileChannel.open(dir.resolve(Store.logName(number)), StandardOpenOption.READ);
                } catch (NoSuchFileException e) {
                    continue; // deleted since the listing
                }
                long first = firstSequence(c);
                if (first < 0) {
                    c.close(); // empty: a new log, or one whose first write is in progress
                    continue;
                }
                if (oldest == 0) {
                    oldest = first;
                }
                if (first > next) {
                    c.close();
                    break; // and every later log starts later still
                }
                if (chosen != null) {
                    chosen.close();
                }
                chosen = c;
                chosenNumber = number;
            }
        } catch (IOException | RuntimeException e) {
            if (chosen != null) {
                chosen.close();
            }
            throw e;
        }
        if (chosen == null) {
            throw new BehindException(next, oldest);
        }
        channel = chosen;
        logNumber = chosenNumber;
        position = 0;
    }

    private List<Long> logsAbove(long after) throws IOException {
        List<Long> numbers = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*" + Store.LOG_SUFFIX)) {
            for (Path file : files) {
                long number = Store.fileNumber(file.getFileName().toString(), Store.LOG_SUFFIX);
                if (number > after) {
                    numbers.add(number);
                }
            }
        }
        Collections.sort(numbers);
        return numbers;
    }

    /** The sequence number of a log's first record, or -1 if it doesn't hold a whole header yet. */
    private static long firstSequence(FileChannel c) throws IOException {
        ByteBuffer first = ByteBuffer.allocate(PEEK_BYTES);
        while (first.hasRemaining()) {
            if (c.read(first, first.position()) < 0) {
                return -1;
            }
        }
        return first.getLong(WalFormat.HEADER_BYTES);
    }

    /** Reads at {@code at} until the buffer is full or the log ends; returns the bytes read. */
    private int readFully(ByteBuffer buffer, long at) throws IOException {
        int start = buffer.position();
        while (buffer.hasRemaining()) {
            int n = channel.read(buffer, at + buffer.position() - start);
            if (n < 0) {
                break;
            }
        }
        return buffer.position() - start;
    }

    private IOException damaged(String what) {
        return new IOException(Store.logName(logNumber) + " is damaged at offset " + position + ": " + what);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("the tail is closed");
        }
    }

    private void closeChannel() throws IOException {
        if (channel != null) {
            FileChannel c = channel;
            channel = null;
            c.close();
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        closeChannel();
    }
}
