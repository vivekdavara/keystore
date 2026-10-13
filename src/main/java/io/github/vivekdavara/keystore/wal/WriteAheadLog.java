package io.github.vivekdavara.keystore.wal;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * The write-ahead log file: recovered when opened, then appended to.
 *
 * <p>Not thread-safe: the store calls it under its write lock.
 */
public final class WriteAheadLog implements Closeable {
    /** Recovery reads the whole file into one array, so the file must fit in one. */
    public static final long MAX_RECOVERABLE_BYTES = Integer.MAX_VALUE - 8;

    private final FileChannel channel;
    private final long maxBytes;
    private long size;

    /** An open log and what recovering it found. */
    public record Opened(WriteAheadLog log, Recovery recovery) {}

    private WriteAheadLog(FileChannel channel, long size, long maxBytes) {
        this.channel = channel;
        this.size = size;
        this.maxBytes = maxBytes;
    }

    /**
     * Opens the log at {@code file}, creating it if it doesn't exist, and replays it into
     * {@code apply} (DESIGN.md, "Recovery"). A torn tail, or damage {@code mode} allows dropping, is
     * truncated away and the truncation forced before this returns. A refused open leaves the file
     * as it was.
     *
     * @throws WalCorruptionException if the log is damaged in the middle and {@code mode} refuses it
     */
    public static Opened open(Path file, RecoveryMode mode, Consumer<WalRecord> apply) throws IOException {
        return open(file, mode, apply, MAX_RECOVERABLE_BYTES, (path, channel) -> channel);
    }

    /**
     * {@link #open(Path, RecoveryMode, Consumer)}, refusing appends that would take the log past
     * {@code maxBytes}, and with every channel it opens (the log's and the directory's) replaced by
     * {@code wrap.apply(path, channel)}, for fault injection and counting in tests.
     */
    public static Opened open(
            Path file, RecoveryMode mode, Consumer<WalRecord> apply, long maxBytes,
            BiFunction<Path, FileChannel, FileChannel> wrap) throws IOException {
        if (maxBytes < 1 || maxBytes > MAX_RECOVERABLE_BYTES) {
            throw new IllegalArgumentException("the log's limit must be 1 to " + MAX_RECOVERABLE_BYTES + ", not " + maxBytes);
        }
        FileChannel channel = wrap.apply(file, FileChannel.open(
                file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE));
        try {
            // Forced on every open, not only when the file is new: a crash between creating the file
            // and forcing its directory would otherwise leave the entry unforced for good.
            forceDirectory(file.toAbsolutePath().getParent(), wrap);
            long length = channel.size();
            if (length > MAX_RECOVERABLE_BYTES) {
                throw new IOException(file + " is " + length + " bytes; recovery reads a log whole, up to "
                        + MAX_RECOVERABLE_BYTES);
            }
            byte[] bytes = readFully(channel, (int) length);
            Recovery recovery = WalReader.read(bytes, bytes.length, 1, mode, apply);
            if (recovery.validBytes() < length) {
                channel.truncate(recovery.validBytes());
                // Forced before anything is appended: otherwise a crash after new appends could leave
                // the old bytes beyond them for the next recovery to find.
                channel.force(true);
            }
            channel.position(recovery.validBytes());
            return new Opened(new WriteAheadLog(channel, recovery.validBytes(), maxBytes), recovery);
        } catch (IOException | RuntimeException | Error e) {
            try {
                channel.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /**
     * Throws {@link LogFullException} if appending the record would take the log past its limit.
     * Nothing is written either way, so the log is as it was.
     */
    public void ensureRoomFor(WalRecord record) throws LogFullException {
        long after = size + WalFormat.encodedSize(record);
        if (after > maxBytes) {
            throw new LogFullException("the log is " + size + " bytes; this write would take it to " + after
                    + ", past its limit of " + maxBytes + " (day 1 has no log rotation)");
        }
    }

    /**
     * Appends a record, all of it, at the end of the log. It is in the OS's hands when this returns,
     * not yet on the device: call {@link #sync} for that. After an exception, part of the record may
     * have been written; the caller must stop appending (the store does). Call
     * {@link #ensureRoomFor} first: this checks the limit too, but as a bug, after nothing is written.
     */
    public void append(WalRecord record) throws IOException {
        if (size + WalFormat.encodedSize(record) > maxBytes) {
            throw new IllegalStateException("append without room; call ensureRoomFor first");
        }
        ByteBuffer bytes = WalFormat.encode(record);
        int length = bytes.remaining();
        while (bytes.hasRemaining()) {
            channel.write(bytes);
        }
        size += length;
    }

    /** Forces everything appended so far to the device. */
    public void sync() throws IOException {
        channel.force(false);
    }

    /** Bytes in the log: what recovery kept, plus everything appended since. */
    public long size() {
        return size;
    }

    /** False once the log is closed, by {@link #close}, {@link #abandon}, or an interrupt. */
    public boolean isOpen() {
        return channel.isOpen();
    }

    /** Forces the log and closes it. */
    @Override
    public void close() throws IOException {
        try (channel) {
            channel.force(false);
        }
    }

    /**
     * Closes the log without forcing it, for a log whose append or flush failed: its state is
     * unknown, a flush after a failed one proves nothing, and the channel may already be closed (an
     * interrupt closes it).
     */
    public void abandon() throws IOException {
        channel.close();
    }

    /**
     * Forces a directory, so a file just created in it is still there after a crash. Linux and macOS
     * allow this through a channel opened for reading.
     */
    public static void forceDirectory(Path directory) throws IOException {
        forceDirectory(directory, (path, channel) -> channel);
    }

    /** {@link #forceDirectory(Path)} through {@code wrap}'s channel, for counting in tests. */
    public static void forceDirectory(Path directory, BiFunction<Path, FileChannel, FileChannel> wrap)
            throws IOException {
        try (FileChannel dir = wrap.apply(directory, FileChannel.open(directory, StandardOpenOption.READ))) {
            dir.force(true);
        }
    }

    private static byte[] readFully(FileChannel channel, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        long position = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new EOFException("the log shrank while being read, at " + position + " of " + length);
            }
            position += read;
        }
        return buffer.array();
    }
}
