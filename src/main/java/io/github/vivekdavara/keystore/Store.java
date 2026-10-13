package io.github.vivekdavara.keystore;

import io.github.vivekdavara.keystore.memtable.Entry;
import io.github.vivekdavara.keystore.memtable.MemTable;
import io.github.vivekdavara.keystore.wal.Recovery;
import io.github.vivekdavara.keystore.wal.WalFormat;
import io.github.vivekdavara.keystore.wal.WalRecord;
import io.github.vivekdavara.keystore.wal.WriteAheadLog;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

/**
 * A key-value store in one directory: a write-ahead log and a memtable (day 1).
 *
 * <p>Writes are serialised by one lock: each is appended to the log, forced under
 * {@link SyncPolicy#EVERY_WRITE}, applied to the memtable, and only then acknowledged. Reads take
 * no lock. Thread-safe.
 */
public final class Store implements Closeable {
    static final String LOCK_FILE = "LOCK";
    static final String LOG_FILE = "wal.log";

    /**
     * Directories open in this JVM. POSIX file locks belong to the process, and closing any channel
     * on the file releases them, so a second open in this JVM must be refused before it opens the
     * lock file at all: closing its channel would drop the first store's lock. Lucene's native lock
     * factory keeps the same kind of set.
     */
    private static final Set<Path> OPEN_IN_THIS_JVM = ConcurrentHashMap.newKeySet();

    private final Path dir;
    private final Options options;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final WriteAheadLog log;
    private final MemTable memtable;
    private final Recovery recovery;

    private final ReentrantLock writeLock = new ReentrantLock();
    private volatile long lastSeq; // written only under writeLock
    private Throwable failure; // guarded by writeLock
    private volatile boolean closed;

    private Store(
            Path dir, Options options, FileChannel lockChannel, FileLock lock, WriteAheadLog log,
            MemTable memtable, Recovery recovery) {
        this.dir = dir;
        this.options = options;
        this.lockChannel = lockChannel;
        this.lock = lock;
        this.log = log;
        this.memtable = memtable;
        this.recovery = recovery;
        this.lastSeq = recovery.lastSeq();
    }

    /** Opens the store in {@code dir} with {@link Options#defaults()}, creating it if needed. */
    public static Store open(Path dir) throws IOException {
        return open(dir, Options.defaults());
    }

    /**
     * Opens the store in {@code dir}, creating the directory if needed, and recovers it from its
     * log; {@link #recovery()} says what recovery found.
     *
     * @throws StoreLockedException if the directory is open in another store
     * @throws io.github.vivekdavara.keystore.wal.WalCorruptionException if the log is damaged in a
     *     way {@code options.recovery()} refuses
     */
    public static Store open(Path dir, Options options) throws IOException {
        return open(dir, options, UnaryOperator.identity());
    }

    /** {@link #open(Path, Options)}, with the log's channel wrapped: for fault injection in tests. */
    static Store open(Path dir, Options options, UnaryOperator<FileChannel> wrapLog) throws IOException {
        Objects.requireNonNull(options, "options");
        createDirectory(dir);
        Path real = dir.toRealPath();
        if (!OPEN_IN_THIS_JVM.add(real)) {
            throw new StoreLockedException(dir + " is already open in this process");
        }
        FileChannel lockChannel = null;
        try {
            lockChannel = FileChannel.open(
                    real.resolve(LOCK_FILE), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            FileLock lock;
            try {
                lock = lockChannel.tryLock();
            } catch (OverlappingFileLockException e) {
                lock = null; // can't happen while the set above works; refuse all the same
            }
            if (lock == null) {
                throw new StoreLockedException(dir + " is open in another process");
            }
            MemTable memtable = new MemTable();
            WriteAheadLog.Opened opened =
                    WriteAheadLog.open(real.resolve(LOG_FILE), options.recovery(), r -> apply(memtable, r), wrapLog);
            return new Store(real, options, lockChannel, lock, opened.log(), memtable, opened.recovery());
        } catch (IOException | RuntimeException | Error e) {
            if (lockChannel != null) {
                try {
                    lockChannel.close(); // releases the lock, if it was taken
                } catch (IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            OPEN_IN_THIS_JVM.remove(real);
            throw e;
        }
    }

    /** Creates the directory if needed, and then forces its parent so the new entry survives a crash. */
    private static void createDirectory(Path dir) throws IOException {
        if (Files.isDirectory(dir)) {
            return;
        }
        Files.createDirectories(dir);
        Path parent = dir.toAbsolutePath().getParent();
        if (parent != null) {
            try (FileChannel channel = FileChannel.open(parent, StandardOpenOption.READ)) {
                channel.force(true);
            }
        }
    }

    /**
     * Sets {@code key} to {@code value}. When this returns the write is in the log (and, under
     * {@link SyncPolicy#EVERY_WRITE}, on the device) and visible to readers. If it throws an
     * {@link IOException}, the write may or may not have reached the log, and the store accepts no
     * more writes ({@link StoreFailedException}) until it is reopened.
     *
     * @throws IllegalArgumentException if the key is empty or longer than 65,536 bytes, or the value
     *     is longer than 16 MiB; nothing is written
     */
    public void put(byte[] key, byte[] value) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        WalFormat.checkLimits(1, key, value);
        write(key.clone(), value.clone()); // the caller may reuse its arrays
    }

    /**
     * Deletes {@code key}: later reads find nothing, until the key is put again. Deleting a key that
     * isn't there is still a write. Fails like {@link #put}.
     */
    public void delete(byte[] key) throws IOException {
        Objects.requireNonNull(key, "key");
        WalFormat.checkLimits(1, key, null);
        write(key.clone(), null);
    }

    private void write(byte[] key, byte[] value) throws IOException {
        writeLock.lock();
        try {
            ensureOpen();
            if (failure != null) {
                throw new StoreFailedException(failure);
            }
            long seq = lastSeq + 1;
            WalRecord record = value == null ? WalRecord.delete(seq, key) : WalRecord.put(seq, key, value);
            try {
                log.append(record);
                lastSeq = seq; // in the log now, so the next write must not reuse the number
                if (options.sync() == SyncPolicy.EVERY_WRITE) {
                    log.sync();
                }
                apply(memtable, record);
            } catch (IOException | RuntimeException | Error e) {
                // The log may now end in part of a record, or a flush may have dropped pages it was
                // meant to write: appending more could bury that, and retrying a failed flush can
                // falsely succeed. Stop writing; reopening recovers.
                failure = e;
                throw e;
            }
        } finally {
            writeLock.unlock();
        }
    }

    /** The value for {@code key}, or empty if there is none or it was deleted. Takes no lock. */
    public Optional<byte[]> get(byte[] key) throws IOException {
        Objects.requireNonNull(key, "key");
        ensureOpen();
        Entry entry = memtable.get(key);
        if (entry == null || entry.isTombstone()) {
            return Optional.empty();
        }
        return Optional.of(entry.value().clone()); // the caller can't change the memtable's copy
    }

    /** What recovery found when the store was opened. */
    public Recovery recovery() {
        return recovery;
    }

    /** The sequence number of the last write, or 0 if there has been none. */
    public long lastSequence() {
        return lastSeq;
    }

    /** The store's directory (its real path). */
    public Path directory() {
        return dir;
    }

    /** Keys in the memtable, tombstones included. */
    public int memtableSize() {
        return memtable.size();
    }

    /** Bytes in the log. */
    public long logSize() {
        writeLock.lock();
        try {
            return log.size();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Forces the log, closes it and releases the directory. Waits for a write in progress. Calls
     * after this fail with {@link IllegalStateException}; closing again does nothing.
     */
    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            try {
                log.close();
            } finally {
                try {
                    lock.release();
                } finally {
                    try {
                        lockChannel.close();
                    } finally {
                        OPEN_IN_THIS_JVM.remove(dir);
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("the store at " + dir + " is closed");
        }
    }

    private static void apply(MemTable memtable, WalRecord record) {
        switch (record.op()) {
            case PUT -> memtable.put(record.seq(), record.key(), record.value());
            case DELETE -> memtable.delete(record.seq(), record.key());
        }
    }
}
