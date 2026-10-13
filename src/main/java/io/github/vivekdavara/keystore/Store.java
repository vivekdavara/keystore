package io.github.vivekdavara.keystore;

import io.github.vivekdavara.keystore.memtable.Entry;
import io.github.vivekdavara.keystore.memtable.MemTable;
import io.github.vivekdavara.keystore.wal.LogFullException;
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
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

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
     * Directories open in this JVM, by {@link #identity}. POSIX file locks belong to the process,
     * and closing any channel on the file releases them, so a second open in this JVM must be
     * refused before it opens the lock file at all: closing its channel would drop the first
     * store's lock. Lucene's native lock factory keeps the same kind of set.
     */
    private static final Set<Object> OPEN_IN_THIS_JVM = ConcurrentHashMap.newKeySet();

    /**
     * Lock-file channels that must never be closed: each found the lock already held by this JVM
     * through another channel, which means the set above missed a second path to an open
     * directory. Closing one would release the other store's lock, so they stay open, and
     * referenced so that no cleaner closes them, for the life of the JVM.
     */
    private static final List<FileChannel> KEPT_OPEN = new CopyOnWriteArrayList<>();

    private final Path dir;
    private final Object identity;
    private final Options options;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final WriteAheadLog log;
    private final MemTable memtable;
    private final Recovery recovery;

    private final ReentrantLock writeLock = new ReentrantLock();
    /**
     * Written only under writeLock. An AtomicLong in a final field, not a volatile long, so that a
     * Store handed to another thread without synchronisation still shows the recovered value: the
     * final-field guarantee covers what the field refers to, but not a volatile set in a constructor.
     */
    private final AtomicLong lastSeq;
    private Throwable failure; // guarded by writeLock
    private boolean failedFlush; // guarded by writeLock: the failure came from forcing the log
    private volatile boolean closed;

    private Store(
            Path dir, Object identity, Options options, FileChannel lockChannel, FileLock lock, WriteAheadLog log,
            MemTable memtable, Recovery recovery) {
        this.dir = dir;
        this.identity = identity;
        this.options = options;
        this.lockChannel = lockChannel;
        this.lock = lock;
        this.log = log;
        this.memtable = memtable;
        this.recovery = recovery;
        this.lastSeq = new AtomicLong(recovery.lastSeq());
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
        return open(dir, options, (path, channel) -> channel);
    }

    /**
     * {@link #open(Path, Options)}, with every channel on the log and on directories wrapped: for
     * fault injection and counting in tests.
     */
    static Store open(Path dir, Options options, BiFunction<Path, FileChannel, FileChannel> wrap) throws IOException {
        Objects.requireNonNull(options, "options");
        createDirectory(dir, wrap);
        Path real = dir.toRealPath();
        Object identity = identity(real);
        if (!OPEN_IN_THIS_JVM.add(identity)) {
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
                // This JVM holds the lock through another channel: the set missed another path to an
                // open directory. Closing this channel would release that lock, so it stays open.
                KEPT_OPEN.add(lockChannel);
                lockChannel = null;
                throw new StoreLockedException(dir + " is already open in this process, by another path");
            }
            if (lock == null) {
                throw new StoreLockedException(dir + " is open in another process");
            }
            MemTable memtable = new MemTable();
            WriteAheadLog.Opened opened = WriteAheadLog.open(
                    real.resolve(LOG_FILE), options.recovery(), r -> apply(memtable, r), options.maxLogBytes(), wrap);
            return new Store(real, identity, options, lockChannel, lock, opened.log(), memtable, opened.recovery());
        } catch (IOException | RuntimeException | Error e) {
            if (lockChannel != null) {
                try {
                    lockChannel.close(); // releases the lock, if it was taken
                } catch (IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            OPEN_IN_THIS_JVM.remove(identity);
            throw e;
        }
    }

    /**
     * What identifies a directory however it is reached: its file key (device and inode, on Linux
     * and macOS), or its real path where the platform has none. Real paths alone aren't enough: a
     * macOS firmlink or a Linux bind mount gives one directory two of them.
     */
    private static Object identity(Path real) throws IOException {
        Object key = Files.readAttributes(real, BasicFileAttributes.class).fileKey();
        return key != null ? key : real;
    }

    /**
     * Creates the directory and any missing ancestors, then forces the parent of each one created,
     * top down, so every new entry survives a crash (forcing only the last parent would leave the
     * entries above it unforced).
     */
    private static void createDirectory(Path dir, BiFunction<Path, FileChannel, FileChannel> wrap) throws IOException {
        List<Path> missing = new ArrayList<>(); // deepest first
        for (Path p = dir.toAbsolutePath(); p != null && !Files.isDirectory(p); p = p.getParent()) {
            missing.add(p);
        }
        if (missing.isEmpty()) {
            return;
        }
        Files.createDirectories(dir);
        for (int i = missing.size() - 1; i >= 0; i--) {
            Path parent = missing.get(i).getParent();
            if (parent != null) {
                WriteAheadLog.forceDirectory(parent, wrap);
            }
        }
    }

    /**
     * Sets {@code key} to {@code value}. When this returns the write is in the log (and, under
     * {@link SyncPolicy#EVERY_WRITE}, on the device) and visible to readers. If it throws
     * {@link LogFullException}, nothing was written and the store goes on. If it throws another
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
            long seq = lastSeq.get() + 1;
            WalRecord record = value == null ? WalRecord.delete(seq, key) : WalRecord.put(seq, key, value);
            log.ensureRoomFor(record); // LogFullException: nothing written, the store goes on
            boolean flushing = false;
            try {
                log.append(record);
                if (options.sync() == SyncPolicy.EVERY_WRITE) {
                    flushing = true;
                    log.sync();
                    flushing = false;
                }
                apply(memtable, record);
            } catch (IOException | RuntimeException | Error e) {
                // The log may now end in part of a record, or a flush may have dropped pages it was
                // meant to write: appending more could bury that, and retrying a failed flush can
                // falsely succeed. Stop writing; reopening recovers. (No later write can reuse this
                // sequence number, since there are no later writes.)
                failure = e;
                failedFlush = flushing;
                throw e;
            }
            lastSeq.set(seq); // only now: lastSequence() never names a write that reads can't see
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

    /**
     * The sequence number of the last write visible to reads (and, under
     * {@link SyncPolicy#EVERY_WRITE}, on the device), or 0 if there has been none. Still answers
     * after {@link #close}.
     */
    public long lastSequence() {
        return lastSeq.get();
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
     * Forces the log, closes it and releases the directory, so that when it returns every
     * acknowledged write is on the device, whatever the sync policy. Waits for a write in progress.
     * Afterwards {@link #put}, {@link #delete} and {@link #get} fail with
     * {@link IllegalStateException} (the accessors still answer); closing again does nothing.
     *
     * <p>A store whose writes failed: after a failed flush (under {@link SyncPolicy#EVERY_WRITE})
     * every write acknowledged before it was forced on its own, and forcing again proves nothing,
     * so the log is closed as it is. After a failed append the log is still forced, since under
     * {@link SyncPolicy#NEVER} the writes before it need it (the partial record goes with them, and
     * recovery drops it). If the log can't be forced at all (an interrupt closed it), under
     * {@code NEVER} this throws {@link StoreFailedException}, since acknowledged writes may not be on
     * the device; the directory is released either way.
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
                closeLog();
            } finally {
                try {
                    lock.release();
                } finally {
                    try {
                        lockChannel.close();
                    } finally {
                        OPEN_IN_THIS_JVM.remove(identity);
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void closeLog() throws IOException {
        if (failure == null || (!failedFlush && log.isOpen())) {
            log.close(); // forces, then closes
            return;
        }
        log.abandon();
        if (!failedFlush && options.sync() == SyncPolicy.NEVER) {
            throw new StoreFailedException(
                    "the log could not be forced at close, so writes acknowledged under SyncPolicy.NEVER may"
                            + " not be on the device", failure);
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
