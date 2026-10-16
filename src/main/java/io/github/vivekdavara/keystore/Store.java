package io.github.vivekdavara.keystore;

import io.github.vivekdavara.keystore.memtable.Entry;
import io.github.vivekdavara.keystore.memtable.MemTable;
import io.github.vivekdavara.keystore.sstable.BloomFilter;
import io.github.vivekdavara.keystore.sstable.CorruptTableException;
import io.github.vivekdavara.keystore.sstable.Table;
import io.github.vivekdavara.keystore.sstable.TableWriter;
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
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * A key-value store in one directory: a write-ahead log, a memtable, and tables (LSM).
 *
 * <p>Writes are serialised by one lock: each is appended to the log, forced under
 * {@link SyncPolicy#EVERY_WRITE}, applied to the memtable, and only then acknowledged. When the log
 * reaches {@link Options#flushLogBytes}, the next write first flushes the memtable to a table, starts
 * a new log, and runs any compaction that is due. Reads take no lock: each pins the current
 * {@link Version} and looks in the memtable, the frozen memtable, then the tables newest first.
 * Thread-safe.
 *
 * <p>The directory holds {@code LOCK}, {@code MANIFEST} (the live tables and the oldest log to
 * replay), logs {@code NNNNNN.log} and tables {@code NNNNNN.sst}, numbered from one counter.
 */
public final class Store implements Closeable {
    static final String LOCK_FILE = "LOCK";
    static final String LOG_SUFFIX = ".log";
    static final String TABLE_SUFFIX = ".sst";

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
    private final BiFunction<Path, FileChannel, FileChannel> wrap;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final Recovery recovery;

    private final ReentrantLock writeLock = new ReentrantLock();
    /**
     * Written only under writeLock. An AtomicLong in a final field, not a volatile long, so that a
     * Store handed to another thread without synchronisation still shows the recovered value: the
     * final-field guarantee covers what the field refers to, but not a volatile set in a constructor.
     */
    private final AtomicLong lastSeq;
    /**
     * The stored checksum of the record lastSeq names, or -1 if unknown (see {@link #lastChecksum}).
     * Written under writeLock, just before lastSeq.
     */
    private volatile long lastChecksum;
    /**
     * For {@link #awaitSequence}: signalled when lastSeq advances or the store closes, and only if
     * someone waits, so writes nobody follows don't take a second lock. A ReentrantLock, not a
     * monitor: in Java 21 a virtual thread blocked in Object.wait pins its carrier thread.
     */
    private final ReentrantLock sequenceLock = new ReentrantLock();
    private final Condition sequenceAdvanced = sequenceLock.newCondition();
    private final AtomicInteger sequenceWaiters = new AtomicInteger();
    private WriteAheadLog log; // guarded by writeLock
    private Manifest manifest; // guarded by writeLock: as last written
    private long nextFileNumber; // guarded by writeLock
    /** What reads consult. Replaced under writeLock; see {@link #install}. */
    private volatile Version current;
    private Throwable failure; // guarded by writeLock
    private boolean failedFlush; // guarded by writeLock: the failure came from forcing the log on a write
    /**
     * Guarded by writeLock: a flush forced and closed the log, and starting the next one failed, so
     * {@link #log} is closed but everything in it is on the device.
     */
    private boolean rotatedOut;
    private volatile boolean closed;
    private long flushes; // guarded by writeLock, like the counts below
    private long flushedBytes;
    private long merges;
    private long mergeInputBytes;
    private long mergedBytes;

    private Store(
            Path dir, Object identity, Options options, BiFunction<Path, FileChannel, FileChannel> wrap,
            FileChannel lockChannel, FileLock lock, WriteAheadLog log, Manifest manifest, long nextFileNumber,
            Version current, Recovery recovery, long lastChecksum) {
        this.dir = dir;
        this.identity = identity;
        this.options = options;
        this.wrap = wrap;
        this.lockChannel = lockChannel;
        this.lock = lock;
        this.log = log;
        this.manifest = manifest;
        this.nextFileNumber = nextFileNumber;
        this.current = current;
        this.recovery = recovery;
        this.lastSeq = new AtomicLong(recovery.lastSeq());
        this.lastChecksum = lastChecksum;
    }

    /** A log's file name: its number, six digits or more. */
    static String logName(long number) {
        return String.format("%06d%s", number, LOG_SUFFIX);
    }

    /** A table's file name. */
    static String tableName(long number) {
        return String.format("%06d%s", number, TABLE_SUFFIX);
    }

    /** The number in a log's or table's file name, or -1 if {@code name} isn't one with {@code suffix}. */
    static long fileNumber(String name, String suffix) {
        if (!name.endsWith(suffix)) {
            return -1;
        }
        String digits = name.substring(0, name.length() - suffix.length());
        if (digits.isEmpty() || digits.length() > 18 || !digits.chars().allMatch(c -> c >= '0' && c <= '9')) {
            return -1;
        }
        return Long.parseLong(digits);
    }

    /** Opens the store in {@code dir} with {@link Options#defaults()}, creating it if needed. */
    public static Store open(Path dir) throws IOException {
        return open(dir, Options.defaults());
    }

    /**
     * Opens the store in {@code dir}, creating the directory if needed, and recovers it: the
     * manifest's tables are opened and checked, and the logs it doesn't cover are replayed;
     * {@link #recovery()} says what recovery found. Files the manifest doesn't account for (an
     * unfinished flush's table, a compaction's inputs, logs the tables cover) are deleted, once
     * recovery has succeeded; a refused open changes nothing.
     *
     * @throws StoreLockedException if the directory is open in another store
     * @throws io.github.vivekdavara.keystore.wal.WalCorruptionException if a log is damaged in a
     *     way {@code options.recovery()} refuses
     * @throws CorruptTableException if a live table fails a check
     */
    public static Store open(Path dir, Options options) throws IOException {
        return open(dir, options, (path, channel) -> channel);
    }

    /**
     * {@link #open(Path, Options)}, with every channel the store opens (logs, tables, the manifest,
     * directories) wrapped: for fault injection and counting in tests.
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
        List<Table> tables = new ArrayList<>();
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
            return recover(real, identity, options, wrap, lockChannel, lock, tables);
        } catch (IOException | RuntimeException | Error e) {
            for (Table table : tables) {
                table.release();
            }
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

    /** The open's recovery, once the directory is locked; {@code tables} collects what it opens. */
    private static Store recover(
            Path real, Object identity, Options options, BiFunction<Path, FileChannel, FileChannel> wrap,
            FileChannel lockChannel, FileLock lock, List<Table> tables) throws IOException {
        // The directory's files, by kind.
        List<Long> logs = new ArrayList<>();
        Set<Long> tableFiles = new HashSet<>();
        boolean dayOneLog = false;
        long highest = 0;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(real)) {
            for (Path file : files) {
                String name = file.getFileName().toString();
                long logNumber = fileNumber(name, LOG_SUFFIX);
                long tableNumber = fileNumber(name, TABLE_SUFFIX);
                if (logNumber >= 0) {
                    logs.add(logNumber);
                } else if (tableNumber >= 0) {
                    tableFiles.add(tableNumber);
                }
                dayOneLog |= name.equals("wal.log");
                highest = Math.max(highest, Math.max(logNumber, tableNumber));
            }
        }
        Manifest manifest = Manifest.read(real);
        if (manifest == null) {
            if (!logs.isEmpty() || !tableFiles.isEmpty() || dayOneLog) {
                // Without the manifest nothing says which tables are live, or where the logs start:
                // starting a new one would delete the tables as garbage.
                throw new IOException(real + " has logs or tables but no " + Manifest.FILE
                        + "; refusing to open it as a new store");
            }
            manifest = Manifest.empty();
            manifest.write(real, wrap);
        }

        Set<Long> live = new HashSet<>();
        for (Manifest.TableRef ref : manifest.tables()) {
            Table table = Table.open(real.resolve(tableName(ref.number())), ref.number(), wrap);
            tables.add(table);
            live.add(ref.number());
            if (table.fileBytes() != ref.fileBytes() || table.smallestSeq() != ref.smallestSeq()
                    || table.largestSeq() != ref.largestSeq()) {
                throw new CorruptTableException(table.file(), "doesn't match the manifest's record of it, " + ref);
            }
        }

        // Replay the logs the tables don't cover, oldest first. Only the newest can end in a torn
        // write: each older one was forced and closed before the next one took a write.
        Collections.sort(logs);
        List<Long> replay = new ArrayList<>();
        for (long number : logs) {
            if (number >= manifest.logNumber()) {
                replay.add(number);
            }
        }
        if (replay.isEmpty()) {
            replay.add(manifest.logNumber()); // created by the open below
        }
        MemTable memtable = new MemTable();
        long seq = manifest.lastSequence();
        long records = 0;
        WalRecord[] lastReplayed = new WalRecord[1];
        Consumer<WalRecord> replayOne = r -> {
            apply(memtable, r);
            lastReplayed[0] = r;
        };
        for (int i = 0; i < replay.size() - 1; i++) {
            Recovery older = WriteAheadLog.replayClosed(
                    real.resolve(logName(replay.get(i))), seq + 1, replayOne, wrap);
            seq = older.lastSeq();
            records += older.records();
        }
        WriteAheadLog.Opened newest = WriteAheadLog.open(
                real.resolve(logName(replay.get(replay.size() - 1))), seq + 1, options.recovery(),
                replayOne, options.maxLogBytes(), wrap);
        Recovery last = newest.recovery();
        // The checksum of the last write: from the log that holds it, or, if the tables hold every
        // write, from the manifest (day 4; a day-2 manifest doesn't know it).
        long lastChecksum = lastReplayed[0] != null && lastReplayed[0].seq() == last.lastSeq()
                ? WalFormat.storedChecksum(WalFormat.encode(lastReplayed[0]))
                : manifest.lastSequence() == last.lastSeq() ? manifest.lastChecksum() : -1;
        Recovery recovery = new Recovery(last.outcome(), records + last.records(), last.lastSeq(),
                last.validBytes(), last.droppedBytes(), last.detail());

        // Recovery succeeded: remove what the manifest doesn't account for. Best effort: a file that
        // can't be deleted now is removed by the next open, and failing here would fail an open
        // whose recovery is done (and leave the newest log's channel open). The deletions come
        // after the directory was forced (opening the newest log forces it), so the manifest they
        // follow from, perhaps renamed into place just before a crash, is on the device first.
        List<Long> covered = new ArrayList<>();
        for (long number : logs) {
            if (number < manifest.logNumber()) {
                covered.add(number);
            }
        }
        for (long number : beyondRetention(real, covered, options.retainLogBytes())) {
            deleteQuietly(real.resolve(logName(number)));
        }
        for (long number : tableFiles) {
            if (!live.contains(number)) {
                deleteQuietly(real.resolve(tableName(number)));
            }
        }
        deleteQuietly(real.resolve(Manifest.TEMP_FILE));

        Version version = new Version(memtable, null, tables);
        for (Table table : tables) {
            table.release(); // the version holds them now
        }
        tables.clear();
        long next = Math.max(manifest.nextFileNumber(), highest + 1);
        return new Store(real, identity, options, wrap, lockChannel, lock, newest.log(), manifest, next, version, recovery,
                lastChecksum);
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
     * more writes ({@link StoreFailedException}) until it is reopened. A write that finds the log
     * full enough flushes the memtable first, and a failed flush fails it the same way (the write
     * itself was not made).
     *
     * @return the write's sequence number
     * @throws IllegalArgumentException if the key is empty or longer than 65,536 bytes, or the value
     *     is longer than 16 MiB; nothing is written
     */
    public long put(byte[] key, byte[] value) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        WalFormat.checkLimits(1, key, value);
        return write(key.clone(), value.clone()); // the caller may reuse its arrays
    }

    /**
     * Deletes {@code key}: later reads find nothing, until the key is put again. Deleting a key that
     * isn't there is still a write. Fails like {@link #put}.
     *
     * @return the write's sequence number
     */
    public long delete(byte[] key) throws IOException {
        Objects.requireNonNull(key, "key");
        WalFormat.checkLimits(1, key, null);
        return write(key.clone(), null);
    }

    private long write(byte[] key, byte[] value) throws IOException {
        writeLock.lock();
        try {
            ensureWritable();
            if (log.size() >= options.flushLogBytes()) {
                flushAndCompact();
            }
            long seq = lastSeq.get() + 1;
            WalRecord record = value == null ? WalRecord.delete(seq, key) : WalRecord.put(seq, key, value);
            log.ensureRoomFor(record); // LogFullException: nothing written, the store goes on
            appendAndApply(List.of(record));
            return seq;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Applies writes a leader made, with the leader's sequence numbers: how a follower takes its
     * writes. The first must be the one after {@link #lastSequence()}, and the rest must follow
     * without a gap. They are appended to this store's own log, forced once for all of them under
     * {@link SyncPolicy#EVERY_WRITE} (a follower's group commit: one force a batch, not a record),
     * applied, and only then visible. Fails like {@link #put}. Keeps the records' arrays.
     *
     * <p>A batch that would take the log past {@link Options#flushLogBytes} is applied in runs, with
     * a flush before each run that finds the log full enough, just as the leader flushed between
     * the same writes; each run is forced once. So a batch never needs more room in a log than the
     * leader's writes did, however big it is. If a run doesn't fit ({@link LogFullException}), the
     * runs before it are applied and {@link #lastSequence()} says how far.
     *
     * @throws IllegalArgumentException if the records don't continue from {@link #lastSequence()},
     *     or one is outside the limits; nothing is written
     */
    public void replicate(List<WalRecord> records) throws IOException {
        if (records.isEmpty()) {
            return;
        }
        for (int i = 0; i < records.size(); i++) {
            WalRecord r = records.get(i);
            WalFormat.checkLimits(r.seq(), r.key(), r.value());
            if (i > 0 && r.seq() != records.get(i - 1).seq() + 1) {
                throw new IllegalArgumentException("record " + r.seq() + " follows " + records.get(i - 1).seq());
            }
        }
        writeLock.lock();
        try {
            ensureWritable();
            if (records.get(0).seq() != lastSeq.get() + 1) {
                throw new IllegalArgumentException(
                        "record " + records.get(0).seq() + " doesn't follow this store's last, " + lastSeq.get());
            }
            // Found by review: the batch went into the log in one piece, and a follower with its
            // leader's options failed with LogFullException on writes the leader had taken one at a
            // time, flushing between them.
            int from = 0;
            while (from < records.size()) {
                if (log.size() >= options.flushLogBytes()) {
                    flushAndCompact();
                }
                long size = log.size();
                long bytes = 0;
                int to = from;
                do { // the records a leader would append before its log reached flushLogBytes
                    bytes += WalFormat.encodedSize(records.get(to++));
                } while (to < records.size() && size + bytes < options.flushLogBytes());
                log.ensureRoomFor(bytes); // LogFullException: the runs before this one are applied
                appendAndApply(records.subList(from, to));
                from = to;
            }
        } finally {
            writeLock.unlock();
        }
    }

    /** Appends records, forces them under EVERY_WRITE, applies them, then publishes them. Under writeLock. */
    private void appendAndApply(List<WalRecord> records) throws IOException {
        boolean flushing = false;
        long checksum = -1;
        try {
            for (WalRecord record : records) {
                checksum = log.append(record);
            }
            if (options.sync() == SyncPolicy.EVERY_WRITE) {
                flushing = true;
                log.sync();
                flushing = false;
            }
            for (WalRecord record : records) {
                apply(current.active, record);
            }
        } catch (IOException | RuntimeException | Error e) {
            // The log may now end in part of a record, or a flush may have dropped pages it was
            // meant to write: appending more could bury that, and retrying a failed flush can
            // falsely succeed. Stop writing; reopening recovers. (No later write can reuse this
            // sequence number, since there are no later writes.)
            failure = e;
            failedFlush = flushing;
            throw e;
        }
        // Only now: lastSequence() never names a write that reads can't see.
        lastChecksum = checksum;
        lastSeq.set(records.get(records.size() - 1).seq());
        signalSequence();
    }

    /**
     * Waits until {@link #lastSequence()} reaches {@code seq}, the timeout passes, or the store
     * closes; returns whether it reached {@code seq}. For readers that follow the log.
     */
    public boolean awaitSequence(long seq, long timeout, TimeUnit unit) throws InterruptedException {
        if (lastSeq.get() >= seq) {
            return true;
        }
        long nanos = unit.toNanos(timeout);
        // Counted before lastSeq is read under the lock, and a writer sets lastSeq before reading
        // the count: so either the writer sees this waiter and signals, or this waiter sees the write.
        sequenceWaiters.incrementAndGet();
        sequenceLock.lock();
        try {
            while (lastSeq.get() < seq) {
                if (nanos <= 0 || closed) {
                    return false;
                }
                nanos = sequenceAdvanced.awaitNanos(nanos);
            }
            return true;
        } finally {
            sequenceLock.unlock();
            sequenceWaiters.decrementAndGet();
        }
    }

    /**
     * A reader of this store's logs from sequence number {@code from} on, which then follows new
     * writes: what a leader ships to followers (see {@link LogTail}). Close it when done.
     */
    public LogTail tail(long from) {
        ensureOpen();
        return new LogTail(this, from);
    }

    boolean isClosed() {
        return closed;
    }

    /** Threads in {@link #awaitSequence} now, for tests. */
    int sequenceWaiters() {
        return sequenceWaiters.get();
    }

    private void signalSequence() {
        if (sequenceWaiters.get() > 0) {
            sequenceLock.lock();
            try {
                sequenceAdvanced.signalAll();
            } finally {
                sequenceLock.unlock();
            }
        }
    }

    private void ensureWritable() throws StoreFailedException {
        ensureOpen();
        if (failure != null) {
            throw new StoreFailedException(failure);
        }
    }

    /**
     * Flushes the memtable to a table now, if it holds anything, then runs any compaction that is
     * due. Fails like {@link #put}'s flush: an exception stops the store's writes.
     */
    public void flush() throws IOException {
        writeLock.lock();
        try {
            ensureWritable();
            flushAndCompact();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Merges every table into one, dropping overwritten values and, since nothing older remains for
     * them to hide, tombstones. The memtable is left alone. Fails like {@link #flush}.
     */
    public void compactAll() throws IOException {
        writeLock.lock();
        try {
            ensureWritable();
            try {
                List<Table> tables = current.tables;
                if (tables.size() > 1 || (tables.size() == 1 && tables.get(0).tombstones() > 0)) {
                    merge(tables, 0, tables.size());
                }
            } catch (IOException | RuntimeException | Error e) {
                fail(e);
                throw e;
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void flushAndCompact() throws IOException {
        try {
            flushMemtable();
            compactDue();
        } catch (IOException | RuntimeException | Error e) {
            fail(e);
            throw e;
        }
    }

    /**
     * A failed flush or compaction stops writes, as a failed append does: the directory's state is
     * uncertain, and a reopen recovers from what reached the device. Not a failed per-write force, so
     * close forces the log it has if it can (see {@link #closeLog}).
     */
    private void fail(Throwable e) {
        failure = e;
        failedFlush = false;
    }

    /**
     * The flush, under writeLock:
     *
     * <ol>
     *   <li>Close the log, forcing it whatever the sync policy, and start a new one: from now on the
     *       old log is complete on the device, and only the newest log can end in a torn write.
     *   <li>Install a version with a new memtable and the old one frozen, so reads still find its
     *       keys while it is written out.
     *   <li>Write the frozen memtable to a table, force it, and force the directory, so the table
     *       and its entry are durable before the manifest names it.
     *   <li>Write a manifest listing the table, with the new log as the oldest to replay.
     *   <li>Install a version without the frozen memtable, and delete the logs the table covers.
     * </ol>
     *
     * A crash before step 4 leaves the old manifest: recovery replays both logs, and the new table,
     * which it doesn't name, is deleted. A crash after it leaves the new one, and the old log is
     * garbage.
     */
    private void flushMemtable() throws IOException {
        Version before = current;
        MemTable frozen = before.active;
        if (frozen.size() == 0) {
            return;
        }
        long newLog = nextFileNumber++;
        log.close(); // forces first; if it throws, the old log is still the store's, and closed
        rotatedOut = true; // until the new log is in place: if opening it fails, close has nothing to force
        log = WriteAheadLog.open(dir.resolve(logName(newLog)), lastSeq.get() + 1, options.recovery(), r -> {
            throw new IllegalStateException("a new log holds a record: " + r);
        }, options.maxLogBytes(), wrap).log();
        rotatedOut = false;
        MemTable active = new MemTable();
        install(new Version(active, frozen, before.tables));

        Table table = writeTable(frozen.iterator());
        List<Table> tables = new ArrayList<>();
        tables.add(table);
        tables.addAll(before.tables);
        try {
            WriteAheadLog.forceDirectory(dir, wrap);
            // Under writeLock with no write running, the frozen memtable's last write is the store's.
            writeManifest(newLog, frozen.lastSequence(), frozen.lastSequence() == lastSeq.get() ? lastChecksum : -1,
                    tables);
        } catch (IOException | RuntimeException | Error e) {
            table.release(); // its file stays: the manifest on the device may name it
            throw e;
        }
        install(new Version(active, null, tables));
        table.release(); // the version holds it
        flushes++;
        flushedBytes += table.fileBytes();
        deleteLogsBefore(newLog);
    }

    /** Writes entries (in key order) to a new table and opens it; the caller holds its one reference. */
    private Table writeTable(Iterator<Map.Entry<byte[], Entry>> entries) throws IOException {
        long number = nextFileNumber++;
        Path file = dir.resolve(tableName(number));
        TableWriter writer = TableWriter.create(file, options.blockBytes(), options.bloomBitsPerKey(), wrap);
        try {
            while (entries.hasNext()) {
                Map.Entry<byte[], Entry> e = entries.next();
                writer.add(e.getKey(), e.getValue());
            }
            writer.finish();
            return Table.open(file, number, wrap);
        } catch (IOException | RuntimeException | Error e) {
            writer.abandon(); // nothing names it yet
            throw e;
        }
    }

    private void writeManifest(long logNumber, long lastSequence, long lastChecksum, List<Table> tables)
            throws IOException {
        Manifest next = new Manifest(nextFileNumber, logNumber, lastSequence, lastChecksum, refs(tables));
        next.write(dir, wrap);
        manifest = next;
    }

    private static List<Manifest.TableRef> refs(List<Table> tables) {
        List<Manifest.TableRef> refs = new ArrayList<>(tables.size());
        for (Table t : tables) {
            refs.add(new Manifest.TableRef(t.number(), t.fileBytes(), t.smallestSeq(), t.largestSeq()));
        }
        return refs;
    }

    private static void deleteQuietly(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException ignored) {
            // Garbage the next open removes.
        }
    }

    /** Deletes the logs before {@code number}, which the tables now cover, except those retention keeps. */
    private void deleteLogsBefore(long number) {
        try {
            List<Long> covered = new ArrayList<>();
            try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*" + LOG_SUFFIX)) {
                for (Path file : files) {
                    long n = fileNumber(file.getFileName().toString(), LOG_SUFFIX);
                    if (n >= 0 && n < number) {
                        covered.add(n);
                    }
                }
            }
            for (long n : beyondRetention(dir, covered, options.retainLogBytes())) {
                Files.deleteIfExists(dir.resolve(logName(n)));
            }
        } catch (IOException ignored) {
            // Logs the manifest says are covered are deleted by the next open.
        }
    }

    /**
     * Of the covered logs, the ones {@link Options#retainLogBytes} doesn't keep, oldest first (the
     * order to delete them in, so the logs left are always an unbroken run of the newest). Newest
     * first, logs are kept until the kept ones total at least {@code retainBytes}.
     */
    static List<Long> beyondRetention(Path dir, List<Long> covered, long retainBytes) {
        List<Long> newestFirst = new ArrayList<>(covered);
        newestFirst.sort(Collections.reverseOrder());
        List<Long> delete = new ArrayList<>();
        long kept = 0;
        for (long number : newestFirst) {
            if (kept < retainBytes) {
                kept += sizeOrZero(dir.resolve(logName(number)));
            } else {
                delete.add(number);
            }
        }
        Collections.reverse(delete);
        return delete;
    }

    private static long sizeOrZero(Path file) {
        try {
            return Files.size(file);
        } catch (IOException e) {
            return 0; // gone already
        }
    }

    /** Merges size tiers until none has {@link Options#mergeWidth} tables (see {@link Compaction}). */
    private void compactDue() throws IOException {
        for (;;) {
            List<Table> tables = current.tables;
            long[] sizes = new long[tables.size()];
            for (int i = 0; i < sizes.length; i++) {
                sizes[i] = tables.get(i).fileBytes();
            }
            int[] run = Compaction.pick(sizes, options.mergeWidth(), options.flushLogBytes());
            if (run == null) {
                return;
            }
            merge(tables, run[0], run[1]);
        }
    }

    /**
     * Merges the adjacent tables {@code tables[from, to)} into one, which takes their place: the
     * manifest is written first, then the inputs are marked obsolete and a version without them is
     * installed. Reads still in an older version keep the inputs open; the last one out deletes them.
     */
    private void merge(List<Table> tables, int from, int to) throws IOException {
        List<Table> inputs = tables.subList(from, to);
        // Merging into the oldest table, nothing older remains for a tombstone to hide.
        boolean dropTombstones = to == tables.size();
        Table output = writeMerged(inputs, dropTombstones);
        List<Table> result = new ArrayList<>(tables.subList(0, from));
        if (output != null) {
            result.add(output);
        }
        result.addAll(tables.subList(to, tables.size()));
        try {
            if (output != null) {
                WriteAheadLog.forceDirectory(dir, wrap);
            }
            writeManifest(manifest.logNumber(), manifest.lastSequence(), manifest.lastChecksum(), result);
        } catch (IOException | RuntimeException | Error e) {
            if (output != null) {
                output.release(); // its file stays: the manifest on the device may name it
            }
            throw e;
        }
        for (Table input : inputs) {
            input.markObsolete();
        }
        Version before = current;
        install(new Version(before.active, before.frozen, result));
        merges++;
        for (Table input : inputs) {
            mergeInputBytes += input.fileBytes();
        }
        if (output != null) {
            mergedBytes += output.fileBytes();
            output.release();
        }
    }

    /**
     * Writes the newest entry of every key in {@code inputs} to a new table, dropping tombstones if
     * asked; returns it opened, or null if nothing was left to write.
     */
    private Table writeMerged(List<Table> inputs, boolean dropTombstones) throws IOException {
        // Smallest key first; for one key, the newest entry (the highest sequence number) first.
        PriorityQueue<Table.Cursor> heap = new PriorityQueue<>((a, b) -> {
            int c = Bytes.compare(a.key(), b.key());
            return c != 0 ? c : Long.compare(b.entry().seq(), a.entry().seq());
        });
        for (Table input : inputs) {
            Table.Cursor cursor = input.cursor();
            if (cursor.next()) {
                heap.add(cursor);
            }
        }
        long number = nextFileNumber++;
        Path file = dir.resolve(tableName(number));
        TableWriter writer = TableWriter.create(file, options.blockBytes(), options.bloomBitsPerKey(), wrap);
        try {
            long written = 0;
            byte[] previous = null;
            while (!heap.isEmpty()) {
                Table.Cursor cursor = heap.poll();
                byte[] key = cursor.key();
                Entry entry = cursor.entry();
                if (previous == null || !Arrays.equals(previous, key)) { // older versions of a key come after its newest
                    if (!(dropTombstones && entry.isTombstone())) {
                        writer.add(key, entry);
                        written++;
                    }
                    previous = key;
                }
                if (cursor.next()) {
                    heap.add(cursor);
                }
            }
            if (written == 0) {
                writer.abandon();
                return null;
            }
            writer.finish();
            return Table.open(file, number, wrap);
        } catch (IOException | RuntimeException | Error e) {
            writer.abandon(); // nothing names it yet
            throw e;
        }
    }

    /**
     * What {@link #snapshot} made.
     *
     * @param directory the snapshot: a store directory (its real path)
     * @param lastSequence the last write it holds
     * @param lastChecksum that write's stored checksum, or -1 if unknown
     * @param tables how many tables it holds
     * @param bytes their total size
     * @param linked how many of them are hard links to this store's files; the rest were copied
     */
    public record Snapshot(Path directory, long lastSequence, long lastChecksum, int tables, long bytes, int linked) {}

    /**
     * Writes a copy of the store as it is now into {@code target}, which must not exist: a store
     * directory holding exactly the writes up to the returned sequence number, with that write's
     * checksum, which {@link #open} opens like any other. A follower can start from it.
     *
     * <p>The memtable is flushed first, under the write lock, so that every write is in a table
     * (the flush may run a merge, as {@link #flush} does). Then, outside the lock, while writes go
     * on, the snapshot is built in a new directory beside the target, named
     * {@code .TARGET.snapshot-N}: the tables, which never change once written, are hard-linked in
     * (copied and forced where the file system can't link), the directory is forced, and a
     * manifest naming them is written. The version pinned for this keeps the tables a merge
     * replaces until they are linked. Last, the directory is renamed to the target and the parent
     * forced. So the target, if it exists, is a whole snapshot: a crash leaves at most a
     * {@code .TARGET.snapshot-N} directory, which can be deleted, and a snapshot that fails is
     * deleted.
     *
     * @throws java.nio.file.FileAlreadyExistsException if {@code target} exists (nothing is flushed)
     * @throws IllegalArgumentException if {@code target} would be in this store's own directory
     */
    public Snapshot snapshot(Path target) throws IOException {
        return snapshot(target, true);
    }

    /** {@link #snapshot(Path)}; with {@code link} false, every table is copied, as across file systems. */
    Snapshot snapshot(Path target, boolean link) throws IOException {
        Objects.requireNonNull(target, "target");
        Path absolute = target.toAbsolutePath().normalize();
        if (absolute.getParent() == null || Files.exists(absolute, LinkOption.NOFOLLOW_LINKS)) {
            throw new FileAlreadyExistsException(target.toString(), null, "a snapshot's directory must not exist");
        }
        createDirectory(absolute.getParent(), wrap);
        Path parent = absolute.getParent().toRealPath();
        if (parent.equals(dir)) {
            // Its name could be one the store uses (MANIFEST.tmp, a log's), and the store's next
            // write of that file would fail.
            throw new IllegalArgumentException("a snapshot can't go in the store's own directory: " + target);
        }
        Path destination = parent.resolve(absolute.getFileName());
        Version version;
        long seq;
        long checksum;
        writeLock.lock();
        try {
            ensureWritable();
            flushAndCompact();
            version = pin();
            seq = lastSeq.get();
            checksum = lastChecksum;
            if (version.active.size() != 0 || version.frozen != null || manifest.lastSequence() != seq) {
                version.release();
                throw new IllegalStateException("after a flush, the tables hold " + manifest.lastSequence()
                        + " of " + seq + " writes");
            }
        } finally {
            writeLock.unlock();
        }
        Path building = null; // what to delete if this fails: the directory being built, then the snapshot
        try {
            building = newSnapshotDirectory(parent, absolute.getFileName().toString());
            long bytes = 0;
            int linked = 0;
            long highest = 0;
            for (Table table : version.tables) {
                Path copy = building.resolve(table.file().getFileName());
                if (link && link(copy, table.file())) {
                    linked++;
                } else {
                    Files.copy(table.file(), copy);
                    try (FileChannel channel = wrap.apply(copy, FileChannel.open(copy, StandardOpenOption.WRITE))) {
                        channel.force(false);
                    }
                }
                bytes += table.fileBytes();
                highest = Math.max(highest, table.number());
            }
            WriteAheadLog.forceDirectory(building, wrap); // the tables are in place before a manifest names them
            new Manifest(highest + 2, highest + 1, seq, checksum, refs(version.tables)).write(building, wrap);
            Files.move(building, destination); // rename(2); fails if the target appeared since the check
            building = destination; // until its name is durable too: a failed force still fails the snapshot
            WriteAheadLog.forceDirectory(parent, wrap);
            building = null;
            return new Snapshot(destination, seq, checksum, version.tables.size(), bytes, linked);
        } catch (IOException | RuntimeException | Error e) {
            if (building != null) {
                deleteTree(building, e);
            }
            throw e;
        } finally {
            version.release();
        }
    }

    /** Creates {@code parent/.NAME.snapshot-N} for a snapshot to be built in. */
    private static Path newSnapshotDirectory(Path parent, String name) throws IOException {
        for (int attempt = 0; ; attempt++) {
            Path building = parent.resolve("." + name + ".snapshot-" + (System.nanoTime() + attempt));
            try {
                return Files.createDirectory(building);
            } catch (FileAlreadyExistsException e) {
                if (attempt == 100) {
                    throw e;
                }
            }
        }
    }

    /** Deletes a directory this store made and what is in it; failures are added to {@code cause}. */
    private static void deleteTree(Path building, Throwable cause) {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(building)) {
            for (Path file : files) {
                Files.deleteIfExists(file); // links and copies of tables, MANIFEST.tmp, MANIFEST
            }
            Files.deleteIfExists(building);
        } catch (IOException e) {
            cause.addSuppressed(e);
        }
    }

    /** Hard-links {@code link} to {@code file}; false if this file system can't. */
    private static boolean link(Path link, Path file) throws IOException {
        try {
            Files.createLink(link, file);
            return true;
        } catch (FileAlreadyExistsException e) {
            throw e;
        } catch (UnsupportedOperationException | FileSystemException e) {
            return false; // across file systems, say: copy it
        }
    }

    /**
     * Makes {@code next} the version reads pin, and drops the store's reference to the one before,
     * which closes its tables once the reads still in it are done. Under writeLock.
     */
    private void install(Version next) {
        Version before = current;
        current = next;
        before.release();
    }

    /**
     * The value for {@code key}, or empty if there is none or it was deleted. Takes no lock: it pins
     * the current version and looks in the memtable, the frozen memtable, then each table newest
     * first (a table whose key range or Bloom filter rules the key out is skipped without a read),
     * stopping at the first value or tombstone.
     *
     * @throws CorruptTableException if a table block it reads fails its checks
     */
    public Optional<byte[]> get(byte[] key) throws IOException {
        Objects.requireNonNull(key, "key");
        Version version = pin();
        try {
            Entry entry = version.active.get(key);
            if (entry == null && version.frozen != null) {
                entry = version.frozen.get(key);
            }
            if (entry != null) {
                return entry.isTombstone() ? Optional.empty() : Optional.of(entry.value().clone()); // the memtable's own
            }
            if (version.tables.isEmpty()) {
                return Optional.empty();
            }
            long hash = BloomFilter.hash(key);
            for (Table table : version.tables) {
                Entry found = table.get(key, hash);
                if (found != null) {
                    return found.isTombstone() ? Optional.empty() : Optional.of(found.value()); // read fresh
                }
            }
            return Optional.empty();
        } finally {
            version.release();
        }
    }

    /** The current version, with a reference the caller must release. */
    private Version pin() {
        for (;;) {
            ensureOpen();
            Version version = current;
            if (version.tryRetain()) {
                return version;
            }
            // Released: the store installed a newer version (or closed) since the read above.
        }
    }

    /** What recovery found when the store was opened: the newest log's outcome, records from every log replayed. */
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

    /**
     * The checksum the record {@link #lastSequence()} names was stored with, as an unsigned 32-bit
     * number ({@link WalFormat#storedChecksum}), or -1 if this store doesn't know it: a new store, or
     * one opened with every write in its tables under a day-2 manifest (since day 4 the manifest
     * keeps it, so a flush doesn't lose it). With the sequence number, it identifies the last
     * write well enough for a follower to show its leader that their histories match. Consistent
     * with {@link #lastSequence()} when no write runs at the same time (a follower reads both on the
     * thread that makes its writes).
     */
    public long lastChecksum() {
        return lastChecksum;
    }

    /** The store's directory (its real path). */
    public Path directory() {
        return dir;
    }

    /** Keys in the memtable taking writes, tombstones included. */
    public int memtableSize() {
        return current.active.size();
    }

    /** Bytes in the current log. */
    public long logSize() {
        writeLock.lock();
        try {
            return log.size();
        } finally {
            writeLock.unlock();
        }
    }

    /** The live tables, newest first. */
    public List<TableInfo> tables() {
        Version version = current; // a racy look is fine for statistics: the fields read are final
        List<TableInfo> out = new ArrayList<>();
        for (Table t : version.tables) {
            out.add(new TableInfo(t.number(), t.fileBytes(), t.entries(), t.tombstones(), t.smallestSeq(),
                    t.largestSeq(), t.blocksRead()));
        }
        return out;
    }

    /**
     * What the store has written to tables since it opened.
     *
     * @param flushedBytes bytes of the tables flushes wrote
     * @param mergeInputBytes bytes of the tables merges read
     * @param mergedBytes bytes of the tables merges wrote
     */
    public record Stats(long flushes, long flushedBytes, long merges, long mergeInputBytes, long mergedBytes) {}

    public Stats stats() {
        writeLock.lock();
        try {
            return new Stats(flushes, flushedBytes, merges, mergeInputBytes, mergedBytes);
        } finally {
            writeLock.unlock();
        }
    }

    /** The manifest as last written. */
    Manifest manifest() {
        writeLock.lock();
        try {
            return manifest;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Forces the log, closes it and releases the directory, so that when it returns every
     * acknowledged write is on the device, whatever the sync policy. Waits for a write, flush or
     * compaction in progress. Afterwards {@link #put}, {@link #delete} and {@link #get} fail with
     * {@link IllegalStateException} (the accessors still answer); closing again does nothing. Tables
     * close when the last read still in them finishes.
     *
     * <p>A store whose writes failed: after a failed flush (under {@link SyncPolicy#EVERY_WRITE})
     * every write acknowledged before it was forced on its own, and forcing again proves nothing,
     * so the log is closed as it is. After a failed append the log is still forced, since under
     * {@link SyncPolicy#NEVER} the writes before it need it (the partial record goes with them, and
     * recovery drops it). If the log can't be forced at all (an interrupt closed it, or a failed
     * memtable flush did), under {@code NEVER} this throws {@link StoreFailedException}, since
     * acknowledged writes may not be on the device; the directory is released either way.
     */
    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            signalSequence(); // waiters give up
            try {
                closeLog();
            } finally {
                try {
                    current.release(); // tables close once reads still in them finish
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
        if (rotatedOut) {
            // A flush forced and closed the log, then failed to start the next one. Found by review:
            // this read as a log close couldn't force, and close threw under NEVER for nothing.
            return;
        }
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
