package io.github.vivekdavara.keystore;

import io.github.vivekdavara.keystore.wal.RecoveryMode;
import io.github.vivekdavara.keystore.wal.WriteAheadLog;
import java.util.Objects;

/**
 * How a store is opened.
 *
 * @param sync when the log is forced to the device
 * @param recovery what recovery does with damage in the middle of the log
 * @param maxLogBytes the largest a log may grow; a write past it fails with
 *     {@link io.github.vivekdavara.keystore.wal.LogFullException} and writes nothing. At most what
 *     recovery can read back, {@link WriteAheadLog#MAX_RECOVERABLE_BYTES}, so the store never
 *     acknowledges a write it couldn't recover. A flush starts a new log long before this, unless
 *     it is set below {@code flushLogBytes}.
 * @param flushLogBytes when the log reaches this size, the next write first flushes the memtable to
 *     a table and starts a new log (4 MiB by default)
 * @param blockBytes the size a table's data blocks are cut at (4 KiB by default)
 * @param bloomBitsPerKey the Bloom filter's bits a key (10 by default: about a 1% false-positive
 *     rate)
 * @param mergeWidth how many tables of a similar size, next to each other in age, a compaction
 *     merges (4 by default)
 * @param retainLogBytes how much log to keep once the tables cover it, for followers to catch up
 *     from: after a flush, and when the store opens, the newest covered logs are kept until they
 *     total at least this many bytes, and older ones are deleted (0 by default: none are kept). A
 *     leader needs some, or a follower that is only a moment behind finds its next record gone.
 */
public record Options(
        SyncPolicy sync, RecoveryMode recovery, long maxLogBytes, long flushLogBytes, int blockBytes,
        int bloomBitsPerKey, int mergeWidth, long retainLogBytes) {
    public Options {
        Objects.requireNonNull(sync, "sync");
        Objects.requireNonNull(recovery, "recovery");
        if (maxLogBytes < 1 || maxLogBytes > WriteAheadLog.MAX_RECOVERABLE_BYTES) {
            throw new IllegalArgumentException(
                    "maxLogBytes must be 1 to " + WriteAheadLog.MAX_RECOVERABLE_BYTES + ", not " + maxLogBytes);
        }
        if (flushLogBytes < 1) {
            throw new IllegalArgumentException("flushLogBytes must be at least 1, not " + flushLogBytes);
        }
        if (blockBytes < 64 || blockBytes > (1 << 24)) {
            throw new IllegalArgumentException("blockBytes must be 64 to 16 MiB, not " + blockBytes);
        }
        if (bloomBitsPerKey < 1 || bloomBitsPerKey > 64) {
            throw new IllegalArgumentException("bloomBitsPerKey must be 1 to 64, not " + bloomBitsPerKey);
        }
        if (mergeWidth < 2 || mergeWidth > 64) {
            throw new IllegalArgumentException("mergeWidth must be 2 to 64, not " + mergeWidth);
        }
        if (retainLogBytes < 0) {
            throw new IllegalArgumentException("retainLogBytes must be at least 0, not " + retainLogBytes);
        }
    }

    /**
     * {@link SyncPolicy#EVERY_WRITE}, {@link RecoveryMode#TOLERATE_TORN_TAIL}, the largest log
     * recovery can read, a flush every 4 MiB of log, 4 KiB blocks, 10 bits a key, merges of 4, no
     * logs kept once covered.
     */
    public static Options defaults() {
        return new Options(SyncPolicy.EVERY_WRITE, RecoveryMode.TOLERATE_TORN_TAIL, WriteAheadLog.MAX_RECOVERABLE_BYTES,
                4L << 20, 4096, 10, 4, 0);
    }

    public Options withSync(SyncPolicy sync) {
        return new Options(sync, recovery, maxLogBytes, flushLogBytes, blockBytes, bloomBitsPerKey, mergeWidth, retainLogBytes);
    }

    public Options withRecovery(RecoveryMode recovery) {
        return new Options(sync, recovery, maxLogBytes, flushLogBytes, blockBytes, bloomBitsPerKey, mergeWidth, retainLogBytes);
    }

    public Options withMaxLogBytes(long maxLogBytes) {
        return new Options(sync, recovery, maxLogBytes, flushLogBytes, blockBytes, bloomBitsPerKey, mergeWidth, retainLogBytes);
    }

    public Options withFlushLogBytes(long flushLogBytes) {
        return new Options(sync, recovery, maxLogBytes, flushLogBytes, blockBytes, bloomBitsPerKey, mergeWidth, retainLogBytes);
    }

    public Options withBlockBytes(int blockBytes) {
        return new Options(sync, recovery, maxLogBytes, flushLogBytes, blockBytes, bloomBitsPerKey, mergeWidth, retainLogBytes);
    }

    public Options withBloomBitsPerKey(int bloomBitsPerKey) {
        return new Options(sync, recovery, maxLogBytes, flushLogBytes, blockBytes, bloomBitsPerKey, mergeWidth, retainLogBytes);
    }

    public Options withMergeWidth(int mergeWidth) {
        return new Options(sync, recovery, maxLogBytes, flushLogBytes, blockBytes, bloomBitsPerKey, mergeWidth, retainLogBytes);
    }

    public Options withRetainLogBytes(long retainLogBytes) {
        return new Options(sync, recovery, maxLogBytes, flushLogBytes, blockBytes, bloomBitsPerKey, mergeWidth, retainLogBytes);
    }
}
