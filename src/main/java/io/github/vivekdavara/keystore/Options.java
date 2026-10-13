package io.github.vivekdavara.keystore;

import io.github.vivekdavara.keystore.wal.RecoveryMode;
import io.github.vivekdavara.keystore.wal.WriteAheadLog;
import java.util.Objects;

/**
 * How a store is opened.
 *
 * @param sync when the log is forced to the device
 * @param recovery what recovery does with damage in the middle of the log
 * @param maxLogBytes the largest the log may grow; a write past it fails with
 *     {@link io.github.vivekdavara.keystore.wal.LogFullException} and writes nothing. At most what
 *     recovery can read back, {@link WriteAheadLog#MAX_RECOVERABLE_BYTES}, so the store never
 *     acknowledges a write it couldn't recover.
 */
public record Options(SyncPolicy sync, RecoveryMode recovery, long maxLogBytes) {
    public Options {
        Objects.requireNonNull(sync, "sync");
        Objects.requireNonNull(recovery, "recovery");
        if (maxLogBytes < 1 || maxLogBytes > WriteAheadLog.MAX_RECOVERABLE_BYTES) {
            throw new IllegalArgumentException(
                    "maxLogBytes must be 1 to " + WriteAheadLog.MAX_RECOVERABLE_BYTES + ", not " + maxLogBytes);
        }
    }

    /**
     * {@link SyncPolicy#EVERY_WRITE}, {@link RecoveryMode#TOLERATE_TORN_TAIL}, and the largest log
     * recovery can read.
     */
    public static Options defaults() {
        return new Options(SyncPolicy.EVERY_WRITE, RecoveryMode.TOLERATE_TORN_TAIL, WriteAheadLog.MAX_RECOVERABLE_BYTES);
    }

    public Options withSync(SyncPolicy sync) {
        return new Options(sync, recovery, maxLogBytes);
    }

    public Options withRecovery(RecoveryMode recovery) {
        return new Options(sync, recovery, maxLogBytes);
    }

    public Options withMaxLogBytes(long maxLogBytes) {
        return new Options(sync, recovery, maxLogBytes);
    }
}
