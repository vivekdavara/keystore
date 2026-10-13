package io.github.vivekdavara.keystore;

import io.github.vivekdavara.keystore.wal.RecoveryMode;
import java.util.Objects;

/**
 * How a store is opened.
 *
 * @param sync when the log is forced to the device
 * @param recovery what recovery does with damage in the middle of the log
 */
public record Options(SyncPolicy sync, RecoveryMode recovery) {
    public Options {
        Objects.requireNonNull(sync, "sync");
        Objects.requireNonNull(recovery, "recovery");
    }

    /** {@link SyncPolicy#EVERY_WRITE} and {@link RecoveryMode#TOLERATE_TORN_TAIL}. */
    public static Options defaults() {
        return new Options(SyncPolicy.EVERY_WRITE, RecoveryMode.TOLERATE_TORN_TAIL);
    }

    public Options withSync(SyncPolicy sync) {
        return new Options(sync, recovery);
    }

    public Options withRecovery(RecoveryMode recovery) {
        return new Options(sync, recovery);
    }
}
