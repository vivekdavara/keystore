package io.github.vivekdavara.keystore.wal;

/** What recovery does when the log is damaged somewhere other than its tail. */
public enum RecoveryMode {
    /**
     * Drop a torn tail (damage with no intact record after it, which is what a crash leaves), but
     * refuse to open if an intact record follows the damage, since stopping there would lose writes
     * made after it. The default.
     */
    TOLERATE_TORN_TAIL,

    /**
     * Stop at the first damage of any kind, open with the state as of the last good record, and
     * drop the rest of the log, reporting what was dropped. Use it with {@code SyncPolicy.NEVER},
     * where a power loss can tear several records, or to open a log the default refused.
     */
    POINT_IN_TIME
}
