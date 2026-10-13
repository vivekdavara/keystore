package io.github.vivekdavara.keystore.wal;

/**
 * What recovery found in the log.
 *
 * @param outcome whether the log was clean, had a torn tail, or had damage that was dropped
 * @param records records replayed
 * @param lastSeq the sequence number of the last record replayed, or the one before the first
 *     expected if none was
 * @param validBytes bytes of the log kept: the end of the last record replayed
 * @param droppedBytes bytes after that, truncated away
 * @param detail what was wrong, for a log or an operator; empty when clean
 */
public record Recovery(Outcome outcome, long records, long lastSeq, long validBytes, long droppedBytes, String detail) {
    public enum Outcome {
        /** Every byte of the log was a good record. */
        CLEAN,
        /** The log ended in damage with no intact record after it, as a crash leaves it; dropped. */
        TORN_TAIL,
        /** Intact records followed damage, and {@link RecoveryMode#POINT_IN_TIME} dropped them. */
        DAMAGE_DROPPED
    }
}
