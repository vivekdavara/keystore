package io.github.vivekdavara.keystore.wal;

import io.github.vivekdavara.keystore.wal.WalFormat.Bad;
import io.github.vivekdavara.keystore.wal.WalFormat.Decoded;
import io.github.vivekdavara.keystore.wal.WalFormat.Ok;
import java.util.function.Consumer;

/**
 * Replays a log with the recovery rules (DESIGN.md, "Recovery"): apply the longest run of good
 * records from the start, then decide what the rest is.
 */
public final class WalReader {
    private WalReader() {}

    /**
     * Reads the records in {@code log[0, end)}, passing each good one to {@code apply} in order. A
     * record is good if it decodes and its sequence number is one more than the previous record's
     * ({@code firstSeq} for the first).
     *
     * <p>Reading stops at the first record that isn't good. If no intact record starts anywhere
     * after it, the rest is a torn tail. Otherwise the log is damaged in the middle: under
     * {@link RecoveryMode#TOLERATE_TORN_TAIL} this throws, under {@link RecoveryMode#POINT_IN_TIME}
     * the rest is dropped. Either way the caller truncates the log to
     * {@link Recovery#validBytes()}. If this throws, records already applied must be discarded.
     *
     * @throws WalCorruptionException if the log is damaged in the middle and the mode refuses that
     */
    public static Recovery read(byte[] log, int end, long firstSeq, RecoveryMode mode, Consumer<WalRecord> apply)
            throws WalCorruptionException {
        long expected = firstSeq;
        long records = 0;
        int offset = 0;
        String problem = null;
        boolean badRecordIsIntact = false;
        while (offset < end) {
            Decoded decoded = WalFormat.decode(log, offset, end);
            if (decoded instanceof Bad bad) {
                problem = bad.problem() + " (" + bad.detail() + ")";
                break;
            }
            Ok ok = (Ok) decoded;
            if (ok.record().seq() != expected) {
                problem = "OUT_OF_SEQUENCE (expected " + expected + ", found " + ok.record().seq() + ")";
                badRecordIsIntact = true;
                break;
            }
            apply.accept(ok.record());
            records++;
            expected++;
            offset = ok.next();
        }
        long lastSeq = expected - 1;
        if (problem == null) {
            return new Recovery(Recovery.Outcome.CLEAN, records, lastSeq, end, 0, "");
        }

        String damage = "record at offset " + offset + ": " + problem;
        int intact = badRecordIsIntact ? offset : findIntactRecord(log, offset + 1, end);
        if (intact < 0) {
            return new Recovery(Recovery.Outcome.TORN_TAIL, records, lastSeq, offset, end - offset, damage);
        }
        String found = intact == offset
                ? "the record itself is intact"
                : "an intact record (#" + ((Ok) WalFormat.decode(log, intact, end)).record().seq()
                        + ") starts at offset " + intact;
        String message = damage + "; " + found + ", so the log was written past the damage";
        if (mode == RecoveryMode.POINT_IN_TIME) {
            return new Recovery(Recovery.Outcome.DAMAGE_DROPPED, records, lastSeq, offset, end - offset, message);
        }
        throw new WalCorruptionException(
                message + ". Dropping it would lose those writes; open with POINT_IN_TIME to drop them.",
                offset);
    }

    /** The first offset in {@code [from, end)} where an intact record starts, or -1. */
    static int findIntactRecord(byte[] log, int from, int end) {
        for (int offset = from; offset < end; offset++) {
            if (WalFormat.intactAt(log, offset, end)) {
                return offset;
            }
        }
        return -1;
    }
}
