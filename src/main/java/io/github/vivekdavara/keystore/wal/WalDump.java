package io.github.vivekdavara.keystore.wal;

import io.github.vivekdavara.keystore.Bytes;
import io.github.vivekdavara.keystore.wal.WalFormat.Bad;
import io.github.vivekdavara.keystore.wal.WalFormat.Decoded;
import io.github.vivekdavara.keystore.wal.WalFormat.Ok;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A read-only view of a log for an operator: every record with its offset, what recovery would do,
 * and, past any damage, the intact records recovery would drop. Never changes the file.
 */
public final class WalDump {
    private WalDump() {}

    /** What the default recovery mode would do with the log. */
    public enum Verdict {
        CLEAN,
        TORN_TAIL,
        /** The default refuses to open; POINT_IN_TIME would drop the records listed after the damage. */
        REFUSED
    }

    /** What the dump found: the verdict, and the last good record's sequence number. */
    public record Result(Verdict verdict, long lastSeq) {}

    /**
     * Prints the log at {@code file}, whose first record should be number {@code firstSeq}, and
     * what recovery would do with it.
     */
    public static Result dump(Path file, long firstSeq, PrintStream out) throws IOException {
        byte[] log = Files.readAllBytes(file);
        out.printf("%s: %d bytes%n", file, log.length);
        Verdict verdict;
        long kept;
        long lastSeq = firstSeq - 1;
        String refusal = "";
        try {
            Recovery recovery = WalReader.read(log, log.length, firstSeq, RecoveryMode.TOLERATE_TORN_TAIL, r -> {});
            kept = recovery.validBytes();
            lastSeq = recovery.lastSeq();
            verdict = recovery.outcome() == Recovery.Outcome.CLEAN ? Verdict.CLEAN : Verdict.TORN_TAIL;
        } catch (WalCorruptionException e) {
            kept = e.offset();
            verdict = Verdict.REFUSED;
            refusal = e.getMessage();
        }

        int offset = 0;
        while (offset < kept) {
            Ok ok = (Ok) WalFormat.decode(log, offset, log.length);
            out.printf("%10d  %s%n", offset, describe(ok.record()));
            offset = ok.next();
            lastSeq = ok.record().seq();
        }
        switch (verdict) {
            case CLEAN -> out.printf("clean: %d bytes, every one a good record%n", log.length);
            case TORN_TAIL -> out.printf(
                    "torn tail: recovery drops %d bytes from offset %d (%s), and nothing intact follows%n",
                    log.length - kept, kept, problemAt(log, (int) kept));
            case REFUSED -> {
                out.printf("refused: %s%n", refusal);
                out.printf("damage at offset %d (%s); intact records after it:%n", kept, problemAt(log, (int) kept));
                int dropped = listIntactAfter(log, (int) kept, out);
                out.printf(
                        "the default mode refuses to open this log; POINT_IN_TIME would keep %d bytes and drop"
                                + " %d bytes, including the %d intact records above%n",
                        kept, log.length - kept, dropped);
            }
        }
        return new Result(verdict, lastSeq);
    }

    /** Prints the intact records after {@code from}, scanning past damage; returns how many. */
    private static int listIntactAfter(byte[] log, int from, PrintStream out) {
        int count = 0;
        int offset = WalFormat.decode(log, from, log.length) instanceof Ok ? from : WalReader.findIntactRecord(log, from + 1, log.length);
        while (offset >= 0) { // NONE or UNDECIDED end the listing
            Decoded decoded = WalFormat.decode(log, offset, log.length);
            if (decoded instanceof Ok ok) {
                out.printf("%10d  %s%n", offset, describe(ok.record()));
                count++;
                offset = ok.next() < log.length ? ok.next() : -1;
            } else {
                out.printf("%10d  (damaged: %s)%n", offset, ((Bad) decoded).problem());
                offset = WalReader.findIntactRecord(log, offset + 1, log.length);
            }
        }
        return count;
    }

    private static String problemAt(byte[] log, int offset) {
        Decoded decoded = WalFormat.decode(log, offset, log.length);
        if (decoded instanceof Bad bad) {
            return bad.problem() + ", " + bad.detail();
        }
        return "intact but out of sequence: #" + ((Ok) decoded).record().seq();
    }

    private static String describe(WalRecord record) {
        return switch (record.op()) {
            case PUT -> String.format("#%d put %s (%d bytes)", record.seq(), Bytes.show(record.key()), record.value().length);
            case DELETE -> String.format("#%d delete %s", record.seq(), Bytes.show(record.key()));
        };
    }
}
