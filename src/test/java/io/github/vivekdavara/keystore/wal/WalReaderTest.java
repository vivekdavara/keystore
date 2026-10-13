package io.github.vivekdavara.keystore.wal;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.wal.Recovery.Outcome;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class WalReaderTest {
    /** Records of different shapes and sizes, seq 1..6. */
    private static final List<WalRecord> RECORDS = List.of(
            WalRecord.put(1, utf8("alpha"), utf8("one")),
            WalRecord.put(2, utf8("beta"), new byte[0]),
            WalRecord.delete(3, utf8("alpha")),
            WalRecord.put(4, utf8("gamma"), new byte[300]),
            WalRecord.put(5, new byte[] {(byte) 0x80, 0}, utf8("binary key")),
            WalRecord.delete(6, utf8("d")));

    @Test
    void aCleanLogReplaysEveryRecord() throws Exception {
        Log log = Log.of(RECORDS);
        List<WalRecord> applied = new ArrayList<>();
        Recovery recovery = WalReader.read(log.bytes, log.bytes.length, 1, RecoveryMode.TOLERATE_TORN_TAIL, applied::add);
        assertEquals(RECORDS, applied);
        assertEquals(new Recovery(Outcome.CLEAN, 6, 6, log.bytes.length, 0, ""), recovery);
    }

    @Test
    void anEmptyLogIsClean() throws Exception {
        Recovery recovery = WalReader.read(new byte[0], 0, 1, RecoveryMode.TOLERATE_TORN_TAIL, r -> {
            throw new AssertionError("nothing to apply");
        });
        assertEquals(new Recovery(Outcome.CLEAN, 0, 0, 0, 0, ""), recovery);
    }

    /**
     * A crash can cut the log anywhere. At every cut, recovery replays exactly the records that ended
     * before it, keeps their bytes, drops the rest, and never refuses to open.
     */
    @Test
    void aCutAtEveryByteKeepsExactlyTheWholeRecordsBeforeIt() throws Exception {
        Log log = Log.of(RECORDS);
        for (int cut = 0; cut <= log.bytes.length; cut++) {
            int whole = log.wholeRecordsBefore(cut);
            for (RecoveryMode mode : RecoveryMode.values()) {
                List<WalRecord> applied = new ArrayList<>();
                Recovery recovery = WalReader.read(log.bytes, cut, 1, mode, applied::add);
                assertEquals(RECORDS.subList(0, whole), applied, "cut " + cut);
                assertEquals(log.ends[whole], recovery.validBytes(), "cut " + cut);
                assertEquals(cut - log.ends[whole], recovery.droppedBytes(), "cut " + cut);
                assertEquals(whole, recovery.lastSeq());
                assertEquals(cut == log.ends[whole] ? Outcome.CLEAN : Outcome.TORN_TAIL, recovery.outcome(), "cut " + cut);
            }
        }
    }

    /** Any bit of the last record flipped: it can't be told from a torn write, so it is dropped as one. */
    @Test
    void aFlipInTheLastRecordIsATornTail() throws Exception {
        Log log = Log.of(RECORDS);
        int last = RECORDS.size() - 1;
        for (int bit = log.ends[last] * 8; bit < log.bytes.length * 8; bit++) {
            byte[] bytes = flip(log.bytes, bit);
            List<WalRecord> applied = new ArrayList<>();
            Recovery recovery = WalReader.read(bytes, bytes.length, 1, RecoveryMode.TOLERATE_TORN_TAIL, applied::add);
            assertEquals(Outcome.TORN_TAIL, recovery.outcome(), "bit " + bit);
            assertEquals(RECORDS.subList(0, last), applied, "bit " + bit);
            assertEquals(log.ends[last], recovery.validBytes());
        }
    }

    /**
     * Any bit of an earlier record flipped: the records after it are intact, so the default refuses
     * to open, naming the damaged record's offset, and POINT_IN_TIME opens without them.
     */
    @Test
    void aFlipInAnEarlierRecordIsDamageInTheMiddle() throws Exception {
        Log log = Log.of(RECORDS);
        for (int record = 0; record < RECORDS.size() - 1; record++) {
            for (int bit = log.ends[record] * 8; bit < log.ends[record + 1] * 8; bit++) {
                byte[] bytes = flip(log.bytes, bit);
                WalCorruptionException refused = assertThrows(
                        WalCorruptionException.class,
                        () -> WalReader.read(bytes, bytes.length, 1, RecoveryMode.TOLERATE_TORN_TAIL, r -> {}),
                        "record " + record + ", bit " + bit);
                assertEquals(log.ends[record], refused.offset());

                List<WalRecord> applied = new ArrayList<>();
                Recovery recovery = WalReader.read(bytes, bytes.length, 1, RecoveryMode.POINT_IN_TIME, applied::add);
                assertEquals(Outcome.DAMAGE_DROPPED, recovery.outcome());
                assertEquals(RECORDS.subList(0, record), applied);
                assertEquals(log.ends[record], recovery.validBytes());
                assertEquals(bytes.length - log.ends[record], recovery.droppedBytes());
            }
        }
    }

    @Test
    void theMessageSaysWhatWasFoundAndWhatToDo() {
        Log log = Log.of(RECORDS);
        byte[] bytes = flip(log.bytes, (log.ends[2] + 9) * 8); // a payload byte of record #3
        WalCorruptionException refused = assertThrows(
                WalCorruptionException.class,
                () -> WalReader.read(bytes, bytes.length, 1, RecoveryMode.TOLERATE_TORN_TAIL, r -> {}));
        String message = refused.getMessage();
        assertTrue(message.startsWith("record at offset " + log.ends[2] + ": BAD_CHECKSUM"), message);
        assertTrue(message.contains("an intact record (#4) starts at offset " + log.ends[3]), message);
        assertTrue(message.contains("POINT_IN_TIME"), message);
    }

    /** A crash can leave the file longer than its data; the missing part reads as zeros. */
    @Test
    void aZeroFilledTailIsTorn() throws Exception {
        Log log = Log.of(RECORDS);
        byte[] bytes = Arrays.copyOf(log.bytes, log.bytes.length + 65_536);
        Recovery recovery = WalReader.read(bytes, bytes.length, 1, RecoveryMode.TOLERATE_TORN_TAIL, r -> {});
        assertEquals(Outcome.TORN_TAIL, recovery.outcome());
        assertEquals(6, recovery.records());
        assertEquals(log.bytes.length, recovery.validBytes());
        assertEquals(65_536, recovery.droppedBytes());
        assertTrue(recovery.detail().contains("BAD_LENGTH"), recovery.detail());
    }

    @Test
    void aRandomGarbageTailIsTorn() throws Exception {
        Log log = Log.of(RECORDS);
        SplittableRandom random = new SplittableRandom(5);
        for (int round = 0; round < 50; round++) {
            byte[] garbage = new byte[random.nextInt(1, 20_000)];
            random.nextBytes(garbage);
            byte[] bytes = ByteBuffer.allocate(log.bytes.length + garbage.length).put(log.bytes).put(garbage).array();
            Recovery recovery = WalReader.read(bytes, bytes.length, 1, RecoveryMode.TOLERATE_TORN_TAIL, r -> {});
            assertEquals(Outcome.TORN_TAIL, recovery.outcome(), "round " + round);
            assertEquals(log.bytes.length, recovery.validBytes());
        }
    }

    /** Intact bytes in the wrong place (a stale copy, say) are caught by their sequence number. */
    @Test
    void aRecordOutOfSequenceIsDamageEvenIfItIsIntact() throws Exception {
        List<WalRecord> gap = new ArrayList<>(RECORDS.subList(0, 2));
        gap.add(WalRecord.put(4, utf8("skipped"), utf8("3"))); // #3 is missing
        Log log = Log.of(gap);
        WalCorruptionException refused = assertThrows(
                WalCorruptionException.class,
                () -> WalReader.read(log.bytes, log.bytes.length, 1, RecoveryMode.TOLERATE_TORN_TAIL, r -> {}));
        assertEquals(log.ends[2], refused.offset());
        assertTrue(refused.getMessage().contains("OUT_OF_SEQUENCE (expected 3, found 4)"), refused.getMessage());
        assertTrue(refused.getMessage().contains("the record itself is intact"), refused.getMessage());

        Recovery recovery = WalReader.read(log.bytes, log.bytes.length, 1, RecoveryMode.POINT_IN_TIME, r -> {});
        assertEquals(Outcome.DAMAGE_DROPPED, recovery.outcome());
        assertEquals(2, recovery.records());
        assertEquals(2, recovery.lastSeq());
    }

    @Test
    void aRepeatedSequenceNumberIsDamage() {
        Log log = Log.of(List.of(RECORDS.get(0), RECORDS.get(1), WalRecord.put(2, utf8("again"), utf8("x"))));
        assertThrows(
                WalCorruptionException.class,
                () -> WalReader.read(log.bytes, log.bytes.length, 1, RecoveryMode.TOLERATE_TORN_TAIL, r -> {}));
    }

    @Test
    void theFirstRecordMustHaveTheFirstSequenceNumber() throws Exception {
        Log log = Log.of(RECORDS);
        assertThrows(
                WalCorruptionException.class,
                () -> WalReader.read(log.bytes, log.bytes.length, 2, RecoveryMode.TOLERATE_TORN_TAIL, r -> {}));
        Recovery recovery = WalReader.read(log.bytes, log.bytes.length, 2, RecoveryMode.POINT_IN_TIME, r -> {});
        assertEquals(new Recovery(Outcome.DAMAGE_DROPPED, 0, 1, 0, log.bytes.length,
                recovery.detail()), recovery);
    }

    /**
     * The documented false refusal: the torn last record's value holds an encoded log record, which
     * the scan finds intact. The default refuses an ordinary crash; POINT_IN_TIME opens it. This is
     * the price of never mistaking real damage for a torn tail.
     */
    @Test
    void aValueHoldingAnEncodedRecordMakesATornTailLookLikeDamage() throws Exception {
        WalRecord inner = WalRecord.put(99, utf8("inner"), utf8("looks real"));
        byte[] innerBytes = WalFormat.encode(inner).array();
        // The value is the encoded record and then a few more bytes, so a cut at the end leaves the
        // inner record whole.
        byte[] value = ByteBuffer.allocate(innerBytes.length + 4).put(innerBytes).put(utf8("tail")).array();
        List<WalRecord> records = new ArrayList<>(RECORDS);
        records.add(WalRecord.put(7, utf8("carrier"), value));
        Log log = Log.of(records);
        byte[] torn = Arrays.copyOf(log.bytes, log.bytes.length - 1); // cut the last byte
        assertThrows(
                WalCorruptionException.class,
                () -> WalReader.read(torn, torn.length, 1, RecoveryMode.TOLERATE_TORN_TAIL, r -> {}));
        Recovery recovery = WalReader.read(torn, torn.length, 1, RecoveryMode.POINT_IN_TIME, r -> {});
        assertEquals(6, recovery.records());
        assertEquals(log.ends[6], recovery.validBytes());
    }

    @Test
    void theScanFindsTheFirstIntactRecordOnly() {
        Log log = Log.of(RECORDS);
        assertEquals(log.ends[1], WalReader.findIntactRecord(log.bytes, 1, log.bytes.length));
        assertEquals(-1, WalReader.findIntactRecord(log.bytes, log.ends[5] + 1, log.bytes.length));
        assertEquals(-1, WalReader.findIntactRecord(new byte[1 << 20], 0, 1 << 20));
    }

    private static byte[] flip(byte[] bytes, int bit) {
        byte[] out = bytes.clone();
        out[bit / 8] ^= (byte) (1 << (bit % 8));
        return out;
    }

    /** A log built from records, with where each record ends ({@code ends[i]} = start of record i). */
    private record Log(byte[] bytes, int[] ends) {
        static Log of(List<WalRecord> records) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int[] ends = new int[records.size() + 1];
            for (int i = 0; i < records.size(); i++) {
                out.writeBytes(WalFormat.encode(records.get(i)).array());
                ends[i + 1] = out.size();
            }
            return new Log(out.toByteArray(), ends);
        }

        /** How many records end at or before {@code cut}. */
        int wholeRecordsBefore(int cut) {
            int whole = 0;
            while (whole + 1 < ends.length && ends[whole + 1] <= cut) {
                whole++;
            }
            return whole;
        }
    }
}
