package io.github.vivekdavara.keystore.wal;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.wal.WalFormat.Bad;
import io.github.vivekdavara.keystore.wal.WalFormat.Decoded;
import io.github.vivekdavara.keystore.wal.WalFormat.Ok;
import io.github.vivekdavara.keystore.wal.WalFormat.Problem;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class WalFormatTest {
    @Test
    void putsAndDeletesRoundTrip() {
        for (WalRecord record : new WalRecord[] {
            WalRecord.put(1, utf8("k"), utf8("v")),
            WalRecord.put(2, utf8("key"), new byte[0]),
            WalRecord.delete(3, utf8("key")),
            WalRecord.put(Long.MAX_VALUE, new byte[] {0, (byte) 0xff}, new byte[] {0}),
        }) {
            byte[] bytes = bytes(WalFormat.encode(record));
            assertEquals(WalFormat.encodedSize(record), bytes.length);
            assertEquals(new Ok(record, bytes.length), WalFormat.decode(bytes, 0, bytes.length));
        }
    }

    @Test
    void theLargestKeyAndValueRoundTrip() {
        byte[] key = new byte[WalFormat.MAX_KEY_BYTES];
        byte[] value = new byte[WalFormat.MAX_VALUE_BYTES];
        key[0] = 1;
        value[value.length - 1] = 2;
        WalRecord record = WalRecord.put(9, key, value);
        byte[] bytes = bytes(WalFormat.encode(record));
        assertEquals(WalFormat.HEADER_BYTES + WalFormat.MAX_PAYLOAD, bytes.length);
        Ok ok = assertInstanceOf(Ok.class, WalFormat.decode(bytes, 0, bytes.length));
        assertEquals(record, ok.record());
    }

    /** The exact bytes, so an accidental change to the format fails here first. */
    @Test
    void theLayoutIsFixed() {
        byte[] bytes = bytes(WalFormat.encode(WalRecord.put(1, utf8("k"), utf8("v"))));
        byte[] payload = {
            0, 0, 0, 0, 0, 0, 0, 1, // seq
            1, // op: put
            0, 0, 0, 1, 'k', // key
            0, 0, 0, 1, 'v', // value
        };
        byte[] length = {0, 0, 0, 19};
        int crc = Crc32cOracle.of(length, payload);
        ByteBuffer want = ByteBuffer.allocate(8 + payload.length).put(length).putInt(crc).put(payload);
        assertArrayEquals(want.array(), bytes);

        byte[] delete = bytes(WalFormat.encode(WalRecord.delete(2, utf8("k"))));
        assertEquals(WalFormat.HEADER_BYTES + WalFormat.MIN_PAYLOAD, delete.length);
        assertEquals(2, delete[16]);
    }

    @Test
    void theOracleMatchesTheStandardCheckValue() {
        // CRC-32C of "123456789" is e3069283 (RFC 3720, appendix B.4).
        assertEquals(0xe3069283, Crc32cOracle.of(utf8("123456789"), new byte[0]));
    }

    @Test
    void writesOutsideTheLimitsAreRefused() {
        assertThrows(IllegalArgumentException.class, () -> WalFormat.encode(WalRecord.put(1, new byte[0], utf8("v"))));
        assertThrows(IllegalArgumentException.class, () -> WalFormat.encode(
                WalRecord.put(1, new byte[WalFormat.MAX_KEY_BYTES + 1], utf8("v"))));
        assertThrows(IllegalArgumentException.class, () -> WalFormat.encode(
                WalRecord.put(1, utf8("k"), new byte[WalFormat.MAX_VALUE_BYTES + 1])));
        assertThrows(IllegalArgumentException.class, () -> WalFormat.encode(WalRecord.delete(0, utf8("k"))));
        assertThrows(IllegalArgumentException.class, () -> WalFormat.encode(WalRecord.delete(-1, utf8("k"))));
    }

    @Test
    void aShortHeaderIsTruncated() {
        byte[] bytes = bytes(WalFormat.encode(WalRecord.put(1, utf8("k"), utf8("v"))));
        for (int cut = 0; cut < WalFormat.HEADER_BYTES; cut++) {
            assertEquals(Problem.TRUNCATED_HEADER, problem(WalFormat.decode(bytes, 0, cut)), "cut at " + cut);
        }
    }

    @Test
    void aShortPayloadIsTruncated() {
        byte[] bytes = bytes(WalFormat.encode(WalRecord.put(1, utf8("k"), utf8("v"))));
        for (int cut = WalFormat.HEADER_BYTES; cut < bytes.length; cut++) {
            assertEquals(Problem.TRUNCATED_PAYLOAD, problem(WalFormat.decode(bytes, 0, cut)), "cut at " + cut);
        }
    }

    @Test
    void anImplausibleLengthIsRejectedBeforeAnythingIsRead() {
        for (int length : new int[] {0, 13, WalFormat.MAX_PAYLOAD + 1, Integer.MAX_VALUE, -1}) {
            byte[] bytes = new byte[64];
            ByteBuffer.wrap(bytes).putInt(length);
            assertEquals(Problem.BAD_LENGTH, problem(WalFormat.decode(bytes, 0, bytes.length)), "length " + length);
        }
    }

    @Test
    void zerosAreNeverARecord() {
        byte[] zeros = new byte[256];
        for (int offset = 0; offset < zeros.length; offset++) {
            assertInstanceOf(Bad.class, WalFormat.decode(zeros, offset, zeros.length));
            assertFalse(WalFormat.intactAt(zeros, offset, zeros.length));
        }
    }

    @Test
    void everySingleBitFlipIsCaught() {
        byte[] bytes = bytes(WalFormat.encode(WalRecord.put(7, utf8("key"), utf8("value"))));
        for (int bit = 0; bit < bytes.length * 8; bit++) {
            byte[] flipped = bytes.clone();
            flipped[bit / 8] ^= (byte) (1 << (bit % 8));
            assertInstanceOf(Bad.class, WalFormat.decode(flipped, 0, flipped.length), "bit " + bit);
        }
    }

    /** The checksum covers the length: the same payload under another length doesn't pass. */
    @Test
    void theChecksumCoversTheLength() {
        byte[] bytes = bytes(WalFormat.encode(WalRecord.put(1, utf8("k"), utf8("vv"))));
        // Re-seal with a checksum of the payload alone, as a format without the length would.
        byte[] payloadOnly = bytes.clone();
        int crc = Crc32cOracle.of(new byte[0], Arrays.copyOfRange(bytes, 8, bytes.length));
        ByteBuffer.wrap(payloadOnly).putInt(4, crc);
        assertEquals(Problem.BAD_CHECKSUM, problem(WalFormat.decode(payloadOnly, 0, payloadOnly.length)));
    }

    @Test
    void aValidChecksumOverBadFieldsIsABadStructure() {
        byte[] good = bytes(WalFormat.encode(WalRecord.put(1, utf8("k"), utf8("v"))));
        assertStructureProblem(good, b -> b[16] = 3, "unknown op 3");
        assertStructureProblem(good, b -> ByteBuffer.wrap(b).putLong(8, 0), "sequence number 0");
        assertStructureProblem(good, b -> ByteBuffer.wrap(b).putInt(17, 0), "key length 0");
        assertStructureProblem(good, b -> ByteBuffer.wrap(b).putInt(17, 7), "key length 7"); // past the payload
        assertStructureProblem(good, b -> ByteBuffer.wrap(b).putInt(17, 6), "no room for a value length");
        assertStructureProblem(good, b -> ByteBuffer.wrap(b).putInt(22, 0), "value length 0 in 1 bytes");
        assertStructureProblem(good, b -> ByteBuffer.wrap(b).putInt(22, -1), "value length 4294967295 in 1 bytes");
        assertStructureProblem(good, b -> b[16] = 2, "5 bytes after a delete's key"); // a put relabelled
        // A delete relabelled as a put has no room for a value length.
        byte[] delete = bytes(WalFormat.encode(WalRecord.delete(1, utf8("k"))));
        assertStructureProblem(delete, b -> b[16] = 1, "no room for a value length");
    }

    /** intactAt is a fast path for the scan; it must agree with decode at every offset. */
    @Test
    void intactAtAgreesWithDecodeOnDamagedLogs() {
        SplittableRandom random = new SplittableRandom(3);
        for (int round = 0; round < 200; round++) {
            ByteBuffer log = ByteBuffer.allocate(4096);
            while (log.remaining() > 200) {
                if (random.nextInt(4) == 0) {
                    byte[] junk = new byte[random.nextInt(1, 40)];
                    random.nextBytes(junk);
                    log.put(junk);
                } else {
                    byte[] value = new byte[random.nextInt(0, 60)];
                    random.nextBytes(value);
                    log.put(WalFormat.encode(WalRecord.put(random.nextLong(1, 1000), utf8("k" + round), value)));
                }
            }
            byte[] bytes = log.array();
            int end = log.position();
            for (int offset = 0; offset < end; offset++) {
                assertEquals(
                        WalFormat.decode(bytes, offset, end) instanceof Ok,
                        WalFormat.intactAt(bytes, offset, end),
                        "round " + round + ", offset " + offset);
            }
        }
    }

    @Test
    void decodingStartsAtTheGivenOffset() {
        byte[] first = bytes(WalFormat.encode(WalRecord.put(1, utf8("a"), utf8("1"))));
        byte[] second = bytes(WalFormat.encode(WalRecord.delete(2, utf8("b"))));
        byte[] log = ByteBuffer.allocate(first.length + second.length).put(first).put(second).array();
        Ok one = assertInstanceOf(Ok.class, WalFormat.decode(log, 0, log.length));
        assertEquals(first.length, one.next());
        Ok two = assertInstanceOf(Ok.class, WalFormat.decode(log, one.next(), log.length));
        assertEquals(WalRecord.delete(2, utf8("b")), two.record());
        assertEquals(log.length, two.next());
        assertTrue(WalFormat.intactAt(log, first.length, log.length));
    }

    private static void assertStructureProblem(byte[] good, Consumer<byte[]> damage, String detail) {
        byte[] bytes = good.clone();
        damage.accept(bytes);
        Crc32cOracle.reseal(bytes);
        Decoded decoded = WalFormat.decode(bytes, 0, bytes.length);
        Bad bad = assertInstanceOf(Bad.class, decoded);
        assertEquals(Problem.BAD_STRUCTURE, bad.problem());
        assertEquals(detail, bad.detail());
        assertFalse(WalFormat.intactAt(bytes, 0, bytes.length));
    }

    private static Problem problem(Decoded decoded) {
        return assertInstanceOf(Bad.class, decoded).problem();
    }

    private static byte[] bytes(ByteBuffer buffer) {
        byte[] out = new byte[buffer.remaining()];
        buffer.get(out);
        return out;
    }
}
