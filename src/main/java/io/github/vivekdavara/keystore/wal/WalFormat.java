package io.github.vivekdavara.keystore.wal;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32C;

/**
 * The log's record format. All integers are big-endian.
 *
 * <pre>
 * offset  size  field
 * 0       4     length: payload bytes, n
 * 4       4     checksum: CRC32C of the 4 length bytes, then the n payload bytes
 * 8       8     seq
 * 16      1     op: 1 = put, 2 = delete
 * 17      4     key length k, 1..65,536
 * 21      k     key
 * 21+k    4     value length v, 0..16,777,216 (put only)
 * 25+k    v     value (put only)
 * </pre>
 */
public final class WalFormat {
    private WalFormat() {}

    public static final int MAX_KEY_BYTES = 65_536;
    public static final int MAX_VALUE_BYTES = 16 * 1024 * 1024;

    /** The length and the checksum. */
    public static final int HEADER_BYTES = 8;

    /** The smallest payload: a delete with a 1-byte key. */
    public static final int MIN_PAYLOAD = 8 + 1 + 4 + 1;

    /** The largest payload: a put with the largest key and value. */
    public static final int MAX_PAYLOAD = 8 + 1 + 4 + MAX_KEY_BYTES + 4 + MAX_VALUE_BYTES;

    private static final int SEQ_AT = HEADER_BYTES;
    private static final int OP_AT = SEQ_AT + 8;
    private static final int KEY_LENGTH_AT = OP_AT + 1;
    private static final int KEY_AT = KEY_LENGTH_AT + 4;

    /** Why the bytes at an offset are not a record. */
    public enum Problem {
        /** Fewer than 8 bytes left: the header was cut off. */
        TRUNCATED_HEADER,
        /** The length field is outside 14..MAX_PAYLOAD. */
        BAD_LENGTH,
        /** The length is plausible but runs past the end of the log. */
        TRUNCATED_PAYLOAD,
        /** The checksum doesn't match. */
        BAD_CHECKSUM,
        /** The checksum matches, but the fields don't make a record. */
        BAD_STRUCTURE
    }

    /** What decoding at an offset found: a record and where the next one starts, or a problem. */
    public sealed interface Decoded permits Ok, Bad {}

    public record Ok(WalRecord record, int next) implements Decoded {}

    public record Bad(Problem problem, String detail) implements Decoded {}

    /** The bytes a record takes in the log, header included. */
    public static int encodedSize(WalRecord record) {
        return HEADER_BYTES + payloadSize(record);
    }

    private static int payloadSize(WalRecord record) {
        int size = 8 + 1 + 4 + record.key().length;
        if (record.op() == WalRecord.Op.PUT) {
            size += 4 + record.value().length;
        }
        return size;
    }

    /**
     * Encodes a record, ready to write (position 0, limit at the end).
     *
     * @throws IllegalArgumentException if the sequence number is below 1 or the key or value is
     *     outside the limits
     */
    public static ByteBuffer encode(WalRecord record) {
        checkLimits(record.seq(), record.key(), record.value());
        int payload = payloadSize(record);
        ByteBuffer out = ByteBuffer.allocate(HEADER_BYTES + payload);
        out.putInt(payload);
        out.putInt(0); // the checksum, filled in below
        out.putLong(record.seq());
        out.put(record.op().code);
        out.putInt(record.key().length);
        out.put(record.key());
        if (record.op() == WalRecord.Op.PUT) {
            out.putInt(record.value().length);
            out.put(record.value());
        }
        out.putInt(4, checksum(out.array(), 0, payload));
        return out.flip();
    }

    /**
     * Checks a write against the limits.
     *
     * @param value null for a delete
     */
    public static void checkLimits(long seq, byte[] key, byte[] value) {
        if (seq < 1) {
            throw new IllegalArgumentException("sequence numbers start at 1: " + seq);
        }
        if (key.length < 1 || key.length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException(
                    "a key must be 1 to " + MAX_KEY_BYTES + " bytes, not " + key.length);
        }
        if (value != null && value.length > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException(
                    "a value must be at most " + MAX_VALUE_BYTES + " bytes, not " + value.length);
        }
    }

    /**
     * Decodes the record that starts at {@code offset} in {@code log[0, end)}. It checks, in order:
     * a whole header, a plausible length, a whole payload, the checksum, then the structure.
     */
    public static Decoded decode(byte[] log, int offset, int end) {
        if (end - offset < HEADER_BYTES) {
            return new Bad(Problem.TRUNCATED_HEADER, (end - offset) + " bytes left");
        }
        int length = intAt(log, offset);
        if (length < MIN_PAYLOAD || length > MAX_PAYLOAD) {
            return new Bad(Problem.BAD_LENGTH, "length " + Integer.toUnsignedString(length));
        }
        if (length > end - offset - HEADER_BYTES) {
            return new Bad(
                    Problem.TRUNCATED_PAYLOAD,
                    "length " + length + " but " + (end - offset - HEADER_BYTES) + " bytes left");
        }
        int stored = intAt(log, offset + 4);
        int actual = checksum(log, offset, length);
        if (stored != actual) {
            return new Bad(
                    Problem.BAD_CHECKSUM,
                    String.format("stored %08x, computed %08x", stored, actual));
        }
        Bad problem = structureProblem(log, offset, length);
        if (problem != null) {
            return problem;
        }
        return new Ok(build(log, offset, length), offset + HEADER_BYTES + length);
    }

    /**
     * Whether an intact record starts at {@code offset}. The cheap field checks come before the
     * checksum, and nothing is copied, so most offsets cost a few comparisons (the recovery scan
     * calls this at every offset after damage).
     */
    static boolean intactAt(byte[] log, int offset, int end) {
        int length = plausibleLength(log, offset, end);
        return length >= 0 && checksumMatches(log, offset, length);
    }

    /**
     * The payload length of the record at {@code offset} if its cheap fields all pass (a length in
     * bounds that fits, and a structure that fills the payload exactly), else -1. Copies nothing.
     */
    static int plausibleLength(byte[] log, int offset, int end) {
        if (end - offset < HEADER_BYTES + MIN_PAYLOAD) {
            return -1;
        }
        int length = intAt(log, offset);
        if (length < MIN_PAYLOAD || length > MAX_PAYLOAD || length > end - offset - HEADER_BYTES) {
            return -1;
        }
        return structureProblem(log, offset, length) == null ? length : -1;
    }

    /** Whether the stored checksum of the record at {@code offset}, of this payload length, matches. */
    static boolean checksumMatches(byte[] log, int offset, int length) {
        return intAt(log, offset + 4) == checksum(log, offset, length);
    }

    /** What is wrong with a payload's fields, or null if nothing is; the length is known to fit. */
    private static Bad structureProblem(byte[] log, int offset, int length) {
        long seq = longAt(log, offset + SEQ_AT);
        if (seq < 1) {
            return new Bad(Problem.BAD_STRUCTURE, "sequence number " + seq);
        }
        WalRecord.Op op = WalRecord.Op.of(log[offset + OP_AT]);
        if (op == null) {
            return new Bad(Problem.BAD_STRUCTURE, "unknown op " + (log[offset + OP_AT] & 0xff));
        }
        int payloadEnd = offset + HEADER_BYTES + length;
        int keyLength = intAt(log, offset + KEY_LENGTH_AT);
        int afterKey = offset + KEY_AT + keyLength;
        if (keyLength < 1 || keyLength > MAX_KEY_BYTES || keyLength > payloadEnd - (offset + KEY_AT)) {
            return new Bad(Problem.BAD_STRUCTURE, "key length " + Integer.toUnsignedString(keyLength));
        }
        if (op == WalRecord.Op.DELETE) {
            return afterKey == payloadEnd
                    ? null
                    : new Bad(Problem.BAD_STRUCTURE, (payloadEnd - afterKey) + " bytes after a delete's key");
        }
        if (payloadEnd - afterKey < 4) {
            return new Bad(Problem.BAD_STRUCTURE, "no room for a value length");
        }
        int valueLength = intAt(log, afterKey);
        int room = payloadEnd - (afterKey + 4);
        if (valueLength < 0 || valueLength > MAX_VALUE_BYTES || valueLength != room) {
            return new Bad(
                    Problem.BAD_STRUCTURE,
                    "value length " + Integer.toUnsignedString(valueLength) + " in " + room + " bytes");
        }
        return null;
    }

    /** Builds the record from a payload that passed every check. */
    private static WalRecord build(byte[] log, int offset, int length) {
        long seq = longAt(log, offset + SEQ_AT);
        int keyLength = intAt(log, offset + KEY_LENGTH_AT);
        int keyAt = offset + KEY_AT;
        byte[] key = Arrays.copyOfRange(log, keyAt, keyAt + keyLength);
        if (WalRecord.Op.of(log[offset + OP_AT]) == WalRecord.Op.DELETE) {
            return WalRecord.delete(seq, key);
        }
        int valueAt = keyAt + keyLength + 4;
        return WalRecord.put(seq, key, Arrays.copyOfRange(log, valueAt, offset + HEADER_BYTES + length));
    }

    /** CRC32C of the 4 length bytes at {@code offset}, then the payload after the header. */
    private static int checksum(byte[] log, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(log, offset, 4);
        crc.update(log, offset + HEADER_BYTES, length);
        return (int) crc.getValue();
    }

    private static int intAt(byte[] b, int at) {
        return ((b[at] & 0xff) << 24) | ((b[at + 1] & 0xff) << 16) | ((b[at + 2] & 0xff) << 8) | (b[at + 3] & 0xff);
    }

    private static long longAt(byte[] b, int at) {
        return ((long) intAt(b, at) << 32) | (intAt(b, at + 4) & 0xffffffffL);
    }
}
