package io.github.vivekdavara.keystore.wal;

import io.github.vivekdavara.keystore.Bytes;
import java.util.Arrays;
import java.util.Objects;

/** One write as the log stores it: its sequence number, put or delete, the key, and a put's value. */
public final class WalRecord {
    /** The operation. The code is the byte the log stores. */
    public enum Op {
        PUT(1),
        DELETE(2);

        final byte code;

        Op(int code) {
            this.code = (byte) code;
        }

        static Op of(byte code) {
            return switch (code) {
                case 1 -> PUT;
                case 2 -> DELETE;
                default -> null;
            };
        }
    }

    private final long seq;
    private final Op op;
    private final byte[] key;
    private final byte[] value; // null for a delete

    private WalRecord(long seq, Op op, byte[] key, byte[] value) {
        this.seq = seq;
        this.op = op;
        this.key = Objects.requireNonNull(key, "key");
        this.value = value;
    }

    /** A put. Keeps the arrays; the caller must not change them. */
    public static WalRecord put(long seq, byte[] key, byte[] value) {
        return new WalRecord(seq, Op.PUT, key, Objects.requireNonNull(value, "value"));
    }

    /** A delete. Keeps the array; the caller must not change it. */
    public static WalRecord delete(long seq, byte[] key) {
        return new WalRecord(seq, Op.DELETE, key, null);
    }

    public long seq() {
        return seq;
    }

    public Op op() {
        return op;
    }

    /** The key itself, not a copy. */
    public byte[] key() {
        return key;
    }

    /** The value itself, not a copy; null for a delete. */
    public byte[] value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof WalRecord other
                && seq == other.seq
                && op == other.op
                && Arrays.equals(key, other.key)
                && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(seq, op, Arrays.hashCode(key), Arrays.hashCode(value));
    }

    @Override
    public String toString() {
        return op == Op.PUT
                ? "#" + seq + " put " + Bytes.show(key) + " = " + Bytes.show(value)
                : "#" + seq + " delete " + Bytes.show(key);
    }
}
