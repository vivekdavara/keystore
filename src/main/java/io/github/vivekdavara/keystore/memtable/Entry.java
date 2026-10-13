package io.github.vivekdavara.keystore.memtable;

import io.github.vivekdavara.keystore.Bytes;
import java.util.Arrays;

/**
 * A key's newest version: the sequence number of the write that made it, and the value, or no
 * value for a tombstone (the key was deleted). Immutable, as long as nobody changes the array.
 */
public final class Entry {
    private final long seq;
    private final byte[] value; // null for a tombstone

    private Entry(long seq, byte[] value) {
        if (seq <= 0) {
            throw new IllegalArgumentException("sequence numbers start at 1: " + seq);
        }
        this.seq = seq;
        this.value = value;
    }

    /** A value, written by write number {@code seq}. Keeps the array; the caller must not change it. */
    public static Entry value(long seq, byte[] value) {
        if (value == null) {
            throw new NullPointerException("value");
        }
        return new Entry(seq, value);
    }

    /** A deletion, written by write number {@code seq}. */
    public static Entry tombstone(long seq) {
        return new Entry(seq, null);
    }

    public long seq() {
        return seq;
    }

    public boolean isTombstone() {
        return value == null;
    }

    /** The value itself, not a copy; null for a tombstone. */
    public byte[] value() {
        return value;
    }

    /** The bytes this entry holds: the value's length, or 0 for a tombstone. */
    int valueLength() {
        return value == null ? 0 : value.length;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Entry other && seq == other.seq && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(seq) * 31 + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return isTombstone() ? "tombstone@" + seq : Bytes.show(value) + "@" + seq;
    }
}
