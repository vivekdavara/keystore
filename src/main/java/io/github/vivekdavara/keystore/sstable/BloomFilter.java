package io.github.vivekdavara.keystore.sstable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * A Bloom filter over a table's keys: "definitely not here" or "maybe here". A read asks it before
 * reading a block, so a key that isn't in a table usually costs no disk read there.
 *
 * <p>Each key sets {@code k} bits chosen by double hashing one 64-bit hash (Kirsch and
 * Mitzenmacher: probe i is {@code h + i * delta}, as LevelDB's filter does). With {@code b} bits per
 * key the false-positive rate is about {@code (1 - e^(-k/b))^k}, lowest at {@code k = b ln 2}: for
 * 10 bits a key, k = 7 and about 0.8%. {@code BloomFilterTest} measures it.
 *
 * <p>Stored as: {@code k} (1 byte), then the bit array, its length a multiple of 8 bytes. The
 * table's block trailer checksums it. Immutable once built.
 */
public final class BloomFilter {
    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /** At least this many bits, so that a filter over a few keys isn't all ones. */
    static final int MIN_BITS = 64;

    private final int k;
    private final byte[] bits;
    private final long numBits;

    private BloomFilter(int k, byte[] bits) {
        this.k = k;
        this.bits = bits;
        this.numBits = 8L * bits.length;
    }

    /** The number of probes for {@code bitsPerKey}: {@code bitsPerKey * ln 2}, rounded, 1 to 30. */
    static int probesFor(int bitsPerKey) {
        return (int) Math.max(1, Math.min(30, Math.round(bitsPerKey * Math.log(2))));
    }

    /**
     * A filter over the keys whose {@link #hash}es are {@code hashes[0, count)}.
     *
     * @throws IllegalArgumentException if {@code bitsPerKey} is not 1 to 64
     */
    public static BloomFilter build(long[] hashes, int count, int bitsPerKey) {
        if (bitsPerKey < 1 || bitsPerKey > 64) {
            throw new IllegalArgumentException("bits per key must be 1 to 64, not " + bitsPerKey);
        }
        long wanted = Math.max(MIN_BITS, (long) count * bitsPerKey);
        long bytes = (wanted + 63) / 64 * 8; // whole 64-bit words
        if (bytes > Integer.MAX_VALUE - 16) {
            throw new IllegalArgumentException("a filter over " + count + " keys would be " + bytes + " bytes");
        }
        BloomFilter filter = new BloomFilter(probesFor(bitsPerKey), new byte[(int) bytes]);
        for (int i = 0; i < count; i++) {
            filter.add(hashes[i]);
        }
        return filter;
    }

    private void add(long h) {
        long delta = (h >>> 33) | 1;
        for (int i = 0; i < k; i++) {
            long bit = Long.remainderUnsigned(h, numBits);
            bits[(int) (bit >>> 3)] |= (byte) (1 << (bit & 7));
            h += delta;
        }
    }

    /** False if the key with this {@link #hash} is certainly not in the set; true if it may be. */
    public boolean mightContain(long h) {
        long delta = (h >>> 33) | 1;
        for (int i = 0; i < k; i++) {
            long bit = Long.remainderUnsigned(h, numBits);
            if ((bits[(int) (bit >>> 3)] & (1 << (bit & 7))) == 0) {
                return false;
            }
            h += delta;
        }
        return true;
    }

    /** {@link #mightContain(long)} for a key. */
    public boolean mightContain(byte[] key) {
        return mightContain(hash(key));
    }

    /** The filter as the table stores it. */
    byte[] toBytes() {
        byte[] out = new byte[1 + bits.length];
        out[0] = (byte) k;
        System.arraycopy(bits, 0, out, 1, bits.length);
        return out;
    }

    /**
     * Reads a filter stored by {@link #toBytes}; the caller has checked its checksum.
     *
     * @throws IllegalArgumentException if the bytes can't be a filter
     */
    static BloomFilter fromBytes(byte[] stored) {
        if (stored.length < 1 + 8 || (stored.length - 1) % 8 != 0) {
            throw new IllegalArgumentException("a filter of " + stored.length + " bytes");
        }
        int k = stored[0];
        if (k < 1 || k > 30) {
            throw new IllegalArgumentException("a filter with " + k + " probes");
        }
        byte[] bits = new byte[stored.length - 1];
        System.arraycopy(stored, 1, bits, 0, bits.length);
        return new BloomFilter(k, bits);
    }

    /** Probes per key. */
    public int probes() {
        return k;
    }

    /** The size of the bit array, in bytes. */
    public int sizeInBytes() {
        return bits.length;
    }

    /**
     * The key's 64-bit hash: MurmurHash64A (Austin Appleby, public domain) with a fixed seed. Tables
     * store filters built with it, so it must never change.
     */
    public static long hash(byte[] key) {
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;
        long h = 0x9747b28cL ^ (key.length * m);
        int words = key.length / 8;
        for (int i = 0; i < words; i++) {
            long w = (long) LONG_LE.get(key, i * 8);
            w *= m;
            w ^= w >>> r;
            w *= m;
            h ^= w;
            h *= m;
        }
        int tail = words * 8;
        int left = key.length - tail;
        if (left > 0) {
            for (int i = left - 1; i >= 0; i--) {
                h ^= (long) (key[tail + i] & 0xff) << (8 * i);
            }
            h *= m;
        }
        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        return h;
    }
}
