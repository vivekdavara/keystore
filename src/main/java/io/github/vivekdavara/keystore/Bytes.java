package io.github.vivekdavara.keystore;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

/** Byte-string helpers: the key order, and a readable rendering for messages and tools. */
public final class Bytes {
    private Bytes() {}

    /**
     * The key order: bytewise and unsigned, so {@code 0x7f} sorts before {@code 0x80}, and a key
     * sorts before every longer key it is a prefix of. LevelDB's default comparator is the same.
     */
    public static final Comparator<byte[]> ORDER = Arrays::compareUnsigned;

    /** {@link #ORDER} as a method. */
    public static int compare(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b);
    }

    /** The UTF-8 bytes of a string. */
    public static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * A readable form: printable ASCII as itself, a backslash as {@code \\}, every other byte as
     * {@code \xNN}. At most {@code limit} bytes are shown; a longer string ends with
     * {@code ...(N bytes)}.
     */
    public static String show(byte[] bytes, int limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must not be negative: " + limit);
        }
        StringBuilder out = new StringBuilder();
        int shown = Math.min(bytes.length, limit);
        for (int i = 0; i < shown; i++) {
            int b = bytes[i] & 0xff;
            if (b == '\\') {
                out.append("\\\\");
            } else if (b >= 0x20 && b < 0x7f) {
                out.append((char) b);
            } else {
                out.append(String.format("\\x%02x", b));
            }
        }
        if (shown < bytes.length) {
            out.append("...(").append(bytes.length).append(" bytes)");
        }
        return out.toString();
    }

    /** {@link #show(byte[], int)} with a limit of 64 bytes. */
    public static String show(byte[] bytes) {
        return show(bytes, 64);
    }
}
