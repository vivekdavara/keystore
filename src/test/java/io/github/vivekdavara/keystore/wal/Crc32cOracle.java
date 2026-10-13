package io.github.vivekdavara.keystore.wal;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * CRC-32C written out bit by bit (reflected polynomial 0x82f63b78), independent of
 * {@code java.util.zip.CRC32C}, so the format tests don't check the code against itself.
 */
final class Crc32cOracle {
    private Crc32cOracle() {}

    /** CRC-32C of {@code first} followed by {@code second}. */
    static int of(byte[] first, byte[] second) {
        int crc = 0xffffffff;
        for (byte[] part : new byte[][] {first, second}) {
            for (byte b : part) {
                crc ^= b & 0xff;
                for (int i = 0; i < 8; i++) {
                    crc = (crc & 1) != 0 ? (crc >>> 1) ^ 0x82f63b78 : crc >>> 1;
                }
            }
        }
        return ~crc;
    }

    /** Recomputes the checksum of one record that fills {@code record}, after a test changed it. */
    static void reseal(byte[] record) {
        byte[] length = Arrays.copyOfRange(record, 0, 4);
        byte[] payload = Arrays.copyOfRange(record, WalFormat.HEADER_BYTES, record.length);
        ByteBuffer.wrap(record).putInt(4, of(length, payload));
    }
}
