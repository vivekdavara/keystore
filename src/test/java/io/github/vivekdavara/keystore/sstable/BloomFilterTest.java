package io.github.vivekdavara.keystore.sstable;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class BloomFilterTest {
    private static BloomFilter filterOver(int keys, int bitsPerKey) {
        long[] hashes = new long[keys];
        for (int i = 0; i < keys; i++) {
            hashes[i] = BloomFilter.hash(utf8("key-" + i));
        }
        return BloomFilter.build(hashes, keys, bitsPerKey);
    }

    @Test
    void everyKeyAddedIsFound() {
        BloomFilter filter = filterOver(10_000, 10);
        for (int i = 0; i < 10_000; i++) {
            assertTrue(filter.mightContain(utf8("key-" + i)), "key-" + i);
        }
    }

    /**
     * The false-positive rate is near the formula's: 10,000 keys, then 100,000 keys that were never
     * added. At 10 bits a key (7 probes) the formula gives 0.82%; at 5 bits (3 probes), 9.2%.
     */
    @ParameterizedTest(name = "{0} bits a key")
    @CsvSource({"10, 0.0082", "5, 0.092"})
    void theFalsePositiveRateIsNearTheFormulas(int bitsPerKey, double expected) {
        BloomFilter filter = filterOver(10_000, bitsPerKey);
        int falsePositives = 0;
        for (int i = 0; i < 100_000; i++) {
            if (filter.mightContain(utf8("absent-" + i))) {
                falsePositives++;
            }
        }
        double rate = falsePositives / 100_000.0;
        System.out.printf("bloom: %d bits a key, %d probes: %d of 100000 absent keys passed (%.2f%%)%n",
                bitsPerKey, filter.probes(), falsePositives, 100 * rate);
        assertTrue(rate < expected * 1.5, "rate " + rate + ", formula " + expected);
        assertTrue(rate > expected * 0.5, "rate " + rate + " is suspiciously low; formula " + expected);
    }

    @Test
    void probesFollowBitsPerKeyTimesLn2() {
        assertEquals(7, BloomFilter.probesFor(10));
        assertEquals(3, BloomFilter.probesFor(5));
        assertEquals(1, BloomFilter.probesFor(1));
        assertEquals(30, BloomFilter.probesFor(64));
    }

    @Test
    void anEmptyFilterRejectsEverything() {
        BloomFilter filter = BloomFilter.build(new long[0], 0, 10);
        assertEquals(BloomFilter.MIN_BITS / 8, filter.sizeInBytes());
        for (int i = 0; i < 1_000; i++) {
            assertFalse(filter.mightContain(utf8("k" + i)));
        }
    }

    @Test
    void aStoredFilterAnswersTheSame() {
        BloomFilter filter = filterOver(1_000, 10);
        BloomFilter read = BloomFilter.fromBytes(filter.toBytes());
        assertEquals(filter.probes(), read.probes());
        for (int i = 0; i < 20_000; i++) {
            byte[] key = utf8("probe-" + i);
            assertEquals(filter.mightContain(key), read.mightContain(key), "probe-" + i);
        }
    }

    @Test
    void bytesThatCantBeAFilterAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> BloomFilter.fromBytes(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> BloomFilter.fromBytes(new byte[1 + 7]));
        byte[] zeroProbes = new byte[1 + 8];
        assertThrows(IllegalArgumentException.class, () -> BloomFilter.fromBytes(zeroProbes));
        byte[] tooMany = new byte[1 + 8];
        tooMany[0] = 31;
        assertThrows(IllegalArgumentException.class, () -> BloomFilter.fromBytes(tooMany));
        assertThrows(IllegalArgumentException.class, () -> BloomFilter.build(new long[1], 1, 0));
    }

    /**
     * Tables store filters built with this hash, so it must never change. The expected values are
     * the reference C implementation's, with the same seed: {@code scripts/murmur64a.c}, run as its
     * header comment says. "abcdefgh" is exactly one 8-byte word; "abcdefghi" adds a 1-byte tail.
     */
    @Test
    void theHashMatchesTheReferenceImplementation() {
        assertEquals(-8964588313662369710L, BloomFilter.hash(new byte[0]));
        assertEquals(-1627098790175280210L, BloomFilter.hash(utf8("a")));
        assertEquals(187593484090523012L, BloomFilter.hash(utf8("The quick brown fox jumps over the lazy dog")));
        assertEquals(7024297616001814202L, BloomFilter.hash(utf8("abcdefgh")));
        assertEquals(7188486381016474263L, BloomFilter.hash(utf8("abcdefghi")));
    }
}
