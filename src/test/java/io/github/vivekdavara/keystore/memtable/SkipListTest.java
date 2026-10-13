package io.github.vivekdavara.keystore.memtable;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.Bytes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SplittableRandom;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SkipListTest {
    private static final Comparator<Integer> INTS = Comparator.naturalOrder();

    @Test
    void anEmptyListHasNothing() {
        SkipList<Integer, String> list = new SkipList<>(INTS);
        assertNull(list.get(1));
        assertEquals(0, list.size());
        assertTrue(list.isEmpty());
        assertFalse(list.iterator().hasNext());
        assertFalse(list.iterator(0).hasNext());
        assertThrows(NoSuchElementException.class, () -> list.iterator().next());
        list.checkInvariants();
    }

    @Test
    void putReturnsThePreviousValue() {
        SkipList<Integer, String> list = new SkipList<>(INTS);
        assertNull(list.put(5, "five"));
        assertEquals("five", list.put(5, "FIVE"));
        assertEquals("FIVE", list.get(5));
        assertEquals(1, list.size());
        assertNull(list.get(4));
        assertNull(list.get(6));
    }

    @Test
    void nullsAreRefused() {
        SkipList<Integer, String> list = new SkipList<>(INTS);
        assertThrows(NullPointerException.class, () -> list.put(null, "x"));
        assertThrows(NullPointerException.class, () -> list.put(1, null));
        assertThrows(NullPointerException.class, () -> list.get(null));
        assertThrows(NullPointerException.class, () -> list.iterator(null));
    }

    /**
     * Random puts and gets checked step by step against TreeMap, then the whole order and the
     * structure. A small key space makes most puts replacements; a large one, most inserts.
     */
    @ParameterizedTest(name = "seed {0}, {1} keys")
    @CsvSource({"1, 10", "2, 100", "3, 1000", "4, 1000000", "5, 2147483647"})
    void behavesLikeATreeMap(long seed, int keySpace) {
        SplittableRandom random = new SplittableRandom(seed);
        SkipList<Integer, Integer> list = new SkipList<>(INTS, new SplittableRandom(seed + 100));
        TreeMap<Integer, Integer> model = new TreeMap<>();
        for (int step = 0; step < 20_000; step++) {
            int key = random.nextInt(keySpace);
            if (random.nextInt(3) == 0) {
                assertEquals(model.get(key), list.get(key), "get at step " + step);
            } else {
                int value = random.nextInt();
                assertEquals(model.put(key, value), list.put(key, value), "put at step " + step);
            }
            assertEquals(model.size(), list.size());
        }
        assertEquals(new ArrayList<>(model.entrySet()), entries(list.iterator()));
        int[] levels = list.checkInvariants();
        assertEquals(model.size(), levels[0]);
    }

    @Test
    void iterationFromAKeyStartsAtTheFirstKeyAtOrAfterIt() {
        SplittableRandom random = new SplittableRandom(7);
        SkipList<Integer, Integer> list = new SkipList<>(INTS, new SplittableRandom(8));
        TreeMap<Integer, Integer> model = new TreeMap<>();
        for (int i = 0; i < 2_000; i++) {
            int key = random.nextInt(10_000) * 2; // even keys, so odd starting points fall between keys
            list.put(key, i);
            model.put(key, i);
        }
        for (int from = -3; from <= 20_003; from += 37) {
            assertEquals(
                    new ArrayList<>(model.tailMap(from, true).entrySet()),
                    entries(list.iterator(from)),
                    "from " + from);
        }
    }

    /** Byte-string keys in the store's order: unsigned bytes, prefixes first. */
    @Test
    void byteKeysFollowTheStoreOrder() {
        SkipList<byte[], String> list = new SkipList<>(Bytes.ORDER, new SplittableRandom(9));
        TreeMap<byte[], String> model = new TreeMap<>(Bytes.ORDER);
        SplittableRandom random = new SplittableRandom(10);
        for (int i = 0; i < 5_000; i++) {
            byte[] key = new byte[random.nextInt(1, 4)];
            for (int j = 0; j < key.length; j++) {
                key[j] = (byte) (0x7e + random.nextInt(4)); // 0x7e..0x81: both sides of the sign bit
            }
            String value = "v" + i;
            assertEquals(model.put(key, value), list.put(key, value));
        }
        List<Map.Entry<byte[], String>> got = entries(list.iterator());
        List<Map.Entry<byte[], String>> want = new ArrayList<>(model.entrySet());
        assertEquals(want.size(), got.size());
        for (int i = 0; i < want.size(); i++) {
            assertArrayEquals(want.get(i).getKey(), got.get(i).getKey());
            assertEquals(want.get(i).getValue(), got.get(i).getValue());
        }
        for (Map.Entry<byte[], String> entry : want) {
            assertEquals(entry.getValue(), list.get(entry.getKey().clone()), "a copy of the key finds it");
        }
        assertNull(list.get(new byte[] {(byte) 0x82}));
        list.checkInvariants();
    }

    @Test
    void anIteratorSeesEachValueAsOfItsVisit() {
        SkipList<Integer, String> list = new SkipList<>(INTS);
        list.put(1, "a");
        list.put(2, "b");
        Iterator<Map.Entry<Integer, String>> it = list.iterator();
        assertEquals("a", it.next().getValue());
        list.put(2, "B"); // replaced after the iterator started, before it got there
        list.put(3, "c"); // inserted ahead of the iterator
        assertEquals("B", it.next().getValue());
        assertEquals(3, it.next().getKey());
        assertFalse(it.hasNext());
    }

    /**
     * Node heights follow the geometric distribution: a node reaches level i with probability
     * 4^-i. With a fixed seed this is deterministic; the tolerance is about 6 standard deviations.
     */
    @Test
    void heightsAreGeometricWithBranchingFour() {
        SkipList<Integer, Integer> list = new SkipList<>(INTS, new SplittableRandom(11));
        int n = 200_000;
        for (int i = 0; i < n; i++) {
            list.put(i, i);
        }
        int[] levels = list.checkInvariants();
        assertEquals(n, levels[0]);
        for (int level = 1; level <= 4; level++) {
            double expected = n * Math.pow(0.25, level);
            double sd = Math.sqrt(expected);
            assertTrue(
                    Math.abs(levels[level] - expected) < 6 * sd + 1,
                    "level " + level + ": " + levels[level] + " nodes, expected about " + expected);
        }
        assertTrue(levels[SkipList.MAX_HEIGHT - 1] <= levels[0]);
    }

    /** Inserting in descending order puts every node at the front: the head's links do the work. */
    @Test
    void descendingInsertsStaySorted() {
        SkipList<Integer, Integer> list = new SkipList<>(INTS, new SplittableRandom(12));
        for (int i = 10_000; i > 0; i--) {
            list.put(i, i);
        }
        int expected = 1;
        for (Map.Entry<Integer, Integer> entry : list) {
            assertEquals(expected++, entry.getKey());
        }
        assertEquals(10_001, expected);
        list.checkInvariants();
    }

    private static <K, V> List<Map.Entry<K, V>> entries(Iterator<Map.Entry<K, V>> it) {
        List<Map.Entry<K, V>> out = new ArrayList<>();
        it.forEachRemaining(out::add);
        return out;
    }
}
