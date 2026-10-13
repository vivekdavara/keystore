package io.github.vivekdavara.keystore.memtable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.RepeatedTest;

/**
 * One writer, several readers, no locks. The writer publishes how far it has got through an
 * {@link AtomicInteger}; a reader reads it first, which gives the reader a happens-before edge with
 * every insert and replacement before that point. So a reader that misses such a key, sees a value
 * older than one it must see, or iterates out of order has found a real bug, not a timing accident.
 *
 * <p>What it can't catch: a missing volatile (a plain write where a volatile one belongs). The same
 * counter that makes the checks exact also covers up a missing edge for every key it checks; only a
 * node inserted at that very moment could show one. That needs a jcstress-style harness, so those
 * properties rest on the argument in DESIGN.md.
 */
class SkipListConcurrencyTest {
    private static final int KEYS = 200_000;
    private static final int READERS = 4;

    @RepeatedTest(3)
    void readersSeeEveryInsertTheWriterHadFinished() throws Exception {
        int[] keys = shuffled(KEYS, new SplittableRandom(42));
        int[] indexOf = new int[KEYS + 1];
        for (int i = 0; i < KEYS; i++) {
            indexOf[keys[i]] = i;
        }
        SkipList<Integer, Integer> list = new SkipList<>(Comparator.naturalOrder());
        AtomicInteger published = new AtomicInteger(-1); // index of the last finished step
        AtomicReference<String> failure = new AtomicReference<>();
        AtomicInteger checks = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(READERS + 1);
        try {
            List<Future<?>> readers = new ArrayList<>();
            for (int r = 0; r < READERS; r++) {
                SplittableRandom random = new SplittableRandom(1000 + r);
                boolean iterates = r % 2 == 0;
                readers.add(pool.submit(() -> {
                    while (failure.get() == null) {
                        int upTo = published.get();
                        if (iterates) {
                            checkIteration(list, indexOf, upTo, failure);
                        } else {
                            checkGets(list, keys, upTo, random, failure);
                        }
                        checks.incrementAndGet();
                        if (upTo == KEYS - 1) {
                            return; // checked the final state once
                        }
                    }
                }));
            }
            Future<?> writer = pool.submit(() -> {
                for (int i = 0; i < KEYS; i++) {
                    list.put(keys[i], keys[i]);
                    if (i % 4 == 3) {
                        int earlier = keys[i / 2];
                        list.put(earlier, -earlier); // a replacement, racing with readers of that key
                    }
                    published.set(i);
                }
            });
            writer.get(60, TimeUnit.SECONDS);
            for (Future<?> reader : readers) {
                reader.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        assertNull(failure.get());
        assertEquals(KEYS, list.size());
        list.checkInvariants();
        assertTrue(checks.get() >= READERS, "every reader checked at least once");
    }

    /**
     * Whether {@code value} is one key {@code index} may have once step {@code upTo} is published.
     * Step i (when i % 4 == 3) replaces the key at index i / 2, so a key at an odd index j is
     * replaced at step 2j + 1: from then on only the replacement will do. Even indices are never
     * replaced. Before its replacement step, either value is possible (it may be in flight).
     */
    static boolean allowed(int key, int index, int upTo, Integer value) {
        if (value == null) {
            return false;
        }
        if (index % 2 == 0) {
            return value == key;
        }
        return 2 * index + 1 <= upTo ? value == -key : Math.abs(value) == key;
    }

    /** Every key finished by step upTo is found, with exactly the value it must have by then. */
    private static void checkGets(
            SkipList<Integer, Integer> list, int[] keys, int upTo, SplittableRandom random,
            AtomicReference<String> failure) {
        if (upTo < 0) {
            return;
        }
        for (int probe = 0; probe < 200; probe++) {
            int index = random.nextInt(upTo + 1);
            int key = keys[index];
            Integer value = list.get(key);
            if (!allowed(key, index, upTo, value)) {
                failure.compareAndSet(null, "get(" + key + ") = " + value + " after step " + upTo);
                return;
            }
        }
        if (list.size() < upTo + 1) {
            failure.compareAndSet(null, "size " + list.size() + " after step " + upTo);
        }
    }

    /**
     * Iteration is strictly sorted, reaches at least every key finished by step upTo, and shows each
     * key with a value it may have by then.
     */
    private static void checkIteration(
            SkipList<Integer, Integer> list, int[] indexOf, int upTo, AtomicReference<String> failure) {
        int count = 0;
        int last = Integer.MIN_VALUE;
        for (Map.Entry<Integer, Integer> entry : list) {
            int key = entry.getKey();
            if (count > 0 && key <= last) {
                failure.compareAndSet(null, "iteration went from " + last + " to " + key);
                return;
            }
            if (!allowed(key, indexOf[key], upTo, entry.getValue())) {
                failure.compareAndSet(null, "iteration saw " + key + " = " + entry.getValue() + " after step " + upTo);
                return;
            }
            last = key;
            count++;
        }
        if (count < upTo + 1) {
            failure.compareAndSet(null, "iteration saw " + count + " keys after step " + upTo);
        }
    }

    private static int[] shuffled(int n, SplittableRandom random) {
        int[] keys = new int[n];
        for (int i = 0; i < n; i++) {
            keys[i] = i + 1; // positive, so a replacement (-key) is always different
        }
        for (int i = n - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            int t = keys[i];
            keys[i] = keys[j];
            keys[j] = t;
        }
        return keys;
    }
}
