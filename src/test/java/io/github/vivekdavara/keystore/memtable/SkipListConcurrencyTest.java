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
 * every insert before that point. So a reader that misses such a key, sees a wrong value, or
 * iterates out of order has found a real bug, not a timing accident.
 */
class SkipListConcurrencyTest {
    private static final int KEYS = 200_000;
    private static final int READERS = 4;

    @RepeatedTest(3)
    void readersSeeEveryInsertTheWriterHadFinished() throws Exception {
        int[] keys = shuffled(KEYS, new SplittableRandom(42));
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
                            checkIteration(list, upTo, failure);
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

    /** Every key finished by step upTo is found, with its value or its replacement. */
    private static void checkGets(
            SkipList<Integer, Integer> list, int[] keys, int upTo, SplittableRandom random,
            AtomicReference<String> failure) {
        if (upTo < 0) {
            return;
        }
        for (int probe = 0; probe < 200; probe++) {
            int key = keys[random.nextInt(upTo + 1)];
            Integer value = list.get(key);
            if (value == null || Math.abs(value) != key) {
                failure.compareAndSet(null, "get(" + key + ") = " + value + " after step " + upTo);
                return;
            }
        }
        if (list.size() < upTo + 1) {
            failure.compareAndSet(null, "size " + list.size() + " after step " + upTo);
        }
    }

    /** Iteration is strictly sorted and reaches at least every key finished by step upTo. */
    private static void checkIteration(SkipList<Integer, Integer> list, int upTo, AtomicReference<String> failure) {
        int count = 0;
        int last = Integer.MIN_VALUE;
        for (Map.Entry<Integer, Integer> entry : list) {
            int key = entry.getKey();
            if (count > 0 && key <= last) {
                failure.compareAndSet(null, "iteration went from " + last + " to " + key);
                return;
            }
            if (Math.abs(entry.getValue()) != key) {
                failure.compareAndSet(null, "iteration saw " + key + " = " + entry.getValue());
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
