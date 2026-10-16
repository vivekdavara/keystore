package io.github.vivekdavara.keystore;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;

/**
 * A stream of writes in which write {@code seq} is a pure function of the seed and {@code seq}, so
 * that a process that didn't make the writes (a test whose child was killed, a leader's test
 * checking its follower) can say what a store holds at any sequence number. Keys {@code k0} to
 * {@code k(keys-1)}; one write in eight is a delete; values are 0 to 300 bytes and name their own
 * sequence number, so a stale value can't pass for the right one.
 */
final class Workload {
    /** A write: {@code value} is null for a delete. */
    record Write(long seq, byte[] key, byte[] value) {}

    private final long seed;
    private final int keys;

    Workload(long seed, int keys) {
        this.seed = seed;
        this.keys = keys;
    }

    int keys() {
        return keys;
    }

    static byte[] key(int k) {
        return ("k" + k).getBytes(StandardCharsets.UTF_8);
    }

    Write write(long seq) {
        SplittableRandom random = new SplittableRandom(seed * 0x9E3779B97F4A7C15L + seq);
        byte[] key = key(random.nextInt(keys));
        if (random.nextInt(8) == 0) {
            return new Write(seq, key, null);
        }
        byte[] value = ("v" + seq + "-" + "x".repeat(random.nextInt(0, 300))).getBytes(StandardCharsets.UTF_8);
        return new Write(seq, key, value);
    }

    /** Makes write {@code seq} in {@code store}; returns the sequence number the store gave it. */
    long apply(Store store, long seq) throws java.io.IOException {
        Write w = write(seq);
        return w.value() == null ? store.delete(w.key()) : store.put(w.key(), w.value());
    }

    /** What a store holds after writes 1 to {@code seq}, advanced one write at a time. */
    static final class Model {
        private final Workload workload;
        private final Map<String, byte[]> values = new HashMap<>();
        private long seq;

        Model(Workload workload) {
            this.workload = workload;
        }

        long seq() {
            return seq;
        }

        /** Applies the writes up to {@code target}, which must not be behind. */
        void advanceTo(long target) {
            if (target < seq) {
                throw new IllegalArgumentException("the model is at " + seq + ", past " + target);
            }
            while (seq < target) {
                Write w = workload.write(++seq);
                String key = new String(w.key(), StandardCharsets.UTF_8);
                if (w.value() == null) {
                    values.remove(key);
                } else {
                    values.put(key, w.value());
                }
            }
        }

        Optional<byte[]> get(int k) {
            return Optional.ofNullable(values.get("k" + k));
        }
    }
}
