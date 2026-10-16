package io.github.vivekdavara.keystore.jmh;

import io.github.vivekdavara.keystore.Options;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.SyncPolicy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Write throughput: one thread putting random 16-byte keys with 100-byte values into a store that
 * starts empty each trial (iterations go on into the same store, so flushes and merges are in the
 * numbers, as they are for a real writer). Under {@code NEVER} each put is an append to the page
 * cache; under {@code EVERY_WRITE} each one also waits for a device flush.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class WriteBenchmark {
    private static final int KEYS = 1 << 20;

    @Param({"NEVER", "EVERY_WRITE"})
    public SyncPolicy sync;

    private Path dir;
    private Store store;
    private byte[][] keys;
    private byte[] value;
    private int next;

    @Setup(Level.Trial)
    public void open() throws IOException {
        dir = Files.createTempDirectory("keystore-jmh-write");
        store = Store.open(dir, Options.defaults().withSync(sync));
        SplittableRandom random = new SplittableRandom(1);
        keys = new byte[KEYS][16];
        for (byte[] key : keys) {
            random.nextBytes(key);
        }
        value = new byte[100];
        random.nextBytes(value);
    }

    @Benchmark
    public long put() throws IOException {
        byte[] key = keys[next++ & (KEYS - 1)];
        return store.put(key, value);
    }

    @TearDown(Level.Trial)
    public void close() throws IOException {
        store.close();
        Benchmarks.delete(dir);
    }
}
