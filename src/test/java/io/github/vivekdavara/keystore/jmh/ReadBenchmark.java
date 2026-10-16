package io.github.vivekdavara.keystore.jmh;

import io.github.vivekdavara.keystore.Options;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.SyncPolicy;
import io.github.vivekdavara.keystore.TableInfo;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
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
 * Read throughput, and what the Bloom filter is worth. A store of 200,000 keys (16 bytes, values
 * of 100) is written with merges off, so it holds about a dozen tables of similar size, newest
 * first, and an empty memtable. Present keys are spread over all of them; absent keys sort among
 * the present ones, so no table's key range rules them out, and only the filters stand between a
 * lookup and a block read in every table. Run at 1, 5, 10 and 16 bits a key.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ReadBenchmark {
    static final int KEYS = 200_000;

    @Param({"1", "5", "10", "16"})
    public int bloomBitsPerKey;

    private Path dir;
    private Store store;
    private byte[][] present;
    private byte[][] absent;
    private int tables;
    /** Lookups made in the trial, warm-up included; a plain field, so exact with one thread (JMH's default). */
    private long lookups;

    /** Key i: 8 random bytes, then i, then a 0 (present) or 1 (absent) byte, then padding to 16. */
    private static byte[] key(long prefix, int i, boolean isPresent) {
        return ByteBuffer.allocate(16).putLong(prefix).putInt(i).put((byte) (isPresent ? 0 : 1)).array();
    }

    @Setup(Level.Trial)
    public void fill() throws IOException {
        dir = Files.createTempDirectory("keystore-jmh-read");
        Options options = Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(2 << 20)
                .withMergeWidth(64).withBloomBitsPerKey(bloomBitsPerKey);
        store = Store.open(dir, options);
        SplittableRandom random = new SplittableRandom(2);
        present = new byte[KEYS][];
        absent = new byte[KEYS][];
        byte[] value = new byte[100];
        for (int i = 0; i < KEYS; i++) {
            long prefix = random.nextLong();
            present[i] = key(prefix, i, true);
            absent[i] = key(prefix, i, false);
            random.nextBytes(value);
            store.put(present[i], value);
        }
        store.flush();
        tables = store.tables().size();
        if (tables < 8) {
            throw new IllegalStateException(tables + " tables; the benchmark needs several");
        }
    }

    /** Per thread: where in the shuffled order of keys it is. */
    @State(Scope.Thread)
    public static class Cursor {
        int next = new SplittableRandom(Thread.currentThread().threadId()).nextInt(KEYS);
    }

    @Benchmark
    public Optional<byte[]> getPresent(Cursor cursor) throws IOException {
        int i = cursor.next;
        cursor.next = (int) ((i + 7_919L) % KEYS); // a prime stride: every key, in no table's order
        lookups++;
        return store.get(present[i]);
    }

    @Benchmark
    public Optional<byte[]> getAbsent(Cursor cursor) throws IOException {
        int i = cursor.next;
        cursor.next = (int) ((i + 7_919L) % KEYS);
        lookups++;
        return store.get(absent[i]);
    }

    @TearDown(Level.Trial)
    public void close() throws IOException {
        long blocks = 0;
        for (TableInfo t : store.tables()) {
            blocks += t.blocksRead();
        }
        System.out.printf("%n%d tables at %d bits a key: %.3f blocks read a lookup (%,d blocks, %,d lookups,"
                + " warm-up included)%n", tables, bloomBitsPerKey, (double) blocks / lookups, blocks, lookups);
        store.close();
        Benchmarks.delete(dir);
    }
}
