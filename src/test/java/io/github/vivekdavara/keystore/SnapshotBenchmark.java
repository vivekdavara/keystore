package io.github.vivekdavara.keystore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
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
 * What a snapshot costs: a store of 200,000 keys (16 bytes, values of 100: about 24 MB of tables)
 * snapshotted with its tables hard-linked, and copied (as across file systems, where links fail).
 * Each snapshot is deleted after it, outside the timing. In this package because the copy mode,
 * {@link Store#snapshot(Path, boolean)}, is package-private; {@code scripts/jmh.sh} runs it with
 * the others.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SnapshotBenchmark {
    @Param({"true", "false"})
    public boolean link;

    private Path dir;
    private Store store;
    private Path target;
    private int n;

    @Setup(Level.Trial)
    public void fill() throws IOException {
        dir = Files.createTempDirectory("keystore-jmh-snapshot");
        store = Store.open(dir.resolve("store"), Options.defaults().withSync(SyncPolicy.NEVER));
        SplittableRandom random = new SplittableRandom(3);
        byte[] key = new byte[16];
        byte[] value = new byte[100];
        for (int i = 0; i < 200_000; i++) {
            random.nextBytes(key);
            random.nextBytes(value);
            store.put(key, value);
        }
        store.flush();
    }

    @Setup(Level.Invocation)
    public void name() {
        target = dir.resolve("snapshot-" + n++);
    }

    @Benchmark
    public Store.Snapshot snapshot() throws IOException {
        return store.snapshot(target, link);
    }

    @TearDown(Level.Invocation)
    public void deleteSnapshot() throws IOException {
        delete(target);
    }

    @TearDown(Level.Trial)
    public void close() throws IOException {
        System.out.printf("%n%d tables, %,d bytes%n", store.tables().size(),
                store.tables().stream().mapToLong(TableInfo::fileBytes).sum());
        store.close();
        delete(dir);
    }

    private static void delete(Path dir) throws IOException {
        try (Stream<Path> files = Files.walk(dir)) {
            for (Path p : (Iterable<Path>) files.sorted(Comparator.reverseOrder())::iterator) {
                Files.delete(p);
            }
        }
    }
}
