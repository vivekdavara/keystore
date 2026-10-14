package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Crash-and-recover cycles with flushes and compactions in them. Every channel the store opens
 * (logs, tables, the manifest, directories) shares one countdown of I/O calls; when it runs out, that
 * write (cut at a random byte) or force fails, wherever it is: an append, a table being written, the
 * manifest, a directory force. The directory is then copied as it stands, which is what a killed
 * process leaves (the page cache survives), and the next cycle recovers the copy. After every
 * recovery the store must hold exactly the writes that returned.
 */
class TableCrashCycleTest {
    private static final int KEYS = 60;

    @TempDir
    Path tmp;

    /** A channel whose writes and forces count down a shared budget; the last one fails. */
    private static final class Countdown extends FaultyChannel {
        private final AtomicInteger left;
        private final SplittableRandom random;
        private final String kind;
        private final Map<String, Integer> faults;

        Countdown(FileChannel inner, AtomicInteger left, SplittableRandom random, Path path, Map<String, Integer> faults) {
            super(inner);
            this.left = left;
            this.random = random;
            String name = path.getFileName().toString();
            this.kind = name.endsWith(".log") ? "log" : name.endsWith(".sst") ? "table"
                    : name.startsWith("MANIFEST") ? "manifest" : "directory";
            this.faults = faults;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            if (left.decrementAndGet() == 0) {
                faults.merge(kind + " write", 1, Integer::sum);
                failWritesAfter(random.nextInt(Math.max(1, src.remaining()))); // torn: some bytes, never all
            }
            return super.write(src);
        }

        @Override
        public void force(boolean metaData) throws IOException {
            if (left.decrementAndGet() == 0) {
                faults.merge(kind + " force", 1, Integer::sum);
                throw new IOException("injected: force failed");
            }
            super.force(metaData);
        }
    }

    /** Three seeds, and then a check that the faults landed on every kind of step at least once. */
    @Test
    @Timeout(value = 180, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void acknowledgedWritesSurviveCrashesAnywhere() throws IOException {
        Map<String, Integer> faults = new TreeMap<>();
        for (long seed = 1; seed <= 3; seed++) {
            cycles(seed, faults);
        }
        for (String step : new String[] {"log write", "table write", "table force", "manifest write", "directory force"}) {
            assertTrue(faults.containsKey(step), "no fault landed on a " + step + ": " + faults);
        }
    }

    private void cycles(long seed, Map<String, Integer> faults) throws IOException {
        SplittableRandom random = new SplittableRandom(seed);
        Map<String, String> model = new HashMap<>();
        long acknowledged = 0;
        Options options = Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(1024).withMergeWidth(3);
        AtomicInteger left = new AtomicInteger(Integer.MAX_VALUE);
        BiFunction<Path, FileChannel, FileChannel> wrap = (path, inner) -> new Countdown(inner, left, random, path, faults);
        Path dir = tmp.resolve("store-" + seed);
        int crashes = 0;
        int flushes = 0;

        for (int cycle = 0; cycle < 120; cycle++) {
            left.set(Integer.MAX_VALUE);
            Store store = Store.open(dir, options, wrap);
            try {
                assertEquals(acknowledged, store.lastSequence(), "cycle " + cycle);
                for (int k = 0; k < KEYS; k++) {
                    assertEquals(Optional.ofNullable(model.get("k" + k)), string(store.get(utf8("k" + k))),
                            "cycle " + cycle + ", key k" + k);
                }
                boolean crash = random.nextInt(3) != 0;
                if (crash) {
                    left.set(random.nextInt(1, 60));
                }
                int writes = random.nextInt(0, 80);
                for (int w = 0; w < writes || crash; w++) {
                    String key = "k" + random.nextInt(KEYS);
                    String value = random.nextInt(5) == 0 ? null : "v" + cycle + "-" + w + "-" + "x".repeat(random.nextInt(0, 60));
                    try {
                        if (value == null) {
                            store.delete(utf8(key));
                            model.remove(key);
                        } else {
                            store.put(utf8(key), utf8(value));
                            model.put(key, value);
                        }
                        acknowledged++;
                    } catch (IOException expected) {
                        assertTrue(crash, "only an injected fault fails a write: " + expected);
                        break; // the store has failed; what the directory holds now is the crash
                    }
                    if (w > 500) {
                        throw new AssertionError("the countdown never ran out");
                    }
                }
                flushes += store.tables().size() > 0 ? 1 : 0;
                if (crash) {
                    crashes++;
                    Path image = tmp.resolve("image");
                    copy(dir, image);
                    closeQuietly(store);
                    delete(dir);
                    Files.move(image, dir);
                    continue;
                }
            } catch (IOException | RuntimeException | Error e) {
                closeQuietly(store);
                throw e;
            }
            store.close();
        }
        System.out.printf("table crash cycles, seed %d: 120 cycles, %d crashes, %d acknowledged writes, %d cycles"
                + " with tables; faults so far: %s%n", seed, crashes, acknowledged, flushes, faults);
        assertTrue(crashes > 40 && flushes > 40, crashes + " crashes, " + flushes + " cycles with tables");
    }

    private static void closeQuietly(Store store) {
        try {
            store.close();
        } catch (IOException | RuntimeException ignored) {
            // A failed store's close can throw; the crash image is already taken.
        }
    }

    private static void copy(Path from, Path to) throws IOException {
        Files.createDirectories(to);
        try (Stream<Path> files = Files.list(from)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                Files.copy(file, to.resolve(file.getFileName()), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private static void delete(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                Files.delete(file);
            }
        }
        Files.delete(dir);
    }

    private static Optional<String> string(Optional<byte[]> value) {
        return value.map(v -> new String(v, StandardCharsets.UTF_8));
    }
}
