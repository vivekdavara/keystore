package io.github.vivekdavara.keystore.jmh;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/** Helpers for the JMH benchmarks; run them with {@code scripts/jmh.sh}. */
final class Benchmarks {
    private Benchmarks() {}

    /** Deletes a benchmark's store directory. */
    static void delete(Path dir) throws IOException {
        try (Stream<Path> files = Files.walk(dir)) {
            for (Path p : (Iterable<Path>) files.sorted(Comparator.reverseOrder())::iterator) {
                Files.delete(p);
            }
        }
    }
}
