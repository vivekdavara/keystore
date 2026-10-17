package io.github.vivekdavara.keystore.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class MediansTest {
    @Test
    void eachBenchmarkGetsItsMediansAndRangesInTheOrderItFirstAppeared() {
        List<String> lines = List.of(
                "writes sync=NEVER count=200000 value=100B: 336578 writes/s, p50 1.7 us, p99 3.8 us, max 85.0 us",
                "a line without a colon and a space",
                "lag count=10000 value=100B sync=NEVER: p50 28.0 us, p99 73.3 us, max 1400.0 us",
                "writes sync=NEVER count=200000 value=100B: 211400 writes/s, p50 1.7 us, p99 4.0 us, max 90.5 us",
                "writes sync=NEVER count=200000 value=100B: 339257 writes/s, p50 1.8 us, p99 3.9 us, max 70.0 us");
        assertEquals(List.of(
                "writes sync=NEVER count=200000 value=100B, median of 3: 336578 [211400..339257] writes/s,"
                        + " p50 1.7 [1.7..1.8] us, p99 3.9 [3.8..4.0] us, max 85.0 [70.0..90.5] us",
                "lag count=10000 value=100B sync=NEVER, median of 1: p50 28.0 us, p99 73.3 us, max 1400.0 us"),
                Medians.summarise(lines));
    }

    /** "p99.9" and "MB/s" are words; the numbers are the ones standing alone, fractions included. */
    @Test
    void labelsWithDigitsInThemAreNotNumbers() {
        List<String> lines = List.of(
                "fill count=10 value=100B: p99.9 8.3 us; 33 flushes (x2 3.37); reopen 9 ms",
                "fill count=10 value=100B: p99.9 8.5 us; 33 flushes (x2 3.39); reopen 11 ms");
        assertEquals(List.of("fill count=10 value=100B, median of 2: p99.9 8.4 [8.3..8.5] us; 33 flushes (x2 3.38"
                + " [3.37..3.39]); reopen 10 [9..11] ms"), Medians.summarise(lines));
    }

    @Test
    void anEvenNumberOfRunsTakesTheMeanOfTheMiddleTwo() {
        List<String> lines = List.of("scan: 60 ms", "scan: 75 ms", "scan: 61 ms", "scan: 100 ms");
        assertEquals(List.of("scan, median of 4: 68 [60..100] ms"), Medians.summarise(lines));
    }

    @Test
    void runsWhoseWordsDifferAreRefused() {
        List<String> lines = List.of("server connections=1: puts 35096/s", "server connections=1: gets 38633/s");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Medians.summarise(lines));
        assertTrue(e.getMessage().startsWith("server connections=1: the runs' words differ"), e.getMessage());
    }
}
