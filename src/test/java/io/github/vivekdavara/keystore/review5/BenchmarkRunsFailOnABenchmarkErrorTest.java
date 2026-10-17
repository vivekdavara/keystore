package io.github.vivekdavara.keystore.review5;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * CI's "JMH benchmarks, one short iteration each (a smoke run)" and the release check's {@code jmh}
 * step can't fail on a benchmark that throws: JMH 1.37 reports the iteration as {@code <failure>} and
 * still exits 0 unless it is given {@code -foe true} (checked with a benchmark that throws:
 * {@code java org.openjdk.jmh.Main -f 1 -wi 0 -i 1 -r 100ms probe.Throws} prints the exception and
 * exits 0). So a benchmark broken by a change keeps CI green, and release_check.sh, which "stops at
 * the first step that fails", says "all steps passed" with the README's JMH numbers missing.
 *
 * <p>Passes if {@code scripts/jmh.sh} always passes {@code -foe true}, or every call of it does.
 */
class BenchmarkRunsFailOnABenchmarkErrorTest {
    private static String read(String path) throws IOException {
        return Files.readString(Path.of(path), StandardCharsets.UTF_8);
    }

    @Test
    void everyJmhRunFailsWhenABenchmarkThrows() throws IOException {
        String jmh = read("scripts/jmh.sh");
        String main = jmh.lines().filter(l -> l.contains("org.openjdk.jmh.Main")).findFirst().orElseThrow();
        if (main.contains("-foe true")) {
            return; // the script always asks for it
        }
        List<String> calls = new ArrayList<>();
        for (String file : List.of(".github/workflows/ci.yml", "scripts/release_check.sh")) {
            read(file).lines().filter(l -> l.contains("scripts/jmh.sh") && !l.strip().startsWith("#"))
                    .forEach(l -> calls.add(file + ": " + l.strip()));
        }
        assertFalse(calls.isEmpty());
        for (String call : calls) {
            assertTrue(call.contains("-foe true"), "a benchmark that throws doesn't fail this run: " + call);
        }
    }
}
