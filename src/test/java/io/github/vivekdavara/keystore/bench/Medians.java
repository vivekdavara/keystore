package io.github.vivekdavara.keystore.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The medians of {@code scripts/bench.sh}'s output, which the release check prints at its end. Lines
 * are grouped by what comes before their first ": " (the benchmark and its parameters), in the order
 * the groups first appear; each number after it is replaced by its median over the group's runs, with
 * the range in brackets when the runs differ.
 *
 * <pre>
 * java -cp target/classes:target/test-classes io.github.vivekdavara.keystore.bench.Medians FILE
 * </pre>
 *
 * A number is digits with an optional fraction, not part of a word: in "p99.9 8.3 us" only 8.3 is one.
 * Lines without ": " are skipped. A median keeps the decimals the first run printed; for an even
 * number of runs it is the mean of the middle two.
 */
public final class Medians {
    private Medians() {}

    private static final Pattern NUMBER = Pattern.compile("(?<![\\w.])\\d+(?:\\.(\\d+))?(?!\\w)");

    public static void main(String[] args) throws IOException {
        for (String line : summarise(Files.readAllLines(Path.of(args[0]), StandardCharsets.UTF_8))) {
            System.out.println(line);
        }
    }

    /** One line a benchmark: its median and range for each number its runs printed. */
    static List<String> summarise(List<String> lines) {
        Map<String, List<String>> runs = new LinkedHashMap<>();
        for (String line : lines) {
            int colon = line.indexOf(": ");
            if (colon > 0) {
                runs.computeIfAbsent(line.substring(0, colon), k -> new ArrayList<>()).add(line.substring(colon + 2));
            }
        }
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, List<String>> group : runs.entrySet()) {
            out.add(group.getKey() + ", median of " + group.getValue().size() + ": "
                    + summarise(group.getKey(), group.getValue()));
        }
        return out;
    }

    private static String summarise(String benchmark, List<String> texts) {
        String words = NUMBER.matcher(texts.get(0)).replaceAll("#");
        List<double[]> values = new ArrayList<>();
        for (String text : texts) {
            if (!NUMBER.matcher(text).replaceAll("#").equals(words)) {
                throw new IllegalArgumentException(benchmark + ": the runs' words differ:\n" + texts.get(0) + "\n" + text);
            }
            values.add(NUMBER.matcher(text).results().mapToDouble(m -> Double.parseDouble(m.group())).toArray());
        }
        StringBuilder line = new StringBuilder();
        Matcher first = NUMBER.matcher(texts.get(0));
        int i = 0;
        while (first.find()) {
            int decimals = first.group(1) == null ? 0 : first.group(1).length();
            double[] column = new double[values.size()];
            for (int run = 0; run < column.length; run++) {
                column[run] = values.get(run)[i];
            }
            Arrays.sort(column);
            int n = column.length;
            double median = n % 2 == 1 ? column[n / 2] : (column[n / 2 - 1] + column[n / 2]) / 2;
            String summary = format(median, decimals);
            if (column[0] != column[n - 1]) {
                summary += " [" + format(column[0], decimals) + ".." + format(column[n - 1], decimals) + "]";
            }
            first.appendReplacement(line, Matcher.quoteReplacement(summary));
            i++;
        }
        first.appendTail(line);
        return line.toString();
    }

    private static String format(double value, int decimals) {
        return String.format(Locale.ROOT, "%." + decimals + "f", value);
    }
}
