package io.github.vivekdavara.keystore.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The README's "Run it" commands, run as written, print what the README says they print. */
class ReadmeTest {
    @TempDir
    Path dir;

    /** The README's store directories, and where this test runs them instead. Longest first. */
    private static final List<String> DIRECTORIES = List.of("target/demo-snapshot", "target/demo");

    private static String readme() throws IOException {
        return Files.readString(Path.of("README.md"), StandardCharsets.UTF_8);
    }

    private static String between(String text, String start, String end) {
        int from = text.indexOf(start);
        assertTrue(from >= 0, start);
        int to = text.indexOf(end, from);
        assertTrue(to >= 0, end);
        return text.substring(from, to);
    }

    /** Runs a README command in this test's directory; returns what it printed, with the README's names put back. */
    private String run(String command) {
        String[] args = command.split(" ");
        for (int i = 0; i < args.length; i++) {
            if (DIRECTORIES.contains(args[i])) {
                args[i] = dir.resolve(Path.of(args[i]).getFileName()).toString();
            }
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int status = Main.run(args, new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(new ByteArrayOutputStream()));
        assertEquals(0, status, command);
        String printed = out.toString(StandardCharsets.UTF_8);
        for (String name : DIRECTORIES) {
            printed = printed.replace(dir.resolve(Path.of(name).getFileName()).toString(), name);
        }
        return printed;
    }

    @Test
    void theRunItExampleIsWhatTheCommandsPrint() throws IOException {
        String section = between(readme(), "## Run it", "## Tests");

        List<String> commands = new ArrayList<>();
        // The commands on a store directory; the server commands (serve, follow, client) run until stopped,
        // and scripts/replication-demo.sh runs those in CI.
        Matcher command = Pattern.compile("```bash\nscripts/keystore ((?:put|get|delete|flush|compact|dump) .+)\n```")
                .matcher(section);
        while (command.find()) {
            commands.add(command.group(1));
        }
        assertEquals(5, commands.size(), "put, flush, get, delete and dump");

        Matcher shown = Pattern.compile("it prints:\n\n```\n(.*?)```", Pattern.DOTALL).matcher(section);
        assertTrue(shown.find(), "the README shows the dump's output");

        String printed = "";
        for (String c : commands) {
            assertTrue(c.split(" ")[1].equals("target/demo"), c);
            printed = run(c);
            if (c.startsWith("get ")) {
                assertTrue(section.contains("`get` prints `" + printed.strip() + "`"), printed);
            }
        }
        assertEquals(shown.group(1), printed);
    }

    /** "Check and copy a store": what verify reports, the snapshot's line, and a read from the snapshot. */
    @Test
    void theCheckAndCopyExampleIsWhatTheCommandsPrint() throws IOException {
        String section = between(readme(), "**Check and copy a store.**", "**Benchmarks**");

        Matcher after = Pattern.compile("After\\s+(.+?):\n", Pattern.DOTALL).matcher(section);
        assertTrue(after.find(), "the commands that make the store");
        Matcher quoted = Pattern.compile("`([^`]+)`").matcher(after.group(1));
        int setUp = 0;
        while (quoted.find()) {
            run(quoted.group(1));
            setUp++;
        }
        assertEquals(3, setUp, "put, flush, put");

        Matcher verify = Pattern.compile("```bash\nscripts/keystore (verify [^\n]+)\n```\n\nprints\n\n```\n(.*?)```",
                Pattern.DOTALL).matcher(section);
        assertTrue(verify.find(), "verify and what it prints");
        assertEquals(verify.group(2), run(verify.group(1)));

        Matcher snapshot = Pattern.compile("```bash\nscripts/keystore (snapshot [^\n]+)\n```\n\nprints `([^`]+)`, and\\s+"
                + "`(get [^`]+)` prints `([^`]+)`").matcher(section);
        assertTrue(snapshot.find(), "snapshot, what it prints, and a read from the snapshot");
        assertEquals(snapshot.group(2) + "\n", run(snapshot.group(1)));
        assertEquals(snapshot.group(4) + "\n", run(snapshot.group(3)));
    }

    /**
     * "Which build": the formats line exactly; the first line's version and Java runtime are this build's,
     * which the README's needn't be.
     */
    @Test
    void theVersionExampleIsWhatTheCommandPrints() throws IOException {
        String section = between(readme(), "**Which build.**", "**Check and copy a store.**");
        Matcher shown = Pattern.compile("```bash\nscripts/keystore version\n```\n\nprints\n\n```\n(.*?)```", Pattern.DOTALL)
                .matcher(section);
        assertTrue(shown.find(), "version and what it prints");
        Pattern firstLine = Pattern.compile("keystore [^ ]+ \\(Java [^ )]+\\)\n");
        String expected = shown.group(1);
        String printed = run("version");
        Matcher e = firstLine.matcher(expected);
        Matcher p = firstLine.matcher(printed);
        assertTrue(e.lookingAt(), expected);
        assertTrue(p.lookingAt(), printed);
        assertEquals(expected.substring(e.end()), printed.substring(p.end()));
    }
}
