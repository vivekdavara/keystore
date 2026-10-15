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

    @Test
    void theRunItExampleIsWhatTheCommandsPrint() throws IOException {
        String readme = Files.readString(Path.of("README.md"), StandardCharsets.UTF_8);
        String section = readme.substring(readme.indexOf("## Run it"), readme.indexOf("## Tests"));

        List<String[]> commands = new ArrayList<>();
        // The commands on a store directory; the server commands (serve, follow, client) run until stopped,
        // and scripts/replication-demo.sh runs those in CI.
        Matcher command = Pattern.compile("```bash\nscripts/keystore ((?:put|get|delete|flush|compact|dump) .+)\n```")
                .matcher(section);
        while (command.find()) {
            commands.add(command.group(1).split(" "));
        }
        assertEquals(5, commands.size(), "put, flush, get, delete and dump");

        Matcher shown = Pattern.compile("it prints:\n\n```\n(.*?)```", Pattern.DOTALL).matcher(section);
        assertTrue(shown.find(), "the README shows the dump's output");
        String expected = shown.group(1);

        // The README's directory is target/demo; run in a fresh one and put the README's name back.
        String demo = "target/demo";
        String printed = "";
        for (String[] args : commands) {
            assertEquals(demo, args[1]);
            args[1] = dir.toString();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            int status = Main.run(args, new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(new ByteArrayOutputStream()));
            assertEquals(0, status, String.join(" ", args));
            printed = out.toString(StandardCharsets.UTF_8);
            if (args[0].equals("get")) {
                assertTrue(section.contains("`get` prints `" + printed.strip() + "`"), printed);
            }
        }
        assertEquals(expected, printed.replace(dir.toString(), demo));
    }
}
