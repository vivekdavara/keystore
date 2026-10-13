package io.github.vivekdavara.keystore.cli;

import io.github.vivekdavara.keystore.Bytes;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.wal.WalDump;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * The {@code keystore} command.
 *
 * <pre>
 * keystore put DIR KEY VALUE    set KEY to VALUE (UTF-8)
 * keystore get DIR KEY          print KEY's value; exit 1 if there is none (or no store)
 * keystore delete DIR KEY       delete KEY
 * keystore dump DIR             list the log's records and what recovery would do; changes nothing
 * </pre>
 *
 * Exit status: 0 done, 1 not found or an error, 2 usage, 3 the log is damaged so that a store
 * would refuse to open (dump only).
 */
public final class Main {
    private Main() {}

    static final String USAGE = String.join("\n",
            "usage: keystore put DIR KEY VALUE",
            "       keystore get DIR KEY",
            "       keystore delete DIR KEY",
            "       keystore dump DIR");

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        String command = args.length > 0 ? args[0] : "";
        int arity = switch (command) {
            case "put" -> 4;
            case "get", "delete" -> 3;
            case "dump" -> 2;
            default -> -1;
        };
        if (arity < 0 || args.length != arity) {
            err.println(USAGE);
            return 2;
        }
        Path dir = Path.of(args[1]);
        Path log = dir.resolve("wal.log");
        try {
            if (command.equals("dump")) {
                if (!Files.exists(log)) {
                    err.println("keystore: no log at " + log);
                    return 1;
                }
                return WalDump.dump(log, out) == WalDump.Verdict.REFUSED ? 3 : 0;
            }
            if (command.equals("get") && !Files.exists(log)) {
                // Opening would create a store there; a read shouldn't create anything.
                err.println("keystore: no store at " + dir);
                return 1;
            }
            try (Store store = Store.open(dir)) {
                byte[] key = Bytes.utf8(args[2]);
                switch (command) {
                    case "put" -> store.put(key, Bytes.utf8(args[3]));
                    case "delete" -> store.delete(key);
                    default -> {
                        Optional<byte[]> value = store.get(key);
                        if (value.isEmpty()) {
                            err.println("keystore: " + args[2] + ": not found");
                            return 1;
                        }
                        out.write(value.get());
                        out.println();
                    }
                }
            }
            return 0;
        } catch (IOException | IllegalArgumentException e) {
            err.println("keystore: " + e.getMessage());
            return 1;
        }
    }
}
