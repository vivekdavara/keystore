package io.github.vivekdavara.keystore.cli;

import io.github.vivekdavara.keystore.Bytes;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.StoreDump;
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
 * keystore flush DIR            flush the memtable to a table now
 * keystore compact DIR          merge every table into one
 * keystore dump DIR             list the tables, the logs' records, and what recovery would do;
 *                               changes nothing
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
            "       keystore flush DIR",
            "       keystore compact DIR",
            "       keystore dump DIR");

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        String command = args.length > 0 ? args[0] : "";
        int arity = switch (command) {
            case "put" -> 4;
            case "get", "delete" -> 3;
            case "flush", "compact", "dump" -> 2;
            default -> -1;
        };
        if (arity < 0 || args.length != arity) {
            err.println(USAGE);
            return 2;
        }
        Path dir = Path.of(args[1]);
        try {
            boolean exists = Files.exists(dir.resolve("MANIFEST"));
            if (!exists && !command.equals("put") && !command.equals("delete")) {
                // Opening would create a store there; only a write should create one.
                err.println("keystore: no store at " + dir);
                return 1;
            }
            if (command.equals("dump")) {
                return StoreDump.dump(dir, out) == WalDump.Verdict.REFUSED ? 3 : 0;
            }
            try (Store store = Store.open(dir)) {
                switch (command) {
                    case "put" -> store.put(Bytes.utf8(args[2]), Bytes.utf8(args[3]));
                    case "delete" -> store.delete(Bytes.utf8(args[2]));
                    case "flush" -> store.flush();
                    case "compact" -> store.compactAll();
                    default -> {
                        byte[] key = Bytes.utf8(args[2]);
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
