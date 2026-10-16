package io.github.vivekdavara.keystore.cli;

import io.github.vivekdavara.keystore.Bytes;
import io.github.vivekdavara.keystore.Options;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.StoreCheck;
import io.github.vivekdavara.keystore.StoreDump;
import io.github.vivekdavara.keystore.server.Client;
import io.github.vivekdavara.keystore.server.Follower;
import io.github.vivekdavara.keystore.server.Protocol;
import io.github.vivekdavara.keystore.server.Server;
import io.github.vivekdavara.keystore.wal.WalDump;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

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
 * keystore verify DIR           read every table block and log record and check them; exit 3 if
 *                               anything is damaged; changes nothing
 * keystore snapshot DIR TARGET  copy the store into TARGET (a new directory), which opens as a
 *                               store; its tables are hard links where the file system allows
 * keystore serve DIR PORT       serve DIR as a leader on 127.0.0.1:PORT until stopped, keeping
 *                               64 MiB of flushed logs for followers (PORT 0: any free port)
 * keystore follow DIR PORT LEADER_HOST:PORT
 *                               keep DIR a copy of the leader's, and serve reads on PORT
 * keystore client HOST:PORT put KEY VALUE | get KEY | delete KEY | info
 *                               one request to a server; put and delete print the sequence number
 * </pre>
 *
 * Exit status: 0 done, 1 not found or an error, 2 usage, 3 the log is damaged so that a store
 * would refuse to open (dump), or a table or log is damaged (verify). {@code serve} and {@code follow} run until SIGTERM or SIGINT,
 * then close the server, the follower and the store, and exit as the signal says (143 for
 * SIGTERM, 130 for SIGINT); they exit 1 or 2 if they can't start.
 */
public final class Main {
    private Main() {}

    static final String USAGE = String.join("\n",
            "usage: keystore put DIR KEY VALUE",
            "       keystore get DIR KEY",
            "       keystore delete DIR KEY",
            "       keystore flush DIR",
            "       keystore compact DIR",
            "       keystore dump DIR",
            "       keystore verify DIR",
            "       keystore snapshot DIR TARGET",
            "       keystore serve DIR PORT",
            "       keystore follow DIR PORT LEADER_HOST:PORT",
            "       keystore client HOST:PORT put KEY VALUE | get KEY | delete KEY | info");

    /** The flushed log a leader started by {@code serve} keeps for its followers. */
    static final long SERVE_RETAIN_LOG_BYTES = 64L << 20;

    public static void main(String[] args) {
        String command = args.length > 0 ? args[0] : "";
        if (command.equals("serve") || command.equals("follow")) {
            int status = serveUntilStopped(args, System.out, System.err);
            if (status >= 0) {
                System.exit(status);
            }
            return; // stopped by a signal: the JVM exits (143 for SIGTERM, 130 for SIGINT) once the hook is done
        }
        System.exit(run(args, System.out, System.err));
    }

    /**
     * {@code serve} and {@code follow}: start, then wait for SIGTERM or SIGINT, whose shutdown hook
     * closes everything. Returns an exit status if it couldn't start, or -1 once stopped.
     */
    private static int serveUntilStopped(String[] args, PrintStream out, PrintStream err) {
        Closeable node;
        try {
            node = start(args, out, err);
        } catch (UsageException e) {
            err.println(USAGE);
            return 2;
        } catch (IOException | IllegalArgumentException e) {
            err.println("keystore: " + e.getMessage());
            return 1;
        }
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                node.close();
                err.println("keystore: stopped");
            } catch (IOException e) {
                err.println("keystore: " + e.getMessage());
            }
            stopped.countDown();
        }));
        try {
            stopped.await(); // the hook runs on a signal, and the JVM exits after it
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return -1;
    }

    static final class UsageException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Starts {@code serve DIR PORT} or {@code follow DIR PORT LEADER}, printing where it listens;
     * closing the result stops the server (and the follower), then closes the store.
     */
    static Closeable start(String[] args, PrintStream out, PrintStream err) throws IOException, UsageException {
        boolean follow = args[0].equals("follow");
        if (args.length != (follow ? 4 : 3)) {
            throw new UsageException();
        }
        Path dir = Path.of(args[1]);
        int port = args[2].equals("0") ? 0 : port(args[2]); // 0: any free port (the line printed says which)
        InetSocketAddress leader = follow ? address(args[3]) : null;
        Options options = follow ? Options.defaults() : Options.defaults().withRetainLogBytes(SERVE_RETAIN_LOG_BYTES);
        Store store = Store.open(dir, options);
        Follower follower = null;
        Server server = null;
        try {
            if (follow) {
                follower = Follower.start(store, Follower.Config.of(leader));
                server = Server.follower(store, Server.Config.loopback(port), follower::info);
                out.printf("keystore: following %s into %s (at #%d), serving reads on %s%n",
                        args[3], store.directory(), store.lastSequence(), hostAndPort(server));
            } else {
                server = Server.leader(store, Server.Config.loopback(port));
                out.printf("keystore: leader for %s (at #%d) on %s%n", store.directory(), store.lastSequence(),
                        hostAndPort(server));
            }
            out.flush();
        } catch (IOException | RuntimeException e) {
            closeAll(server, follower, store);
            throw e;
        }
        Server s = server;
        Follower f = follower;
        return () -> closeAll(s, f, store);
    }

    private static String hostAndPort(Server server) {
        return server.address().getAddress().getHostAddress() + ":" + server.port();
    }

    private static void closeAll(Server server, Follower follower, Store store) throws IOException {
        try {
            if (server != null) {
                server.close();
            }
        } finally {
            try {
                if (follower != null) {
                    follower.close();
                }
            } finally {
                store.close();
            }
        }
    }

    private static int port(String word) throws UsageException {
        try {
            long port = Protocol.number(word);
            if (port < 1 || port > 65_535) {
                throw new UsageException();
            }
            return (int) port;
        } catch (Protocol.ProtocolException e) {
            throw new UsageException();
        }
    }

    /** {@code HOST:PORT}. */
    static InetSocketAddress address(String word) throws UsageException {
        int colon = word.lastIndexOf(':');
        if (colon <= 0) {
            throw new UsageException();
        }
        return new InetSocketAddress(word.substring(0, colon), port(word.substring(colon + 1)));
    }

    /** {@code client HOST:PORT ...}: one request. */
    private static int client(String[] args, PrintStream out, PrintStream err) {
        String request = args.length > 2 ? args[2] : "";
        int arity = switch (request) {
            case "put" -> 5;
            case "get", "delete" -> 4;
            case "info" -> 3;
            default -> -1;
        };
        InetSocketAddress address;
        try {
            if (arity < 0 || args.length != arity) {
                throw new UsageException();
            }
            address = address(args[1]);
        } catch (UsageException e) {
            err.println(USAGE);
            return 2;
        }
        try (Client client = Client.connect(address)) {
            switch (request) {
                case "put" -> out.println(client.put(Bytes.utf8(args[3]), Bytes.utf8(args[4])));
                case "delete" -> out.println(client.delete(Bytes.utf8(args[3])));
                case "info" -> out.println(client.info());
                default -> {
                    Optional<byte[]> value = client.get(Bytes.utf8(args[3]));
                    if (value.isEmpty()) {
                        err.println("keystore: " + args[3] + ": not found");
                        return 1;
                    }
                    out.write(value.get());
                    out.println();
                }
            }
            return 0;
        } catch (IOException | IllegalArgumentException e) {
            err.println("keystore: " + e.getMessage());
            return 1;
        }
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        String command = args.length > 0 ? args[0] : "";
        if (command.equals("client")) {
            return client(args, out, err);
        }
        int arity = switch (command) {
            case "put" -> 4;
            case "get", "delete", "snapshot" -> 3;
            case "flush", "compact", "dump", "verify" -> 2;
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
            if (command.equals("verify")) {
                return StoreCheck.verify(dir, out) > 0 ? 3 : 0;
            }
            try (Store store = Store.open(dir)) {
                switch (command) {
                    case "put" -> store.put(Bytes.utf8(args[2]), Bytes.utf8(args[3]));
                    case "delete" -> store.delete(Bytes.utf8(args[2]));
                    case "flush" -> store.flush();
                    case "compact" -> store.compactAll();
                    case "snapshot" -> {
                        Store.Snapshot s = store.snapshot(Path.of(args[2]));
                        out.printf("keystore: snapshot of %s at #%d in %s: %d tables, %d bytes, %d hard-linked%n",
                                args[1], s.lastSequence(), args[2], s.tables(), s.bytes(), s.linked());
                    }
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
