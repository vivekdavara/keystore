package io.github.vivekdavara.keystore.server;

import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.StoreFailedException;
import io.github.vivekdavara.keystore.server.Protocol.ErrorCode;
import io.github.vivekdavara.keystore.server.Protocol.ProtocolException;
import io.github.vivekdavara.keystore.server.Protocol.Request;
import io.github.vivekdavara.keystore.wal.LogFullException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

/**
 * A TCP server for one store, speaking {@link Protocol}. Each connection gets a virtual thread that
 * reads a request, answers it, and reads the next: a connection's requests are answered in order,
 * and a reply is flushed only when no further request is already waiting, so pipelined requests
 * share writes. Reads and writes go straight to the store, which is thread-safe.
 *
 * <p>A leader takes writes and serves {@code SYNC} to followers; a follower answers reads and
 * {@code INFO}, and refuses writes with {@code ERR READONLY} (its writes come from its leader).
 *
 * <p>Handler threads are never interrupted: an interrupt during a store's file I/O closes the
 * channel, which would fail the store. {@link #close} closes the sockets instead.
 */
public final class Server implements Closeable {
    /** What a server does with writes. */
    public enum Role {
        LEADER, FOLLOWER
    }

    /**
     * How a server listens.
     *
     * @param address where to listen; port 0 picks a free one ({@link #port()} says which)
     * @param maxConnections connections beyond this many get {@code ERR BUSY} and are closed
     * @param heartbeat how often an idle {@code SYNC} stream sends {@code HEARTBEAT}
     */
    public record Config(InetSocketAddress address, int maxConnections, Duration heartbeat) {
        public Config {
            Objects.requireNonNull(address, "address");
            Objects.requireNonNull(heartbeat, "heartbeat");
            if (maxConnections < 1) {
                throw new IllegalArgumentException("maxConnections must be at least 1, not " + maxConnections);
            }
            if (heartbeat.toMillis() < 1) {
                throw new IllegalArgumentException("heartbeat must be at least a millisecond, not " + heartbeat);
            }
        }

        /** The loopback address and {@code port}, 256 connections, a heartbeat a second. */
        public static Config loopback(int port) {
            return new Config(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 256, Duration.ofSeconds(1));
        }

        public Config withMaxConnections(int maxConnections) {
            return new Config(address, maxConnections, heartbeat);
        }

        public Config withHeartbeat(Duration heartbeat) {
            return new Config(address, maxConnections, heartbeat);
        }
    }

    private static final int BUFFER_BYTES = 64 * 1024;

    private final Store store;
    private final Role role;
    private final Config config;
    private final Supplier<String> status;
    private final ServerSocket listener;
    private final Semaphore slots;
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private final Set<Thread> handlers = ConcurrentHashMap.newKeySet();
    private final Thread acceptor;
    private volatile boolean closing;

    private Server(Store store, Role role, Config config, Supplier<String> status) throws IOException {
        this.store = Objects.requireNonNull(store, "store");
        this.role = Objects.requireNonNull(role, "role");
        this.config = Objects.requireNonNull(config, "config");
        this.status = status;
        this.slots = new Semaphore(config.maxConnections());
        this.listener = new ServerSocket();
        try {
            listener.setReuseAddress(true);
            listener.bind(config.address(), 128);
        } catch (IOException | RuntimeException e) {
            listener.close();
            throw e;
        }
        this.acceptor = Thread.ofPlatform().name("keystore-accept-" + listener.getLocalPort()).daemon().unstarted(this::accept);
    }

    /** Starts a leader: it takes writes and serves {@code SYNC}. */
    public static Server leader(Store store, Config config) throws IOException {
        return start(new Server(store, Role.LEADER, config, null));
    }

    /**
     * Starts a follower's server: reads only. {@code status} adds words to {@code INFO} (the
     * follower's state and its leader's position); it may be null.
     */
    public static Server follower(Store store, Config config, Supplier<String> status) throws IOException {
        return start(new Server(store, Role.FOLLOWER, config, status));
    }

    private static Server start(Server server) {
        server.acceptor.start();
        return server;
    }

    /** The port the server listens on. */
    public int port() {
        return listener.getLocalPort();
    }

    /** Where the server listens. */
    public InetSocketAddress address() {
        return new InetSocketAddress(listener.getInetAddress(), listener.getLocalPort());
    }

    public Role role() {
        return role;
    }

    /** Connections open now. */
    public int connections() {
        return connections.size();
    }

    private void accept() {
        while (!closing) {
            Socket socket;
            try {
                socket = listener.accept();
            } catch (IOException e) {
                if (closing) {
                    return;
                }
                pause(); // out of file descriptors, say: don't spin
                continue;
            }
            if (!slots.tryAcquire()) {
                refuse(socket);
                continue;
            }
            connections.add(socket);
            Thread handler = Thread.ofVirtual().name("keystore-connection").unstarted(() -> serve(socket));
            handlers.add(handler);
            if (closing) { // close() may have run between accept and the add: it missed this socket
                handlers.remove(handler);
                connections.remove(socket);
                slots.release();
                closeQuietly(socket);
                return;
            }
            handler.start();
        }
    }

    /** Answers {@code ERR BUSY} and closes, on a thread of its own so the acceptor never waits on a client. */
    private void refuse(Socket socket) {
        Thread.ofVirtual().name("keystore-refuse").start(() -> {
            try (socket) {
                OutputStream out = socket.getOutputStream();
                Protocol.writeLine(out, Protocol.errorLine(ErrorCode.BUSY, config.maxConnections() + " connections already"));
                out.flush();
                drain(socket);
            } catch (IOException ignored) {
                // The client went first.
            }
        });
    }

    /**
     * Ends a connection after an error reply in a way that lets the client read the reply. Closing a
     * socket with unread bytes in its receive buffer sends a reset instead of an end of stream, and a
     * reset can make the client's next read fail before it reads the reply. So: half-close (the
     * client reads the reply, then the end of the stream), then read and discard what the client
     * still sends, for at most a second and a MiB, until it closes its end.
     */
    private static void drain(Socket socket) {
        try {
            socket.shutdownOutput();
            socket.setSoTimeout(1_000);
            InputStream in = socket.getInputStream();
            byte[] sink = new byte[8192];
            for (long total = 0; total < (1 << 20); ) {
                int n = in.read(sink);
                if (n < 0) {
                    return;
                }
                total += n;
            }
        } catch (IOException ignored) {
            // A timeout, or the client reset: closing anyway.
        }
    }

    private void serve(Socket socket) {
        try (socket) {
            socket.setTcpNoDelay(true);
            InputStream in = new BufferedInputStream(socket.getInputStream(), BUFFER_BYTES);
            OutputStream out = new BufferedOutputStream(socket.getOutputStream(), BUFFER_BYTES);
            try {
                for (;;) {
                    String line = Protocol.readLine(in);
                    if (line == null) {
                        return; // the client closed between requests
                    }
                    if (!answer(Protocol.parse(line), in, out)) {
                        return;
                    }
                    if (in.available() == 0) {
                        out.flush(); // nothing more is waiting: send what this connection has
                    }
                }
            } catch (ProtocolException e) {
                Protocol.writeLine(out, Protocol.errorLine(ErrorCode.PROTOCOL, e.getMessage()));
                out.flush();
                drain(socket); // where the next request starts is unknown: this connection is done
            }
        } catch (IOException e) {
            // The connection broke, or the server is closing: nothing to answer.
        } finally {
            connections.remove(socket);
            handlers.remove(Thread.currentThread());
            slots.release();
        }
    }

    /** Answers one request; false if the connection should close. */
    private boolean answer(Request request, InputStream in, OutputStream out) throws IOException {
        switch (request.command()) {
            case PING -> Protocol.writeLine(out, "PONG");
            case INFO -> Protocol.writeLine(out, info());
            case GET -> {
                byte[] key = Protocol.readExactly(in, request.keyLength());
                Optional<byte[]> value;
                try {
                    value = store.get(key);
                } catch (IOException | IllegalStateException e) {
                    Protocol.writeLine(out, Protocol.errorLine(ErrorCode.INTERNAL, e.getMessage()));
                    return true;
                }
                if (value.isEmpty()) {
                    Protocol.writeLine(out, "NOTFOUND");
                } else {
                    Protocol.writeLine(out, "VALUE " + value.get().length);
                    out.write(value.get());
                }
            }
            case PUT, DEL -> {
                // Read the data first, even to refuse it: the next request starts after it.
                byte[] key = Protocol.readExactly(in, request.keyLength());
                byte[] value = request.command() == Protocol.Command.PUT
                        ? Protocol.readExactly(in, request.valueLength())
                        : null;
                Protocol.writeLine(out, write(key, value));
            }
            case SYNC -> {
                if (role == Role.FOLLOWER) {
                    Protocol.writeLine(out, Protocol.errorLine(ErrorCode.READONLY, "a follower serves no SYNC"));
                    return true;
                }
                Protocol.writeLine(out, Protocol.errorLine(ErrorCode.INTERNAL, "SYNC is not built yet"));
                return true;
            }
        }
        return true;
    }

    /** Makes a write and returns the reply line. */
    private String write(byte[] key, byte[] value) {
        if (role == Role.FOLLOWER) {
            return Protocol.errorLine(ErrorCode.READONLY, "a follower takes no writes; write to its leader");
        }
        try {
            long seq = value == null ? store.delete(key) : store.put(key, value);
            return "OK " + seq;
        } catch (LogFullException e) {
            return Protocol.errorLine(ErrorCode.FULL, e.getMessage());
        } catch (StoreFailedException e) {
            return Protocol.errorLine(ErrorCode.FAILED, e.getMessage());
        } catch (IOException e) {
            // The write may or may not be in the log, and the store takes no more: as for a crash,
            // the client can't know whether it happened.
            return Protocol.errorLine(ErrorCode.FAILED, e.getMessage());
        } catch (IllegalStateException e) {
            return Protocol.errorLine(ErrorCode.INTERNAL, e.getMessage()); // the store is closed
        }
    }

    private String info() {
        String line = "INFO role=" + role.name().toLowerCase(Locale.ROOT) + " seq=" + store.lastSequence()
                + " connections=" + connections.size();
        if (status != null) {
            line += " " + status.get();
        }
        return line.length() < Protocol.MAX_LINE_BYTES ? line : line.substring(0, Protocol.MAX_LINE_BYTES - 1);
    }

    /**
     * Stops accepting, closes every connection, and waits for their threads to finish (a request in
     * progress finishes its store call; its reply fails). Doesn't close the store.
     */
    @Override
    public void close() throws IOException {
        closing = true;
        listener.close();
        for (Socket socket : connections) {
            closeQuietly(socket);
        }
        try {
            acceptor.join(5_000);
            for (Thread handler : handlers) {
                handler.join(5_000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Closing anyway.
        }
    }

    private static void pause() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
