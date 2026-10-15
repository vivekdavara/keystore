package io.github.vivekdavara.keystore.server;

import io.github.vivekdavara.keystore.server.Protocol.ErrorCode;
import io.github.vivekdavara.keystore.wal.WalFormat;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * A blocking client for one connection: each call sends a request and reads its reply. Not
 * thread-safe; use one client a thread.
 *
 * <p>An {@code ERR} reply throws {@link ServerException}. After any other exception (a timeout, a
 * broken connection, a reply that isn't one) the connection's state is unknown: close the client.
 * A write whose reply never came may or may not have happened.
 */
public final class Client implements Closeable {
    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;

    private Client(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedInputStream(socket.getInputStream(), 64 * 1024);
        this.out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
    }

    /** Connects with a 5-second connect timeout and a 30-second read timeout. */
    public static Client connect(InetSocketAddress address) throws IOException {
        return connect(address, Duration.ofSeconds(30));
    }

    /** Connects; a reply that takes longer than {@code timeout} throws {@link java.net.SocketTimeoutException}. */
    public static Client connect(InetSocketAddress address, Duration timeout) throws IOException {
        Objects.requireNonNull(address, "address");
        Socket socket = new Socket();
        try {
            socket.connect(address, 5_000);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(Math.toIntExact(Math.max(1, timeout.toMillis())));
            return new Client(socket);
        } catch (IOException | RuntimeException e) {
            socket.close();
            throw e;
        }
    }

    public void ping() throws IOException {
        Protocol.writeLine(out, "PING");
        expect("PONG");
    }

    /** The value of {@code key}, or empty if it has none. */
    public Optional<byte[]> get(byte[] key) throws IOException {
        checkKey(key);
        Protocol.writeLine(out, "GET " + key.length);
        out.write(key);
        String reply = reply();
        if (reply.equals("NOTFOUND")) {
            return Optional.empty();
        }
        String[] words = reply.split(" ", -1);
        if (words.length != 2 || !words[0].equals("VALUE")) {
            throw new IOException("not a reply to GET: " + reply);
        }
        long length = Protocol.number(words[1]);
        if (length > WalFormat.MAX_VALUE_BYTES) {
            throw new IOException("a value of " + length + " bytes, past the limit");
        }
        return Optional.of(Protocol.readExactly(in, (int) length));
    }

    /** Sets {@code key} to {@code value}; returns the write's sequence number. */
    public long put(byte[] key, byte[] value) throws IOException {
        checkKey(key);
        Objects.requireNonNull(value, "value");
        if (value.length > WalFormat.MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("a value must be at most " + WalFormat.MAX_VALUE_BYTES + " bytes");
        }
        Protocol.writeLine(out, "PUT " + key.length + " " + value.length);
        out.write(key);
        out.write(value);
        return sequence(reply());
    }

    /** Deletes {@code key}; returns the write's sequence number. */
    public long delete(byte[] key) throws IOException {
        checkKey(key);
        Protocol.writeLine(out, "DEL " + key.length);
        out.write(key);
        return sequence(reply());
    }

    /** The words of the server's {@code INFO} line after {@code INFO}, such as {@code role=leader seq=12}. */
    public String info() throws IOException {
        Protocol.writeLine(out, "INFO");
        String reply = reply();
        if (!reply.startsWith("INFO ")) {
            throw new IOException("not a reply to INFO: " + reply);
        }
        return reply.substring(5);
    }

    /** One value from {@link #info()}, such as {@code info("seq")}; null if it isn't there. */
    public String info(String name) throws IOException {
        for (String word : info().split(" ")) {
            if (word.startsWith(name + "=")) {
                return word.substring(name.length() + 1);
            }
        }
        return null;
    }

    private static void checkKey(byte[] key) {
        Objects.requireNonNull(key, "key");
        if (key.length < 1 || key.length > WalFormat.MAX_KEY_BYTES) {
            throw new IllegalArgumentException("a key must be 1 to " + WalFormat.MAX_KEY_BYTES + " bytes");
        }
    }

    private void expect(String line) throws IOException {
        String reply = reply();
        if (!reply.equals(line)) {
            throw new IOException("expected " + line + ", got " + reply);
        }
    }

    private static long sequence(String reply) throws IOException {
        if (!reply.startsWith("OK ")) {
            throw new IOException("not a reply to a write: " + reply);
        }
        return Protocol.number(reply.substring(3));
    }

    /** Sends the request and reads one reply line; an {@code ERR} line throws {@link ServerException}. */
    private String reply() throws IOException {
        out.flush();
        String line = Protocol.readLine(in);
        if (line == null) {
            throw new EOFException("the server closed the connection");
        }
        if (line.startsWith("ERR ")) {
            throw error(line);
        }
        return line;
    }

    /** The exception for an {@code ERR} line. */
    static ServerException error(String line) {
        String rest = line.substring(4);
        int space = rest.indexOf(' ');
        String name = space < 0 ? rest : rest.substring(0, space);
        String text = space < 0 ? "" : rest.substring(space + 1);
        ErrorCode code;
        try {
            code = ErrorCode.valueOf(name);
        } catch (IllegalArgumentException e) {
            code = ErrorCode.INTERNAL;
            text = rest;
        }
        return new ServerException(code, text);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
