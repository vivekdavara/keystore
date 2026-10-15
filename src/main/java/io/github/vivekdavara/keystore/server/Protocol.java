package io.github.vivekdavara.keystore.server;

import io.github.vivekdavara.keystore.wal.WalFormat;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * The wire protocol. A request is one line of printable ASCII ending in a newline, naming a command
 * and the lengths of the binary data that follows the newline; a reply is a line too, followed by
 * data when it gives a length. Lengths before data, as in memcached's text protocol, so keys and
 * values are any bytes and nothing is escaped or scanned for.
 *
 * <pre>
 * request                         reply
 * PING                            PONG
 * GET klen \n key                 VALUE vlen \n value      or  NOTFOUND
 * PUT klen vlen \n key value      OK seq
 * DEL klen \n key                 OK seq
 * INFO                            INFO name=value ...
 * SYNC from [checksum]            a stream of REC n \n record, and HEARTBEAT seq (leaders only)
 * anything                        ERR CODE text
 * </pre>
 *
 * Words are separated by one space. A number is decimal digits, without a sign or a leading zero.
 * A line is at most {@link #MAX_LINE_BYTES} bytes with its newline. Key lengths are 1 to 65,536 and
 * value lengths 0 to 16 MiB, as in the store, and are checked before anything is allocated. A
 * request that breaks these rules gets {@code ERR PROTOCOL} and the connection is closed, since
 * where the next request starts is no longer known.
 */
public final class Protocol {
    private Protocol() {}

    /** The longest line, newline included. */
    public static final int MAX_LINE_BYTES = 128;

    /** Why a request failed, the second word of an {@code ERR} reply. */
    public enum ErrorCode {
        /** The request broke the protocol; the server closes the connection. */
        PROTOCOL,
        /** A write, or SYNC, sent to a follower. */
        READONLY,
        /** The log is full: nothing was written, and the store goes on. */
        FULL,
        /** The store's writes failed; it takes none until it is reopened. */
        FAILED,
        /** The server has as many connections as it allows; it closes this one. */
        BUSY,
        /** SYNC: the leader no longer has the log for that sequence number. The text is the oldest it has. */
        BEHIND,
        /** SYNC: the follower has writes the leader doesn't. The text is the leader's last sequence number. */
        AHEAD,
        /**
         * SYNC: the follower's write before {@code from} isn't the leader's (their checksums differ):
         * their histories have diverged. The text is that sequence number.
         */
        DIVERGED,
        /** Anything else: an I/O error, a closed store. */
        INTERNAL
    }

    /** A request that breaks the protocol. */
    public static final class ProtocolException extends IOException {
        private static final long serialVersionUID = 1L;

        public ProtocolException(String message) {
            super(message);
        }
    }

    /** The commands. */
    public enum Command {
        PING, GET, PUT, DEL, INFO, SYNC
    }

    /**
     * A request's line, parsed.
     *
     * @param keyLength GET, PUT and DEL: the key's bytes, which follow the line
     * @param valueLength PUT: the value's bytes, which follow the key
     * @param from SYNC: the first sequence number wanted
     * @param checksum SYNC: the stored checksum of the follower's write {@code from - 1}, which the
     *     leader checks against its own, or -1 if the follower didn't send one
     */
    public record Request(Command command, int keyLength, int valueLength, long from, long checksum) {}

    /**
     * Parses a request line (without its newline).
     *
     * @throws ProtocolException if it isn't a request, or a length is outside the limits
     */
    public static Request parse(String line) throws ProtocolException {
        String[] words = words(line);
        Command command = switch (words[0]) {
            case "PING" -> Command.PING;
            case "GET" -> Command.GET;
            case "PUT" -> Command.PUT;
            case "DEL" -> Command.DEL;
            case "INFO" -> Command.INFO;
            case "SYNC" -> Command.SYNC;
            default -> throw new ProtocolException("unknown command " + quote(words[0]));
        };
        int arity = switch (command) {
            case PING, INFO -> 1;
            case GET, DEL -> 2;
            case SYNC -> words.length == 3 ? 3 : 2; // the checksum is optional
            case PUT -> 3;
        };
        if (words.length != arity) {
            throw new ProtocolException(command + " takes " + (arity - 1) + " numbers, not " + (words.length - 1));
        }
        return switch (command) {
            case PING, INFO -> new Request(command, 0, 0, 0, -1);
            case GET, DEL -> new Request(command, keyLength(words[1]), 0, 0, -1);
            case PUT -> new Request(command, keyLength(words[1]), valueLength(words[2]), 0, -1);
            case SYNC -> {
                long from = number(words[1]);
                if (from < 1) {
                    throw new ProtocolException("sequence numbers start at 1, not " + from);
                }
                long checksum = -1;
                if (words.length == 3) {
                    checksum = number(words[2]);
                    if (checksum > 0xffffffffL || from == 1) {
                        throw new ProtocolException("a checksum is 0 to 4294967295, of the write before from");
                    }
                }
                yield new Request(command, 0, 0, from, checksum);
            }
        };
    }

    /** Splits a line into words separated by exactly one space. */
    static String[] words(String line) throws ProtocolException {
        if (line.isEmpty()) {
            throw new ProtocolException("an empty line");
        }
        String[] words = line.split(" ", -1);
        for (String word : words) {
            if (word.isEmpty()) {
                throw new ProtocolException("words must be separated by one space: " + quote(line));
            }
        }
        return words;
    }

    /**
     * A number: decimal digits, no sign, no leading zero, at most 18 of them (so it fits a long).
     *
     * @throws ProtocolException if it isn't one
     */
    public static long number(String word) throws ProtocolException {
        if (word.isEmpty() || word.length() > 18 || (word.length() > 1 && word.charAt(0) == '0')) {
            throw new ProtocolException("not a number: " + quote(word));
        }
        long n = 0;
        for (int i = 0; i < word.length(); i++) {
            char c = word.charAt(i);
            if (c < '0' || c > '9') {
                throw new ProtocolException("not a number: " + quote(word));
            }
            n = n * 10 + (c - '0');
        }
        return n;
    }

    private static int keyLength(String word) throws ProtocolException {
        long n = number(word);
        if (n < 1 || n > WalFormat.MAX_KEY_BYTES) {
            throw new ProtocolException("a key must be 1 to " + WalFormat.MAX_KEY_BYTES + " bytes, not " + n);
        }
        return (int) n;
    }

    private static int valueLength(String word) throws ProtocolException {
        long n = number(word);
        if (n > WalFormat.MAX_VALUE_BYTES) {
            throw new ProtocolException("a value must be at most " + WalFormat.MAX_VALUE_BYTES + " bytes, not " + n);
        }
        return (int) n;
    }

    /**
     * Reads one line and returns it without its newline, or null if the stream ends before its first
     * byte (the peer closed the connection between requests).
     *
     * @throws ProtocolException if the line is too long or holds a byte that isn't printable ASCII
     * @throws EOFException if the stream ends inside the line
     */
    public static String readLine(InputStream in) throws IOException {
        byte[] line = new byte[MAX_LINE_BYTES];
        int n = 0;
        for (;;) {
            int b = in.read();
            if (b < 0) {
                if (n == 0) {
                    return null;
                }
                throw new EOFException("the stream ended inside a line, after " + n + " bytes");
            }
            if (b == '\n') {
                return new String(line, 0, n, StandardCharsets.US_ASCII);
            }
            if (b < 0x20 || b > 0x7e) {
                throw new ProtocolException("byte " + b + " in a line; lines are printable ASCII");
            }
            if (n == MAX_LINE_BYTES - 1) {
                throw new ProtocolException("a line longer than " + MAX_LINE_BYTES + " bytes");
            }
            line[n++] = (byte) b;
        }
    }

    /** Reads exactly {@code n} bytes. @throws EOFException if the stream ends first */
    public static byte[] readExactly(InputStream in, int n) throws IOException {
        byte[] bytes = in.readNBytes(n);
        if (bytes.length != n) {
            throw new EOFException("the stream ended after " + bytes.length + " of " + n + " bytes");
        }
        return bytes;
    }

    /** Writes a line and its newline. The caller keeps lines to printable ASCII. */
    public static void writeLine(OutputStream out, String line) throws IOException {
        out.write(line.getBytes(StandardCharsets.US_ASCII));
        out.write('\n');
    }

    /** An {@code ERR} line: the code, then the text with anything unprintable replaced, cut to fit. */
    public static String errorLine(ErrorCode code, String text) {
        StringBuilder line = new StringBuilder("ERR ").append(code.name());
        if (text != null && !text.isEmpty()) {
            line.append(' ');
            for (int i = 0; i < text.length() && line.length() < MAX_LINE_BYTES - 1; i++) {
                char c = text.charAt(i);
                line.append(c >= 0x20 && c <= 0x7e ? c : '?');
            }
        }
        return line.toString();
    }

    /** A word for an error message, cut short (lines are printable ASCII already). */
    private static String quote(String word) {
        String cut = word.length() > 24 ? word.substring(0, 24) + "..." : word;
        return "\"" + cut + "\"";
    }
}
