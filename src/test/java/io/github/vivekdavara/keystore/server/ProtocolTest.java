package io.github.vivekdavara.keystore.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.server.Protocol.Command;
import io.github.vivekdavara.keystore.server.Protocol.ErrorCode;
import io.github.vivekdavara.keystore.server.Protocol.ProtocolException;
import io.github.vivekdavara.keystore.server.Protocol.Request;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ProtocolTest {
    @Test
    void parsesEveryCommand() throws ProtocolException {
        assertEquals(new Request(Command.PING, 0, 0, 0, -1), Protocol.parse("PING"));
        assertEquals(new Request(Command.INFO, 0, 0, 0, -1), Protocol.parse("INFO"));
        assertEquals(new Request(Command.GET, 3, 0, 0, -1), Protocol.parse("GET 3"));
        assertEquals(new Request(Command.DEL, 65536, 0, 0, -1), Protocol.parse("DEL 65536"));
        assertEquals(new Request(Command.PUT, 1, 0, 0, -1), Protocol.parse("PUT 1 0"));
        assertEquals(new Request(Command.PUT, 7, 16 * 1024 * 1024, 0, -1), Protocol.parse("PUT 7 16777216"));
        assertEquals(new Request(Command.SYNC, 0, 0, 1, -1), Protocol.parse("SYNC 1"));
        assertEquals(new Request(Command.SYNC, 0, 0, 999_999_999_999_999_999L, -1),
                Protocol.parse("SYNC 999999999999999999"));
        assertEquals(new Request(Command.SYNC, 0, 0, 2, 0), Protocol.parse("SYNC 2 0"));
        assertEquals(new Request(Command.SYNC, 0, 0, 12, 4_294_967_295L), Protocol.parse("SYNC 12 4294967295"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "", " ", "PING ", " PING", "PING  ", "ping", "NOPE", "GET", "GET 1 2", "PUT 1", "INFO 1",
        "GET  1", "GET 1 ", "PUT 1  2", // spacing
        "GET 0", "DEL 0", "GET 65537", "PUT 65537 1", "PUT 1 16777217", // limits, checked before any allocation
        "GET 01", "GET -1", "GET +1", "GET 1.0", "GET 0x10", "GET 1e3", "GET ١", // numbers
        "PUT 1 9999999999999999999", "SYNC 0", "SYNC 1000000000000000000",
        "SYNC 2 4294967296", "SYNC 1 5", "SYNC 2 3 4", "SYNC 2 -1", // a checksum is 32 bits, of the write before from
    })
    void refusesWhatIsntARequest(String line) {
        assertThrows(ProtocolException.class, () -> Protocol.parse(line));
    }

    @Test
    void readsLinesUpToTheLimit() throws IOException {
        String longest = "x".repeat(Protocol.MAX_LINE_BYTES - 1);
        ByteArrayInputStream in = stream(longest + "\nPING\n");
        assertEquals(longest, Protocol.readLine(in));
        assertEquals("PING", Protocol.readLine(in));
        assertNull(Protocol.readLine(in)); // the peer closed between requests

        assertThrows(ProtocolException.class, () -> Protocol.readLine(stream(longest + "x\n")));
        assertThrows(EOFException.class, () -> Protocol.readLine(stream("PIN")));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 9, 13, 0x1f, 0x7f, 0x80, 0xff})
    void refusesBytesThatArentPrintableAscii(int b) {
        byte[] line = {'G', 'E', 'T', ' ', (byte) b, '\n'};
        assertThrows(ProtocolException.class, () -> Protocol.readLine(new ByteArrayInputStream(line)));
    }

    @Test
    void readsExactlyTheLengthGiven() throws IOException {
        ByteArrayInputStream in = stream("abcdef");
        assertArrayEquals("abc".getBytes(StandardCharsets.US_ASCII), Protocol.readExactly(in, 3));
        assertArrayEquals(new byte[0], Protocol.readExactly(in, 0));
        assertThrows(EOFException.class, () -> Protocol.readExactly(in, 4)); // only "def" is left
    }

    @Test
    void errorLinesArePrintableAndFitALine() throws IOException {
        String line = Protocol.errorLine(ErrorCode.INTERNAL, "bad\nthingé" + "y".repeat(500));
        assertTrue(line.startsWith("ERR INTERNAL bad?thing?yyy"), line);
        assertTrue(line.length() < Protocol.MAX_LINE_BYTES, "length " + line.length());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Protocol.writeLine(out, line);
        assertEquals(line, Protocol.readLine(new ByteArrayInputStream(out.toByteArray()))); // a client can read it
        assertEquals("ERR BUSY", Protocol.errorLine(ErrorCode.BUSY, ""));
    }

    private static ByteArrayInputStream stream(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.ISO_8859_1));
    }
}
