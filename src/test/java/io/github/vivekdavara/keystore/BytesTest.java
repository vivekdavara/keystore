package io.github.vivekdavara.keystore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class BytesTest {
    @Test
    void bytesCompareUnsigned() {
        // As signed bytes 0x80 is -128 and would sort first.
        assertTrue(Bytes.compare(new byte[] {0x7f}, new byte[] {(byte) 0x80}) < 0);
        assertTrue(Bytes.compare(new byte[] {(byte) 0xff}, new byte[] {0x00}) > 0);
    }

    @Test
    void aPrefixSortsFirst() {
        assertTrue(Bytes.compare(Bytes.utf8("ab"), Bytes.utf8("abc")) < 0);
        assertTrue(Bytes.compare(new byte[] {1}, new byte[] {1, 0}) < 0);
        assertEquals(0, Bytes.compare(Bytes.utf8("abc"), Bytes.utf8("abc")));
    }

    @Test
    void theComparatorIsTheSameOrder() {
        byte[] a = {0x10, (byte) 0x90};
        byte[] b = {0x10, 0x20};
        assertEquals(Integer.signum(Bytes.compare(a, b)), Integer.signum(Bytes.ORDER.compare(a, b)));
        assertTrue(Bytes.ORDER.compare(a, b) > 0);
    }

    @Test
    void showEscapesWhatIsNotPrintable() {
        assertEquals("key\\x00\\xff\\\\", Bytes.show(new byte[] {'k', 'e', 'y', 0, (byte) 0xff, '\\'}));
        assertEquals("", Bytes.show(new byte[0]));
    }

    @Test
    void showCutsLongStrings() {
        assertEquals("ab...(5 bytes)", Bytes.show(Bytes.utf8("abcde"), 2));
        assertEquals("abcde", Bytes.show(Bytes.utf8("abcde"), 5));
        assertThrows(IllegalArgumentException.class, () -> Bytes.show(new byte[1], -1));
    }
}
