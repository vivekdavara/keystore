package io.github.vivekdavara.keystore.memtable;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.Bytes;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MemTableTest {
    @Test
    void putThenGet() {
        MemTable table = new MemTable();
        table.put(1, utf8("k"), utf8("v1"));
        Entry entry = table.get(utf8("k"));
        assertEquals(1, entry.seq());
        assertArrayEquals(utf8("v1"), entry.value());
        assertFalse(entry.isTombstone());
        assertNull(table.get(utf8("other")));
    }

    @Test
    void aDeleteLeavesATombstoneThatALaterPutReplaces() {
        MemTable table = new MemTable();
        table.put(1, utf8("k"), utf8("v1"));
        table.delete(2, utf8("k"));
        assertEquals(Entry.tombstone(2), table.get(utf8("k")));
        assertTrue(table.get(utf8("k")).isTombstone());
        assertNull(table.get(utf8("k")).value());
        table.put(3, utf8("k"), utf8("v3"));
        assertEquals(Entry.value(3, utf8("v3")), table.get(utf8("k")));
        assertEquals(1, table.size());
        assertEquals(3, table.lastSequence());
    }

    @Test
    void aDeleteOfAKeyNeverWrittenIsStillRecorded() {
        // From day 2 the tombstone must hide the key's older values in SSTables.
        MemTable table = new MemTable();
        table.delete(1, utf8("gone"));
        assertEquals(Entry.tombstone(1), table.get(utf8("gone")));
        assertEquals(1, table.size());
    }

    @Test
    void writesMustArriveInSequenceOrder() {
        MemTable table = new MemTable();
        table.put(5, utf8("a"), utf8("1"));
        assertThrows(IllegalArgumentException.class, () -> table.put(5, utf8("b"), utf8("2")));
        assertThrows(IllegalArgumentException.class, () -> table.delete(4, utf8("a")));
        table.put(7, utf8("b"), utf8("2")); // gaps are the store's business, not the memtable's
        assertEquals(7, table.lastSequence());
        assertThrows(IllegalArgumentException.class, () -> Entry.tombstone(0));
    }

    @Test
    void iterationIsInKeyOrderWithTombstones() {
        MemTable table = new MemTable();
        table.put(1, utf8("b"), utf8("2"));
        table.put(2, utf8("a"), utf8("1"));
        table.delete(3, utf8("c"));
        table.put(4, new byte[] {(byte) 0x80}, utf8("high"));
        List<String> seen = new ArrayList<>();
        for (Iterator<Map.Entry<byte[], Entry>> it = table.iterator(); it.hasNext(); ) {
            Map.Entry<byte[], Entry> e = it.next();
            seen.add(Bytes.show(e.getKey()) + "=" + e.getValue());
        }
        assertEquals(List.of("a=1@2", "b=2@1", "c=tombstone@3", "\\x80=high@4"), seen);
        assertEquals("b", new String(table.iterator(utf8("aa")).next().getKey(), StandardCharsets.UTF_8));
    }

    @Test
    void theSizeCountsNewKeysAndReplacements() {
        MemTable table = new MemTable();
        table.put(1, utf8("key"), new byte[10]);
        assertEquals(3 + 10 + MemTable.PER_KEY_OVERHEAD, table.approximateBytes());

        table.put(2, utf8("key"), new byte[4]); // a smaller value replaces the old one
        assertEquals(3 + 4 + MemTable.PER_KEY_OVERHEAD, table.approximateBytes());

        table.delete(3, utf8("key")); // a tombstone holds no value
        assertEquals(3 + MemTable.PER_KEY_OVERHEAD, table.approximateBytes());
    }

    @Test
    void overwritesKeepTheTableSmall() {
        MemTable table = new MemTable();
        byte[] value = new byte[100];
        for (int seq = 1; seq <= 10_000; seq++) {
            table.put(seq, utf8("hot"), value);
        }
        assertEquals(1, table.size());
        assertEquals(3 + 100 + MemTable.PER_KEY_OVERHEAD, table.approximateBytes());
    }

    @Test
    void entriesCompareByContent() {
        assertEquals(Entry.value(1, utf8("x")), Entry.value(1, utf8("x")));
        assertEquals(Entry.value(1, utf8("x")).hashCode(), Entry.value(1, utf8("x")).hashCode());
        assertNotEquals(Entry.value(1, utf8("x")), Entry.value(2, utf8("x")));
        assertNotEquals(Entry.value(1, utf8("x")), Entry.tombstone(1));
        assertNotEquals(Entry.value(1, new byte[0]), Entry.tombstone(1)); // empty value is not a delete
        assertEquals("x@1", Entry.value(1, utf8("x")).toString());
        assertEquals("tombstone@2", Entry.tombstone(2).toString());
        assertThrows(NullPointerException.class, () -> Entry.value(1, null));
    }
}
