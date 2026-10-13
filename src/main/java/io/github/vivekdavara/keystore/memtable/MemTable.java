package io.github.vivekdavara.keystore.memtable;

import io.github.vivekdavara.keystore.Bytes;
import java.util.Iterator;
import java.util.Map;

/**
 * The in-memory table of recent writes: each key's newest {@link Entry}, in key order.
 *
 * <p>Writes ({@link #put}, {@link #delete}) come from one thread at a time, in sequence-number
 * order; the store's write lock ensures both. Reads take no lock. The memtable keeps the arrays it
 * is given, so callers hand it arrays nobody will change (the store copies the caller's).
 */
public final class MemTable {
    /**
     * Bytes charged per key on top of its key and value: the node, its next pointers, the entry and
     * the two array headers, roughly, on a 64-bit JVM with compressed pointers.
     */
    static final int PER_KEY_OVERHEAD = 96;

    private final SkipList<byte[], Entry> entries;
    private volatile long approximateBytes;
    private volatile long bytesWritten;
    private volatile long lastSeq;

    public MemTable() {
        this.entries = new SkipList<>(Bytes.ORDER);
    }

    /** Applies write number {@code seq}: {@code key} now has {@code value}. */
    public void put(long seq, byte[] key, byte[] value) {
        apply(seq, key, Entry.value(seq, value));
    }

    /** Applies write number {@code seq}: {@code key} is deleted (a tombstone). */
    public void delete(long seq, byte[] key) {
        apply(seq, key, Entry.tombstone(seq));
    }

    private void apply(long seq, byte[] key, Entry entry) {
        if (seq <= lastSeq) {
            throw new IllegalArgumentException(
                    "write " + seq + " applied after write " + lastSeq + ": writes must arrive in order");
        }
        Entry previous = entries.put(key, entry);
        if (previous == null) {
            approximateBytes += key.length + entry.valueLength() + PER_KEY_OVERHEAD;
        } else {
            approximateBytes += entry.valueLength() - previous.valueLength();
        }
        bytesWritten += key.length + entry.valueLength();
        lastSeq = seq;
    }

    /** The key's newest entry (perhaps a tombstone), or null if the key was never written here. */
    public Entry get(byte[] key) {
        return entries.get(key);
    }

    /** Every key's newest entry, tombstones included, in key order. Weakly consistent. */
    public Iterator<Map.Entry<byte[], Entry>> iterator() {
        return entries.iterator();
    }

    /** The entries at or after {@code from}, in key order. Weakly consistent. */
    public Iterator<Map.Entry<byte[], Entry>> iterator(byte[] from) {
        return entries.iterator(from);
    }

    /** Keys held, tombstones included. */
    public int size() {
        return entries.size();
    }

    /**
     * What the memtable holds now, roughly: its keys and current values plus a per-key overhead. An
     * overwrite replaces the old value, so this can stay small while {@link #bytesWritten} grows.
     */
    public long approximateBytes() {
        return approximateBytes;
    }

    /**
     * Key and value bytes of every write applied, overwrites included. This tracks the log's growth,
     * which is what a flush must bound.
     */
    public long bytesWritten() {
        return bytesWritten;
    }

    /** The sequence number of the last write applied, or 0. */
    public long lastSequence() {
        return lastSeq;
    }
}
