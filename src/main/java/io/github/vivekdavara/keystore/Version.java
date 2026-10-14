package io.github.vivekdavara.keystore;

import io.github.vivekdavara.keystore.memtable.MemTable;
import io.github.vivekdavara.keystore.sstable.Table;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Everything a read consults, as one immutable snapshot: the memtable taking writes, the frozen
 * memtable being flushed (or none), and the live tables, newest first. The store replaces its
 * current version on every flush and compaction; a read pins one version and reads only it, so it
 * never sees a key leave the frozen memtable before its table is listed.
 *
 * <p>A version counts references: the store holds one while it is current, and each read holds one
 * while it runs. A version holds a reference on each of its tables, released when the version's own
 * count reaches zero, so a table a compaction dropped stays open until the last read that could be
 * in it has finished.
 */
final class Version {
    final MemTable active;
    final MemTable frozen; // null when no flush is in progress
    final List<Table> tables; // newest first

    private final AtomicInteger refs = new AtomicInteger(1); // the store's, while current

    Version(MemTable active, MemTable frozen, List<Table> tables) {
        this.active = active;
        this.frozen = frozen;
        this.tables = List.copyOf(tables);
        for (Table table : this.tables) {
            if (!table.tryRetain()) {
                throw new IllegalStateException(table + " was closed before a version listed it");
            }
        }
    }

    /**
     * Takes a reference for a read; false if the version is already released, which happens only
     * after the store moved to a newer one, so the caller reads the current version again.
     */
    boolean tryRetain() {
        for (;;) {
            int r = refs.get();
            if (r == 0) {
                return false;
            }
            if (refs.compareAndSet(r, r + 1)) {
                return true;
            }
        }
    }

    /** Drops a reference; the last one releases the version's tables. */
    void release() {
        int r = refs.decrementAndGet();
        if (r < 0) {
            throw new IllegalStateException("a version released more often than retained");
        }
        if (r == 0) {
            for (Table table : tables) {
                table.release();
            }
        }
    }
}
