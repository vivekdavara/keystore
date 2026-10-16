package io.github.vivekdavara.keystore;

import io.github.vivekdavara.keystore.memtable.Entry;
import io.github.vivekdavara.keystore.sstable.BloomFilter;
import io.github.vivekdavara.keystore.sstable.CorruptTableException;
import io.github.vivekdavara.keystore.sstable.Table;
import io.github.vivekdavara.keystore.wal.WalDump;
import io.github.vivekdavara.keystore.wal.WalDump.Verdict;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reads a whole store directory and checks everything in it, for an operator: what a store never
 * does on its own. A store checks a table's footer, filter and index when it opens it, but a data
 * block only when a read or a merge reaches it, so damage in a block nobody reads goes unseen until
 * then. This reads every block of every table the manifest names, and every log recovery would
 * replay. Like {@link StoreDump}, it changes nothing and doesn't take the store's lock.
 *
 * <p>Per table: it opens (footer, filter and index checks) and matches the manifest's record of it;
 * every data block passes its checksum and holds well-formed entries in strictly increasing key
 * order, ending with the index's key; the footer's entry and tombstone counts are right; every
 * entry's sequence number is within the footer's range (not that the range's ends occur); and a
 * point lookup (index, Bloom filter, block) finds every entry, which reads a block per entry (there
 * is no block cache). Per log: what recovery would do with it ({@link WalDump}). A manifest that
 * fails its checks is damage too; with no manifest there is no store.
 */
public final class StoreCheck {
    private StoreCheck() {}

    /**
     * Checks the store in {@code dir}, printing a line per table and log; returns how many of them
     * are damaged (a log recovery would refuse counts; a torn tail in the newest, which recovery
     * truncates, doesn't). A damaged manifest is reported as the one damaged file.
     *
     * @throws IOException if there is no manifest, or a directory can't be listed
     */
    public static int verify(Path dir, PrintStream out) throws IOException {
        Manifest manifest;
        try {
            manifest = Manifest.read(dir);
        } catch (IOException e) { // there is one, and it fails its checks: nothing else can be read
            out.printf("%s: DAMAGED: %s%n1 damaged%n", Manifest.FILE, e.getMessage());
            return 1;
        }
        if (manifest == null) {
            throw new IOException("no store at " + dir + " (no " + Manifest.FILE + ")");
        }
        int damaged = 0;
        for (Manifest.TableRef ref : manifest.tables()) {
            String name = Store.tableName(ref.number());
            String problem = checkTable(dir.resolve(name), ref, out);
            if (problem != null) {
                out.printf("%s: DAMAGED: %s%n", name, problem);
                damaged++;
            }
        }
        List<Long> logs = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*" + Store.LOG_SUFFIX)) {
            for (Path file : files) {
                long number = Store.fileNumber(file.getFileName().toString(), Store.LOG_SUFFIX);
                if (number >= manifest.logNumber()) {
                    logs.add(number);
                }
            }
        }
        Collections.sort(logs);
        long seq = manifest.lastSequence();
        PrintStream nowhere = new PrintStream(OutputStream.nullOutputStream());
        boolean afterDamage = false;
        for (int i = 0; i < logs.size(); i++) {
            String name = Store.logName(logs.get(i));
            if (afterDamage) {
                // Found by review: checked from the damaged log's last good record, an intact log after
                // it failed as out of sequence and was counted too. Where its writes should start isn't
                // known, so it isn't judged (as dump stops at the first log recovery refuses).
                out.printf("%s: not checked: it follows a damaged log%n", name);
                continue;
            }
            WalDump.Result result = WalDump.dump(dir.resolve(name), seq + 1, nowhere);
            boolean older = i < logs.size() - 1;
            if (result.verdict() == Verdict.REFUSED || (older && result.verdict() != Verdict.CLEAN)) {
                out.printf("%s: DAMAGED: recovery would refuse it (`keystore dump` shows where)%n", name);
                damaged++;
                afterDamage = true;
            } else {
                out.printf("%s: ok, %s%s%n", name,
                        result.lastSeq() == seq ? "no writes" : "writes #" + (seq + 1) + " to #" + result.lastSeq(),
                        result.verdict() == Verdict.TORN_TAIL ? ", then a torn write recovery truncates" : "");
            }
            seq = result.lastSeq();
        }
        out.println(damaged == 0 ? "ok" : damaged + " damaged");
        return damaged;
    }

    /** Reads the whole table; returns what is wrong with it, or null after printing that it's ok. */
    private static String checkTable(Path file, Manifest.TableRef ref, PrintStream out) throws IOException {
        Table table;
        try {
            table = Table.open(file, ref.number(), (path, channel) -> channel);
        } catch (CorruptTableException e) {
            return e.getMessage();
        } catch (NoSuchFileException e) {
            return "missing";
        }
        try {
            if (table.fileBytes() != ref.fileBytes() || table.smallestSeq() != ref.smallestSeq()
                    || table.largestSeq() != ref.largestSeq()) {
                return "doesn't match the manifest's record of it, " + ref;
            }
            Table.Cursor cursor = table.cursor();
            long tombstones = 0;
            while (cursor.next()) {
                Entry entry = cursor.entry();
                if (entry.seq() < table.smallestSeq() || entry.seq() > table.largestSeq()) {
                    return "an entry with sequence number " + entry.seq() + ", outside the table's #"
                            + table.smallestSeq() + " to #" + table.largestSeq();
                }
                tombstones += entry.isTombstone() ? 1 : 0;
                Entry found = table.get(cursor.key(), BloomFilter.hash(cursor.key()));
                if (found == null || found.seq() != entry.seq()) {
                    return "a lookup of a key it holds (written as #" + entry.seq() + ") doesn't find it";
                }
            }
            if (tombstones != table.tombstones()) {
                return tombstones + " tombstones, but the footer says " + table.tombstones();
            }
            out.printf("%s: ok, %d entries in %d blocks, writes #%d to #%d%n", file.getFileName(), table.entries(),
                    table.blocks(), table.smallestSeq(), table.largestSeq());
            return null;
        } catch (CorruptTableException e) {
            return e.getMessage();
        } finally {
            table.release();
        }
    }
}
