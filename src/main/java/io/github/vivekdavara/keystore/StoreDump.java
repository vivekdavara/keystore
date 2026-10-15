package io.github.vivekdavara.keystore;

import io.github.vivekdavara.keystore.wal.WalDump;
import io.github.vivekdavara.keystore.wal.WalDump.Verdict;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A read-only view of a store directory for an operator: the manifest's tables, then every log
 * recovery would replay, record by record ({@link WalDump}). Never changes a file, and doesn't take
 * the store's lock, so it can look at a store another process has open.
 */
public final class StoreDump {
    private StoreDump() {}

    /**
     * Prints the store in {@code dir}; returns what the default recovery mode would do with its
     * logs ({@link Verdict#REFUSED} if it would refuse to open).
     *
     * @throws IOException if there is no manifest, or it can't be read
     */
    public static Verdict dump(Path dir, PrintStream out) throws IOException {
        Manifest manifest = Manifest.read(dir);
        if (manifest == null) {
            throw new IOException("no store at " + dir + " (no " + Manifest.FILE + ")");
        }
        out.printf("%s: tables cover writes up to #%d; logs start at %s; next file number %d%n",
                dir.resolve(Manifest.FILE), manifest.lastSequence(), Store.logName(manifest.logNumber()),
                manifest.nextFileNumber());
        for (Manifest.TableRef t : manifest.tables()) {
            out.printf("  %s: %d bytes, writes #%d to #%d%n",
                    Store.tableName(t.number()), t.fileBytes(), t.smallestSeq(), t.largestSeq());
        }
        List<Long> logs = new ArrayList<>();
        List<Long> kept = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, "*" + Store.LOG_SUFFIX)) {
            for (Path file : files) {
                long number = Store.fileNumber(file.getFileName().toString(), Store.LOG_SUFFIX);
                if (number >= manifest.logNumber()) {
                    logs.add(number);
                } else if (number >= 0) {
                    kept.add(number);
                }
            }
        }
        Collections.sort(logs);
        Collections.sort(kept);
        for (long number : kept) {
            out.printf("  %s: %d bytes, covered by the tables, kept for followers (or garbage the next open deletes)%n",
                    Store.logName(number), Files.size(dir.resolve(Store.logName(number))));
        }
        long seq = manifest.lastSequence();
        Verdict verdict = Verdict.CLEAN;
        for (int i = 0; i < logs.size(); i++) {
            WalDump.Result result = WalDump.dump(dir.resolve(Store.logName(logs.get(i))), seq + 1, out);
            seq = result.lastSeq();
            verdict = result.verdict();
            if (i < logs.size() - 1 && verdict != Verdict.CLEAN) {
                out.printf("refused: an older log must be clean, since writes in newer logs follow it%n");
                return Verdict.REFUSED;
            }
            if (verdict == Verdict.REFUSED) {
                return verdict;
            }
        }
        return verdict;
    }
}
