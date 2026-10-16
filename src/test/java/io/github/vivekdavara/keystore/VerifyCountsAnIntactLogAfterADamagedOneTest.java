package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.github.vivekdavara.keystore.wal.RecoveryMode;
import io.github.vivekdavara.keystore.wal.WalRecord;
import io.github.vivekdavara.keystore.wal.WriteAheadLog;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Review 5: StoreCheck.verify goes on past an older log recovery refuses, taking the damaged log's
 * last good record as where the next log starts. So the next log, intact, fails as "out of
 * sequence" and is counted, and printed, as damaged too: one torn older log reads as "2 damaged",
 * and the operator is sent to `keystore dump` for a newest log that has nothing wrong with it.
 * (StoreDump stops at the first log recovery refuses.)
 */
class VerifyCountsAnIntactLogAfterADamagedOneTest {
    @TempDir
    Path dir;

    @Test
    void oneDamagedOlderLogIsOneDamagedFile() throws IOException {
        try (Store store = Store.open(dir, Options.defaults().withSync(SyncPolicy.NEVER))) {
            for (int i = 1; i <= 5; i++) {
                store.put(utf8("k" + i), utf8("v" + i)); // #1 to #5, in the store's log
            }
        }
        List<Path> logs = logs();
        assertEquals(1, logs.size(), logs.toString());
        Path older = logs.get(0);
        // A newer log after it, as a flush that rotated the log but didn't write its manifest leaves.
        long number = Store.fileNumber(older.getFileName().toString(), Store.LOG_SUFFIX) + 10;
        Path newest = dir.resolve(Store.logName(number));
        WriteAheadLog log = WriteAheadLog.open(newest, 6, RecoveryMode.TOLERATE_TORN_TAIL, r -> {},
                WriteAheadLog.MAX_RECOVERABLE_BYTES, (path, channel) -> channel).log();
        log.append(WalRecord.put(6, utf8("k6"), utf8("v6")));
        log.close();
        assertEquals(0, verify(new ByteArrayOutputStream()), "both logs clean");

        try (RandomAccessFile file = new RandomAccessFile(older.toFile(), "rw")) {
            file.setLength(file.length() - 3); // #5 torn: an older log recovery refuses
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int damaged = verify(out);
        String report = out.toString(StandardCharsets.UTF_8);
        assertFalse(report.contains(newest.getFileName() + ": DAMAGED"), "the newest log is intact:\n" + report);
        assertEquals(1, damaged, report);
    }

    private int verify(ByteArrayOutputStream out) throws IOException {
        return StoreCheck.verify(dir, new PrintStream(out, true, StandardCharsets.UTF_8));
    }

    private List<Path> logs() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(f -> f.getFileName().toString().endsWith(Store.LOG_SUFFIX)).sorted().toList();
        }
    }
}
