package io.github.vivekdavara.keystore.wal;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.wal.Recovery.Outcome;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteAheadLogTest {
    @TempDir
    Path dir;

    @Test
    void aMissingLogIsCreatedEmpty() throws IOException {
        Path file = dir.resolve("wal.log");
        WriteAheadLog.Opened opened = WriteAheadLog.open(file, RecoveryMode.TOLERATE_TORN_TAIL, r -> {});
        try (WriteAheadLog log = opened.log()) {
            assertEquals(new Recovery(Outcome.CLEAN, 0, 0, 0, 0, ""), opened.recovery());
            assertEquals(0, log.size());
        }
        assertTrue(Files.exists(file));
    }

    @Test
    void appendedRecordsAreReplayedOnReopen() throws IOException {
        Path file = dir.resolve("wal.log");
        List<WalRecord> written = List.of(
                WalRecord.put(1, utf8("a"), utf8("1")),
                WalRecord.delete(2, utf8("a")),
                WalRecord.put(3, utf8("b"), new byte[1000]));
        try (WriteAheadLog log = WriteAheadLog.open(file, RecoveryMode.TOLERATE_TORN_TAIL, r -> {}).log()) {
            for (WalRecord record : written) {
                log.append(record);
            }
            log.sync();
            assertEquals(Files.size(file), log.size());
        }
        List<WalRecord> replayed = new ArrayList<>();
        WriteAheadLog.Opened opened = WriteAheadLog.open(file, RecoveryMode.TOLERATE_TORN_TAIL, replayed::add);
        opened.log().close();
        assertEquals(written, replayed);
        assertEquals(Outcome.CLEAN, opened.recovery().outcome());
        assertEquals(3, opened.recovery().lastSeq());
    }

    /** A torn tail is cut off on disk, and appends continue from the last good record. */
    @Test
    void aTornTailIsTruncatedOnDiskAndAppendsContinueAfterIt() throws IOException {
        Path file = dir.resolve("wal.log");
        long goodEnd;
        try (WriteAheadLog log = WriteAheadLog.open(file, RecoveryMode.TOLERATE_TORN_TAIL, r -> {}).log()) {
            log.append(WalRecord.put(1, utf8("a"), utf8("1")));
            goodEnd = log.size();
            log.append(WalRecord.put(2, utf8("b"), utf8("2")));
        }
        try (RandomAccessFile raw = new RandomAccessFile(file.toFile(), "rw")) {
            raw.setLength(Files.size(file) - 3); // a crash in the middle of record #2
        }

        WriteAheadLog.Opened opened = WriteAheadLog.open(file, RecoveryMode.TOLERATE_TORN_TAIL, r -> {});
        try (WriteAheadLog log = opened.log()) {
            assertEquals(Outcome.TORN_TAIL, opened.recovery().outcome());
            assertEquals(1, opened.recovery().lastSeq());
            assertEquals(goodEnd, Files.size(file), "truncated on disk");
            log.append(WalRecord.put(2, utf8("b"), utf8("again")));
        }

        List<WalRecord> replayed = new ArrayList<>();
        WriteAheadLog.Opened again = WriteAheadLog.open(file, RecoveryMode.TOLERATE_TORN_TAIL, replayed::add);
        again.log().close();
        assertEquals(Outcome.CLEAN, again.recovery().outcome());
        assertEquals(List.of(WalRecord.put(1, utf8("a"), utf8("1")), WalRecord.put(2, utf8("b"), utf8("again"))), replayed);
    }

    @Test
    void aZeroFilledTailIsTruncated() throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog log = WriteAheadLog.open(file, RecoveryMode.TOLERATE_TORN_TAIL, r -> {}).log()) {
            log.append(WalRecord.put(1, utf8("a"), utf8("1")));
        }
        long goodEnd = Files.size(file);
        try (RandomAccessFile raw = new RandomAccessFile(file.toFile(), "rw")) {
            raw.setLength(goodEnd + 8192); // the file grew, its data never arrived
        }
        WriteAheadLog.Opened opened = WriteAheadLog.open(file, RecoveryMode.TOLERATE_TORN_TAIL, r -> {});
        opened.log().close();
        assertEquals(Outcome.TORN_TAIL, opened.recovery().outcome());
        assertEquals(8192, opened.recovery().droppedBytes());
        assertEquals(goodEnd, Files.size(file));
    }

    /** A refusal must not touch the file: the operator decides what to drop. */
    @Test
    void aRefusedOpenLeavesTheFileAsItWas() throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog log = WriteAheadLog.open(file, RecoveryMode.TOLERATE_TORN_TAIL, r -> {}).log()) {
            for (int seq = 1; seq <= 3; seq++) {
                log.append(WalRecord.put(seq, utf8("k" + seq), utf8("v" + seq)));
            }
        }
        byte[] before = Files.readAllBytes(file);
        byte[] damaged = before.clone();
        damaged[10] ^= 1; // inside record #1
        Files.write(file, damaged);

        assertThrows(
                WalCorruptionException.class,
                () -> WriteAheadLog.open(file, RecoveryMode.TOLERATE_TORN_TAIL, r -> {}));
        assertArrayEquals(damaged, Files.readAllBytes(file));

        WriteAheadLog.Opened opened = WriteAheadLog.open(file, RecoveryMode.POINT_IN_TIME, r -> {});
        opened.log().close();
        assertEquals(Outcome.DAMAGE_DROPPED, opened.recovery().outcome());
        assertEquals(0, opened.recovery().records());
        assertEquals(0, Files.size(file));
    }

    @Test
    void aLogTooLargeToReadWholeIsRefused() throws IOException {
        Path file = dir.resolve("wal.log");
        try (RandomAccessFile raw = new RandomAccessFile(file.toFile(), "rw")) {
            raw.setLength(WriteAheadLog.MAX_RECOVERABLE_BYTES + 1); // sparse: no disk space used
        }
        IOException refused = assertThrows(
                IOException.class, () -> WriteAheadLog.open(file, RecoveryMode.POINT_IN_TIME, r -> {}));
        assertTrue(refused.getMessage().contains("recovery reads a log whole"), refused.getMessage());
        assertEquals(WriteAheadLog.MAX_RECOVERABLE_BYTES + 1, Files.size(file), "left as it was");
    }

    @Test
    void anExceptionWhileApplyingClosesTheFile() throws IOException {
        Path file = dir.resolve("wal.log");
        try (WriteAheadLog log = WriteAheadLog.open(file, RecoveryMode.TOLERATE_TORN_TAIL, r -> {}).log()) {
            log.append(WalRecord.put(1, utf8("a"), utf8("1")));
        }
        assertThrows(IllegalStateException.class, () -> WriteAheadLog.open(file, RecoveryMode.TOLERATE_TORN_TAIL, r -> {
            throw new IllegalStateException("apply failed");
        }));
        // Still usable: the failed open released its channel.
        WriteAheadLog.Opened opened = WriteAheadLog.open(file, RecoveryMode.TOLERATE_TORN_TAIL, r -> {});
        opened.log().close();
        assertEquals(1, opened.recovery().records());
    }

    @Test
    void theDirectoryCanBeForced() throws IOException {
        WriteAheadLog.forceDirectory(dir); // throws on a platform that can't
    }
}
