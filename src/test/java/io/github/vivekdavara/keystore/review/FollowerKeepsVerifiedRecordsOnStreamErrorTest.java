package io.github.vivekdavara.keystore.review;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.Options;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.SyncPolicy;
import io.github.vivekdavara.keystore.server.Follower;
import io.github.vivekdavara.keystore.server.Server;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Review finding: Follower.stream() applies its pending batch only on a REC boundary, a HEARTBEAT,
 * or ERR BEHIND. When the leader ends the stream with any other ERR (INTERNAL: a damaged record in
 * its log, its store closing, an I/O error) right after records the follower has already received
 * and verified (checksum and sequence number), stream() throws and the batch is thrown away. The
 * leader's ship loop writes those records and the ERR line in one flush, so the follower always
 * sees the ERR before it applies: on every reconnect it gets the same good records and drops them
 * again, and its store never gets past them.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FollowerKeepsVerifiedRecordsOnStreamErrorTest {
    @TempDir
    Path dir;

    @Test
    void theGoodRecordsBeforeADamagedOneAreApplied() throws Exception {
        Options leaderOptions = Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(4 << 10)
                .withRetainLogBytes(1L << 30);
        Path leaderDir = dir.resolve("leader");
        try (Store store = Store.open(leaderDir, leaderOptions)) {
            for (int i = 1; i <= 500; i++) {
                assertEquals(i, store.put(utf8("key-" + i), utf8("value-" + i)));
            }
            assertTrue(store.stats().flushes() > 1, "the first log is covered by tables, and retained");
        }
        // Damage record #6 in the first log (covered, so recovery doesn't read it): flip the last
        // byte of its value, so its checksum fails. Records #1 to #5 are intact.
        damageValueOf(firstLog(leaderDir), 6);

        try (Store leaderStore = Store.open(leaderDir, leaderOptions);
                Server leader = Server.leader(leaderStore, Server.Config.loopback(0).withHeartbeat(Duration.ofMillis(50)));
                Store followerStore = Store.open(dir.resolve("follower"), Options.defaults().withSync(SyncPolicy.NEVER));
                Follower follower = Follower.start(followerStore, Follower.Config.of(leader.address())
                        .withTimeout(Duration.ofSeconds(5)).withMaxBackoff(Duration.ofMillis(100)))) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (follower.status().connects() < 4 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            Follower.Status status = follower.status();
            assertTrue(status.connects() >= 4, "the follower kept reconnecting: " + status);
            assertTrue(status.detail().contains("damaged"), "the leader reported the damage: " + status);
            assertEquals(5, followerStore.lastSequence(),
                    "records #1-#5 arrived intact on every connection, so the follower should have applied them: " + status);
        }
    }

    /**
     * The same drop in a plain shutdown: the leader's store closes (before its server), its tail
     * sends the records the store acknowledged, then ERR INTERNAL "the store ... is closed". The
     * follower has received every record (its status shows leader=2000) but applies only the
     * batches it finished before the ERR line arrived: 19 of 20 runs ended at 1,024 to 1,972 here.
     * A failover to this follower loses acknowledged writes it was sent.
     */
    @Test
    void theLastRecordsSentBeforeTheLeadersStoreClosesAreApplied() throws Exception {
        for (int run = 0; run < 5; run++) {
            Store leaderStore = Store.open(dir.resolve("leader-" + run),
                    Options.defaults().withSync(SyncPolicy.NEVER).withRetainLogBytes(1L << 30));
            try (Server leader = Server.leader(leaderStore, Server.Config.loopback(0).withHeartbeat(Duration.ofMillis(50)));
                    Store followerStore = Store.open(dir.resolve("follower-" + run), Options.defaults().withSync(SyncPolicy.NEVER));
                    Follower follower = Follower.start(followerStore, Follower.Config.of(leader.address())
                            .withTimeout(Duration.ofSeconds(5)).withMaxBackoff(Duration.ofMillis(100)))) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (leader.followers().isEmpty() && System.nanoTime() < deadline) {
                    Thread.sleep(1);
                }
                assertEquals(1, leader.followers().size(), "the follower is streaming");
                for (int i = 1; i <= 2_000; i++) {
                    leaderStore.put(utf8("key-" + i), utf8("value-" + i));
                }
                leaderStore.close(); // shutting down: the store, then the server
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!follower.status().detail().startsWith("INTERNAL: the store at") && System.nanoTime() < deadline) {
                    Thread.sleep(5);
                }
                Thread.sleep(200);
                Follower.Status status = follower.status();
                assertEquals(2_000, status.leader(), "the follower received every record: " + status);
                assertEquals(2_000, followerStore.lastSequence(), "run " + run + ": " + status);
            }
        }
    }

    private static Path firstLog(Path storeDir) throws IOException {
        List<Path> logs = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(storeDir, "[0-9]*.log")) {
            for (Path file : files) {
                logs.add(file);
            }
        }
        Collections.sort(logs);
        return logs.get(0);
    }

    /** Flips the last byte of the record with sequence number {@code seq} (a put's value). */
    private static void damageValueOf(Path log, long seq) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(log.toFile(), "rw")) {
            long offset = 0;
            while (offset < file.length()) {
                file.seek(offset);
                int length = file.readInt();
                file.readInt(); // the checksum
                long recordSeq = file.readLong();
                if (recordSeq == seq) {
                    long last = offset + 8 + length - 1;
                    file.seek(last);
                    int b = file.read();
                    file.seek(last);
                    file.write(b ^ 0x01);
                    return;
                }
                offset += 8 + length;
            }
        }
        throw new AssertionError("no record " + seq + " in " + log);
    }
}
