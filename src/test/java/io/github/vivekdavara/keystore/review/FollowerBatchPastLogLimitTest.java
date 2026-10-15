package io.github.vivekdavara.keystore.review;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.vivekdavara.keystore.Options;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.SyncPolicy;
import io.github.vivekdavara.keystore.server.Follower;
import io.github.vivekdavara.keystore.server.Server;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Review finding: the follower batches whatever has arrived (up to 1,024 records / 4 MiB, plus one
 * record), and Store.replicate appends a batch to the current log in one piece: it flushes only if
 * the log is already at flushLogBytes, then calls ensureRoomFor(the whole batch). With a log limit
 * (Options.maxLogBytes) that the leader never reaches, because it flushes at flushLogBytes before
 * each single write, a catch-up batch is larger than the room left and replicate throws
 * LogFullException. The Follower treats that as a failed store and stops in FAILED for good, while
 * the leader, with the same options, took every one of those writes.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FollowerBatchPastLogLimitTest {
    @TempDir
    Path dir;

    /** A log flushed at 1 KiB and capped at 4 KiB: every single write fits, as the leader shows. */
    private static Options options() {
        return Options.defaults().withSync(SyncPolicy.NEVER).withFlushLogBytes(1024).withMaxLogBytes(4096);
    }

    @Test
    void aFollowerWithTheLeadersOptionsCatchesUp() throws Exception {
        try (Store leaderStore = Store.open(dir.resolve("leader"), options().withRetainLogBytes(1L << 30));
                Server leader = Server.leader(leaderStore, Server.Config.loopback(0).withHeartbeat(Duration.ofMillis(50)))) {
            for (int i = 1; i <= 300; i++) {
                assertEquals(i, leaderStore.put(utf8("key-" + i), utf8("value-" + i)), "the leader takes every write");
            }
            try (Store followerStore = Store.open(dir.resolve("follower"), options());
                    Follower follower = Follower.start(followerStore, Follower.Config.of(leader.address())
                            .withTimeout(Duration.ofSeconds(5)).withMaxBackoff(Duration.ofMillis(100)))) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (followerStore.lastSequence() < 300 && follower.status().state() != Follower.State.FAILED
                        && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                Follower.Status status = follower.status();
                assertEquals(Follower.State.STREAMING, status.state(), status.toString());
                assertEquals(300, followerStore.lastSequence(), status.toString());
            }
        }
    }
}
