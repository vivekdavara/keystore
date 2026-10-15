package io.github.vivekdavara.keystore.review;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.Options;
import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.SyncPolicy;
import io.github.vivekdavara.keystore.server.Server;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Review finding: an idle SYNC stream's thread waits in LogTail.next(heartbeat), i.e. in
 * Store.awaitSequence, which only a write or the store's close wakes. Server.close() closes the
 * stream's socket but can't wake that wait, so it sits in handler.join(5_000) for each idle stream
 * and then returns with the stream's thread still running (its LogTail and a channel on a log
 * still open, the stream still listed in followers()) until the heartbeat ends. With a 30-second
 * heartbeat: close() took 5 s and left the stream behind.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ServerCloseWaitsOutIdleSyncStreamsTest {
    @TempDir
    Path dir;

    @Test
    void closeEndsAnIdleStreamPromptly() throws Exception {
        try (Store store = Store.open(dir, Options.defaults().withSync(SyncPolicy.NEVER));
                Socket follower = new Socket()) {
            Server server = Server.leader(store, Server.Config.loopback(0).withHeartbeat(Duration.ofSeconds(30)));
            follower.connect(server.address(), 5_000);
            OutputStream out = follower.getOutputStream();
            out.write("SYNC 1\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (server.followers().isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            assertEquals(1, server.followers().size(), "the stream is open, caught up, waiting");

            long start = System.nanoTime();
            server.close();
            long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertEquals(0, server.followers().size(), "close() returned with the stream still running (took "
                    + tookMillis + " ms)");
            assertTrue(tookMillis < 2_000, "close() took " + tookMillis + " ms");
        }
    }
}
