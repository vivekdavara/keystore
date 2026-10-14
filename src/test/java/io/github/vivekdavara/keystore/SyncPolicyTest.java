package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** What each sync policy forces, counted at the log's channel. */
class SyncPolicyTest {
    @TempDir
    Path dir;

    private final AtomicReference<FaultyChannel> channel = new AtomicReference<>();

    private Store open(SyncPolicy sync) throws IOException {
        return Store.open(dir, Options.defaults().withSync(sync), (path, inner) -> {
            if (!path.endsWith(Store.logName(1))) {
                return inner;
            }
            FaultyChannel counting = new FaultyChannel(inner);
            channel.set(counting);
            return counting;
        });
    }

    @Test
    void everyWriteForcesEachWriteBeforeItReturns() throws IOException {
        Store store = open(SyncPolicy.EVERY_WRITE);
        for (int i = 1; i <= 5; i++) {
            store.put(utf8("k" + i), utf8("v"));
            assertEquals(i, channel.get().forces(), "forced by the time put returned");
        }
        store.delete(utf8("k1"));
        assertEquals(6, channel.get().forces());
        store.close();
        assertEquals(7, channel.get().forces(), "and once more at close");
    }

    @Test
    void neverForcesOnlyAtClose() throws IOException {
        Store store = open(SyncPolicy.NEVER);
        for (int i = 1; i <= 5; i++) {
            store.put(utf8("k" + i), utf8("v"));
        }
        store.delete(utf8("k1"));
        assertEquals(0, channel.get().forces());
        store.close();
        assertEquals(1, channel.get().forces());
    }

    @Test
    void aRefusedWriteForcesNothing() throws IOException {
        try (Store store = open(SyncPolicy.EVERY_WRITE)) {
            try {
                store.put(new byte[0], utf8("v"));
            } catch (IllegalArgumentException expected) {
                // the empty key is refused before the log
            }
            assertEquals(0, channel.get().forces());
            assertEquals(0, store.logSize());
        }
    }
}
