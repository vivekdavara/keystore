package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.vivekdavara.keystore.wal.Recovery.Outcome;
import io.github.vivekdavara.keystore.wal.WalFormat;
import io.github.vivekdavara.keystore.wal.WalRecord;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Many crash-and-recover cycles against a model of the acknowledged writes. Each cycle reopens the
 * store, checks every key against the model, makes random writes, and then leaves the log as a
 * crash would: a write that dies partway (at a random byte of its record), sometimes followed by a
 * zero-filled or random tail (the file grew, its data never arrived). Only writes that returned
 * are in the model, and after every recovery the store must hold exactly them.
 */
class CrashCycleTest {
    private static final int KEYS = 40;

    @TempDir
    Path dir;

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3})
    void acknowledgedWritesSurviveEveryCycle(long seed) throws IOException {
        SplittableRandom random = new SplittableRandom(seed);
        Map<String, String> model = new HashMap<>();
        long acknowledged = 0;
        AtomicReference<FaultyChannel> channel = new AtomicReference<>();
        Options options = Options.defaults().withSync(SyncPolicy.NEVER);

        for (int cycle = 0; cycle < 200; cycle++) {
            try (Store store = Store.open(dir, options, (path, inner) -> {
                if (!path.endsWith(Store.logName(1))) {
                    return inner;
                }
                FaultyChannel faulty = new FaultyChannel(inner);
                channel.set(faulty);
                return faulty;
            })) {
                assertEquals(acknowledged, store.lastSequence(), "cycle " + cycle);
                for (int k = 0; k < KEYS; k++) {
                    assertEquals(Optional.ofNullable(model.get("k" + k)), store.get(utf8("k" + k)).map(CrashCycleTest::string),
                            "cycle " + cycle + ", key k" + k);
                }
                if (cycle > 0) {
                    Outcome outcome = store.recovery().outcome();
                    assertEquals(true, outcome == Outcome.TORN_TAIL || outcome == Outcome.CLEAN, "cycle " + cycle + ": " + outcome);
                }

                int writes = random.nextInt(0, 30);
                for (int w = 0; w < writes; w++) {
                    String key = "k" + random.nextInt(KEYS);
                    if (random.nextInt(4) == 0) {
                        store.delete(utf8(key));
                        model.remove(key);
                    } else {
                        String value = "v" + cycle + "-" + w + "-" + "x".repeat(random.nextInt(0, 200));
                        store.put(utf8(key), utf8(value));
                        model.put(key, value);
                    }
                    acknowledged++;
                }

                if (random.nextInt(3) != 0) {
                    // A write in flight dies partway: some of its record reaches the log, not all.
                    String key = "k" + random.nextInt(KEYS);
                    byte[] value = utf8("lost-" + cycle);
                    int size = WalFormat.encodedSize(WalRecord.put(acknowledged + 1, utf8(key), value));
                    channel.get().failWritesAfter(random.nextInt(size));
                    assertThrows(IOException.class, () -> store.put(utf8(key), value));
                }
            } // close forces what was acknowledged (a failed append doesn't stop that)

            switch (random.nextInt(4)) {
                case 0 -> appendToLog(new byte[random.nextInt(1, 5_000)]); // zeros
                case 1 -> {
                    byte[] garbage = new byte[random.nextInt(1, 5_000)];
                    random.nextBytes(garbage);
                    appendToLog(garbage);
                }
                default -> { } // just the torn record, if there was one
            }
        }
    }

    private void appendToLog(byte[] bytes) throws IOException {
        try (FileChannel log = FileChannel.open(dir.resolve(Store.logName(1)), StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            log.write(ByteBuffer.wrap(bytes));
        }
    }

    private static String string(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
