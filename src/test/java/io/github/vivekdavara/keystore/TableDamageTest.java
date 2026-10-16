package io.github.vivekdavara.keystore;

import static io.github.vivekdavara.keystore.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.vivekdavara.keystore.server.Client;
import io.github.vivekdavara.keystore.server.Protocol.ErrorCode;
import io.github.vivekdavara.keystore.server.Server;
import io.github.vivekdavara.keystore.server.ServerException;
import io.github.vivekdavara.keystore.sstable.CorruptTableException;
import io.github.vivekdavara.keystore.sstable.TableSurgery;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Damage in a table's data block, found by the store while it runs. A store checks a table's
 * footer, filter and index when it opens it, and a data block when a read or a merge reaches it,
 * so damage there is found late; what matters is what the store does then: a read of that block
 * fails with {@link CorruptTableException} (never a wrong value, never "not found", which would
 * let an older table's value through), everything else goes on, and a merge that reaches it fails
 * the store's writes without changing the manifest. {@link StoreCheck} finds it ahead of time.
 */
class TableDamageTest {
    @TempDir
    Path dir;

    private static Options options() {
        return Options.defaults().withSync(SyncPolicy.NEVER);
    }

    /**
     * Two tables: an older one where every key has value "old", and a newer one with "new"
     * values for keys a000 to a999, about 30 blocks. Returns the newer table's file.
     */
    private static Path twoTables(Store store) throws IOException {
        for (int i = 0; i < 1_000; i++) {
            store.put(key(i), utf8("old-" + i));
        }
        store.flush();
        for (int i = 0; i < 1_000; i++) {
            store.put(key(i), utf8("new-" + i + "-" + "x".repeat(100)));
        }
        store.flush();
        assertEquals(2, store.tables().size());
        return store.directory().resolve(Store.tableName(store.tables().get(0).number()));
    }

    private static byte[] key(int i) {
        return utf8(String.format("a%03d", i));
    }

    /** Flips one byte of the file, in place: the store's open channel reads it through the page cache. */
    private static void flip(Path file, long offset) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer b = ByteBuffer.allocate(1);
            channel.read(b, offset);
            b.put(0, (byte) (b.get(0) ^ 0x20));
            channel.write(b.flip(), offset);
        }
    }

    @Test
    void aReadOfADamagedBlockFailsAndNothingElseDoes() throws IOException {
        try (Store store = Store.open(dir, options())) {
            Path newer = twoTables(store);
            flip(newer, 100); // inside the first data block: a000 and the keys after it
            CorruptTableException e = assertThrows(CorruptTableException.class, () -> store.get(key(0)));
            assertTrue(e.getMessage().contains("fails its checksum") && e.getMessage().contains(newer.getFileName().toString()),
                    e.getMessage());
            // Not the older table's "old-0": falling through to it would return an overwritten value.
            assertArrayEquals(utf8("new-999-" + "x".repeat(100)), store.get(key(999)).orElseThrow(), "another block");
            assertEquals("absent", store.get(utf8("b")).map(v -> "present").orElse("absent"), "a key no table holds");
            store.put(key(0), utf8("rewritten")); // writes go on: a read's failure isn't the store's
            assertArrayEquals(utf8("rewritten"), store.get(key(0)).orElseThrow(), "the memtable's, found first");
        }
    }

    @Test
    void aMergeThatReachesADamagedBlockFailsTheStoreAndChangesNothing() throws IOException {
        Manifest before;
        try (Store store = Store.open(dir, options())) {
            Path newer = twoTables(store);
            before = store.manifest();
            flip(newer, 100);
            assertThrows(CorruptTableException.class, store::compactAll);
            assertThrows(StoreFailedException.class, () -> store.put(utf8("k"), utf8("v")), "writes stop");
            assertArrayEquals(utf8("new-999-" + "x".repeat(100)), store.get(key(999)).orElseThrow(), "reads go on");
        }
        assertEquals(before, Manifest.read(dir), "the merge never got to the manifest");
        try (Store store = Store.open(dir, options())) { // data blocks aren't read at open
            assertEquals(2_000, store.lastSequence());
            assertThrows(CorruptTableException.class, () -> store.get(key(0)));
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            assertEquals(1, StoreCheck.verify(dir, new PrintStream(out, true, StandardCharsets.UTF_8)), out.toString());
        }
    }

    @Test
    void theServerAnswersADamagedReadWithAnErrorAndGoesOn() throws IOException {
        try (Store store = Store.open(dir, options());
                Server server = Server.leader(store, Server.Config.loopback(0))) {
            Path newer = twoTables(store);
            flip(newer, 100);
            try (Client client = Client.connect(server.address())) {
                ServerException e = assertThrows(ServerException.class, () -> client.get(key(0)));
                assertEquals(ErrorCode.INTERNAL, e.code());
                assertTrue(e.getMessage().contains("fails its checksum"), e.getMessage());
                assertArrayEquals(utf8("new-999-" + "x".repeat(100)), client.get(key(999)).orElseThrow(),
                        "the same connection goes on");
                // A merge that reaches the block fails the store; a write's reply names the damage too,
                // which StoreFailedException's own message, path and all, didn't fit (found by review).
                assertThrows(CorruptTableException.class, store::compactAll);
                ServerException failed = assertThrows(ServerException.class, () -> client.put(utf8("k"), utf8("v")));
                assertEquals(ErrorCode.FAILED, failed.code());
                assertTrue(failed.getMessage().contains(newer.getFileName() + ": data block 0")
                        && failed.getMessage().contains("fails its checksum"), failed.getMessage());
            }
        }
    }

    /** Logs a leader keeps for its followers, which the tables cover, aren't checked as logs to replay. */
    @Test
    void verifyPassesAStoreThatKeepsLogsForFollowers() throws IOException {
        try (Store store = Store.open(dir, options().withFlushLogBytes(4 << 10).withRetainLogBytes(1 << 20))) {
            for (int i = 0; i < 2_000; i++) {
                store.put(key(i % 1_000), utf8("v" + i));
            }
        }
        long kept;
        try (var files = java.nio.file.Files.list(dir)) {
            kept = files.filter(f -> f.getFileName().toString().endsWith(Store.LOG_SUFFIX)).count();
        }
        assertTrue(kept > 3, kept + " logs");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertEquals(0, StoreCheck.verify(dir, new PrintStream(out, true, StandardCharsets.UTF_8)), out.toString());
    }

    /** A table that doesn't match the manifest's record of it (here, its recorded size) is damage. */
    @Test
    void verifyChecksEachTableAgainstTheManifestsRecord() throws IOException {
        try (Store store = Store.open(dir, options())) {
            twoTables(store);
        }
        Manifest m = Manifest.read(dir);
        Manifest.TableRef first = m.tables().get(0);
        java.util.List<Manifest.TableRef> refs = new java.util.ArrayList<>(m.tables());
        refs.set(0, new Manifest.TableRef(first.number(), first.fileBytes() + 1, first.smallestSeq(), first.largestSeq()));
        new Manifest(m.nextFileNumber(), m.logNumber(), m.lastSequence(), m.lastChecksum(), refs)
                .write(dir, (path, channel) -> channel);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertEquals(1, StoreCheck.verify(dir, new PrintStream(out, true, StandardCharsets.UTF_8)));
        assertTrue(out.toString(StandardCharsets.UTF_8).contains("doesn't match the manifest's record of it"), out::toString);
    }

    /**
     * Tables that pass every checksum but lie, made by {@link TableSurgery}: a footer with a wrong
     * tombstone count, a footer whose sequence range leaves out an entry (with the manifest's
     * record changed to match, so that check passes), and a Bloom filter that holds no key. Only
     * verify's own checks can find these; the second review found none of the three tested.
     */
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"tombstones", "range", "filter"})
    void verifyFindsATableThatPassesItsChecksumsButLies(String lie) throws IOException {
        try (Store store = Store.open(dir, options())) {
            twoTables(store);
        }
        Manifest m = Manifest.read(dir);
        Manifest.TableRef newest = m.tables().get(0);
        Path file = dir.resolve(Store.tableName(newest.number()));
        String expected;
        switch (lie) {
            case "tombstones" -> {
                TableSurgery.rewriteFooter(file, 1, newest.smallestSeq(), newest.largestSeq());
                expected = "0 tombstones, but the footer says 1";
            }
            case "range" -> {
                TableSurgery.rewriteFooter(file, 0, newest.smallestSeq() + 1, newest.largestSeq());
                java.util.List<Manifest.TableRef> refs = new java.util.ArrayList<>(m.tables());
                refs.set(0, new Manifest.TableRef(newest.number(), newest.fileBytes(), newest.smallestSeq() + 1,
                        newest.largestSeq()));
                new Manifest(m.nextFileNumber(), m.logNumber(), m.lastSequence(), m.lastChecksum(), refs)
                        .write(dir, (path, channel) -> channel);
                expected = "an entry with sequence number " + newest.smallestSeq() + ", outside the table's";
            }
            default -> {
                TableSurgery.emptyTheFilter(file);
                expected = "doesn't find it";
            }
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertEquals(1, StoreCheck.verify(dir, new PrintStream(out, true, StandardCharsets.UTF_8)), out::toString);
        String report = out.toString(StandardCharsets.UTF_8);
        assertTrue(report.startsWith(file.getFileName() + ": DAMAGED: ") && report.contains(expected), report);
        try (Store store = Store.open(dir, options())) { // it opens: nothing at open looks this far
            assertEquals(2_000, store.lastSequence());
        }
    }

    /** {@link StoreCheck} reads every block, so it finds damage no read has reached; a clean store passes. */
    @Test
    void verifyFindsDamageInABlockNoReadHasReached() throws IOException {
        try (Store store = Store.open(dir, options())) {
            twoTables(store);
            store.put(utf8("in the log"), utf8("v"));
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertEquals(0, StoreCheck.verify(dir, new PrintStream(out, true, StandardCharsets.UTF_8)));
        String clean = out.toString(StandardCharsets.UTF_8);
        assertTrue(clean.contains("1000 entries in") && clean.contains("writes #2001 to #2001") && clean.endsWith("ok\n"),
                clean);

        Manifest manifest = Manifest.read(dir);
        Path older = dir.resolve(Store.tableName(manifest.tables().get(1).number()));
        flip(older, 50);
        out.reset();
        assertEquals(1, StoreCheck.verify(dir, new PrintStream(out, true, StandardCharsets.UTF_8)));
        String damaged = out.toString(StandardCharsets.UTF_8);
        assertTrue(damaged.contains(older.getFileName() + ": DAMAGED: ") && damaged.contains("fails its checksum")
                && damaged.endsWith("1 damaged\n"), damaged);
        try (Store store = Store.open(dir, options())) {
            // Every key is in the newer table, so no read reaches the older one's damage.
            assertEquals("v", new String(store.get(utf8("in the log")).orElseThrow(), StandardCharsets.UTF_8));
            for (int i = 0; i < 1_000; i++) {
                assertTrue(store.get(key(i)).isPresent());
            }
        }
    }
}
