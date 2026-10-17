package io.github.vivekdavara.keystore;

import io.github.vivekdavara.keystore.wal.WriteAheadLog;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.zip.CRC32C;

/**
 * The store's list of live tables, and where recovery starts: the one file that says what the
 * directory's other files mean. Rewritten whole, atomically, on every flush and compaction: written
 * to {@code MANIFEST.tmp}, forced, renamed over {@code MANIFEST}, and the directory forced. A crash
 * leaves the old manifest or the new one, never a mix.
 *
 * <pre>
 * magic (8) | next file number (8) | log number (8) | last sequence (8) | last checksum (8)
 * | table count (4)
 * | per table, newest first: file number (8) | file bytes (8) | smallest seq (8) | largest seq (8)
 * | CRC32C of everything before it (4)
 * </pre>
 *
 * The last checksum came on day 4 (the magic's last byte is the version, 2); a day-2 manifest,
 * version 1, has no such field and reads with -1 there.
 *
 * @param nextFileNumber the next number to give a log or table file
 * @param logNumber the oldest log recovery replays: older logs are covered by tables
 * @param lastSequence the last sequence number the tables cover; the replayed logs start after it
 * @param lastChecksum the stored checksum of the write {@code lastSequence} names, as an unsigned
 *     32-bit number, or -1 if unknown (always -1 when {@code lastSequence} is 0). A store whose
 *     every write is in its tables still knows its last write's checksum from this, for
 *     {@link Store#lastChecksum()}: a follower's leader checks it (day 3's {@code ERR DIVERGED})
 * @param tables the live tables, newest first, as reads must consult them
 */
public record Manifest(long nextFileNumber, long logNumber, long lastSequence, long lastChecksum,
        List<TableRef> tables) {
    static final String FILE = "MANIFEST";
    static final String TEMP_FILE = "MANIFEST.tmp";
    /** "KSMANIF" and a 1: day 2's format, without the last checksum. Still read. */
    static final long MAGIC_V1 = 0x4b534d414e494601L;
    /** "KSMANIF" and a 2: the format written since day 4. */
    static final long MAGIC = 0x4b534d414e494602L;
    /** The format version written: the magic's last byte. */
    static final int FORMAT = (int) (MAGIC & 0xff);
    /** The oldest format version read. */
    static final int OLDEST_FORMAT = (int) (MAGIC_V1 & 0xff);

    /** A live table, as the manifest records it; recovery checks the file against it. */
    public record TableRef(long number, long fileBytes, long smallestSeq, long largestSeq) {}

    public Manifest {
        tables = List.copyOf(tables);
        String problem = problem(nextFileNumber, logNumber, lastSequence, lastChecksum, tables);
        if (problem != null) {
            throw new IllegalArgumentException(problem);
        }
    }

    /** A manifest that doesn't know its last write's checksum. */
    public Manifest(long nextFileNumber, long logNumber, long lastSequence, List<TableRef> tables) {
        this(nextFileNumber, logNumber, lastSequence, -1, tables);
    }

    /** A new store's: no tables, log 1, file numbers from 2. */
    static Manifest empty() {
        return new Manifest(2, 1, 0, List.of());
    }

    /**
     * What's wrong with these fields, or null. Tables come newest first, each covering sequence
     * numbers strictly after the next one's, all within the last sequence; numbers are unique and
     * below the next file number.
     */
    private static String problem(long nextFileNumber, long logNumber, long lastSequence, long lastChecksum,
            List<TableRef> tables) {
        if (logNumber < 1 || nextFileNumber <= logNumber || lastSequence < 0) {
            return "file numbers " + nextFileNumber + " (next) and " + logNumber + " (log), last sequence " + lastSequence;
        }
        if (lastChecksum < -1 || lastChecksum > 0xffffffffL || (lastSequence == 0 && lastChecksum != -1)) {
            return "last checksum " + lastChecksum + " for last sequence " + lastSequence;
        }
        Set<Long> numbers = new HashSet<>();
        long below = lastSequence + 1;
        for (TableRef t : tables) {
            if (t.number() < 1 || t.number() >= nextFileNumber || !numbers.add(t.number())) {
                return "table number " + t.number();
            }
            if (t.smallestSeq() < 1 || t.smallestSeq() > t.largestSeq() || t.largestSeq() >= below || t.fileBytes() < 1) {
                return "table " + t.number() + " covers sequence numbers " + t.smallestSeq() + "-" + t.largestSeq()
                        + ", which must be below " + below + ", in " + t.fileBytes() + " bytes";
            }
            below = t.smallestSeq();
        }
        return null;
    }

    byte[] encode() {
        ByteBuffer out = ByteBuffer.allocate(8 + 8 + 8 + 8 + 8 + 4 + 32 * tables.size() + 4);
        out.putLong(MAGIC).putLong(nextFileNumber).putLong(logNumber).putLong(lastSequence).putLong(lastChecksum)
                .putInt(tables.size());
        for (TableRef t : tables) {
            out.putLong(t.number()).putLong(t.fileBytes()).putLong(t.smallestSeq()).putLong(t.largestSeq());
        }
        out.putInt(crc(out.array(), out.position()));
        return out.array();
    }

    /**
     * Decodes a manifest.
     *
     * @throws IOException if the bytes fail the checksum or can't be a manifest
     */
    static Manifest decode(byte[] bytes) throws IOException {
        if (bytes.length < 8 + 8 + 8 + 8 + 4 + 4) {
            throw new IOException("the manifest is " + bytes.length + " bytes, too short to be one");
        }
        ByteBuffer in = ByteBuffer.wrap(bytes);
        long magic = in.getLong();
        if (magic != MAGIC && magic != MAGIC_V1) {
            throw new IOException("the manifest doesn't start with the manifest magic");
        }
        int stored = ByteBuffer.wrap(bytes, bytes.length - 4, 4).getInt();
        if (stored != crc(bytes, bytes.length - 4)) {
            throw new IOException("the manifest fails its checksum");
        }
        try {
            long next = in.getLong();
            long log = in.getLong();
            long last = in.getLong();
            long lastChecksum = magic == MAGIC ? in.getLong() : -1;
            int count = in.getInt();
            if (count < 0 || (long) count * 32 != bytes.length - in.position() - 4L) {
                throw new IOException("the manifest lists " + count + " tables in " + bytes.length + " bytes");
            }
            List<TableRef> tables = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                tables.add(new TableRef(in.getLong(), in.getLong(), in.getLong(), in.getLong()));
            }
            return new Manifest(next, log, last, lastChecksum, tables);
        } catch (IllegalArgumentException | BufferUnderflowException e) {
            throw new IOException("the manifest is inconsistent: " + e.getMessage(), e);
        }
    }

    private static int crc(byte[] bytes, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, length);
        return (int) crc.getValue();
    }

    /** Reads {@code dir}'s manifest, or returns null if it has none. */
    static Manifest read(Path dir) throws IOException {
        Path file = dir.resolve(FILE);
        if (!Files.exists(file)) {
            return null;
        }
        try {
            return decode(Files.readAllBytes(file));
        } catch (IOException e) {
            throw new IOException(file + ": " + e.getMessage(), e);
        }
    }

    /**
     * Replaces {@code dir}'s manifest with this one, atomically and durably: when this returns, a
     * crash leaves this manifest; if it throws, a crash leaves this one or the one before.
     */
    void write(Path dir, BiFunction<Path, FileChannel, FileChannel> wrap) throws IOException {
        Path temp = dir.resolve(TEMP_FILE);
        try (FileChannel out = wrap.apply(temp, FileChannel.open(temp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING))) {
            ByteBuffer bytes = ByteBuffer.wrap(encode());
            while (bytes.hasRemaining()) {
                out.write(bytes);
            }
            out.force(false);
        }
        Files.move(temp, dir.resolve(FILE), StandardCopyOption.ATOMIC_MOVE); // rename(2): replaces
        WriteAheadLog.forceDirectory(dir, wrap);
    }
}
