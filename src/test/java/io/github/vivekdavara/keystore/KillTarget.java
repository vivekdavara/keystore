package io.github.vivekdavara.keystore;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

/**
 * The writer {@link KillTest} runs in a child JVM and kills with SIGKILL. It opens a store and makes
 * {@link Workload}'s writes one after another, from the one after the store's last, printing
 * {@code opened N} once the store is open and {@code ack N} once write N has returned.
 *
 * <pre>
 * KillTarget DIR SYNC SEED KEYS FLUSH_LOG_BYTES [KILL_AT RANDOM_SEED]
 * </pre>
 *
 * <p>Without {@code KILL_AT} it writes until the test kills it. With it, it kills itself with
 * SIGKILL at its {@code KILL_AT}-th write or force of any file (logs, tables, the manifest,
 * directories), counting from the open, so the death lands on a chosen kind of step. A write it
 * dies in is cut at a random byte first, as a write a process dies partway through can be. Just
 * before dying it prints {@code kill KIND write|force} on stderr, KIND being {@code log},
 * {@code table}, {@code manifest} or {@code directory}.
 *
 * <p>It stops when its stdin closes, so a test runner that dies doesn't leave it writing.
 */
final class KillTarget {
    /** A safety stop: a test kills it long before this. */
    private static final long MAX_WRITES = 2_000_000;

    private KillTarget() {}

    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args[0]);
        Options options = Options.defaults()
                .withSync(SyncPolicy.valueOf(args[1]))
                .withFlushLogBytes(Long.parseLong(args[4]));
        Workload workload = new Workload(Long.parseLong(args[2]), Integer.parseInt(args[3]));
        BiFunction<Path, FileChannel, FileChannel> wrap = (path, inner) -> inner;
        if (args.length > 5) {
            AtomicLong left = new AtomicLong(Long.parseLong(args[5]));
            SplittableRandom random = new SplittableRandom(Long.parseLong(args[6]));
            wrap = (path, inner) -> new KillSwitch(inner, path, left, random);
        }
        Thread.ofPlatform().daemon().start(KillTarget::haltWhenStdinCloses);
        PrintStream out = new PrintStream(new FileOutputStream(FileDescriptor.out), false, StandardCharsets.UTF_8);
        try (Store store = Store.open(dir, options, wrap)) {
            out.print("opened " + store.lastSequence() + "\n");
            out.flush();
            long first = store.lastSequence() + 1;
            for (long seq = first; seq < first + MAX_WRITES; seq++) {
                long got = workload.apply(store, seq);
                if (got != seq) {
                    throw new IllegalStateException("write " + seq + " was given sequence number " + got);
                }
                out.print("ack " + seq + "\n"); // one write(2) to the pipe: whole or not at all
                out.flush();
            }
        }
    }

    private static void haltWhenStdinCloses() {
        try {
            while (System.in.read() >= 0) {
                // nothing is sent; EOF means the test runner is gone
            }
        } catch (IOException ignored) {
            // the same
        }
        Runtime.getRuntime().halt(3);
    }

    /** Sends this process SIGKILL. Doesn't return. */
    static void die(String where) {
        System.err.println("kill " + where);
        System.err.flush();
        try {
            new ProcessBuilder("kill", "-KILL", Long.toString(ProcessHandle.current().pid()))
                    .inheritIO().start().waitFor();
            Thread.sleep(60_000);
        } catch (IOException | InterruptedException e) {
            System.err.println("could not kill myself: " + e);
        }
        Runtime.getRuntime().halt(5); // the test expects 137 (SIGKILL), so this would fail it
    }

    /** Counts writes and forces down a shared budget; the one that runs it out kills the process. */
    private static final class KillSwitch extends FaultyChannel {
        private final FileChannel inner;
        private final String kind;
        private final AtomicLong left;
        private final SplittableRandom random;

        KillSwitch(FileChannel inner, Path path, AtomicLong left, SplittableRandom random) {
            super(inner);
            this.inner = inner;
            String name = path.getFileName().toString();
            this.kind = name.endsWith(".log") ? "log" : name.endsWith(".sst") ? "table"
                    : name.startsWith("MANIFEST") ? "manifest" : "directory";
            this.left = left;
            this.random = random;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            if (left.decrementAndGet() == 0) {
                ByteBuffer part = src.duplicate();
                part.limit(part.position() + random.nextInt(Math.max(1, src.remaining()))); // some bytes, never all
                while (part.hasRemaining()) {
                    inner.write(part);
                }
                die(kind + " write");
            }
            return super.write(src);
        }

        @Override
        public void force(boolean metaData) throws IOException {
            if (left.decrementAndGet() == 0) {
                die(kind + " force");
            }
            super.force(metaData);
        }
    }
}
