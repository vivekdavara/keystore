package io.github.vivekdavara.keystore;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A file channel that fails on demand: writes after a byte budget runs out (writing the part that
 * fits first, as a real short write would), and forces. It counts forces, and can hold the next
 * write or force at a {@link Gate} until a test lets it through. Everything else goes to the real
 * channel.
 */
class FaultyChannel extends FileChannel {
    private final FileChannel inner;
    private volatile long writeBudget = Long.MAX_VALUE;
    private volatile boolean failForce;
    private final AtomicInteger forces = new AtomicInteger();
    private volatile Gate writeGate;
    private volatile Gate forceGate;

    /** Holds one operation until {@link #open}: lets a test look at the store mid-write. */
    static final class Gate {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch opened = new CountDownLatch(1);

        void awaitEntered() throws InterruptedException {
            if (!entered.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("nothing reached the gate");
            }
        }

        void open() {
            opened.countDown();
        }

        private void pass() throws InterruptedIOException {
            entered.countDown();
            try {
                if (!opened.await(30, TimeUnit.SECONDS)) {
                    throw new AssertionError("the gate was never opened");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted at the gate");
            }
        }
    }

    /** The next write waits at the returned gate, before writing anything. */
    Gate holdNextWrite() {
        Gate gate = new Gate();
        writeGate = gate;
        return gate;
    }

    /** The next force waits at the returned gate, before forcing. */
    Gate holdNextForce() {
        Gate gate = new Gate();
        forceGate = gate;
        return gate;
    }

    FaultyChannel(FileChannel inner) {
        this.inner = inner;
    }

    /** The next writes may write {@code bytes} more bytes; the write that would pass it fails. */
    void failWritesAfter(long bytes) {
        writeBudget = bytes;
    }

    void failForces() {
        failForce = true;
    }

    /** Forces that reached the real channel. */
    int forces() {
        return forces.get();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        Gate gate = writeGate;
        if (gate != null) {
            writeGate = null;
            gate.pass();
        }
        long budget = writeBudget;
        if (src.remaining() <= budget) {
            int written = inner.write(src);
            writeBudget = budget - written;
            return written;
        }
        ByteBuffer part = src.duplicate();
        part.limit(part.position() + (int) budget);
        int written = inner.write(part);
        src.position(src.position() + written);
        writeBudget = 0;
        throw new IOException("injected: write failed after " + written + " bytes");
    }

    @Override
    public void force(boolean metaData) throws IOException {
        Gate gate = forceGate;
        if (gate != null) {
            forceGate = null;
            gate.pass();
        }
        if (failForce) {
            throw new IOException("injected: force failed");
        }
        inner.force(metaData);
        forces.incrementAndGet();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return inner.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return inner.read(dsts, offset, length);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        throw new UnsupportedOperationException("the log writes one buffer at a time");
    }

    @Override
    public long position() throws IOException {
        return inner.position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        inner.position(newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        return inner.size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        inner.truncate(size);
        return this;
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        return inner.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        return inner.transferFrom(src, position, count);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return inner.read(dst, position);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        throw new UnsupportedOperationException("the log appends at its position");
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        return inner.map(mode, position, size);
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        return inner.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return inner.tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() throws IOException {
        inner.close();
    }
}
