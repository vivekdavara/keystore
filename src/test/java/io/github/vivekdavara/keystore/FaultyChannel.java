package io.github.vivekdavara.keystore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * A file channel that fails on demand: writes after a byte budget runs out (writing the part that
 * fits first, as a real short write would), and forces. Everything else goes to the real channel.
 */
final class FaultyChannel extends FileChannel {
    private final FileChannel inner;
    private volatile long writeBudget = Long.MAX_VALUE;
    private volatile boolean failForce;

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

    @Override
    public int write(ByteBuffer src) throws IOException {
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
        if (failForce) {
            throw new IOException("injected: force failed");
        }
        inner.force(metaData);
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
