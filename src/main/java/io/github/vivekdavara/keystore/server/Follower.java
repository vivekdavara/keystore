package io.github.vivekdavara.keystore.server;

import io.github.vivekdavara.keystore.Store;
import io.github.vivekdavara.keystore.server.Protocol.ProtocolException;
import io.github.vivekdavara.keystore.wal.WalFormat;
import io.github.vivekdavara.keystore.wal.WalRecord;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Keeps a store a copy of a leader's. It connects, asks for the writes after its store's last
 * sequence number ({@code SYNC lastSequence()+1}), and applies what arrives through
 * {@link Store#replicate}, in batches of whatever has arrived. When the connection breaks it
 * reconnects with backoff and asks again from wherever the store got to: that is how a follower
 * that fell behind, or restarted, catches up. Its position is its store's {@code lastSequence()},
 * so it is exactly as durable as the store's writes.
 *
 * <p>Every record is checked twice: its checksum (the leader sends the bytes its log holds) and
 * its sequence number, which must be the next one. A store whose writes fail, or a leader that no
 * longer has the log it needs ({@link State#BEHIND}) or has fewer writes than it
 * ({@link State#AHEAD}), stops the follower: those need someone to act.
 *
 * <p>The follower's thread is never interrupted (an interrupt during the store's file I/O would
 * close its log); {@link #close} closes the socket instead.
 */
public final class Follower implements Closeable {
    /** Where the follower is. */
    public enum State {
        /** Connecting, or waiting to reconnect. */
        CONNECTING,
        /** Applying the leader's stream. */
        STREAMING,
        /** Stopped: the leader no longer has the log for this store's next record (it needs a copy of the tables). */
        BEHIND,
        /** Stopped: this store has writes the leader doesn't. */
        AHEAD,
        /** Stopped: this store's writes failed, or it refused a record. */
        FAILED,
        /** Closed. */
        CLOSED
    }

    /**
     * What the follower knows.
     *
     * @param applied its store's last sequence number
     * @param leader the leader's last sequence number, as of its latest record or heartbeat (0 before any)
     * @param connects connections made to the leader so far
     * @param batches batches applied so far (each one call to {@link Store#replicate}, one force)
     * @param detail why it last disconnected or stopped, or empty
     */
    public record Status(State state, long applied, long leader, long connects, long batches, String detail) {
        /** How many writes the leader had that this store didn't, as of the last news from the leader. */
        public long lag() {
            return Math.max(0, leader - applied);
        }
    }

    /**
     * How a follower runs.
     *
     * @param timeout how long to wait to connect, and for the next line once connected (a leader
     *     heartbeats every second by default)
     * @param maxBackoff the longest wait between reconnections; waits start at 50 ms and double
     * @param maxBatchRecords the most records applied (and forced) together
     * @param maxBatchBytes the most bytes of records applied together
     */
    public record Config(InetSocketAddress leader, Duration timeout, Duration maxBackoff, int maxBatchRecords,
            int maxBatchBytes) {
        public Config {
            Objects.requireNonNull(leader, "leader");
            Objects.requireNonNull(timeout, "timeout");
            Objects.requireNonNull(maxBackoff, "maxBackoff");
            if (timeout.toMillis() < 1 || maxBackoff.toMillis() < 1 || maxBatchRecords < 1 || maxBatchBytes < 1) {
                throw new IllegalArgumentException("timeouts, backoff and batch sizes must be positive");
            }
        }

        /** A 10-second timeout, at most 2 s between reconnections, batches of up to 1,024 records or 4 MiB. */
        public static Config of(InetSocketAddress leader) {
            return new Config(leader, Duration.ofSeconds(10), Duration.ofSeconds(2), 1024, 4 << 20);
        }

        public Config withTimeout(Duration timeout) {
            return new Config(leader, timeout, maxBackoff, maxBatchRecords, maxBatchBytes);
        }

        public Config withMaxBackoff(Duration maxBackoff) {
            return new Config(leader, timeout, maxBackoff, maxBatchRecords, maxBatchBytes);
        }

        public Config withMaxBatchRecords(int maxBatchRecords) {
            return new Config(leader, timeout, maxBackoff, maxBatchRecords, maxBatchBytes);
        }
    }

    private static final long MIN_BACKOFF_MILLIS = 50;

    private final Store store;
    private final Config config;
    private final Thread thread;
    private final CountDownLatch closedLatch = new CountDownLatch(1);
    private volatile boolean closed;
    private volatile Socket socket;
    private volatile State state = State.CONNECTING;
    private volatile long leaderSequence;
    private volatile long connects;
    private volatile long batches; // written only by the follower's thread
    private volatile String detail = "";

    private Follower(Store store, Config config) {
        this.store = Objects.requireNonNull(store, "store");
        this.config = Objects.requireNonNull(config, "config");
        this.thread = Thread.ofPlatform().name("keystore-follower").daemon().unstarted(this::run);
    }

    /** Starts following {@code config.leader()} into {@code store}, on a thread of its own. */
    public static Follower start(Store store, Config config) {
        Follower follower = new Follower(store, config);
        follower.thread.start();
        return follower;
    }

    public Status status() {
        return new Status(state, store.lastSequence(), leaderSequence, connects, batches, detail);
    }

    /** {@link #status()} as {@code INFO} words. */
    public String info() {
        Status s = status();
        return "state=" + s.state() + " leader=" + config.leader().getHostString() + ":" + config.leader().getPort()
                + " leaderSeq=" + s.leader() + " lag=" + s.lag() + " connects=" + s.connects();
    }

    /** Waits until the store has applied {@code seq}; returns whether it did within the timeout. */
    public boolean awaitApplied(long seq, Duration timeout) throws InterruptedException {
        return store.awaitSequence(seq, timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void run() {
        long backoff = MIN_BACKOFF_MILLIS;
        while (!closed) {
            state = State.CONNECTING;
            long before = store.lastSequence();
            try (Socket s = new Socket()) {
                socket = s;
                if (closed) {
                    break;
                }
                int timeout = Math.toIntExact(Math.max(1, config.timeout().toMillis()));
                s.connect(config.leader(), timeout);
                s.setSoTimeout(timeout);
                s.setTcpNoDelay(true);
                connects++;
                InputStream in = new BufferedInputStream(s.getInputStream(), 256 * 1024);
                OutputStream out = new BufferedOutputStream(s.getOutputStream(), 8 * 1024);
                Protocol.writeLine(out, "SYNC " + (store.lastSequence() + 1));
                out.flush();
                state = State.STREAMING;
                State stop = stream(in, out);
                if (stop != null) {
                    state = stop;
                    return;
                }
            } catch (IOException e) {
                detail = String.valueOf(e.getMessage());
            } finally {
                socket = null;
            }
            if (store.lastSequence() > before) {
                backoff = MIN_BACKOFF_MILLIS; // it made progress: the leader is there
            }
            try {
                if (closedLatch.await(backoff, TimeUnit.MILLISECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                break;
            }
            backoff = Math.min(backoff * 2, Math.max(MIN_BACKOFF_MILLIS, config.maxBackoff().toMillis()));
        }
        state = State.CLOSED;
    }

    /**
     * Applies the stream until the connection breaks (an exception: reconnect) or the leader ends it
     * in a way that needs someone to act (the state to stop in).
     */
    private State stream(InputStream in, OutputStream out) throws IOException {
        List<WalRecord> batch = new ArrayList<>();
        long batchBytes = 0;
        long expected = store.lastSequence() + 1;
        for (;;) {
            String line = Protocol.readLine(in);
            if (line == null) {
                throw new EOFException("the leader closed the stream");
            }
            if (line.startsWith("REC ")) {
                long n = Protocol.number(line.substring(4));
                if (n < WalFormat.HEADER_BYTES + WalFormat.MIN_PAYLOAD || n > WalFormat.HEADER_BYTES + WalFormat.MAX_PAYLOAD) {
                    throw new ProtocolException("a record of " + n + " bytes");
                }
                byte[] bytes = Protocol.readExactly(in, (int) n);
                if (!(WalFormat.decode(bytes, 0, bytes.length) instanceof WalFormat.Ok ok) || ok.next() != bytes.length) {
                    throw new IOException("record " + expected + " from the leader fails its checks");
                }
                WalRecord record = ok.record();
                if (record.seq() != expected) {
                    throw new IOException("record " + record.seq() + " from the leader where " + expected + " should be");
                }
                leaderSequence = Math.max(leaderSequence, record.seq());
                batch.add(record);
                batchBytes += n;
                expected++;
                if (in.available() > 0 && batch.size() < config.maxBatchRecords() && batchBytes < config.maxBatchBytes()) {
                    continue; // more has arrived: take it into this batch
                }
            } else if (line.startsWith("HEARTBEAT ")) {
                leaderSequence = Math.max(leaderSequence, Protocol.number(line.substring(10)));
            } else if (line.startsWith("ERR ")) {
                ServerException e = Client.error(line);
                detail = e.getMessage();
                switch (e.code()) {
                    case BEHIND -> {
                        return apply(batch) ? State.BEHIND : State.FAILED;
                    }
                    case AHEAD -> {
                        return State.AHEAD;
                    }
                    default -> throw e;
                }
            } else {
                throw new ProtocolException("not a line of a stream: " + line);
            }
            if (!apply(batch)) {
                return State.FAILED;
            }
            batchBytes = 0;
            Protocol.writeLine(out, "ACK " + store.lastSequence());
            out.flush();
        }
    }

    /** Applies and clears the batch; false if the store refused it or failed, which stops the follower. */
    private boolean apply(List<WalRecord> batch) {
        if (batch.isEmpty()) {
            return true;
        }
        try {
            store.replicate(batch);
            batch.clear();
            batches++;
            return true;
        } catch (IOException | RuntimeException e) {
            detail = "the store refused the leader's writes: " + e;
            return false;
        }
    }

    /** Stops following and waits for the thread. Doesn't close the store. */
    @Override
    public void close() throws IOException {
        closed = true;
        closedLatch.countDown();
        Socket s = socket;
        if (s != null) {
            s.close();
        }
        try {
            thread.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        state = State.CLOSED;
    }
}
