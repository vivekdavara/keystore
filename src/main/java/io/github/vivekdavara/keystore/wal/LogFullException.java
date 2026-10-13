package io.github.vivekdavara.keystore.wal;

import java.io.IOException;

/**
 * A write would make the log larger than its limit, so nothing was written. The store goes on: reads
 * work, and a smaller write that fits is accepted. Day 1 has no log rotation, so the limit is the
 * size recovery can read back (about 2 GiB) unless the options set a smaller one.
 */
public final class LogFullException extends IOException {
    private static final long serialVersionUID = 1L;

    public LogFullException(String message) {
        super(message);
    }
}
