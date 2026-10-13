package io.github.vivekdavara.keystore.wal;

import java.io.IOException;

/** The log is damaged where dropping the damage would lose writes made after it. */
public final class WalCorruptionException extends IOException {
    private static final long serialVersionUID = 1L;

    private final long offset;

    public WalCorruptionException(String message, long offset) {
        super(message);
        this.offset = offset;
    }

    /** Where the first damaged record starts, in bytes from the start of the log. */
    public long offset() {
        return offset;
    }
}
