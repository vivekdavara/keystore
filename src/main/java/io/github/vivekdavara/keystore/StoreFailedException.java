package io.github.vivekdavara.keystore;

import java.io.IOException;

/**
 * An earlier write failed, so the store accepts no more writes; its cause is that failure. The
 * log's state after a failed write or flush is unknown, so the store stops writing rather than
 * append after a record that may be partial. Reads still work. Reopening the store recovers it.
 */
public final class StoreFailedException extends IOException {
    private static final long serialVersionUID = 1L;

    public StoreFailedException(Throwable cause) {
        super("an earlier write failed, so this store accepts no more writes; reopen it to recover: " + cause, cause);
    }
}
