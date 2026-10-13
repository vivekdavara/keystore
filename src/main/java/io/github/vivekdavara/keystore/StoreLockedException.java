package io.github.vivekdavara.keystore;

import java.io.IOException;

/** The store's directory is already open, in this process or another one. */
public final class StoreLockedException extends IOException {
    private static final long serialVersionUID = 1L;

    public StoreLockedException(String message) {
        super(message);
    }
}
