package io.github.vivekdavara.keystore.sstable;

import java.io.IOException;
import java.nio.file.Path;

/** A table file failed a check: a checksum, the footer's layout, or the structure of a block. */
public final class CorruptTableException extends IOException {
    private static final long serialVersionUID = 1L;

    public CorruptTableException(Path file, String problem) {
        super(file + ": " + problem);
    }
}
