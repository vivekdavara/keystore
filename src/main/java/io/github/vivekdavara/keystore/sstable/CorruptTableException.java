package io.github.vivekdavara.keystore.sstable;

import java.io.IOException;
import java.nio.file.Path;

/** A table file failed a check: a checksum, the footer's layout, or the structure of a block. */
public final class CorruptTableException extends IOException {
    private static final long serialVersionUID = 1L;

    private final transient Path file;
    private final String problem;

    public CorruptTableException(Path file, String problem) {
        super(file + ": " + problem);
        this.file = file;
        this.problem = problem;
    }

    /** The table's file. */
    public Path file() {
        return file;
    }

    /** What is wrong with it, without the file's path. */
    public String problem() {
        return problem;
    }
}
