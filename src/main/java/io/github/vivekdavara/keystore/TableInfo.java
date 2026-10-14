package io.github.vivekdavara.keystore;

/**
 * A live table, for tests and tools.
 *
 * @param blocksRead data blocks read from it so far, by gets and compactions
 */
public record TableInfo(
        long number, long fileBytes, long entries, long tombstones, long smallestSeq, long largestSeq,
        long blocksRead) {}
