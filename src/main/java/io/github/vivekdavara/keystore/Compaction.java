package io.github.vivekdavara.keystore;

/**
 * Which tables to merge: size-tiered, over tables next to each other in age.
 *
 * <p>Tables are listed newest first, and each covers sequence numbers after the next one's. A merge
 * must take a run of adjacent tables, so that its output covers one range and sits where the run
 * was: merging two tables with a third between them would put the output either before the third
 * (its older entries would hide the third's newer ones) or after it (the reverse).
 *
 * <p>The tables are grouped into tiers, newest first: a table joins the tier before it if both are
 * small (no bigger than {@code smallBytes}, a flush's worth), or if its size is within half to one
 * and a half times the tier's average so far (Cassandra's size-tiered defaults). The newest tier
 * with at least {@code width} tables is merged, all of it. Each merge makes a table about
 * {@code width} times bigger, which joins the next tier up, so a key is rewritten about
 * {@code log_width(data / flush size)} times.
 */
final class Compaction {
    private Compaction() {}

    /**
     * The run to merge, as {@code [from, to)} indexes into {@code sizes} (newest first), or null if
     * no tier has {@code width} tables.
     */
    static int[] pick(long[] sizes, int width, long smallBytes) {
        int start = 0;
        while (start < sizes.length) {
            int end = start + 1;
            long total = sizes[start];
            while (end < sizes.length && joins(sizes[end], total, end - start, sizes[end - 1], smallBytes)) {
                total += sizes[end];
                end++;
            }
            if (end - start >= width) {
                return new int[] {start, end};
            }
            start = end;
        }
        return null;
    }

    private static boolean joins(long size, long tierTotal, int tierCount, long previous, long smallBytes) {
        if (size <= smallBytes && previous <= smallBytes) {
            return true;
        }
        double average = (double) tierTotal / tierCount;
        return size >= average / 2 && size <= average * 1.5;
    }
}
