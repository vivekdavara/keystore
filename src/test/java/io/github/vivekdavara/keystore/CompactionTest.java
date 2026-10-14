package io.github.vivekdavara.keystore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** Which adjacent tables a compaction merges. Sizes are listed newest first, as the store keeps them. */
class CompactionTest {
    private static final long SMALL = 1_000;

    @Test
    void fourSmallTablesMerge() {
        assertArrayEquals(new int[] {0, 4}, Compaction.pick(new long[] {100, 900, 50, 1_000}, 4, SMALL));
        assertNull(Compaction.pick(new long[] {100, 900, 50}, 4, SMALL), "three aren't a tier of four");
        assertNull(Compaction.pick(new long[0], 4, SMALL));
    }

    @Test
    void aTierIsSimilarSizesNextToEachOther() {
        // 4,000 and 5,000 and 3,000 are within half to one and a half times their running average.
        assertArrayEquals(new int[] {0, 4}, Compaction.pick(new long[] {4_000, 5_000, 3_000, 4_500}, 4, SMALL));
        // 20,000 is more than 1.5 times the average of the first three: the tier ends before it.
        assertNull(Compaction.pick(new long[] {4_000, 5_000, 3_000, 20_000}, 4, SMALL));
    }

    @Test
    void theNewestFullTierIsPicked() {
        long[] sizes = {500, 600, 16_000, 15_000, 17_000, 16_500, 64_000};
        assertArrayEquals(new int[] {2, 6}, Compaction.pick(sizes, 4, SMALL), "the two small ones don't make four");
        long[] both = {500, 600, 700, 800, 16_000, 15_000, 17_000, 16_500};
        assertArrayEquals(new int[] {0, 4}, Compaction.pick(both, 4, SMALL));
    }

    /**
     * A merge takes adjacent tables only: four similar tables with a different one between them are
     * two tiers, and neither has four. Merging around the odd one would put the output on the wrong
     * side of it in age.
     */
    @Test
    void aTableOfAnotherSizeBetweenSimilarOnesSplitsTheTier() {
        assertNull(Compaction.pick(new long[] {4_000, 4_000, 64_000, 4_000, 4_000}, 4, SMALL));
        assertArrayEquals(new int[] {3, 7},
                Compaction.pick(new long[] {4_000, 4_000, 64_000, 4_000, 4_000, 4_000, 4_000}, 4, SMALL));
    }

    @Test
    void aWholeTierMergesEvenPastTheWidth() {
        assertArrayEquals(new int[] {0, 6}, Compaction.pick(new long[] {10, 20, 30, 40, 50, 60}, 4, SMALL));
        assertArrayEquals(new int[] {0, 2}, Compaction.pick(new long[] {10, 20}, 2, SMALL));
    }

    @Test
    void aSmallTableAfterABigTierDoesNotJoinIt() {
        // 900 is small, but the table before it isn't, so the ratio decides: 900 is far below 4,000.
        assertNull(Compaction.pick(new long[] {4_000, 4_000, 4_000, 900}, 4, SMALL));
    }
}
