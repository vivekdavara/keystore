// MurmurHash64A, as in Austin Appleby's SMHasher (MurmurHash2.cpp, public domain), with the
// seed the Bloom filter uses. Prints the hash of each argument as a signed 64-bit decimal, for
// BloomFilterTest's reference values:
//   cc -O2 -o target/murmur64a scripts/murmur64a.c && target/murmur64a "" a "The quick brown fox jumps over the lazy dog"
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

static uint64_t murmur64a(const void *key, size_t len, uint64_t seed) {
    const uint64_t m = 0xc6a4a7935bd1e995ULL;
    const int r = 47;
    uint64_t h = seed ^ (len * m);
    const unsigned char *data = key;
    const unsigned char *end = data + (len / 8) * 8;
    while (data != end) {
        uint64_t k;
        memcpy(&k, data, 8); // little-endian on x86 and ARM, as the Java side reads it
        data += 8;
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
    }
    switch (len & 7) {
    case 7: h ^= (uint64_t)data[6] << 48; // fall through
    case 6: h ^= (uint64_t)data[5] << 40; // fall through
    case 5: h ^= (uint64_t)data[4] << 32; // fall through
    case 4: h ^= (uint64_t)data[3] << 24; // fall through
    case 3: h ^= (uint64_t)data[2] << 16; // fall through
    case 2: h ^= (uint64_t)data[1] << 8;  // fall through
    case 1: h ^= (uint64_t)data[0];
            h *= m;
    }
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

int main(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        printf("%" PRId64 "\n", (int64_t)murmur64a(argv[i], strlen(argv[i]), 0x9747b28cULL));
    }
    return 0;
}
