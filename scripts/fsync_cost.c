/*
 * What one durable append costs on this machine, without Java in the way: append 128 bytes,
 * then flush, COUNT times, for each way of flushing. Prints the median and p99 per append.
 *
 *   cc -O2 -o target/fsync_cost scripts/fsync_cost.c && target/fsync_cost target 1000
 *
 * On macOS, fsync() only hands the data to the drive; fcntl(F_FULLFSYNC) also flushes the
 * drive's cache, which is what the JDK's FileChannel.force does there. On Linux, fsync and
 * fdatasync both flush the drive's cache.
 */
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static double now_us(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec * 1e6 + t.tv_nsec / 1e3;
}

static int cmp(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

static int flush(int fd, const char *how) {
    if (strcmp(how, "fsync") == 0) return fsync(fd);
#ifdef __APPLE__
    if (strcmp(how, "F_FULLFSYNC") == 0) return fcntl(fd, F_FULLFSYNC);
#else
    if (strcmp(how, "fdatasync") == 0) return fdatasync(fd);
#endif
    return 0; /* "none" */
}

static void run(const char *dir, int count, const char *how) {
    char path[4096];
    snprintf(path, sizeof path, "%s/fsync_cost.tmp", dir);
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) { perror(path); exit(1); }
    char record[128];
    memset(record, 'x', sizeof record);
    double *lat = malloc(sizeof(double) * count);
    for (int i = 0; i < count; i++) {
        double t0 = now_us();
        if (write(fd, record, sizeof record) != (ssize_t)sizeof record) { perror("write"); exit(1); }
        if (flush(fd, how) != 0) { perror(how); exit(1); }
        lat[i] = now_us() - t0;
    }
    qsort(lat, count, sizeof(double), cmp);
    printf("%-12s %d appends of 128 bytes: p50 %.1f us, p99 %.1f us\n", how, count, lat[count / 2],
           lat[(int)(count * 0.99)]);
    free(lat);
    close(fd);
    unlink(path);
}

int main(int argc, char **argv) {
    const char *dir = argc > 1 ? argv[1] : ".";
    int count = argc > 2 ? atoi(argv[2]) : 1000;
    run(dir, count, "none");
    run(dir, count, "fsync");
#ifdef __APPLE__
    run(dir, count, "F_FULLFSYNC");
#else
    run(dir, count, "fdatasync");
#endif
    return 0;
}
