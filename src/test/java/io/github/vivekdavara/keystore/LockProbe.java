package io.github.vivekdavara.keystore;

import java.io.IOException;
import java.nio.file.Path;

/** Run in a child JVM by the lock tests: tries to open a store and prints what happened. */
public final class LockProbe {
    private LockProbe() {}

    public static void main(String[] args) throws IOException {
        try (Store store = Store.open(Path.of(args[0]))) {
            System.out.println("opened " + store.lastSequence());
        } catch (StoreLockedException e) {
            System.out.println("locked");
        }
    }
}
