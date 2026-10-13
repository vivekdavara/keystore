package io.github.vivekdavara.keystore;

/** When the store forces the log to the device. */
public enum SyncPolicy {
    /**
     * After every write, before it is acknowledged: an acknowledged write survives a power loss or an
     * OS crash. The default. Costs one device flush per write.
     */
    EVERY_WRITE,

    /**
     * Only when the store is closed. Each write still reaches the OS before it is acknowledged, so it
     * survives the process being killed, but an OS crash or power loss can lose the latest writes.
     */
    NEVER
}
