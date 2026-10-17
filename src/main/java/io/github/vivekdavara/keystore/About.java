package io.github.vivekdavara.keystore;

import io.github.vivekdavara.keystore.sstable.Table;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

/**
 * What this build is: the version Maven built it as, and the versions of the files it writes and
 * reads ({@code keystore version} prints them). A table's footer and the manifest's magic each carry
 * their format's version, so a later build can tell an old file from a new one; a log record carries
 * none (DESIGN.md, "The release (day 5)").
 */
public final class About {
    private About() {}

    /** The table format written, and the only one read. */
    public static final int TABLE_FORMAT = Table.FORMAT_VERSION;
    /** The manifest format written. */
    public static final int MANIFEST_FORMAT = Manifest.FORMAT;
    /** The oldest manifest format read: day 2's, without the last write's checksum. */
    public static final int OLDEST_MANIFEST_FORMAT = Manifest.OLDEST_FORMAT;

    private static final String RESOURCE = "keystore.properties";

    /**
     * The project's version ({@code 1.0.0}), which Maven writes into {@code keystore.properties} when
     * it copies the resources; "unknown" for classes compiled some other way.
     */
    public static String version() {
        try (InputStream in = About.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                return "unknown";
            }
            Properties properties = new Properties();
            properties.load(in);
            return properties.getProperty("version", "unknown");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
