package org.dcache.pool.movers;

/**
 * A <code>IoMode</code> identifies transfers IO mode.
 *
 * TODO: replace this enum with java.nio.file.StandardOpenOption
 *
 * @since 1.9.11
 */
public enum IoMode {
    READ("r"),
    WRITE("rw");

    IoMode(String openString) {
        this.openString = openString;
    }

    private final String openString;
    public String toOpenString()
    {
        return openString;
    }
}
