package org.dcache.boot;

import java.io.PrintStream;

/**
 * A class that implements LayoutPrinter provides the mechanism to
 * serialising a dCache layout.
 */
public interface LayoutPrinter {

    /**
     * Provide output from a layout.
     */
    void print(PrintStream out);
}
