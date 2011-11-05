package org.dcache.gplazma.loader.cli;

import java.io.PrintStream;

import org.dcache.gplazma.loader.PluginRepositoryFactory;

/**
 * The gPlazma plugin-loader CLI commands.
 */
public interface Command {
    int run( String[] args);
    void setFactory( PluginRepositoryFactory factory);
    void setOutput( PrintStream out);
}
