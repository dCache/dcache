package org.dcache.missingfiles.plugins;

/**
 *  A class that implements PluginVisitor will process the list of 
 *  plugins in the order configured.
 */
public interface PluginVisitor
{

    /**
     *  Process a plugin.  Returning true will continue to the next plugin;
     *  returning false results in no further plugins being visited.
     */
    boolean visit(Plugin plugin);
}
