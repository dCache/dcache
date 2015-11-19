package org.dcache.gplazma.plugins;

import diskCacheV111.namespace.NameSpaceProvider;

/**
 * A gPlazma plugin that implements NamespaceAware is able to talk to
 * dCache's namespace.
 */
public interface NamespaceAware
{
    /**
     * The plugin accepts the namespace provider.  This method is called
     * precisely once before any plugin-related methods are called.
     */
    void setNamespace(NameSpaceProvider namespace);
}
