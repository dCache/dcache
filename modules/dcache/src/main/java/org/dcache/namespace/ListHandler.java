package org.dcache.namespace;

import diskCacheV111.util.CacheException;
import org.dcache.vehicles.FileAttributes;

/**
 * Callback interface used by NameSpaceProvider.list.
 */
public interface ListHandler
{
    void addEntry(String name, FileAttributes attrs)
        throws CacheException;
}
