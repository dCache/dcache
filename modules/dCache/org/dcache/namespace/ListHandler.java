package org.dcache.namespace;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import org.dcache.vehicles.FileAttributes;

/**
 * Callback interface used by NameSpaceProvider.list.
 */
public interface ListHandler
{
    void addEntry(String name, PnfsId pnfsId, FileAttributes attrs)
        throws CacheException;
}