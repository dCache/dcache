package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;
import diskCacheV111.util.PnfsId;

/**
 * Repository entry filter which only accepts entries with a specific
 * PNFS id.
 */
public class PnfsIdFilter implements CacheEntryFilter
{
    private PnfsId _pnfsId;

    public PnfsIdFilter(PnfsId pnfsId)
    {
        _pnfsId = pnfsId;
    }

    public boolean accept(CacheEntry entry)
    {
        return _pnfsId.equals(entry.getPnfsId());
    }
}