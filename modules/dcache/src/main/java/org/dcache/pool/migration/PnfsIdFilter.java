package org.dcache.pool.migration;

import java.util.Collection;
import java.util.HashSet;

import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.CacheEntry;

/**
 * Repository entry filter which only accepts entries with specific
 * PNFS ids.
 */
public class PnfsIdFilter implements CacheEntryFilter
{
    private final Collection<PnfsId> _pnfsIds;

    public PnfsIdFilter(Collection<PnfsId> pnfsIds)
    {
        _pnfsIds = new HashSet<>(pnfsIds);
    }

    @Override
    public boolean accept(CacheEntry entry)
    {
        return _pnfsIds.contains(entry.getPnfsId());
    }
}
