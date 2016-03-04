package org.dcache.pool.migration;

import java.util.Comparator;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.Repository;

class CacheEntryOrder implements Comparator<PnfsId>
{
    private final Repository _repository;
    private final Comparator<CacheEntry> _comparator;

    public CacheEntryOrder(Repository repository,
                           Comparator<CacheEntry> comparator)
    {
        _repository = repository;
        _comparator = comparator;
    }

    @Override
    public int compare(PnfsId id1, PnfsId id2)
    {
        try {
            CacheEntry entry1 = _repository.getEntry(id1);
            CacheEntry entry2 = _repository.getEntry(id2);
            return _comparator.compare(entry1, entry2);
        } catch (FileNotInCacheException e) {
            return id1.compareTo(id2);
        } catch (InterruptedException e) {
            throw new RuntimeException("Thread got interupted", e);
        } catch (CacheException e) {
            throw new RuntimeException("Repository failed: " + e.getMessage(), e);
        }
    }
}
