package org.dcache.pool.migration;

import java.util.function.Predicate;
import org.dcache.pool.repository.CacheEntry;

/**
 * Repository entry filter accepting entries with a particular storage
 * class.
 */
public class StorageClassFilter implements Predicate<CacheEntry>
{
    private final String _sc;

    public StorageClassFilter(String sc)
    {
        _sc = sc;
    }

    @Override
    public boolean test(CacheEntry entry)
    {
        return _sc.equals(entry.getFileAttributes().getStorageClass());
    }
}
