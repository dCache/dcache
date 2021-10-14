package org.dcache.pool.migration;

import diskCacheV111.util.AccessLatency;
import java.util.function.Predicate;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.vehicles.FileAttributes;

/**
 * Repository entry filter which only accepts files with a certain access lantecy.
 */
public class AccessLatencyFilter implements Predicate<CacheEntry> {

    private final AccessLatency _accessLatency;

    public AccessLatencyFilter(AccessLatency accessLatency) {
        _accessLatency = accessLatency;
    }

    @Override
    public boolean test(CacheEntry entry) {
        FileAttributes attributes = entry.getFileAttributes();
        return attributes.isDefined(FileAttribute.ACCESS_LATENCY) && _accessLatency.equals(
              attributes.getAccessLatency());
    }
}
