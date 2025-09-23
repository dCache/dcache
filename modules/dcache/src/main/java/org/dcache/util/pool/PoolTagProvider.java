package org.dcache.util.pool;

import java.util.Collection;
import java.util.Map;

/**
 * Interface for providing pool tag information to location extractors.
 * This abstraction allows different modules to provide pool tag data
 * without creating cyclic dependencies.
 */
public interface PoolTagProvider {

    /**
     * Retrieves the tags for a given pool location.
     *
     * @param location the pool location/name
     * @return map of tag names to values, or empty map if no tags or pool not found
     */
    Map<String, String> getPoolTags(String location);
}
