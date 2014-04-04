package org.dcache.pool.repository;

/**
 * Encapsulates the policy of what constitutes a removable file. A
 * space sweeper should implement a space sweeper policy.
 */
public interface SpaceSweeperPolicy
{
    /**
     * Returns true if this file is removable, that is, garbage
     * collected by the sweeper.
     */
    boolean isRemovable(CacheEntry entry);

    /**
     * Returns the last access time of the least recently used
     * removable file.
     */
    long getLru();
}
