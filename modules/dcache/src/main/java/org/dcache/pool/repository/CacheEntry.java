package org.dcache.pool.repository;

import java.util.Collection;

import diskCacheV111.util.PnfsId;

import org.dcache.vehicles.FileAttributes;

public interface CacheEntry
{
    /**
     * Get the PnfsId of this entry.
     */
    PnfsId getPnfsId();

    /**
     * Get the size of the replica.
     */
    long getReplicaSize();

    /**
     * @return file attributes of this entry
     */
    FileAttributes getFileAttributes();

    /**
     *
     * @return entry state
     */
    ReplicaState getState();

    /**
     *
     * @return entry creation time in milliseconds
     */
    long getCreationTime();

    /**
     *
     * @return entry last access time in milliseconds.
     */
    long getLastAccessTime();

    /**
     * @return current link count
     */
    int getLinkCount();

    /**
     * @return true iff entry is sticky.
     */
    boolean isSticky();

    /**
     * @return the sticky records for this entry.
     */
    Collection<StickyRecord> getStickyRecords();
}
