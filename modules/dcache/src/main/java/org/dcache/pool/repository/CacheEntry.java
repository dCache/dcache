package org.dcache.pool.repository;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

import java.util.Collection;

public interface CacheEntry
{
    /**
     * Get the PnfsId of this entry.
     */
    public PnfsId getPnfsId();

    /**
     * Get the size of the replica.
     */
    public long getReplicaSize();

    /**
     * Get the storage info of the related entry.
     * @return storage info of the entry or null if storage info is not available yet.
     */
    public StorageInfo getStorageInfo();

    /**
     *
     * @return entry state
     */
    public EntryState getState();

    /**
     *
     * @return entry creation time in milliseconds
     */
    public long getCreationTime();

    /**
     *
     * @return entry last access time in milliseconds.
     */
    public long getLastAccessTime();

    /**
     * @return current link count
     */
    public int getLinkCount();

    /**
     * @return true iff entry is sticky.
     */
    public boolean isSticky();

    /**
     * @return the sticky records for this entry.
     */
    public Collection<StickyRecord> getStickyRecords();
}
