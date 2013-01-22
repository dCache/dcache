package org.dcache.pool.repository;

import diskCacheV111.util.PnfsId;
import org.dcache.vehicles.FileAttributes;

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
     * @return file attributes of this entry
     */
    public FileAttributes getFileAttributes();

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
