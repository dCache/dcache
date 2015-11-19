package org.dcache.pool.repository;

import java.io.File;
import java.util.Collection;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.vehicles.FileAttributes;

/**
 * Implementations provide access to meta data for replicas.
 *
 * Implementations should be thread safe. Callers may synchronize on
 * the object to prevent modifications.
 */
public interface MetaDataRecord
{
    /**
     * Get the PnfsId of this entry.
     */
    PnfsId getPnfsId();

    /**
     * Set the size of this entry. An entry has a size which normally
     * corresponds to the size of the file on disk. While the file is
     * created there may be a mismatch between the entry size and the
     * physical size.
     *
     * For a healthy entry and complete, the entry size will match the
     * file size stored in PNFS. For broken entries or while the file
     * is created, the two may not match.
     *
     * The size stored in the entries StorageInfo record is a cached
     * copy of the size stored in PNFS.
     */
    void setSize(long size);

    /**
     * Get the size of this entry. May be different from the size of
     * the on-disk file.
     */
    long getSize();

    void setFileAttributes(FileAttributes attributes) throws CacheException;

    FileAttributes getFileAttributes() throws CacheException;

    void setState(EntryState state)
        throws CacheException;

    EntryState getState();

    File getDataFile()
            ;

    long getCreationTime();

    long getLastAccessTime();

    void setLastAccessTime(long time) throws CacheException;

    void touch() throws CacheException;

    void decrementLinkCount();

    void incrementLinkCount();

    int getLinkCount();

    /**
     * Returns true if and only if the entry has one or more sticky
     * flags. Whether the sticky flags have expired or not MUST not
     * influence the return value.
     *
     * @return true if the stickyRecords methods would return a non
     * empty collection, false otherwise.
     */
    boolean isSticky();

    /**
     * Removes expired sticky from the entry.
     *
     * @return The expired sticky flags removed from the record.
     */
    Collection<StickyRecord> removeExpiredStickyFlags() throws CacheException;

    /**
     * Set sticky flag for a given owner and time. There is at most
     * one flag per owner. If <code>overwrite</code> is true, then an
     * existing record for <code>owner</code> will be replaced. If it
     * is false, then the lifetime of an existing record will be
     * extended if and only if the new lifetime is longer.
     *
     * A lifetime of -1 indicates that the flag never expires. A
     * lifetime set in the past, for instance 0, expires immediately.
     *
     * @param owner flag owner
     * @param validTill time milliseconds since 00:00:00 1 Jan. 1970.
     * @param overwrite replace existing flag when true.
     * @throws CacheException
     * @return true if the collection returned by the stickyRecords
     * method has changed due to this call.
     */
    boolean setSticky(String owner, long validTill, boolean overwrite)
        throws CacheException;

    /**
     * @return list of StickyRecords held by the file
     */
    Collection<StickyRecord> stickyRecords();
}
