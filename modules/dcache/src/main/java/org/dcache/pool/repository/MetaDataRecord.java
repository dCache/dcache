package org.dcache.pool.repository;

import java.io.File;
import java.util.Collection;

import diskCacheV111.util.CacheException;
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
     * Get the size of this replica. May be different from the size
     * registered in Chimera.
     */
    long getReplicaSize();

    FileAttributes getFileAttributes() throws CacheException;

    EntryState getState();

    File getDataFile();

    long getCreationTime();

    long getLastAccessTime();

    void setLastAccessTime(long time) throws CacheException;

    int decrementLinkCount();

    int incrementLinkCount();

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
     * @return list of StickyRecords held by the file
     */
    Collection<StickyRecord> stickyRecords();

    /**
     * Bulk update one or more attributes.
     *
     * Callers provide a callback that is called with an UpdatableRecord which
     * provides setters to attributes commonly updated together.
     *
     * The change will be applied atomically (in the sense that other threads will not
     * see the change until the method returns). If the callback throws an exception,
     * the changes applied prior to the exception are not rolled back.
     *
     * The callback may be called with a lock held or within a transaction. The
     * callback should be fast and with minimal access to other resources to
     * avoid the risk of causing deadlocks.
     *
     * @return The return value of {@code Update#apply}
     * @param update
     */
    <T> T update(Update<T> update) throws CacheException;

    /**
     * Callback interface used by {@code update}.
     */
    interface Update<T>
    {
        T apply(UpdatableRecord record) throws CacheException;
    }

    /**
     * Interface that provides means to modify attributes commonly modified
     * together.
     */
    interface UpdatableRecord
    {
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

        Void setState(EntryState state) throws CacheException;

        Void setFileAttributes(FileAttributes attributes) throws CacheException;

        FileAttributes getFileAttributes() throws CacheException;

        EntryState getState();

        int getLinkCount();
    }
}
