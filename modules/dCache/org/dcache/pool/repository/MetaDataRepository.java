package org.dcache.pool.repository;

import org.dcache.pool.repository.v3.RepositoryException;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.repository.CacheRepositoryEntry;

/**
 * The MetaDataRepository interface provides an abstraction of how
 * CacheRepositoryEntry objects are created, retrieved and removed.
 */
public interface MetaDataRepository
{
    /**
     * Retrieves an existing entry previously created with
     * <i>create</i>.
     *
     * @param id PNFS id for which to retrieve the entry.
     * @return The entry or null if the entry does not exist.
     */
    CacheRepositoryEntry get(PnfsId id);

    /**
     * Creates a new entry. The entry must not exist prior to this
     * call.
     *
     * @param id PNFS id for which to create the entry
     * @return The new entry
     * @throws DuplicateEntryException if entry already exists
     * @throws RepositoryException if entry creation fails
     */
    CacheRepositoryEntry create(PnfsId id) 
        throws DuplicateEntryException, RepositoryException;

    /**
     * Creates a new entry from an existing entry. The new entry will
     * have the same PNFS id and the same field values as the existing
     * entry.
     *
     * No entry with the same PNFS id must exist in the repository
     * prior to this call. This implies that the existing entry must
     * be from another repository.
     *
     * Typically used by the entry healer to import old entries into a
     * new repository.
     *
     * Limitations:
     * <ul>
     * <li> Due to limitations in the CacheRepositoryEntry interface, the
     * locking status is currently not copied to the new entry. 
     * <li> The storage info in both entries will refer to the same 
     * storage info object.
     * <li> The sending to client flag is not preserved.
     * </ul>
     *
     * @param entry Cache entry from which to create the new entry
     * @return The new entry
     * @throws DuplicateEntryException if entry already exists
     * @throws CacheException if reading from <i>entry</i> fails
     */
    CacheRepositoryEntry create(CacheRepositoryEntry entry)
        throws DuplicateEntryException, CacheException;
    
    /**
     * Removes a meta data entry. If the entry does not exist, nothing
     * happens.
     *
     * @param id PNFS id of the entry to return.
     */
    void remove(PnfsId id);

    /**
     * Returns whether the repository appears healthy. How this is
     * determined is up to the implementation.
     */
    public boolean isOk();
}
