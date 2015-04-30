package org.dcache.pool.repository;

import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.v3.RepositoryException;

/**
 * Thrown when attempting to create a meta data entry that already
 * exists.
 */
public class DuplicateEntryException extends RepositoryException
{
    private static final long serialVersionUID = -1500062593915851217L;

    public DuplicateEntryException(PnfsId id)
    {
        super("Attempt to create duplicate meta data entry for " + id);
    }
}
