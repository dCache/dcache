package org.dcache.pool.repository.meta;

import java.io.File;
import java.util.Collection;
import java.util.Collections;

import org.dcache.pool.repository.v3.RepositoryException;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.MetaDataStore;
import org.dcache.pool.repository.MetaDataRecord;
import diskCacheV111.util.PnfsId;

/**
 * A meta data store which is always empty.
 */
public class EmptyMetaDataStore
    implements MetaDataStore
{
    public EmptyMetaDataStore()
    {
    }

    public EmptyMetaDataStore(FileStore dataRepository,
                              File directory)
    {
    }

    public Collection<PnfsId> list()
    {
        return Collections.emptyList();
    }

    public MetaDataRecord get(PnfsId id)
    {
        return null;
    }

    public MetaDataRecord create(PnfsId id)
        throws RepositoryException
    {
        throw new RepositoryException("Store is read only");
    }

    public MetaDataRecord create(MetaDataRecord entry)
        throws RepositoryException
    {
        throw new RepositoryException("Store is read only");
    }

    public void remove(PnfsId id)
    {
    }

    public boolean isOk()
    {
        return true;
    }

    public void close()
    {
    }
}
