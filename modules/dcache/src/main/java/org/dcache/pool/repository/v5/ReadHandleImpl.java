package org.dcache.pool.repository.v5;

import diskCacheV111.util.CacheException;
import java.io.File;

import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.util.Checksum;

class ReadHandleImpl implements ReplicaDescriptor
{
    private final CacheRepositoryV5 _repository;
    private final MetaDataRecord _entry;
    private boolean _open;

    ReadHandleImpl(CacheRepositoryV5 repository, MetaDataRecord entry)
    {
        _repository = repository;
        _entry = entry;
        _open = true;
        _entry.incrementLinkCount();
    }

    /**
     * Shutdown EntryIODescriptor. All further attempts to use
     * descriptor will throw IllegalStateException.
     * @throws IllegalStateException if EntryIODescriptor is closed.
     */
    public synchronized void close() throws IllegalStateException
    {
        if (!_open) {
            throw new IllegalStateException("Handle is closed");
        }
        _entry.decrementLinkCount();
        _open = false;
        _repository.destroyWhenRemovedAndUnused(_entry);
    }


    /**
     * @return disk file
     * @throws IllegalStateException if EntryIODescriptor is closed.
     */
    public synchronized File getFile() throws IllegalStateException
    {
        if (!_open) {
            throw new IllegalStateException("Handle is closed");
        }

        return _entry.getDataFile();
    }

    /**
     *
     * @return cache entry
     * @throws IllegalStateException
     */
    public synchronized CacheEntry getEntry()  throws IllegalStateException
    {
        if (!_open) {
            throw new IllegalStateException("Handle is closed");
        }

        return new CacheEntryImpl(_entry);
    }

    public void commit(Checksum checksum) throws IllegalStateException, InterruptedException, CacheException {
        // NOP
    }

    public void allocate(long size) throws IllegalStateException, IllegalArgumentException, InterruptedException {
        throw new IllegalStateException("Read-Only Handle");
    }

    public void free(long size) throws IllegalStateException, IllegalArgumentException {
        throw new IllegalStateException("Read-Only Handle");
    }
}