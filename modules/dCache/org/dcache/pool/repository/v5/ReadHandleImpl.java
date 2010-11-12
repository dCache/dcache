package org.dcache.pool.repository.v5;

import diskCacheV111.util.CacheException;
import java.util.concurrent.TimeoutException;
import java.io.File;

import org.dcache.pool.repository.EntryState;
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
        try {
            _repository = repository;
            _entry = entry;
            _open = true;
            _entry.incrementLinkCount();
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error: " +
                                       e.getMessage());
        }
    }

    /**
     * Shutdown EntryIODescriptor. All further attempts to use
     * descriptor will throw IllegalStateException.
     * @throws IllegalStateException if EntryIODescriptor is closed.
     */
    public synchronized void close() throws IllegalStateException
    {
        if (!_open)
            throw new IllegalStateException("Handle is closed");
        try {
            _entry.decrementLinkCount();
            _open = false;
            _repository.destroyWhenRemovedAndUnused(_entry);
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error: "
                                       + e.getMessage());
        }
    }


    /**
     * @return disk file
     * @throws IllegalStateException if EntryIODescriptor is closed.
     */
    public synchronized File getFile() throws IllegalStateException
    {
        if (!_open)
            throw new IllegalStateException("Handle is closed");

        try {
            return _entry.getDataFile();
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error: "
                                       + e.getMessage());
        }
    }

    /**
     *
     * @return cache entry
     * @throws IllegalStateException
     */
    public synchronized CacheEntry getEntry()  throws IllegalStateException
    {
        if (!_open)
            throw new IllegalStateException("Handle is closed");

        try {
            return new CacheEntryImpl(_entry);
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error: "
                                       + e.getMessage());
        }
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