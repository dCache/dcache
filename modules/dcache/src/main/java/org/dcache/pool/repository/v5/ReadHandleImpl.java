package org.dcache.pool.repository.v5;

import java.io.File;
import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.unmodifiableIterable;

class ReadHandleImpl implements ReplicaDescriptor
{
    private final CacheRepositoryV5 _repository;
    private final PnfsHandler _pnfs;
    private final MetaDataRecord _entry;
    private boolean _open;

    ReadHandleImpl(CacheRepositoryV5 repository,
                   PnfsHandler pnfs,
                   MetaDataRecord entry)
    {
        _repository = checkNotNull(repository);
        _pnfs = checkNotNull(pnfs);
        _entry = checkNotNull(entry);
        _open = true;
        _entry.incrementLinkCount();
    }

    /**
     * Shutdown EntryIODescriptor. All further attempts to use
     * descriptor will throw IllegalStateException.
     * @throws IllegalStateException if EntryIODescriptor is closed.
     */
    @Override
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
    @Override
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
    @Override
    public synchronized FileAttributes getFileAttributes()  throws IllegalStateException {
        if (!_open) {
            throw new IllegalStateException("Handle is closed");
        }
        return _entry.getFileAttributes();
    }

    @Override
    public synchronized Iterable<Checksum> getChecksums() throws CacheException {
        FileAttributes attributes = _entry.getFileAttributes();
        if (attributes.isUndefined(FileAttribute.CHECKSUM)) {
            Set<Checksum> checksums = _pnfs.getFileAttributes(
                    _entry.getPnfsId(), EnumSet.of(FileAttribute.CHECKSUM)).getChecksums();
            synchronized (_entry) {
                attributes = _entry.getFileAttributes();
                if (attributes.isUndefined(FileAttribute.CHECKSUM)) {
                    attributes.setChecksums(checksums);
                    _entry.setFileAttributes(attributes);
                }
            }
        }
        return unmodifiableIterable(attributes.getChecksums());
    }

    @Override
    public void addChecksums(Iterable<Checksum> checksums) {
        throw new IllegalStateException("Read-only handle");
    }

    @Override
    public void commit() throws IllegalStateException, InterruptedException, CacheException {
        // NOP
    }

    @Override
    public void allocate(long size) throws IllegalStateException, IllegalArgumentException, InterruptedException {
        throw new IllegalStateException("Read-only handle");
    }

    @Override
    public void free(long size) throws IllegalStateException, IllegalArgumentException {
        throw new IllegalStateException("Read-only handle");
    }
}
