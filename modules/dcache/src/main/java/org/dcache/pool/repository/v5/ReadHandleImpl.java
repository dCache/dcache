package org.dcache.pool.repository.v5;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.unmodifiableIterable;

class ReadHandleImpl implements ReplicaDescriptor
{
    private final PnfsHandler _pnfs;
    private final ReplicaRecord _entry;
    private FileAttributes _fileAttributes;
    private boolean _open;

    ReadHandleImpl(PnfsHandler pnfs, ReplicaRecord entry, FileAttributes fileAttributes)
    {
        _pnfs = checkNotNull(pnfs);
        _entry = checkNotNull(entry);
        _fileAttributes = checkNotNull(fileAttributes);
        _open = true;
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
    }

    @Override
    public RepositoryChannel createChannel() throws IOException {
        return _entry.openChannel(FileStore.O_READ);
    }

    /**
     * @return disk file
     * @throws IllegalStateException if EntryIODescriptor is closed.
     */
    @Override
    public synchronized URI getReplicaFile() throws IllegalStateException
    {
        if (!_open) {
            throw new IllegalStateException("Handle is closed");
        }

        return _entry.getReplicaUri();
    }

    @Override
    public synchronized FileAttributes getFileAttributes()  throws IllegalStateException {
        return _fileAttributes;
    }

    @Override
    public synchronized Iterable<Checksum> getChecksums() throws CacheException {
        if (_fileAttributes.isUndefined(FileAttribute.CHECKSUM)) {
            Set<Checksum> checksums = _pnfs.getFileAttributes(
                    _entry.getPnfsId(), EnumSet.of(FileAttribute.CHECKSUM)).getChecksums();
            synchronized (_entry) {
                _fileAttributes = _entry.getFileAttributes();
                if (_fileAttributes.isUndefined(FileAttribute.CHECKSUM)) {
                    _fileAttributes.setChecksums(checksums);
                    _entry.update(r -> r.setFileAttributes(_fileAttributes));
                }
            }
        }
        return unmodifiableIterable(_fileAttributes.getChecksums());
    }

    @Override
    public void addChecksums(Iterable<Checksum> checksums) {
        throw new IllegalStateException("Read-only handle");
    }

    @Override
    public void setLastAccessTime(long time)
    {
        throw new IllegalStateException("Read-only handle");
    }

    @Override
    public long getReplicaSize()
    {
        return _entry.getReplicaSize();
    }

    @Override
    public void commit() throws IllegalStateException, InterruptedException, CacheException {
        // NOP
    }
}
