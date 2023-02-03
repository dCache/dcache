package org.dcache.pool.repository.v5;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableSet;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import java.io.IOException;
import java.net.URI;
import java.nio.file.OpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.inotify.InotifyReplicaRecord;
import org.dcache.pool.statistics.IoStatisticsReplicaRecord;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReadHandleImpl implements ReplicaDescriptor {

    private static final Set<OpenOption> OPEN_OPTIONS = ImmutableSet.<OpenOption>builder()
          .addAll(FileStore.O_READ)
          .add(IoStatisticsReplicaRecord.OpenFlags.ENABLE_IO_STATISTICS)
          .build();

    private static final Set<OpenOption> OPEN_OPTIONS_WITH_INOTIFY = ImmutableSet.<OpenOption>builder()
          .addAll(OPEN_OPTIONS)
          .add(InotifyReplicaRecord.OpenFlags.ENABLE_INOTIFY_MONITORING)
          .build();

    protected static final Logger LOGGER = LoggerFactory.getLogger(ReadHandleImpl.class);


    private final PnfsHandler _pnfs;
    private final ReplicaRecord _entry;
    private final Set<? extends OpenOption> _openOptions;
    private FileAttributes _fileAttributes;
    private boolean _open;
    private Exception _closedBy;

    ReadHandleImpl(PnfsHandler pnfs, ReplicaRecord entry, FileAttributes fileAttributes,
          boolean isInternalActivity) {
        _pnfs = requireNonNull(pnfs);
        _entry = requireNonNull(entry);
        _fileAttributes = requireNonNull(fileAttributes);
        _open = true;
        _openOptions = isInternalActivity ? OPEN_OPTIONS : OPEN_OPTIONS_WITH_INOTIFY;
    }

    /**
     * Shutdown EntryIODescriptor. All further attempts to use descriptor will throw
     * IllegalStateException.
     *
     * @throws IllegalStateException if EntryIODescriptor is closed.
     */
    @Override
    public synchronized void close() throws IllegalStateException {
        if (!_open) {
            throw new IllegalStateException("Handle is closed", _closedBy);
        }
        _entry.decrementLinkCount();
        _open = false;
        _closedBy = new Exception("Previous, successful close by " + Thread.currentThread().getName());
    }

    @Override
    public RepositoryChannel createChannel() throws IOException {
        RepositoryChannel channel = _entry.openChannel(_openOptions);
        long fileSizeAlloc = channel.size();
        if (_fileAttributes.getSize() != fileSizeAlloc) {
            IOException ex = new IOException("Failed to read the file, because file is Broken.");
            try {
                _entry.update("Filesystem and pool database file sizes are inconsistent",
                      r -> r.setState(ReplicaState.BROKEN));
            } catch (CacheException e) {
                LOGGER.warn("Filesystem and pool database file sizes inconsistency: {}",
                      e.toString());
                ex.addSuppressed(e);
            } finally {
                channel.close();
            }
            throw ex;
        }
        return channel;
    }

    /**
     * @return disk file
     * @throws IllegalStateException if EntryIODescriptor is closed.
     */
    @Override
    public synchronized URI getReplicaFile() throws IllegalStateException {
        if (!_open) {
            throw new IllegalStateException("Handle is closed", _closedBy);
        }

        return _entry.getReplicaUri();
    }

    @Override
    public synchronized FileAttributes getFileAttributes() throws IllegalStateException {
        return _fileAttributes;
    }

    @Override
    public synchronized Collection<Checksum> getChecksums() throws CacheException {
        if (_fileAttributes.isUndefined(FileAttribute.CHECKSUM)) {
            Set<Checksum> checksums = _pnfs.getFileAttributes(
                  _entry.getPnfsId(), EnumSet.of(FileAttribute.CHECKSUM)).getChecksums();
            synchronized (_entry) {
                _fileAttributes = _entry.getFileAttributes();
                if (_fileAttributes.isUndefined(FileAttribute.CHECKSUM)) {
                    _fileAttributes.setChecksums(checksums);
                    _entry.update("Adding checksums from namespace",
                          r -> r.setFileAttributes(_fileAttributes));
                }
            }
        }
        return Collections.unmodifiableSet(_fileAttributes.getChecksums());
    }

    @Override
    public long getReplicaSize() {
        return _entry.getReplicaSize();
    }

    @Override
    public long getReplicaCreationTime() {
        return _entry.getCreationTime();
    }
}
