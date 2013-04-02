package org.dcache.pool.migration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.UUID;

import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * MigrationModuleServer message to request that a replica is
 * transfered.
 */
public class PoolMigrationCopyReplicaMessage extends PoolMigrationMessage
{
    private static final long serialVersionUID = 6328444770149191656L;

    private FileAttributes _fileAttributes;

    @Deprecated // Remove in 2.7
    private StorageInfo _storageInfo;

    private final EntryState _state;
    private final List<StickyRecord> _stickyRecords;
    private final boolean _computeChecksumOnUpdate;
    private final boolean _forceSourceMode;

    public PoolMigrationCopyReplicaMessage(UUID uuid, String pool,
                                           FileAttributes fileAttributes,
                                           EntryState state,
                                           List<StickyRecord> stickyRecords,
                                           boolean computeChecksumOnUpdate,
                                           boolean forceSourceMode)
    {
        super(uuid, pool, fileAttributes.getPnfsId());

        checkNotNull(state);
        checkNotNull(stickyRecords);

        _fileAttributes = fileAttributes;
        _storageInfo = StorageInfos.extractFrom(fileAttributes);
        _state = state;
        _stickyRecords = stickyRecords;
        _computeChecksumOnUpdate = computeChecksumOnUpdate;
        _forceSourceMode = forceSourceMode;
    }

    public EntryState getState()
    {
        return _state;
    }

    public List<StickyRecord> getStickyRecords()
    {
        return _stickyRecords;
    }

    public boolean getComputeChecksumOnUpdate()
    {
        return _computeChecksumOnUpdate;
    }

    public FileAttributes getFileAttributes()
    {
        return _fileAttributes;
    }

    public boolean isForceSourceMode()
    {
        return _forceSourceMode;
    }

    private void readObject(ObjectInputStream stream)
            throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        if (_fileAttributes == null) {
            _fileAttributes = new FileAttributes();
            if (_storageInfo != null) {
                StorageInfos.injectInto(_storageInfo, _fileAttributes);
            }
            _fileAttributes.setPnfsId(getPnfsId());
        }
    }
}
