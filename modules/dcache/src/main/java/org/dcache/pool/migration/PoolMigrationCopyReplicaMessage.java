package org.dcache.pool.migration;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.List;
import java.util.UUID;

import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;

import static java.util.Objects.requireNonNull;

/**
 * MigrationModuleServer message to request that a replica is
 * transferred.
 */
@ParametersAreNonnullByDefault
public class PoolMigrationCopyReplicaMessage extends PoolMigrationMessage
{
    private static final long serialVersionUID = 6328444770149191656L;

    private final FileAttributes _fileAttributes;
    private final ReplicaState _state;
    private final List<StickyRecord> _stickyRecords;
    private final boolean _computeChecksumOnUpdate;
    private final boolean _forceSourceMode;
    private final Long _atime;
    private final boolean _isMetaOnly;

    public PoolMigrationCopyReplicaMessage(UUID uuid, String pool,
                                           FileAttributes fileAttributes,
                                           ReplicaState state,
                                           List<StickyRecord> stickyRecords,
                                           boolean computeChecksumOnUpdate,
                                           boolean forceSourceMode,
                                           @Nullable Long atime, boolean isMetaOnly)
    {
        super(uuid, pool, fileAttributes.getPnfsId());
        _fileAttributes = requireNonNull(fileAttributes);
        _state = requireNonNull(state);
        _stickyRecords = requireNonNull(stickyRecords);
        _computeChecksumOnUpdate = computeChecksumOnUpdate;
        _forceSourceMode = forceSourceMode;
        _atime = atime;
        _isMetaOnly = isMetaOnly;
    }

    public ReplicaState getState()
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

    public boolean isMetaOnly()
    {
        return _isMetaOnly;
    }

    /**
     * Last access time to use for target replica. null means that no access time is provided
     * and the target should decide which access time to use.
     */
    @Nullable
    public Long getAtime()
    {
        return _atime;
    }
}
