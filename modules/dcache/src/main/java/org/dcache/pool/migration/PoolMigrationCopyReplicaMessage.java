package org.dcache.pool.migration;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.List;
import java.util.UUID;

import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * MigrationModuleServer message to request that a replica is
 * transferred.
 */
@ParametersAreNonnullByDefault
public class PoolMigrationCopyReplicaMessage extends PoolMigrationMessage
{
    private static final long serialVersionUID = 6328444770149191656L;

    private FileAttributes _fileAttributes;
    private final EntryState _state;
    private final List<StickyRecord> _stickyRecords;
    private final boolean _computeChecksumOnUpdate;
    private final boolean _forceSourceMode;
    private final Long _atime;
    private final boolean _isMetaOnly;

    public PoolMigrationCopyReplicaMessage(UUID uuid, String pool,
                                           FileAttributes fileAttributes,
                                           EntryState state,
                                           List<StickyRecord> stickyRecords,
                                           boolean computeChecksumOnUpdate,
                                           boolean forceSourceMode,
                                           Long atime, boolean isMetaOnly)
    {
        super(uuid, pool, fileAttributes.getPnfsId());
        _fileAttributes = checkNotNull(fileAttributes);
        _state = checkNotNull(state);
        _stickyRecords = checkNotNull(stickyRecords);
        _computeChecksumOnUpdate = computeChecksumOnUpdate;
        _forceSourceMode = forceSourceMode;
        _atime = atime;
        _isMetaOnly = isMetaOnly;
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
