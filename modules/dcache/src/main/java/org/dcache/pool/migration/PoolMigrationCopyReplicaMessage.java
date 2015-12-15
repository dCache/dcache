package org.dcache.pool.migration;

import javax.annotation.ParametersAreNonnullByDefault;

import java.util.List;
import java.util.UUID;

import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * MigrationModuleServer message to request that a replica is
 * transfered.
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
    private final boolean _isMetaOnly;

    public PoolMigrationCopyReplicaMessage(UUID uuid, String pool,
                                           FileAttributes fileAttributes,
                                           EntryState state,
                                           List<StickyRecord> stickyRecords,
                                           boolean computeChecksumOnUpdate,
                                           boolean forceSourceMode,
                                           boolean isMetaOnly)
    {
        super(uuid, pool, fileAttributes.getPnfsId());
        _fileAttributes = checkNotNull(fileAttributes);
        _state = checkNotNull(state);
        _stickyRecords = checkNotNull(stickyRecords);
        _computeChecksumOnUpdate = computeChecksumOnUpdate;
        _forceSourceMode = forceSourceMode;
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
}
