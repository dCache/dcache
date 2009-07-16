package org.dcache.pool.migration;

import java.util.UUID;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

/**
 * Base class for MigrationModuleServer messages.
 */
public class PoolMigrationMessage extends Message
{
    static final long serialVersionUID = 7663914032995115090L;

    private final String _pool;
    private final PnfsId _pnfsId;
    private final long _taskId;
    private final UUID _uuid;

    public PoolMigrationMessage(UUID uuid, String pool, PnfsId pnfsId, long taskId)
    {
        _uuid = uuid;
        _pool = pool;
        _pnfsId = pnfsId;
        _taskId = taskId;
    }

    public UUID getUUID()
    {
        return _uuid;
    }

    public String getPool()
    {
        return _pool;
    }

    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    public long getTaskId()
    {
        return _taskId;
    }
}