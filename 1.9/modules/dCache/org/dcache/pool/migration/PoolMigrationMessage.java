package org.dcache.pool.migration;

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

    public PoolMigrationMessage(String pool, PnfsId pnfsId, long taskId)
    {
        _pool = pool;
        _pnfsId = pnfsId;
        _taskId = taskId;
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