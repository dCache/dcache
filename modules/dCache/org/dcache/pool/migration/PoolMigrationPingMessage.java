package org.dcache.pool.migration;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

/**
 * MigrationModuleServer message to check whether a transfer is still
 * alive.
 */
public class PoolMigrationPingMessage extends PoolMigrationMessage
{
    static final long serialVersionUID = -5751734202065289034L;

    public PoolMigrationPingMessage(String pool, PnfsId pnfsId, long taskId)
    {
        super(pool, pnfsId, taskId);
    }
}