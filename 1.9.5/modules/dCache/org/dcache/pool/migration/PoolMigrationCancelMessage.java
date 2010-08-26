package org.dcache.pool.migration;

import java.util.UUID;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

/**
 * MigrationModuleServer message to request that a transfer is
 * aborted.
 */
public class PoolMigrationCancelMessage extends PoolMigrationMessage
{
    static final long serialVersionUID = -7995913634698011318L;

    public PoolMigrationCancelMessage(UUID uuid, String pool, PnfsId pnfsId)
    {
        super(uuid, pool, pnfsId);
    }
}