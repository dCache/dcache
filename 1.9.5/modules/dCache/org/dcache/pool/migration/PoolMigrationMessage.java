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
    private final UUID _uuid;

    public PoolMigrationMessage(UUID uuid, String pool, PnfsId pnfsId)
    {
        _uuid = uuid;
        _pool = pool;
        _pnfsId = pnfsId;
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
}