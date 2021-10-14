package org.dcache.pool.migration;

import diskCacheV111.util.PnfsId;
import java.util.UUID;

/**
 * MigrationModuleServer message to check whether a transfer is still alive.
 */
public class PoolMigrationPingMessage extends PoolMigrationMessage {

    private static final long serialVersionUID = -5751734202065289034L;

    public PoolMigrationPingMessage(UUID uuid, String pool, PnfsId pnfsId) {
        super(uuid, pool, pnfsId);
    }
}
