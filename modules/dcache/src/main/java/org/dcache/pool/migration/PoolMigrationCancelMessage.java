package org.dcache.pool.migration;

import diskCacheV111.util.PnfsId;
import java.util.UUID;

/**
 * MigrationModuleServer message to request that a transfer is aborted.
 */
public class PoolMigrationCancelMessage extends PoolMigrationMessage {

    private static final long serialVersionUID = -7995913634698011318L;

    public PoolMigrationCancelMessage(UUID uuid, String pool, PnfsId pnfsId) {
        super(uuid, pool, pnfsId);
    }
}
