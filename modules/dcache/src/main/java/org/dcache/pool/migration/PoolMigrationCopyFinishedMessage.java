package org.dcache.pool.migration;

import java.util.UUID;

import diskCacheV111.util.PnfsId;

/**
 * MigrationModuleServer response message to report that a transfer
 * finished (either successfully or with an error).
 */
public class PoolMigrationCopyFinishedMessage extends PoolMigrationMessage
{
    static final long serialVersionUID = 4888320379507599050L;

    public PoolMigrationCopyFinishedMessage(UUID uuid, String pool,
                                            PnfsId pnfsId)
    {
        super(uuid, pool, pnfsId);
    }
}