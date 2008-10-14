package org.dcache.pool.migration;

import diskCacheV111.util.PnfsId;

/**
 * MigrationModuleServer response message to report that a transfer
 * finished (either successfully or with an error).
 */
public class PoolMigrationCopyFinishedMessage extends PoolMigrationMessage
{
    static final long serialVersionUID = 4888320379507599050L;

    public PoolMigrationCopyFinishedMessage(String pool, PnfsId pnfsId, long taskId)
    {
        super(pool, pnfsId, taskId);
    }
}