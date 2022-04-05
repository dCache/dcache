package org.dcache.pool.migration;

import diskCacheV111.util.PnfsId;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * MigrationModuleServer message to request that a transfer is aborted.
 */
public class PoolMigrationCancelMessage extends PoolMigrationMessage {

    private static final long serialVersionUID = -7995913634698011318L;

    private final String reason;

    public PoolMigrationCancelMessage(UUID uuid, String pool, PnfsId pnfsId, @Nullable String reason) {
        super(uuid, pool, pnfsId);
        this.reason = reason;
    }

    @Nullable
    public String getReason() {
      return reason;
    }
}
