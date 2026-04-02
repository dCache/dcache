package org.dcache.pool.classic;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.ProtocolInfo;
import javax.annotation.Nullable;

/**
 * Interface for triggering hot file replication in the pool.
 */
public interface HotFileReplicator {

    /**
     * Trigger replication for a hot file.
     *
     * @param pnfsId       the file identifier
     * @param protocolInfo the protocol info of the request, may be {@code null} if unknown
     * @param numReplicas  the number of replicas to create
     */
    void replicate(PnfsId pnfsId, @Nullable ProtocolInfo protocolInfo, int numReplicas);
}

