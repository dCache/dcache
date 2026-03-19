package org.dcache.pool.classic;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.ProtocolInfo;

/**
 * Abstract interface for monitoring file requests in the pool.
 */
public interface FileRequestMonitor {

    /**
     * Report a file request for the given pnfsId and number of requests.
     *
     * @param pnfsId           the file identifier
     * @param numberOfRequests the number of requests for this file
     * @param protocolInfo     the protocol info of the request
     */
    void reportFileRequest(PnfsId pnfsId, long numberOfRequests, ProtocolInfo protocolInfo);
}

