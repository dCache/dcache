package org.dcache.pool.classic;

import diskCacheV111.util.PnfsId;

/**
 * Abstract interface for monitoring file requests in the pool.
 */
public interface FileRequestMonitor {
    /**
     * Report a file request for the given pnfsId and number of requests.
     * @param pnfsId the file identifier
     * @param numberOfRequests the number of requests for this file
     */
    void reportFileRequest(PnfsId pnfsId, long numberOfRequests);
}

