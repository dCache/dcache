package org.dcache.poolmanager;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.NoRouteToCellException;

public interface PoolManagerAdapter {

    /**
     *
     * @param pnfsId The PNFS ID of the file.
     * @param storageInfo Storage info of the file
     * @param protocolInfo Protocol info of the file
     * @param timeout waiting time for the requesting a suitable pool
     * @return PoolIoFileMessage
     * @throws NoRouteToCellException
     * @throws TimeoutCacheException
     * @throws InterruptedException
     * @throws CacheException
     */
    public PoolIoFileMessage readFile(PnfsId pnfsId, StorageInfo storageInfo, ProtocolInfo protocolInfo, long timeout) throws NoRouteToCellException, TimeoutCacheException, InterruptedException, CacheException;

    public PoolIoFileMessage writeFile(PnfsId pnfsId, StorageInfo storageInfo, ProtocolInfo protocolInfo, long timeout) throws NoRouteToCellException, TimeoutCacheException, InterruptedException, CacheException;

}