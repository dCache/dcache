package org.dcache.doors.PoolManagerAdapterSync;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.NoRouteToCellException;

public interface PoolManagerAdapterSync {

    public PoolIoFileMessage getFile(PnfsId pnfsId, StorageInfo storageInfo, ProtocolInfo protocolInfo, boolean isWrite, long timeout) throws NoRouteToCellException, TimeoutCacheException, InterruptedException, CacheException;

}