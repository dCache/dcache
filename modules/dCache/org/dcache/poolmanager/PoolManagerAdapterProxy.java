package org.dcache.poolmanager;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellPath;
import org.dcache.cells.CellStub;
import dmg.cells.nucleus.NoRouteToCellException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PoolManagerAdapterProxy implements PoolManagerAdapter {

    private final static Logger _log = LoggerFactory.getLogger(PoolManagerAdapterProxy.class.getName());
    private String _ioQueueName;
    private long _timeoutForRequest;
    private CellStub _poolManagerCellStub;
    private CellStub _poolCellStub;
    private long _timeoutPool;

    /**
     *
     * @param poolCellStub: CellStub for PoolManager.
     * @param ioQueue: mover queue of the transfer; may be null
     */
    public PoolManagerAdapterProxy(CellStub pooManagerlCellStub, CellStub poolCellStub, String ioQueue) {

        _poolManagerCellStub = pooManagerlCellStub;
        _poolCellStub = poolCellStub;
        _ioQueueName = ioQueue;
        _timeoutPool = poolCellStub.getTimeout();
    }

    /**
     * Generic Method for read file. In case of getting the FileNotInCacheException and NoRouteToCellException will retry to serve the request,
     * if the timeout hasn't been reached, otherwise will throw the TimeoutException.
     *
     *
     * @param pnfsId       The PNFS ID of the file.
     * @param storageInfo  Storage info of the file
     * @param protocolInfo Protocol info of the file
     * @param isWrite      true for write the file, false for read the file
     * @param timeout      waiting time for the requesting for a suitable pool and starting a mover
     * @return PoolIoFileMessage
     * @throws NoRouteToCellException
     * @throws TimeoutCacheException
     * @throws InterruptedException
     * @throws CacheException
     */
    public PoolIoFileMessage readFile(PnfsId pnfsId, StorageInfo storageInfo, ProtocolInfo protocolInfo, long timeout) throws NoRouteToCellException, TimeoutCacheException, InterruptedException, CacheException {


        checkArguments(pnfsId, storageInfo, protocolInfo, timeout);

        _timeoutForRequest = timeout;

        PoolMgrSelectPoolMsg request = new PoolMgrSelectReadPoolMsg(pnfsId,
                storageInfo,
                protocolInfo,
                storageInfo.getFileSize());

        _log.info("asking Poolmanager for readpool for PnfsId {}", pnfsId);

        return askForReadWriteFile(request, false);

    }

    /**
     * Generic Method for read and write a file. Sends a request to the PoolManager to select a pool and
     * asks the pool for being ready to deliver/accept the file.
     * In case of getting the FileNotInCacheException and NoRouteToCellException will retry to serve the request,
     * if the timeout hasn't been reached, otherwise will throw the TimeoutException.
     *
     *
     * @param request  to select a pool for read or write
     * @param isWrite  true for write the file, false for read the file
     * @param timeout  waiting time for the requesting for a suitable pool and starting a mover
     * @return PoolIoFileMessage
     * @throws InterruptedException
     * @throws CacheException
     * @throws TimeoutCacheException
     * @throws NoRouteToCellException
     */
    public PoolIoFileMessage askForReadWriteFile(PoolMgrSelectPoolMsg request, boolean isWrite) throws CacheException, InterruptedException, TimeoutCacheException {


        PoolMgrSelectPoolMsg message = null;
        PoolIoFileMessage poolIOFileMessage = null;

        while (_timeoutForRequest > 0 && poolIOFileMessage == null) {

            long _elapsedTime = System.currentTimeMillis();

            try {

                message = getPoolManagerCellStub().sendAndWait(request, PoolMgrSelectPoolMsg.class, _timeoutForRequest);

                _timeoutForRequest = _timeoutForRequest - (System.currentTimeMillis() - _elapsedTime);

                String selectedPool = message.getPoolName();

                _log.info("Positive reply from pool {}", selectedPool);

                // Now is the pool selected, start trying it for the read/write request.

                _elapsedTime = System.currentTimeMillis();

                try {
                    poolIOFileMessage = askPool(selectedPool,
                            message.getPnfsId(),
                            message.getStorageInfo(),
                            message.getProtocolInfo(),
                            isWrite);

                    //handeling the timeout for the reply from Pool. The request could be retried if the time is left.
                } catch (TimeoutCacheException ex) {

                    _timeoutForRequest = _timeoutForRequest - (System.currentTimeMillis() - _elapsedTime);
                    _log.info("No reply from pool in time, retrying... {}", ex);
                    continue;
                }

            } catch (FileNotInCacheException ex) {

                _timeoutForRequest = _timeoutForRequest - (System.currentTimeMillis() - _elapsedTime);
                _log.info("File not in cache: {}", ex);
                continue;

            } catch (NoRouteToCellException ex) {

                _timeoutForRequest = _timeoutForRequest - (System.currentTimeMillis() - _elapsedTime);
                _log.info("No route to cell: retrying {}", ex);
                continue;

            } catch (TimeoutCacheException ex) {

                throw new TimeoutCacheException("Internal server Timeout ");
            }

        }

        if (poolIOFileMessage == null) {
            throw new TimeoutCacheException("Internal server timeout ");
        }

        return poolIOFileMessage;

    }

    /**
     * The method tries previously selected pool for read/write and starts Mover.
     *
     * @param pool          the name of a pool that should be asked for read/write the file.
     * @param pnfsId The    PNFS ID of the file.
     * @param storageInfo   Storage info of the file
     * @param protocolInfo  Protocol info of the file
     * @return PoolIoFileMessage
     * @throws FileNotInCacheException in case the file is not on the pool
     * @throws NoRouteToCellException
     * @throws CacheException
     * @throws InterruptedException
     */
    private PoolIoFileMessage askPool(String pool, PnfsId pnfsId, StorageInfo storageInfo, ProtocolInfo protocolInfo, boolean isWrite) throws FileNotInCacheException, NoRouteToCellException, CacheException, InterruptedException, TimeoutCacheException {

        assert pool != null : "Poolname invalid";
        assert pnfsId != null : "Pnfs ID invalid";
        assert storageInfo != null : "storageInfo invalid";
        assert protocolInfo != null : "protocolInfo invalid";

        _log.info("asking Poolmanager for {} pool for PnfsId {}", (isWrite ? "write" : "read"), pnfsId );

        PoolIoFileMessage message;

        PoolIoFileMessage poolMessage = isWrite ? (PoolIoFileMessage) new PoolAcceptFileMessage(
                pool,
                pnfsId,
                protocolInfo,
                storageInfo)
                : (PoolIoFileMessage) new PoolDeliverFileMessage(
                pool,
                pnfsId,
                protocolInfo,
                storageInfo);

        poolMessage.setIoQueueName(getIoQueueName());

        /* PoolDeliverFileMessage has to be sent via the
         * PoolManager (which could be the SpaceManager).
         */
        CellPath poolPath = (CellPath) getPoolManagerCellStub().getDestinationPath().clone();
        poolPath.add(pool);

        message = getPoolCellStub().sendAndWait(poolPath, poolMessage, getTimeoutPool());

        String poolName = message.getPoolName();
        PnfsId pnfsIdPoolreply = message.getPnfsId();

        _log.info("Pool {} will {} file:", poolName, (isWrite ? "accept" : "deliver"));
        _log.info("{}", pnfsIdPoolreply);

        return message;

    }

    /**
     * Generic Method for write file. In case of getting the NoRouteToCellException will retry to serve the request,
     * if the timeout hasn't been reached, otherwise will throw the TimeoutException.
     *
     *
     * @param pnfsId        The PNFS ID of the file.
     * @param storageInfo   Storage info of the file
     * @param protocolInfo  Protocol info of the file
     * @param timeout       waiting time for the requesting for a suitable pool and starting a mover
     * @return PoolIoFileMessage
     * @throws NoRouteToCellException
     * @throws TimeoutCacheException
     * @throws InterruptedException
     * @throws CacheException
     */
    public PoolIoFileMessage writeFile(PnfsId pnfsId, StorageInfo storageInfo, ProtocolInfo protocolInfo, long timeout) throws NoRouteToCellException, TimeoutCacheException, InterruptedException, CacheException {

        checkArguments(pnfsId, storageInfo, protocolInfo, timeout);

        _timeoutForRequest = timeout;

        PoolMgrSelectPoolMsg request = new PoolMgrSelectWritePoolMsg(pnfsId,
                storageInfo,
                protocolInfo,
                storageInfo.getFileSize());

        _log.info("asking Poolmanager for writepool for PnfsId {} ", pnfsId);

        return askForReadWriteFile(request, true);

    }

    /**
     * Checks arguments for being set and throws the IllegalArgumentException if it is not the case.
     *
     *
     * @param pnfsId The PNFS ID of the file.
     * @param storageInfo Storage info of the file
     * @param protocolInfo Protocol info of the file
     * @param isWrite: true for write the file, false for read the file
     * @param timeout waiting time for the requesting for a suitable pool and starting a mover
     * @throws IllegalArgumentException
     *
     */
    private void checkArguments(PnfsId pnfsId, StorageInfo storageInfo, ProtocolInfo protocolInfo, long timeout) {
        if (pnfsId == null) {
            throw new IllegalArgumentException("Need PNFS ID before a pool can be selected");
        }

        if (storageInfo == null) {
            throw new IllegalArgumentException("Need StorageInfo before a pool can be selected");
        }

        if (protocolInfo == null) {
            throw new IllegalArgumentException("Need ProtocolInfo before a pool can be selected");
        }

        if (timeout < 0) {
            throw new IllegalArgumentException("Timeout for the PoolManager is negative. The pool can't be selected.");
        }

    }

    /**
     * Gets the pool IO queue to use for WebDAV transfers.
     * @return the _ioQueueName
     */
    public String getIoQueueName() {
        return _ioQueueName;
    }

    /**
     * Sets the pool IO queue to use for WebDAV transfers.
     * @param ioQueueName ioQueue name to set
     */
    public void setIoQueueName(String ioQueueName) {
        this._ioQueueName = ioQueueName;
    }

    /**
     * Gets the cell stub for PoolManager communication.
     * @return the _poolManagerCellStub
     */
    public CellStub getPoolManagerCellStub() {
        return _poolManagerCellStub;
    }

    /**
     * Sets the cell stub for PoolManager communication.
     * @param poolManagerCellStub the poolManagerCellStub to set
     */
    public void setPoolManagerCellStub(CellStub poolManagerCellStub) {
        this._poolManagerCellStub = poolManagerCellStub;
    }

    /**
     * Gets the cell stub for pool communication.
     * @return the _poolCellStub
     */
    public CellStub getPoolCellStub() {
        return _poolCellStub;
    }

    /**
     * Sets the cell stub for pool communication.
     *
     * @param poolCellStub the _poolCellStub to set
     */
    public void setPoolCellStub(CellStub poolCellStub) {
        this._poolCellStub = poolCellStub;
    }

    /**
     * Gets time to wait for the reply from pool
     * @return the _timeoutPool
     */
    public long getTimeoutPool() {
        return _timeoutPool;
    }

    /**
     * Sets time to wait for the reply from pool
     * @param timeoutPool the _timeoutPool to set
     */
    public void setTimeoutPool(long timeoutPool) {
        this._timeoutPool = timeoutPool;
    }
}