package org.dcache.pool.classic;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.IoBatchable;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import java.io.IOException;
import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PoolIORequest encapsulates queuing, execution and notification
 * of a file transfer.
 *
 * The transfer is represented by a PoolIOTransfer instance, and
 * PoolIORequest manages the lifetime of the transfer object.
 *
 * Billing and door notifications are send after completed or
 * failed transfer, or upon dequeuing the request.
 */
public class PoolIORequest implements IoBatchable {

    private final PoolIOTransfer _transfer;
    private final long _id;
    private final String _queue;
    private final String _poolName;
    private final CellPath _door;
    private final String _initiator;
    private CellPath _billingCell;
    private final CellEndpoint _cellEndpoint;
    private Thread _thread;
    private final static Logger _log = LoggerFactory.getLogger(PoolIORequest.class);
    private final FaultListener _faultListener;

    /**
     * @param transfer the read or write transfer to execute
     * @param id the client id of the request
     * @param initiator the initiator string identifying who
     * requested the transfer
     * @param door the cell path to the cell requesting the
     * transfer
     * @param poolName the name of the pool
     * @param pool the name of the pool
     * @param cellEndpoint the cellEndpoint of the pool
     * @param queue the name of the queue used for the request
     * @param cellEndpoint the cellEndpoint of the pool
     * @param billingCell  the CellPath of the billing cell
     */
    public PoolIORequest(PoolIOTransfer transfer, long id, String initiator,
            CellPath door, String poolName, String queue, CellEndpoint cellEndpoint, CellPath billingCell, FaultListener faultListener) {
        _transfer = transfer;
        _id = id;
        _initiator = initiator;
        _door = door;
        _poolName = poolName;
        _queue = queue;
        _cellEndpoint = cellEndpoint;
        _billingCell = billingCell;
        _faultListener = faultListener;
    }

    private void sendBillingMessage(int rc, String message) {
        MoverInfoMessage info =
                new MoverInfoMessage(_cellEndpoint.getCellInfo().getCellName() + "@" + _cellEndpoint.getCellInfo().getDomainName(),
                getPnfsId());
        info.setInitiator(_initiator);
        info.setFileCreated(_transfer instanceof PoolIOWriteTransfer);
        info.setStorageInfo(getStorageInfo());
        info.setFileSize(_transfer.getFileSize());
        info.setResult(rc, message);
        info.setTransferAttributes(getBytesTransferred(),
                getTransferTime(),
                getProtocolInfo());

        try {
            _cellEndpoint.sendMessage(new CellMessage(_billingCell, info));
        } catch (NoRouteToCellException e) {
            _log.error("Cannot send message to " + _billingCell + ": No route to cell");
        }
    }

    private void sendFinished(int rc, String msg) {
        DoorTransferFinishedMessage finished =
                new DoorTransferFinishedMessage(getClientId(),
                getPnfsId(),
                getProtocolInfo(),
                getStorageInfo(),
                _poolName);
        finished.setIoQueueName(_queue);
        if (rc == 0) {
            finished.setSucceeded();
        } else {
            finished.setReply(rc, msg);
        }

        try {
            _cellEndpoint.sendMessage(new CellMessage(_door, finished));
        } catch (NoRouteToCellException e) {
            _log.error("Cannot send message to " + _door + ": No route to cell");
        }
    }

    protected ProtocolInfo getProtocolInfo() {
        return _transfer.getProtocolInfo();
    }

    protected StorageInfo getStorageInfo() {
        return _transfer.getStorageInfo();
    }

    public long getTransferTime() {
        return _transfer.getTransferTime();
    }

    public long getBytesTransferred() {
        return _transfer.getBytesTransferred();
    }

    public double getTransferRate() {
        return _transfer.getTransferRate();
    }

    public long getLastTransferred() {
        return _transfer.getLastTransferred();
    }

    public PnfsId getPnfsId() {
        return _transfer.getPnfsId();
    }

    public void queued(int id) {
    }

    public void unqueued() {
        /* Closing the transfer object should not throw an
         * exception when the transfer has not begun yet. If it
         * does, we log the error, but otherwise there is not much
         * we can do. REVISIT: Consider to disable the pool.
         */
        try {
            _transfer.close();
        } catch (NoRouteToCellException e) {
            _log.error("Failed to cancel transfer: " + e);
        } catch (CacheException e) {
            _log.error("Failed to cancel transfer: " + e);
        } catch (IOException e) {
            _log.error("Failed to cancel transfer: " + e);
        } catch (InterruptedException e) {
            _log.error("Failed to cancel transfer: " + e);
        }

        sendFinished(CacheException.DEFAULT_ERROR_CODE,
                "Transfer was killed");
    }

    public String getClient() {
        return _door.getDestinationAddress().toString();
    }

    public long getClientId() {
        return _id;
    }

    private synchronized void setThread(Thread thread) {
        _thread = thread;
    }

    public synchronized boolean kill() {
        if (_thread == null) {
            return false;
        }

        _thread.interrupt();
        return true;
    }

    private void transfer()
            throws Exception {
        try {
            _transfer.transfer();
        } catch (InterruptedException e) {
            throw e;
        } catch (RuntimeException e) {
            _log.error("Transfer failed due to unexpected exception", e);
            throw e;
        } catch (Exception e) {
            _log.warn("Transfer failed: " + e);
            throw e;
        }
    }

    private void close()
            throws Exception {
        try {
            _transfer.close();
        } catch (RuntimeException e) {
            _log.error("Transfer failed in post-processing due to unexpected exception", e);
            throw e;
        } catch (Exception e) {
            _log.warn("Transfer failed in post-processing: " + e);
            throw e;
        }
    }

    public void run() {
        int rc;
        String msg;
        try {
            setThread(Thread.currentThread());
            try {
                transfer();
            } finally {
                /* Surpress thread interruptions after this point.
                 */
                setThread(null);
                Thread.interrupted();
                close();
            }

            rc = 0;
            msg = "";
        } catch (InterruptedException e) {
            rc = CacheException.DEFAULT_ERROR_CODE;
            msg = "Transfer was killed";
        } catch (CacheException e) {
            rc = e.getRc();
            msg = e.getMessage();
            if (rc == CacheException.ERROR_IO_DISK) {
                _faultListener.faultOccurred(new FaultEvent("repository", FaultAction.DISABLED, msg, e));
            }
            rc = e.getRc();
            msg = e.getMessage();
        } catch (RuntimeException e) {
            rc = CacheException.UNEXPECTED_SYSTEM_EXCEPTION;
            msg = "Transfer failed due to unexpected exception: " + e;
        } catch (Exception e) {
            rc = CacheException.DEFAULT_ERROR_CODE;
            msg = "Transfer failed: " + e.getMessage();
        }

        sendFinished(rc, msg);
        sendBillingMessage(rc, msg);
    }

    @Override
    public String toString() {
        return _transfer.toString();
    }
}


