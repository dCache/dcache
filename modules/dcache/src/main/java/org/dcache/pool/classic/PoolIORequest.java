package org.dcache.pool.classic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.nio.channels.CompletionHandler;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.pool.classic.IoRequestState.*;

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
public class PoolIORequest implements IoProcessable {

    private final PoolIOTransfer _transfer;
    private final long _id;
    private final String _queue;
    private final String _poolName;
    private final String _initiator;
    private final static Logger _log = LoggerFactory.getLogger(PoolIORequest.class);
    private final FaultListener _faultListener;

    private Cancellable _mover;
    /**
     * Request creation time.
     */
    private final long _ctime = System.currentTimeMillis();

    /**
     * Transfer start time.
     */
    private volatile long _startTime;

    private volatile IoRequestState _state = CREATED;

    /** transfer status error code */
    private volatile int _errorCode;

    /** transfer status error message */
    private volatile String _errorMessage = "";

    private final CellStub _billing;
    private final CellStub _door;
    private final CellAddressCore _pool;

    /**
     * @param transfer the read or write transfer to execute
     * @param id the client id of the request
     * @param initiator the initiator string identifying who requested the transfer
     * @param poolName the name of this pool
     * @param queue the name of the queue used for the request
     * @param billing communication stub to the billing cell
     * @param door communication stub to the door that generated the request
     * @param pool the cell address of this pool
     * @param poolName faultListener listener to notify in case of faults
     */
    public PoolIORequest(PoolIOTransfer transfer, long id, String initiator,
            String poolName, String queue, CellStub billing, CellStub door,
            CellAddressCore pool, FaultListener faultListener) {
        _transfer = transfer;
        _id = id;
        _initiator = initiator;
        _poolName = poolName;
        _queue = queue;
        _billing = billing;
        _door = door;
        _pool = pool;
        _faultListener = faultListener;
    }

    void sendBillingMessage() {
        MoverInfoMessage info =
                new MoverInfoMessage(_pool.toString(), getFileAttributes().getPnfsId());

        info.setSubject(_transfer.getSubject());
        info.setInitiator(_initiator);
        info.setFileCreated(_transfer instanceof PoolIOWriteTransfer);
        info.setStorageInfo(getFileAttributes().getStorageInfo());
        info.setFileSize(_transfer.getFileSize());
        info.setResult(_errorCode, _errorMessage);
        info.setTransferAttributes(getBytesTransferred(),
                getTransferTime(),
                getProtocolInfo());

        try {
            _billing.send(info);
        } catch (NoRouteToCellException e) {
            _log.error("Failed to register transfer in billing: {}", e.getMessage());
        }
    }

    void sendFinished() {
        DoorTransferFinishedMessage finished =
                new DoorTransferFinishedMessage(getClientId(),
                getFileAttributes().getPnfsId(),
                getProtocolInfo(),
                getFileAttributes(),
                _poolName,
                _queue);
        if (_errorCode == 0) {
            finished.setSucceeded();
        } else {
            finished.setReply(_errorCode, _errorMessage);
        }

        try {
            _door.send(finished);
        } catch (NoRouteToCellException e) {
            _log.error("Failed to notify door about transfer termination: {}", e.getMessage());
        }
    }

    protected ProtocolInfo getProtocolInfo() {
        return _transfer.getProtocolInfo();
    }

    protected FileAttributes getFileAttributes() {
        return _transfer.getFileAttributes();
    }

    @Override
    public long getTransferTime() {
        return _transfer.getTransferTime();
    }

    @Override
    public long getBytesTransferred() {
        return _transfer.getBytesTransferred();
    }

    @Override
    public double getTransferRate() {
        return _transfer.getTransferRate();
    }

    @Override
    public long getLastTransferred() {
        return _transfer.getLastTransferred();
    }

    public PnfsId getPnfsId() {
        return getFileAttributes().getPnfsId();
    }

    @Override
    public String getClient() {
        return _door.getDestinationPath().getDestinationAddress().toString();
    }

    @Override
    public long getClientId() {
        return _id;
    }

    @Override
    public synchronized void kill() {
        _state = CANCELED;
        if (_mover != null) {
            _mover.cancel();
        }
    }

    synchronized Cancellable transfer(MoverExecutorService moverExecutorService, final CompletionHandler completionHandler) {
        if (_state != QUEUED) {
            completionHandler.failed(new InterruptedException("Mover canceled"), null);
        }

        _state = RUNNING;
        _startTime = System.currentTimeMillis();
        _mover = moverExecutorService.execute(this, new CompletionHandler()
        {
            @Override
            public void completed(Object result, Object attachment)
            {
                completionHandler.completed(result, attachment);
            }

            @Override
            public void failed(Throwable exc, Object attachment)
            {
                int rc;
                String msg;
                if (exc instanceof InterruptedException || exc instanceof InterruptedIOException) {
                    rc = CacheException.DEFAULT_ERROR_CODE;
                    msg = "Transfer was killed";
                } else if (exc instanceof CacheException) {
                    rc = ((CacheException) exc).getRc();
                    msg = exc.getMessage();
                    if (rc == CacheException.ERROR_IO_DISK) {
                        getFaultListener().faultOccurred(new FaultEvent("repository", FaultAction.DISABLED, msg, exc));
                    }
                } else {
                    rc = CacheException.UNEXPECTED_SYSTEM_EXCEPTION;
                    msg = "Transfer failed due to unexpected exception: " + exc;
                }
                setTransferStatus(rc, msg);
                completionHandler.failed(exc, attachment);
            }
        });
        return _mover;
    }

    void close()
        throws CacheException, InterruptedException,
               IOException
    {
        try {
            _transfer.close();
        } catch (CacheException | IOException | InterruptedException e) {
            _log.warn("Transfer failed in post-processing: {}", e.toString());
            throw e;
        } catch (RuntimeException e) {
            _log.error("Transfer failed in post-processing due to unexpected exception", e);
            throw e;
        }
    }

    public void sendToDoor(Serializable msg) throws NoRouteToCellException
    {
        _door.send(msg);
    }

    public PoolIOTransfer getTransfer() {
        return _transfer;
    }

    public CellAddressCore getPoolAddress()
    {
        return _pool;
    }

    public void setState( IoRequestState state) {
        _state = state;
    }

    public IoRequestState getState() {
        return _state;
    }

    public long getCreationTime() {
        return _ctime;
    }

    public long getStartTime() {
        return _startTime;
    }

    public FaultListener getFaultListener() {
        return _faultListener;
    }

    /**
     * Set transfer status. The provided status and error message will be send to
     * the Billing and to the corresponding Door.
     *
     * @param errorCode
     * @param errorMessage
     */
    public void setTransferStatus(int errorCode, String errorMessage) {
        _errorCode = errorCode;
        _errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        return _state + " : " + _transfer.toString();
    }
}
