package org.dcache.pool.classic;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import java.io.IOException;
import java.nio.channels.CompletionHandler;
import org.dcache.pool.FaultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final CellPath _door;
    private final String _initiator;
    private CellPath _billingCell;
    private final CellEndpoint _cellEndpoint;
    private final static Logger _log = LoggerFactory.getLogger(PoolIORequest.class);
    private final FaultListener _faultListener;

    private Cancelable _mover;
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

    private boolean _canceled;

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

    void sendBillingMessage() {
        MoverInfoMessage info =
                new MoverInfoMessage(_cellEndpoint.getCellInfo().getCellName() + "@" + _cellEndpoint.getCellInfo().getDomainName(),
                getPnfsId());

        info.setSubject(_transfer.getSubject());
        info.setInitiator(_initiator);
        info.setFileCreated(_transfer instanceof PoolIOWriteTransfer);
        info.setStorageInfo(getStorageInfo());
        info.setFileSize(_transfer.getFileSize());
        info.setResult(_errorCode, _errorMessage);
        info.setTransferAttributes(getBytesTransferred(),
                getTransferTime(),
                getProtocolInfo());

        try {
            _cellEndpoint.sendMessage(new CellMessage(_billingCell, info));
        } catch (NoRouteToCellException e) {
            _log.error("Cannot send message to " + _billingCell + ": No route to cell");
        }
    }

    void sendFinished() {
        DoorTransferFinishedMessage finished =
                new DoorTransferFinishedMessage(getClientId(),
                getPnfsId(),
                getProtocolInfo(),
                getStorageInfo(),
                _poolName);
        finished.setIoQueueName(_queue);
        if (_errorCode == 0) {
            finished.setSucceeded();
        } else {
            finished.setReply(_errorCode, _errorMessage);
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
        return _transfer.getPnfsId();
    }

    @Override
    public String getClient() {
        return _door.getDestinationAddress().toString();
    }

    @Override
    public long getClientId() {
        return _id;
    }

    @Override
    public synchronized boolean kill() {
        _state = CANCELED;
        _canceled = true;

        if (_mover == null) {
            return false;
        }

        _mover.cancel();
        return true;
    }

    synchronized void transfer(MoverExecutorService moverExecutorService, CompletionHandler<Object,Object> completionHandler) {
        _startTime = System.currentTimeMillis();
        if(_canceled) {
            completionHandler.failed( new InterruptedException("Mover canceled"), null);
        } else {
            _state = RUNNING;
            _mover = moverExecutorService.execute(this, completionHandler);
        }
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

    public PoolIOTransfer getTransfer() {
        return _transfer;
    }

    public CellEndpoint getCellEndpoint() {
        return _cellEndpoint;
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
