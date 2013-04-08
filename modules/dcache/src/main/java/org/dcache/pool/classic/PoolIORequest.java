package org.dcache.pool.classic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.nio.channels.CompletionHandler;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.vehicles.FileAttributes;

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
    private final String _initiator;
    private final boolean _isPoolToPoolTransfer;
    private final static Logger _log = LoggerFactory.getLogger(PoolIORequest.class);

    /** transfer status error code */
    private volatile int _errorCode;

    /** transfer status error message */
    private volatile String _errorMessage = "";

    private final CellStub _door;
    private final CellAddressCore _pool;

    /**
     * @param transfer the read or write transfer to execute
     * @param id the client id of the request
     * @param initiator the initiator string identifying who requested the transfer
     * @param isPoolToPoolTranfer true if the transfer is between to pools
     * @param queue the name of the queue used for the request
     * @param door communication stub to the door that generated the request
     * @param pool the cell address of this pool
     */
    public PoolIORequest(PoolIOTransfer transfer, long id, String initiator,
            boolean isPoolToPoolTranfer, String queue, CellStub door,
            CellAddressCore pool) {
        _transfer = transfer;
        _id = id;
        _initiator = initiator;
        _queue = queue;
        _door = door;
        _pool = pool;
        _isPoolToPoolTransfer = isPoolToPoolTranfer;
    }

    public boolean isPoolToPoolTransfer()
    {
        return _isPoolToPoolTransfer;
    }

    public String getInitiator()
    {
        return _initiator;
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

    synchronized Cancellable transfer(MoverExecutorService moverExecutorService, final CompletionHandler<Void,Void> completionHandler) {
        return moverExecutorService.execute(this, completionHandler);
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

    public int getErrorCode()
    {
        return _errorCode;
    }

    public String getErrorMessage()
    {
        return _errorMessage;
    }

    @Override
    public String toString() {
        return _transfer.toString();
    }

    public String getQueue()
    {
        return _queue;
    }
}
