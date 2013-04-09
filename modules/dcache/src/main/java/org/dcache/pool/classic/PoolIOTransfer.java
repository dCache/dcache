package org.dcache.pool.classic;

import javax.security.auth.Subject;

import java.io.Serializable;
import java.nio.channels.CompletionHandler;

import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.vehicles.FileAttributes;

/**
 * Abstract representation of the pool side of a file tranafer.
 * PoolIOTransfer exposes essential parameters of a mover, and encapsulates
 * information needed to complete a transfer.
 *
 * A transfer is divided into two phases: the data moving phase and a
 * cleanup phase. The data moving phase is interuptible during normal
 * operation, whereas cleanup should only be interrupted when pool
 * shutdown is imminent. The cleanup phase must be executed no matter
 * whether the data movement phase succeeded or has even been
 * attempted.
 *
 * PoolIOTransfer does not depent on the repository, but its
 * subclasses do.
 */
public abstract class PoolIOTransfer implements Mover<ProtocolInfo>
{
    /** client id */
    protected final long _id;

    /** scheduling queue */
    protected final String _queue;

    /** identification of who requested the transfer */
    protected final String _initiator;

    /** true if transfer is between two pools */
    protected final boolean _isPoolToPoolTransfer;

    /** transfer status error code */
    protected volatile int _errorCode;

    /** transfer status error message */
    protected volatile String _errorMessage = "";

    /** stub to talk to the door of this transfer */
    protected final CellStub _door;

    /** mover implementation suitable for this transfer */
    protected final MoverProtocol _mover;

    /** attributes of the file being transferred */
    protected final FileAttributes _fileAttributes;

    /** protocol specific infomation provided by the door */
    protected final ProtocolInfo _protocolInfo;

    /** identify of the entity requesting the transfer */
    protected final Subject _subject;

    /** transfer service to be used with this transfer */
    private final TransferService<PoolIOTransfer> _transferService;

    /** post transfer service to be used with this transfer */
    private final PostTransferService _postTransferService;

    /**
     * @param id the client id of the request
     * @param initiator the initiator string identifying who requested the transfer
     * @param isPoolToPoolTransfer true if the transfer is between to pools
     * @param queue the name of the queue used for the request
     * @param door communication stub to the door that generated the request
     * @param fileAttributes attributes of the file being transferred
     * @param protocolInfo transfer protocol specific attributes
     * @param subject identify of the entity requesting the transfer
     * @param mover the mover
     */
    public PoolIOTransfer(long id, String initiator, boolean isPoolToPoolTransfer,
                          String queue, CellStub door,
                          FileAttributes fileAttributes,
                          ProtocolInfo protocolInfo,
                          Subject subject,
                          MoverProtocol mover,
                          TransferService<PoolIOTransfer> transferService,
                          PostTransferService postTransferService)
    {
        _id = id;
        _initiator = initiator;
        _isPoolToPoolTransfer = isPoolToPoolTransfer;
        _queue = queue;
        _door = door;
        _fileAttributes = fileAttributes;
        _protocolInfo = protocolInfo;
        _subject = subject;
        _mover = mover;
        _transferService = transferService;
        _postTransferService = postTransferService;
    }

    @Override
    public FileAttributes getFileAttributes()
    {
        return _fileAttributes;
    }

    @Override
    public ProtocolInfo getProtocolInfo()
    {
        return _protocolInfo;
    }

    @Override
    public long getTransferTime()
    {
        return _mover.getTransferTime();
    }

    @Override
    public long getBytesTransferred()
    {
        return _mover.getBytesTransferred();
    }

    @Override
    public long getLastTransferred()
    {
        return _mover.getLastTransferred();
    }

    @Override
    public CellPath getPathToDoor() {
        return _door.getDestinationPath();
    }

    @Override
    public long getClientId() {
        return _id;
    }

    public void sendToDoor(Serializable msg) throws NoRouteToCellException
    {
        _door.send(msg);
    }

    /**
     * Set transfer status.
     *
     * The provided status and error message will be sent to billing and to
     * the door. Only the first error status set is kept. Any subsequent
     * errors are suppressed.
     */
    public void setTransferStatus(int errorCode, String errorMessage) {
        if (_errorCode == 0) {
            _errorCode = errorCode;
            _errorMessage = errorMessage;
        }
    }

    @Override
    public String getQueueName()
    {
        return _queue;
    }

    @Override
    public int getErrorCode()
    {
        return _errorCode;
    }

    @Override
    public String getErrorMessage()
    {
        return _errorMessage;
    }

    @Override
    public String getInitiator()
    {
        return _initiator;
    }

    public boolean isPoolToPoolTransfer()
    {
        return _isPoolToPoolTransfer;
    }

    public MoverProtocol getMover() {
        return _mover;
    }

    @Override
        public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(_fileAttributes.getPnfsId());
        sb.append(" h={")
            .append(_mover.toString())
            .append("} bytes=").append(getBytesTransferred())
            .append(" time/sec=").append(getTransferTime() / 1000L)
            .append(" LM=");

        long lastTransferTime = getLastTransferred();
        if (lastTransferTime == 0L) {
            sb.append(0);
        } else {
            sb.append((System.currentTimeMillis() - lastTransferTime) / 1000L);
        }
        return sb.toString();
    }

    @Override
    public Cancellable execute(CompletionHandler<Void, Void> completionHandler) {
        return _transferService.execute(this, completionHandler);
    }

    @Override
    public void postprocess(CompletionHandler<Void, Void> completionHandler) {
        _postTransferService.execute(this, completionHandler);
    }

    /**
     * Implements the data movement phase.
     */
    public abstract void transfer() throws Exception;

    public Subject getSubject()
    {
        return _subject;
    }

    /**
     * Returns the size of the replica that was transferred. Must not
     * be called before <code>close</code>.
     */
    public abstract long getFileSize();
}
