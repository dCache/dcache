package diskCacheV111.util;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.PnfsMessage;
import diskCacheV111.vehicles.PoolFileFlushedMessage;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.vehicles.PnfsSetFileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiPnfsHandler implements CellMessageSender {

    private final String _poolName;
    private static final long DEFAULT_PNFS_TIMEOUT = TimeUnit.MINUTES.toMillis(
          30);

    private final CellStub _cellStub;

    private static final Logger _logNameSpace =
          LoggerFactory.getLogger("logger.org.dcache.namespace."
                + PnfsHandler.class.getName());

    private static CellStub createStub(CellPath path) {
        CellStub stub = new CellStub();
        stub.setDestinationPath(path);
        stub.setTimeout(DEFAULT_PNFS_TIMEOUT);
        stub.setTimeoutUnit(TimeUnit.MILLISECONDS);
        stub.setFlags(CellEndpoint.SendFlag.RETRY_ON_NO_ROUTE_TO_CELL);
        return stub;
    }

    public MultiPnfsHandler(CellStub stub, String poolName) {
        _cellStub = stub;
        _poolName = poolName;
    }

    public MultiPnfsHandler(CellPath pnfsManagerPath,
                       String poolName) {
        this(createStub(pnfsManagerPath), poolName);
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        _cellStub.setCellEndpoint(endpoint);
    }

    /**
     * Sends a PnfsMessage to PnfsManager.
     */
    public void send(PnfsMessage msg) {
        checkState(_cellStub != null, "Missing endpoint");

        _cellStub.notify(msg);
    }


    /**
     * Sends a PnfsMessage to PnfsManager.
     */
    public <T extends Message> ListenableFuture<T> sendAsync(PnfsMessage msg) {
        checkState(_cellStub != null, "Missing endpoint");

        return _cellStub.send((T) msg);
    }

    /**
     * Send a PnfsMessage to PnfsManager indicating that an expected response will be ignored after
     * some timeout has elapsed.  This method exists primarily to support legacy code; new code
     * should consider using the requestAsync method instead.
     *
     * @param msg     The message to send
     * @param timeout The duration, in milliseconds, after which any response will be ignored.
     */
    public void send(PnfsMessage msg, long timeout) {
        checkState(_cellStub != null, "Missing endpoint");

        _cellStub.notify(msg, timeout);
    }

    /**
     * Sends a PnfsMessage notification to PnfsManager. No reply is expected for a notification and
     * no failure is reported if the message could not be delivered.
     */
    public void notify(PnfsMessage msg) {
        msg.setReplyRequired(false);
        send(msg);
    }

    public void clearCacheLocation(PnfsId id) {
        clearCacheLocation(id, false);
    }

    public void clearCacheLocation(PnfsId id, boolean removeIfLast) {
        notify(new PnfsClearCacheLocationMessage(id, _poolName, removeIfLast));
    }

    public void addCacheLocation(PnfsId id) throws CacheException {
        addCacheLocation(id, _poolName);
    }

    public void addCacheLocation(PnfsId id, String pool) throws CacheException {
        request(new PnfsAddCacheLocationMessage(id, pool));
    }

    /**
     * Sends a message to the request manager and blocks until a reply is received. In case of
     * errors in the reply, those are thrown as a CacheException. Timeouts and failure to send the
     * message to the PnfsManager are reported as a timeout CacheException.
     */
    public <T extends PnfsMessage> T request(T msg)
          throws CacheException {
        try {
            return CellStub.getMessage(requestAsync(msg));
        } catch (InterruptedException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  "Sending message to " + _cellStub.getDestinationPath() + " interrupted");
        } catch (NoRouteToCellException e) {
            throw new TimeoutCacheException(e.getMessage(), e);
        }
    }

    /**
     * Sends a message to the pnfs manager and returns a promise of a future reply.
     */
    public <T extends PnfsMessage> ListenableFuture<T> requestAsync(T msg) {
        checkState(_cellStub != null, "Missing endpoint");
        return requestAsync(msg, _cellStub.getTimeoutInMillis());
    }

    /**
     * Sends a message to the pnfs manager and returns a promise of a future reply.
     */
    public <T extends PnfsMessage> ListenableFuture<T> requestAsync(T msg, long timeout) {
        checkState(_cellStub != null, "Missing endpoint");

        msg.setReplyRequired(true);
        return _cellStub.send(msg, timeout);
    }


    public void fileFlushed(PnfsId pnfsId, FileAttributes fileAttributes) throws CacheException {

        PoolFileFlushedMessage fileFlushedMessage = new PoolFileFlushedMessage(_poolName, pnfsId,
              fileAttributes);

        // throws exception if something goes wrong
        request(fileFlushedMessage);
    }

    /**
     * Get path corresponding to given pnfsid.
     *
     * @param pnfsID
     * @return path
     * @throws CacheException
     */
    public FsPath getPathByPnfsId(PnfsId pnfsID) throws CacheException {
        return FsPath.create(request(new PnfsMapPathMessage(pnfsID)).getPnfsPath());
    }

    /**
     * Get file attributes. The PnfsManager is free to return fewer attributes than requested. If
     * <code>attr</code> is an empty array, file existence if checked.
     *
     * @param pnfsid
     * @param attr   array of requested attributes.
     * @return requested attributes
     */
    public FileAttributes getFileAttributes(PnfsId pnfsid, Set<FileAttribute> attr)
          throws CacheException {
        return request(new PnfsGetFileAttributes(pnfsid, attr)).getFileAttributes();
    }


    /**
     * Set file attributes. If <code>attr</code> is an empty array, file existence if checked.
     *
     * @param pnfsid
     * @param attr   array of requested attributes.
     */
    public void setFileAttributes(PnfsId pnfsid, FileAttributes attr) throws CacheException {
        request(new PnfsSetFileAttributes(pnfsid, attr));
    }

}
