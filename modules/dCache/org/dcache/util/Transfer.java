package org.dcache.util;

import java.util.Set;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicInteger;
import javax.security.auth.Subject;

import com.sun.security.auth.UserPrincipal;

import org.dcache.acl.Origin;
import org.dcache.acl.enums.AccessMask;
import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;

import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.FsPath;

import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dcache.namespace.FileAttribute.*;

/**
 * Encapulates information about and typical operations of a
 * transfer. The class is abstract and must be subclassed.
 */
public abstract class Transfer implements Comparable<Transfer>
{
    private static final Logger _log = LoggerFactory.getLogger(Transfer.class);

    private static final int FILE_UMASK_AUTHENTICATED = 0644;
    private static final int FILE_UMASK_ANONYMOUS = 0666;

    private static final AtomicInteger _sessionCounter = new AtomicInteger();

    protected final PnfsHandler _pnfs;
    protected final long _startedAt;
    protected final FsPath _path;
    protected final Subject _subject;
    protected final int _sessionId;

    protected CellStub _poolManager;
    protected CellStub _pool;
    protected CellStub _billing;

    private String _cellName;
    private String _domainName;

    private String _poolName;
    private Integer _moverId;
    private boolean _hasMover;
    private PnfsId _pnfsid;
    private String _status;
    private CacheException _error;
    private StorageInfo _storageInfo;
    private boolean _isWrite;

    /**
     * Constructs a new Transfer object.
     *
     * @param pnfs PnfsHandler used for pnfs communication
     * @param subject The subject performing the transfer
     * @param path The path of the file to transfer
     */
    public Transfer(PnfsHandler pnfs, Subject subject, FsPath path)
    {
        _pnfs = new PnfsHandler(pnfs, subject);
        _subject = subject;
        _path = path;
        _startedAt = System.currentTimeMillis();
        _sessionId = _sessionCounter.getAndIncrement();
    }

    /**
     * Creates a ProtocolInfo suitable for selecting a pool.
     */
    protected abstract ProtocolInfo createProtocolInfoForPoolManager();

    /**
     * Creates a ProtocolInfo suitable for starting a mover.
     */
    protected abstract ProtocolInfo createProtocolInfoForPool();

    /**
     * Orders Transfer objects according to hash value. Makes it
     * possible to add Transfer objects to tree based collections.
     */
    @Override
    public int compareTo(Transfer o)
    {
        return o.getSessionId() - getSessionId();
    }

    /**
     * Returns the session ID of this transfer. The session ID
     * uniquely identifies this transfer object within this VM
     * instance.
     */
    public int getSessionId()
    {
        return _sessionId;
    }

    /**
     * Sets CellStub for PoolManager.
     */
    public synchronized void setPoolManagerStub(CellStub stub)
    {
        _poolManager = stub;
    }

    /**
     * Sets CellStub for pools.
     */
    public synchronized void setPoolStub(CellStub stub)
    {
        _pool = stub;
    }

    /**
     * Sets CellStub for Billing.
     */
    public synchronized void setBillingStub(CellStub stub)
    {
        _billing = stub;
    }

    /**
     * Sets the current status of a pool. May be null.
     */
    public synchronized void setStatus(String status)
    {
        _status = status;
    }

    /**
     * Sets the PnfsId of the file to be transferred.
     */
    public synchronized void setPnfsId(PnfsId pnfsid)
    {
        _pnfsid = pnfsid;
    }


    /**
     * Returns the PnfsId of the file to be transferred.
     */
    public synchronized PnfsId getPnfsId()
    {
        return _pnfsid;
    }

    /**
     * Sets whether this is an upload.
     */
    public synchronized void setWrite(boolean isWrite)
    {
        _isWrite = isWrite;
    }

    /**
     * Returns whether this is an upload.
     */
    public synchronized boolean isWrite()
    {
        return _isWrite;
    }

    /**
     * Registers the fact that the transfer now has a mover.
     *
     * @param moverId The mover ID of the transfer.
     */
    public synchronized void setMoverId(Integer moverId)
    {
        _moverId = moverId;
        _hasMover = (_moverId != null);
    }

    /**
     * Returns the ID of the mover of this transfer.
     */
    public synchronized Integer getMoverId()
    {
        return _moverId;
    }

    /**
     * Returns whether this transfer has a mover (to the best of our
     * knowledge).
     */
    public synchronized boolean hasMover()
    {
        return _hasMover;
    }

    /**
     * Sets the pool to use for this transfer.
     */
    public synchronized void setPool(String pool)
    {
        _poolName = pool;
    }

    /**
     * Returns the pool to use for this transfer.
     */
    public synchronized String getPool()
    {
        return _poolName;
    }

    /**
     * Sets the StorageInfo of the file to transfer.
     */
    public synchronized void setStorageInfo(StorageInfo info)
    {
        _storageInfo = info;
    }

    /**
     * Returns the StorageInfo of the file to transfer.
     */
    public synchronized StorageInfo getStorageInfo()
    {
        return _storageInfo;
    }

    /**
     * Signals that the mover of this transfer finished.
     */
    public synchronized void finished(CacheException error)
    {
        _hasMover = false;
        _error = error;
        notifyAll();
    }

    /**
     * Signals that the mover of this transfer finished.
     */
    public final synchronized void finished(int rc, String error)
    {
        if (rc != 0) {
            finished(new CacheException(rc, error));
        } else {
            finished((CacheException) null);
        }
    }

    /**
     * Signals that the mover of this transfer finished.
     */
    public final synchronized void finished(DoorTransferFinishedMessage msg)
    {
        if (msg.getReturnCode() != 0) {
            finished(CacheExceptionFactory.exceptionOf(msg));
        } else {
            finished((CacheException) null);
        }
    }

    /**
     * Sets the cell name of the door handling the transfer.
     */
    public synchronized void setCellName(String cellName)
    {
        _cellName = cellName;
    }

    /**
     * Returns the cell name of the door handling the transfer.
     */
    public synchronized String getCellName()
    {
        return _cellName;
    }

    /**
     * Sets the domain name of the door handling the transfer.
     */
    public synchronized void setDomainName(String domainName)
    {
        _domainName = domainName;
    }

    /**
     * Returns the domain name of the door handling the transfer.
     */
    public synchronized String getDomainName()
    {
        return _domainName;
    }

    /**
     * Blocks until the mover of this transfer finished, or until
     * a timeout is reached. Relies on the
     * DoorTransferFinishedMessage being injected into the
     * transfer through the <code>finished</code> method.
     *
     * @param millis The timeout in milliseconds
     * @return true when the mover has finished
     * @throws CacheException if the mover failed
     */
    public synchronized boolean waitForMover(long millis)
        throws CacheException, InterruptedException
    {
        long deadline = System.currentTimeMillis() + millis;
        while (_hasMover && System.currentTimeMillis() < deadline) {
            wait(deadline - System.currentTimeMillis());
        }

        if (_error != null) {
            throw _error;
        }

        return !_hasMover;
    }

    /**
     * Returns an IoDoorEntry describing the transfer. This is
     * used by the "Active Transfer" view of the HTTP monitor.
     */
    public synchronized IoDoorEntry getIoDoorEntry()
    {
        Origin origin = Subjects.getOrigin(_subject);
        return new IoDoorEntry(_sessionId,
                               _pnfsid,
                               _poolName,
                               _status,
                               _startedAt,
                               origin.getAddress().getHostAddress());
    }

    /**
     * Creates a new name space entry for the file to
     * transfer. This will fill in the PnfsId and StorageInfo of
     * the file and mark the transfer as an upload.
     *
     * Will fail if the subject of the transfer doesn't have
     * permission to create the file.
     *
     * @param parent FileAttributes containing at least the owner
     *               and group of the parent directory
     * @throws CacheException if creating the entry failed
     */
    public void createNameSpaceEntry(FileAttributes parent)
        throws CacheException
    {
        setStatus("PnfsManager: Creating name space entry");
        try {
            long[] uids = Subjects.getUids(_subject);
            long[] gids = Subjects.getGids(_subject);
            int uid = (uids.length > 0) ? (int) uids[0] : parent.getOwner();
            int gid = (gids.length > 0) ? (int) gids[0] : parent.getGroup();
            int umask =
                (uids.length > 0) ? FILE_UMASK_AUTHENTICATED : FILE_UMASK_ANONYMOUS;
            PnfsCreateEntryMessage msg =
                _pnfs.createPnfsEntry(_path.toString(), uid, gid,
                                      umask & parent.getMode());
            setPnfsId(msg.getPnfsId());
            setStorageInfo(msg.getStorageInfo());
            setWrite(true);
        } finally {
            setStatus(null);
        }
    }

    /**
     * Reads the name space entry of the file to transfer. This
     * will fill in the PnfsId and StorageInfo of the file and
     * mark the transfer as a download.
     *
     * Will fail if the subject of the transfer doesn't have
     * permission to read the file.
     *
     * @throws CacheException if reading the entry failed
     */
    public void readNameSpaceEntry()
        throws CacheException
    {
        setStatus("PnfsManager: Fetching storage info");
        try {
            Set<FileAttribute> request = EnumSet.of(PNFSID, TYPE, STORAGEINFO);
            Set<AccessMask> mask = EnumSet.of(AccessMask.READ_DATA);
            FileAttributes attributes;
            PnfsId pnfsid = getPnfsId();
            if (pnfsid != null) {
                attributes = _pnfs.getFileAttributes(pnfsid, request, mask);
            } else {
                attributes = _pnfs.getFileAttributes(_path.toString(), request, mask);
            }

            /* We can only read regular files.
             */
            FileType type = attributes.getFileType();
            if (type == FileType.DIR || type == FileType.SPECIAL) {
                throw new NotFileCacheException("Not a regular file");
            }

            setStorageInfo(attributes.getStorageInfo());
            setPnfsId(attributes.getPnfsId());
            setWrite(false);
        } finally {
            setStatus(null);
        }
    }

    /**
     * Sets the length of the file to be uploaded. Only valid for
     * uploads.
     */
    public synchronized void setLength(long length)
    {
        if (!isWrite()) {
            throw new IllegalStateException("Can only set length for uploads");
        }
        _storageInfo.setFileSize(length);
    }

    /**
     * Selects a pool suitable for the transfer.
     */
    public void selectPool()
        throws CacheException, InterruptedException
    {
        PnfsId pnfsId = getPnfsId();
        StorageInfo storageInfo = getStorageInfo();

        if (pnfsId == null || storageInfo == null) {
            throw new IllegalStateException("Need both PNFS ID and StorageInfo before a pool can be selected");
        }

        setStatus("PoolManager: Selecting pool");
        try {
            ProtocolInfo protocolInfo = createProtocolInfoForPoolManager();
            PoolMgrSelectPoolMsg request;
            if (isWrite()) {
                request =
                    new PoolMgrSelectWritePoolMsg(pnfsId, storageInfo,
                                                  protocolInfo,
                                                  storageInfo.getFileSize());
            } else {
                request =
                    new PoolMgrSelectReadPoolMsg(pnfsId, storageInfo,
                                                 protocolInfo,
                                                 storageInfo.getFileSize());
            }

            setPool(_poolManager.sendAndWait(request).getPoolName());
        } finally {
            setStatus(null);
        }
    }

    /**
     * Creates a mover for the transfer.
     *
     * @param queue The mover queue of the transfer; may be null
     */
    public void startMover(String queue)
        throws CacheException, InterruptedException
    {
        PnfsId pnfsId = getPnfsId();
        StorageInfo storageInfo = getStorageInfo();
        String pool = getPool();

        if (pnfsId == null || storageInfo == null || pool == null) {
            throw new IllegalStateException("Need PNFS ID, storage info and pool before a mover can be started");
        }

        setStatus("Pool " + pool + ": Creating mover");
        try {
            Origin origin = Subjects.getOrigin(_subject);
            String address = origin.getAddress().getHostAddress();
            ProtocolInfo protocolInfo = createProtocolInfoForPool();
            PoolIoFileMessage message;
            if (isWrite()) {
                message =
                    new PoolAcceptFileMessage(pool, pnfsId,
                                              protocolInfo, storageInfo);
            } else {
                message =
                    new PoolDeliverFileMessage(pool, pnfsId,
                                               protocolInfo, storageInfo);
            }
            message.setIoQueueName(queue);

            // the transaction string will be used by the pool as
            // initiator (-> table join in Billing DB)
            //         poolMessage.setInitiator(_transactionPrefix + protocolInfo.getXrootdFileHandle());

            /* As always, PoolIoFileMessage has to be sent via the
             * PoolManager (which could be the SpaceManager).
             */
            CellPath poolPath =
                (CellPath) _poolManager.getDestinationPath().clone();
            poolPath.add(pool);

            setMoverId(_pool.sendAndWait(poolPath, message).getMoverId());
        } finally {
            setStatus(null);
        }
    }

    /**
     * Kills the mover of the transfer. Blocks until the mover has
     * died or until a timeout is reached. An error is logged if
     * the mover failed to die or if the timeout was reached.
     *
     * @param millis Timeout in milliseconds
     */
    public void killMover(long millis)
    {
        if (!hasMover()) {
            return;
        }

        Integer moverId = getMoverId();
        String pool = getPool();
        setStatus("Mover " + pool + "/" + moverId + ": Killing mover");
        try {
            /* Kill the mover.
             */
            PoolMoverKillMessage message =
                new PoolMoverKillMessage(pool, moverId);
            message.setReplyRequired(false);
            _pool.send(new CellPath(pool), message);

            /* To reduce the risk of orphans when using PNFS, we wait
             * for the transfer confirmation.
             */
            if (!waitForMover(millis)) {
                _log.error("Failed to kill mover " + pool + "/" + moverId
                           + ": Timeout");
            }
        } catch (CacheException e) {
            // Not surprising that the pool reported a failure
            // when we killed the mover.
            _log.debug("Killed mover and pool reported: " +
                       e.getMessage());
        } catch (InterruptedException e) {
            _log.warn("Failed to kill mover " + pool + "/" + moverId
                      + ": " + e.getMessage());
            Thread.currentThread().interrupt();
        } catch (NoRouteToCellException e) {
            _log.error("Failed to kill mover " + pool + "/" + moverId
                       + ": " + e.getMessage());
        } finally {
            setStatus(null);
        }
    }

    public IoJobInfo queryMoverInfo()
        throws CacheException, InterruptedException
    {
        return _pool.sendAndWait(new CellPath(getPool()),
                                 "mover ls -binary " + getMoverId(),
                                 IoJobInfo.class);
    }

    /**
     * Deletes the name space entry of the file. Only valid for
     * uploads. In case of failures, an error is logged.
     */
    public void deleteNameSpaceEntry()
    {
        if (!isWrite()) {
            throw new IllegalStateException("Can only delete name space entry for uploads");
        }
        PnfsId pnfsId = getPnfsId();
        if (pnfsId != null) {
            setStatus("PnfsManager: Deleting name space entry");
            try {
                _pnfs.deletePnfsEntry(pnfsId, _path.toString());
            } catch (CacheException e) {
                _log.error("Failed to delete file after failed upload: " +
                           _path + " (" + pnfsId + "): " + e.getMessage());
            } finally {
                setStatus(null);
            }
        }
    }

    /**
     * Sends billing information to the billing cell.
     *
     * @param code The error code of the transfer; zero indicates success
     * @param error The error string of the transfer; may be empty
     */
    public void notifyBilling(int code, String error)
    {
        try {
            Origin origin = Subjects.getOrigin(_subject);
            String owner = Subjects.getDn(_subject);
            if (owner == null)  {
                Set<UserPrincipal> principals =
                    _subject.getPrincipals(UserPrincipal.class);
                if (!principals.isEmpty()) {
                    owner = principals.iterator().next().getName();
                }
            }

            DoorRequestInfoMessage msg =
                new DoorRequestInfoMessage(getCellName() + "@" + getDomainName());
            msg.setOwner(owner);
            long[] uids = Subjects.getUids(_subject);
            long[] gids = Subjects.getGids(_subject);
            if (uids.length > 0) {
                msg.setUid((int) uids[0]);
            }
            if (gids.length > 0) {
                msg.setGid((int) gids[0]);
            }
            msg.setPath(_path.toString());
            msg.setTransactionTime(_startedAt);
            msg.setClient(origin.getAddress().getHostAddress());
            msg.setPnfsId(getPnfsId());
            msg.setResult(code, error);
            msg.setStorageInfo(_storageInfo);
            _billing.send(msg);
        } catch (NoRouteToCellException e) {
            _log.error("Failed to register transfer in billing: " +
                       e.getMessage());
        }
    }
}
