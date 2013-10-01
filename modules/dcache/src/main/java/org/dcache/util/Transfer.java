package org.dcache.util;

import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.security.auth.Subject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import diskCacheV111.poolManager.RequestContainerV5;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileIsNewCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.TimebasedCounter;

import org.dcache.acl.enums.AccessMask;
import org.dcache.cells.CellStub;
import org.dcache.commons.util.NDC;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.util.MathUtils.addWithInfinity;
import static org.dcache.util.MathUtils.subWithInfinity;

/**
 * Facade for transfer related operations. Encapulates information
 * about and typical operations of a transfer.
 */
public class Transfer implements Comparable<Transfer>
{
    private static final Logger _log = LoggerFactory.getLogger(Transfer.class);

    private static final TimebasedCounter _sessionCounter =
        new TimebasedCounter();

    protected final PnfsHandler _pnfs;
    protected final long _startedAt;
    protected final FsPath _path;
    protected final Subject _subject;
    protected final long _sessionId;
    protected final Object _session;

    protected CellStub _poolManager;
    protected CellStub _pool;
    protected CellStub _billing;
    protected CheckStagePermission _checkStagePermission;

    private String _cellName;
    private String _domainName;

    private String _poolName;
    private CellAddressCore _poolAddress;
    private Integer _moverId;
    private boolean _hasMover;
    private String _status;
    private CacheException _error;
    private FileAttributes _fileAttributes = new FileAttributes();
    private ProtocolInfo _protocolInfo;
    private boolean _isWrite;
    private InetSocketAddress _clientAddress;

    private long _allocated;

    private PoolMgrSelectReadPoolMsg.Context _readPoolSelectionContext;
    private boolean _isBillingNotified;
    private boolean _isOverwriteAllowed;

    private Set<FileAttribute> _additionalAttributes =
            EnumSet.noneOf(FileAttribute.class);

    /**
     * Constructs a new Transfer object.
     *
     * @param pnfs PnfsHandler used for pnfs communication
     * @param namespaceSubject The subject performing the namespace operations
     * @param ioSubject The subject performing the transfer
     * @param path The path of the file to transfer
     */
    public Transfer(PnfsHandler pnfs, Subject namespaceSubject, Subject ioSubject, FsPath path) {
        _pnfs = new PnfsHandler(pnfs, namespaceSubject);
        _subject = ioSubject;
        _path = path;
        _startedAt = System.currentTimeMillis();
        _sessionId = _sessionCounter.next();
        _session = CDC.getSession();
        _checkStagePermission = new CheckStagePermission(null);
    }

    /**
     * Constructs a new Transfer object.
     *
     * @param pnfs PnfsHandler used for pnfs communication
     * @param subject The subject performing the transfer and namespace operations
     * @param path The path of the file to transfer
     */
    public Transfer(PnfsHandler pnfs, Subject subject, FsPath path)
    {
        this(pnfs, subject, subject, path);
    }

    /**
     * Returns a ProtocolInfo suitable for selecting a pool. By
     * default the protocol info set with setProtocolInfo is returned.
     */
    protected ProtocolInfo getProtocolInfoForPoolManager()
    {
        checkNotNull(_protocolInfo);
        return _protocolInfo;
    }

    /**
     * Returns a ProtocolInfo suitable for starting a mover. By
     * default the protocol info set with setProtocolInfo is returned.
     */
    protected ProtocolInfo getProtocolInfoForPool()
    {
        checkNotNull(_protocolInfo);
        return _protocolInfo;
    }

    /**
     * Sets the ProtocolInfo used for the transfer.
     */
    public synchronized void setProtocolInfo(ProtocolInfo info)
    {
        _protocolInfo = info;
    }

    /**
     * Returns the ProtocolInfo used for the transfer. May be null.
     */
    public synchronized ProtocolInfo getProtocolInfo()
    {
        return _protocolInfo;
    }

    /**
     * Orders Transfer objects according to hash value. Makes it
     * possible to add Transfer objects to tree based collections.
     */
    @Override
    public int compareTo(Transfer o)
    {
        return Longs.compare(o.getSessionId(), getSessionId());
    }

    /**
     * Returns the session ID of this transfer. The session ID
     * uniquely identifies this transfer object within this VM
     * instance.
     *
     * The session ID is used as the message ID for both the pool
     * selection message sent to PoolManager and the io file message
     * to the pool. The DoorTransferFinishedMessage from the pool will
     * have the same ID.
     *
     * IoDoorEntry instances provided for monitoring will contain the
     * session ID and the active transfer page of the httpd service
     * reports the session ID in the sequence column.
     *
     * The session ID is not to be confused with session string
     * identifier used for logging. The former identifies a single
     * transfer while the latter identifies a user session and could
     * in theory span multiple transfers.
     */
    public long getSessionId()
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


    public synchronized void
        setCheckStagePermission(CheckStagePermission checkStagePermission)
    {
        _checkStagePermission = checkStagePermission;
    }

    /**
     * Sets the current status of a pool. May be null.
     */
    public synchronized void setStatus(String status)
    {
        if (status != null) {
            _log.debug("Status: {}", status);
        }
        _status = status;
    }

    /**
     * Sets the current status of a pool. May be null.
     */
    public synchronized String getStatus()
    {
        return _status;
    }

    /**
     * When true, existing files will be overwritten on write.
     */
    public synchronized void setOverwriteAllowed(boolean allowed)
    {
        _isOverwriteAllowed = allowed;
    }

    /**
     * Sets the FileAttributes of the file to transfer.
     */
    public synchronized FileAttributes getFileAttributes()
    {
        return _fileAttributes;
    }

    /**
     * Sets the FileAttributes of the file to transfer.
     */
    public synchronized void setFileAttributes(FileAttributes fileAttributes)
    {
        _fileAttributes = fileAttributes;
    }

    /**
     * Returns the PnfsId of the file to be transferred.
     */
    public synchronized PnfsId getPnfsId()
    {
        return _fileAttributes.isDefined(PNFSID) ? _fileAttributes.getPnfsId() : null;
    }

    /**
     * Sets the PnfsId of the file to be transferred.
     */
    public synchronized void setPnfsId(PnfsId pnfsid)
    {
        _fileAttributes.setPnfsId(pnfsid);
    }

    /**
     * Returns the StorageInfo of the file to transfer.
     */
    public synchronized StorageInfo getStorageInfo()
    {
        return _fileAttributes.getStorageInfo();
    }

    /**
     * Sets the StorageInfo of the file to transfer.
     */
    public synchronized void setStorageInfo(StorageInfo info)
    {
        _fileAttributes.setStorageInfo(info);
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
     * Sets the address of the pool to use for this transfer.
     */
    public synchronized void setPoolAddress(CellAddressCore poolAddress)
    {
        _poolAddress = poolAddress;
    }

    /**
     * Returns the address of the pool to use for this transfer.
     */
    public synchronized CellAddressCore getPoolAddress()
    {
        return _poolAddress;
    }

    /**
     * Initialises the session value in the cells diagnostic context
     * (CDC). The session value is attached to the thread.
     *
     * The session key is pushed to the NDC for purposes of logging.
     *
     * The format of the session value is chosen to be compatible with
     * the transaction ID format as found in the
     * InfoMessage.getTransaction method.
     *
     * @throws IllegalStateException when the thread is not already
     *         associcated with a cell through the CDC.
     */
    public static void initSession()
    {
        Object domainName = MDC.get(CDC.MDC_DOMAIN);
        if (domainName == null) {
            throw new IllegalStateException("Missing domain name in MDC");
        }
        Object cellName = MDC.get(CDC.MDC_CELL);
        if (cellName == null) {
            throw new IllegalStateException("Missing cell name in MDC");
        }
        CDC.createSession("door:" + cellName + "@" + domainName + ":");
        NDC.push(CDC.getSession());
    }

    /**
     * The transaction uniquely (with a high probably) identifies this
     * transfer.
     */
    public synchronized String getTransaction()
    {
        if (_session != null) {
            return _session.toString() + "-" + _sessionId;
        } else if (_cellName != null && _domainName != null) {
            return "door:" + _cellName + "@" + _domainName + "-" + _sessionId;
        } else {
            return String.valueOf(_sessionId);
        }
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
        setFileAttributes(msg.getFileAttributes());
        setProtocolInfo(msg.getProtocolInfo());
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
     * The client address is the socket address from which the
     * transfer was initiated.
     */
    public synchronized void setClientAddress(InetSocketAddress address)
    {
        _clientAddress = address;
    }

    public synchronized InetSocketAddress getClientAddress()
    {
        return _clientAddress;
    }

    public boolean waitForMover(long timeout, TimeUnit unit)
            throws CacheException, InterruptedException
    {
        return waitForMover(unit.toMillis(timeout));
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
     * @throws InterruptedException if the thread is interrupted
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
        return new IoDoorEntry(_sessionId,
                               getPnfsId(),
                               _poolName,
                               _status,
                               _startedAt,
                               _clientAddress.getHostString());
    }

    /**
     * Creates a new name space entry for the file to transfer. This
     * will fill in the PnfsId and StorageInfo of the file and mark
     * the transfer as an upload.
     *
     * Will fail if the subject of the transfer doesn't have
     * permission to create the file.
     *
     * If the parent directories don't exist, then they will be
     * created.
     *
     * @throws CacheException if creating the entry failed
     */
    public void createNameSpaceEntryWithParents()
        throws CacheException
    {
        try {
            createNameSpaceEntry();
        } catch (NotInTrashCacheException | FileNotFoundCacheException e) {
            _pnfs.createDirectories(_path.getParent());
            createNameSpaceEntry();
        }
    }

    /**
     * Creates a new name space entry for the file to transfer. This
     * will fill in the PnfsId and StorageInfo of the file and mark
     * the transfer as an upload.
     *
     * Will fail if the subject of the transfer doesn't have
     * permission to create the file.
     *
     * @throws CacheException if creating the entry failed
     */
    public void createNameSpaceEntry()
        throws CacheException
    {
        setStatus("PnfsManager: Creating name space entry");
        try {
            PnfsCreateEntryMessage msg;
            try {
                msg = _pnfs.createPnfsEntry(_path.toString());
            } catch (FileExistsCacheException e) {
                /* REVISIT: This should be moved to PnfsManager with a
                 * flag in the PnfsCreateEntryMessage.
                 */
                if (!_isOverwriteAllowed) {
                    throw e;
                }
                _pnfs.deletePnfsEntry(_path.toString(), EnumSet.of(FileType.REGULAR));
                msg = _pnfs.createPnfsEntry(_path.toString());
            }

            setFileAttributes(msg.getFileAttributes());
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
            Set<FileAttribute> request =
                EnumSet.of(PNFSID, TYPE, STORAGEINFO, SIZE);
            request.addAll(_additionalAttributes);
            request.addAll(PoolMgrSelectReadPoolMsg.getRequiredAttributes());
            Set<AccessMask> mask = EnumSet.of(AccessMask.READ_DATA);
            PnfsId pnfsId = getPnfsId();
            FileAttributes attributes;
            if (pnfsId != null) {
                attributes = _pnfs.getFileAttributes(pnfsId, request, mask);
            } else {
                attributes = _pnfs.getFileAttributes(_path.toString(), request, mask);
            }

            /* We can only read regular files.
             */
            FileType type = attributes.getFileType();
            if (type == FileType.DIR || type == FileType.SPECIAL) {
                throw new NotFileCacheException("Not a regular file");
            }

            setFileAttributes(attributes);
            setWrite(false);
        } finally {
            setStatus(null);
        }
    }

    /**
     * Specify a set of additional attributes as part of this transfer's
     * namespace operation.  Any prior specified extra attributes are removed.
     * In addition, some attributes required by this class and are always
     * fetched.
     */
    protected void setAdditionalAttributes(Set<FileAttribute> attributes)
    {
        _additionalAttributes = Sets.immutableEnumSet(attributes);
    }

    /**
     * Discover the set of additional attributes that will be fetched as part
     * of this transfer's namespace operation.  In addition to the returned
     * set, this class will always fetch certain attributes, which may not be
     * reflected in the returned set.
     */
    protected Set<FileAttribute> getAdditionalAttributes()
    {
        return _additionalAttributes;
    }

    /**
     * Returns the length of the file to be transferred.
     * @throw IllegalStateException if the length isn't known
     */
    public synchronized long getLength()
    {
        return _fileAttributes.getSize();
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
        _fileAttributes.setSize(length);
    }

    /**
     * Sets the size of the preallocation to make.
     *
     * Only affects uploads. If the upload is larger than the
     * preallocation, then the upload may fail.
     */
    public synchronized void setAllocation(long length)
    {
        _allocated = length;
    }

    /**
     * Returns the read pool selection context.
     */
    protected synchronized
        PoolMgrSelectReadPoolMsg.Context getReadPoolSelectionContext()
    {
        return _readPoolSelectionContext;
    }

    /**
     * Sets the previous read pool selection message. The message
     * contains state that is maintained accross repeated pool
     * selections.
     */
    protected synchronized
        void setReadPoolSelectionContext(PoolMgrSelectReadPoolMsg.Context context)
    {
        _readPoolSelectionContext = context;
    }

    /**
     * Selects a pool suitable for the transfer.
     */
    public void selectPool()
        throws CacheException, InterruptedException
    {
        selectPool(_poolManager.getTimeoutInMillis());
    }

    /**
     * Selects a pool suitable for the transfer.
     */
    private void selectPool(long timeout)
        throws CacheException, InterruptedException
    {
        FileAttributes fileAttributes = getFileAttributes();

        setStatus("PoolManager: Selecting pool");
        try {
            ProtocolInfo protocolInfo = getProtocolInfoForPoolManager();
            if (isWrite()) {
                long allocated = _allocated;
                if (allocated == 0) {
                    allocated = fileAttributes.getSize();
                }
                PoolMgrSelectWritePoolMsg request =
                    new PoolMgrSelectWritePoolMsg(fileAttributes,
                                                  protocolInfo,
                                                  allocated);
                request.setId(_sessionId);
                request.setSubject(_subject);
                request.setPnfsPath(_path.toString());

                PoolMgrSelectWritePoolMsg reply =
                    _poolManager.sendAndWait(request, timeout);
                setPool(reply.getPoolName());
                setPoolAddress(reply.getPoolAddress());
                setFileAttributes(reply.getFileAttributes());
            } else if (!_fileAttributes.getStorageInfo().isCreatedOnly()) {
                EnumSet<RequestContainerV5.RequestState> allowedStates =
                    _checkStagePermission.canPerformStaging(_subject, fileAttributes.getStorageInfo())
                    ? RequestContainerV5.allStates
                    : RequestContainerV5.allStatesExceptStage;

                PoolMgrSelectReadPoolMsg request =
                    new PoolMgrSelectReadPoolMsg(fileAttributes,
                                                 protocolInfo,
                                                 getReadPoolSelectionContext(),
                                                 allowedStates);
                request.setId(_sessionId);
                request.setSubject(_subject);
                request.setPnfsPath(_path.toString());

                PoolMgrSelectReadPoolMsg reply =
                    _poolManager.sendAndWait(request, timeout);
                setPool(reply.getPoolName());
                setPoolAddress(reply.getPoolAddress());
                setFileAttributes(reply.getFileAttributes());
                setReadPoolSelectionContext(reply.getContext());
            } else {
                throw new FileIsNewCacheException();
            }
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
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
        startMover(queue, _pool.getTimeoutInMillis());
    }

    /**
     * Creates a mover for the transfer.
     *
     * @param queue The mover queue of the transfer; may be null
     */
    public void startMover(String queue, long timeout)
        throws CacheException, InterruptedException
    {
        FileAttributes fileAttributes = getFileAttributes();
        String pool = getPool();

        if (fileAttributes == null|| pool == null) {
            throw new IllegalStateException("Need PNFS ID, file attributes and pool before a mover can be started");
        }

        setStatus("Pool " + pool + ": Creating mover");
        try {
            ProtocolInfo protocolInfo = getProtocolInfoForPool();
            PoolIoFileMessage message;
            if (isWrite()) {
                message =
                    new PoolAcceptFileMessage(pool, protocolInfo, fileAttributes);
            } else {
                message =
                    new PoolDeliverFileMessage(pool, protocolInfo, fileAttributes);
            }
            message.setIoQueueName(queue);
            message.setInitiator(getTransaction());
            message.setId(_sessionId);
            message.setSubject(_subject);

            /* As always, PoolIoFileMessage has to be sent via the
             * PoolManager (which could be the SpaceManager).
             */
            CellPath poolPath =
                (CellPath) _poolManager.getDestinationPath().clone();
            poolPath.add(getPoolAddress());

            setMoverId(_pool.sendAndWait(poolPath, message, timeout).getMoverId());
        } finally {
            setStatus(null);
        }
    }

    public void killMover(long timeout, TimeUnit unit)
    {
        killMover(unit.toMillis(timeout));
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
        CellAddressCore poolAddress = getPoolAddress();
        setStatus("Mover " + pool + "/" + moverId + ": Killing mover");
        try {
            /* Kill the mover.
             */
            PoolMoverKillMessage message =
                new PoolMoverKillMessage(pool, moverId);
            message.setReplyRequired(false);
            _pool.send(new CellPath(poolAddress), message);

            /* To reduce the risk of orphans when using PNFS, we wait
             * for the transfer confirmation.
             */
            if (millis > 0 && !waitForMover(millis)) {
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
        if (!hasMover()) {
            throw new IllegalStateException("Transfer has no mover");
        }

        return _pool.sendAndWait(new CellPath(getPoolAddress()),
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
     * Sends billing information to the billing cell. Any invocation
     * beyond the first is ignored.
     *
     * @param code The error code of the transfer; zero indicates success
     * @param error The error string of the transfer; may be empty
     */
    public synchronized void notifyBilling(int code, String error)
    {
        if (_isBillingNotified) {
            return;
        }

        try {
            DoorRequestInfoMessage msg =
                new DoorRequestInfoMessage(getCellName() + "@" + getDomainName());
            msg.setSubject(_subject);
            msg.setPath(_path.toString());
            msg.setTransactionDuration(System.currentTimeMillis() - _startedAt);
            msg.setTransaction(getTransaction());
            msg.setClient(_clientAddress.getAddress().getHostAddress());
            msg.setPnfsId(getPnfsId());
            msg.setResult(code, error);
            if (_fileAttributes.isDefined(STORAGEINFO)) {
                msg.setStorageInfo(_fileAttributes.getStorageInfo());
            }
            _billing.send(msg);

            _isBillingNotified = true;
        } catch (NoRouteToCellException e) {
            _log.error("Failed to register transfer in billing: " +
                       e.getMessage());
        }
    }

    /**
     * Select a pool and start a mover. Failed attempts are handled
     * according to the {@link TransferRetryPolicy}. Note, that there
     * will be no retries on uploads.
     *
     * @param queue where mover should be started
     * @param policy to handle error cases
     * @throws CacheException
     * @throws InterruptedException
     */
    public void
        selectPoolAndStartMover(String queue, TransferRetryPolicy policy)
        throws CacheException, InterruptedException
    {
        long deadLine =
                addWithInfinity(System.currentTimeMillis(), policy.getTotalTimeOut());
        long retryCount = policy.getRetryCount();
        long retryPeriod = policy.getRetryPeriod();

        while (true) {
            boolean gotPool = false;
            long start = System.currentTimeMillis();
            CacheException lastFailure;
            try {
                selectPool(subWithInfinity(deadLine, System.currentTimeMillis()));
                gotPool = true;
                startMover(queue,
                        Math.min(subWithInfinity(deadLine, System.currentTimeMillis()),
                                policy.getMoverStartTimeout()));
                return;
            } catch (TimeoutCacheException e) {
                _log.warn(e.getMessage());
                if (gotPool && isWrite()) {
                    /* We cannot know whether the mover was actually
                     * started or not. Retrying is therefore not an
                     * option.
                     */
                    throw e;
                }
                lastFailure = e;
            } catch (CacheException e) {
                switch (e.getRc()) {
                case CacheException.OUT_OF_DATE:
                case CacheException.POOL_DISABLED:
                case CacheException.FILE_NOT_IN_REPOSITORY:
                    _log.info("Retrying pool selection: {}", e.getMessage());
                    if (!isWrite()) {
                        readNameSpaceEntry();
                    }
                    continue;
                case CacheException.FILE_IN_CACHE:
                    throw e;
                default:
                    _log.error(e.toString());
                    break;
                }
                lastFailure = e;
            }

            --retryCount;

            /* We rate limit the retry loop: two consecutive
             * iterations are separated by at least retryPeriod.
             */
            long now = System.currentTimeMillis();
            long timeToSleep =
                Math.max(0, retryPeriod - (now - start));

            if (retryCount == 0 || subWithInfinity(deadLine, now) <= timeToSleep) {
                throw lastFailure;
            }

            setStatus("Sleeping (" + lastFailure.getMessage() + ")");
            try {
                Thread.sleep(timeToSleep);
            } finally {
                setStatus(null);
            }

            if (!isWrite()) {
                readNameSpaceEntry();
            }
        }
    }
}
