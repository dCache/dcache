package org.dcache.util;

import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nullable;
import javax.security.auth.Subject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import diskCacheV111.poolManager.RequestContainerV5;
import diskCacheV111.poolManager.RequestContainerV5.RequestState;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileIsNewCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.Pool;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.TimebasedCounter;

import org.dcache.acl.enums.AccessMask;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.poolmanager.PoolManagerStub;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.*;
import static java.util.Objects.requireNonNull;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.namespace.FileType.REGULAR;
import static org.dcache.util.MathUtils.addWithInfinity;
import static org.dcache.util.MathUtils.subWithInfinity;

/**
 * Facade for transfer related operations. Encapsulates information
 * about and typical operations of a transfer.
 */
public class Transfer implements Comparable<Transfer>
{
    protected static final Logger _log = LoggerFactory.getLogger(Transfer.class);

    private static final TimebasedCounter _sessionCounter =
            new TimebasedCounter();

    private static final BaseEncoding SESSION_ENCODING = BaseEncoding.base64().omitPadding();

    protected final PnfsHandler _pnfs;

    /**
     * Point in time when this transfer is created.
     */
    protected final long _startedAt;
    protected final FsPath _path;
    protected final Subject _subject;
    protected final long _id;
    protected final String _session;

    protected PoolManagerStub _poolManager;
    protected CellStub _poolStub;
    protected CellStub _billing;
    protected CheckStagePermission _checkStagePermission;

    private CellAddressCore _cellAddress;

    private Pool _pool;
    private Integer _moverId;
    private boolean _hasMoverBeenCreated;
    private boolean _hasMoverFinished;
    private String _status;
    private CacheException _error;
    private FileAttributes _fileAttributes = new FileAttributes();
    private ProtocolInfo _protocolInfo;
    private boolean _isWrite;
    private List<InetSocketAddress> _clientAddresses;
    private String _ioQueue;

    private Set<String> _tried;

    private long _allocated;
    private OptionalLong _maximumSize = OptionalLong.empty();

    private PoolMgrSelectReadPoolMsg.Context _readPoolSelectionContext;
    private boolean _isBillingNotified;
    private boolean _isOverwriteAllowed;

    private Set<FileAttribute> _additionalAttributes =
            EnumSet.noneOf(FileAttribute.class);

    private Consumer<DoorRequestInfoMessage> _kafkaSender = (s) -> {};


    private static final ThreadFactory RETRY_THREAD_FACTORY =
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("transfer-retry-timer-%d").build();
    private static final ListeningScheduledExecutorService RETRY_EXECUTOR =
            MoreExecutors.listeningDecorator(
                    new CDCScheduledExecutorServiceDecorator<>(Executors.newScheduledThreadPool(1, RETRY_THREAD_FACTORY))
            );

    /**
     * Which activities poolmanager is allowed to do.
     */
    private EnumSet<RequestState> _allowedRequestStates = EnumSet.allOf(RequestState.class);

    private synchronized EnumSet<RequestState> getAllowedRequestStates()
    {
        return EnumSet.copyOf(_allowedRequestStates);
    }

    /**
     * Constructs a new Transfer object.
     *
     * @param pnfs             PnfsHandler used for pnfs communication
     * @param namespaceSubject The subject performing the namespace operations
     * @param namespaceRestriction Any additional restrictions from this users session
     * @param ioSubject        The subject performing the transfer
     * @param path             The path of the file to transfer
     */
    public Transfer(PnfsHandler pnfs, Subject namespaceSubject,
            Restriction namespaceRestriction, Subject ioSubject, FsPath path)
    {
        _pnfs = new PnfsHandler(pnfs, namespaceSubject, namespaceRestriction);
        _subject = ioSubject;
        _path = path;
        _startedAt = System.currentTimeMillis();
        _id = _sessionCounter.next();
        _session = CDC.getSession();
        _checkStagePermission = new CheckStagePermission(null);
        _checkStagePermission.setAllowAnonymousStaging(true);
    }

    /**
     * Constructs a new Transfer object.
     *
     * @param pnfs    PnfsHandler used for pnfs communication
     * @param subject The subject performing the transfer and namespace operations
     * @param restriction Any additional restrictions from this users session
     * @param path    The path of the file to transfer
     */
    public Transfer(PnfsHandler pnfs, Subject subject, Restriction restriction, FsPath path)
    {
        this(pnfs, subject, restriction, subject, path);
    }

    /**
     * Returns a ProtocolInfo suitable for selecting a pool. By
     * default the protocol info set with setProtocolInfo is returned.
     */
    protected ProtocolInfo getProtocolInfoForPoolManager()
    {
        checkState(_protocolInfo != null);
        return _protocolInfo;
    }

    /**
     * Returns a ProtocolInfo suitable for starting a mover. By
     * default the protocol info set with setProtocolInfo is returned.
     */
    protected ProtocolInfo getProtocolInfoForPool()
    {
        checkState(_protocolInfo != null);
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
    @Nullable
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
        return Long.compare(o.getId(), getId());
    }

    /**
     * Returns the ID of this transfer. The transfer ID
     * uniquely identifies this transfer object within this VM
     * instance.
     * <p>
     * The transfer ID is used as the message ID for both the pool
     * selection message sent to PoolManager and the io file message
     * to the pool. The DoorTransferFinishedMessage from the pool will
     * have the same ID.
     * <p>
     * IoDoorEntry instances provided for monitoring will contain the
     * transfer ID and the active transfer page of the httpd service
     * reports the transfer ID in the sequence column.
     * <p>
     * The transfer ID is not to be confused with session string
     * identifier used for logging. The former identifies a single
     * transfer while the latter identifies a user session and could
     * in theory span multiple transfers.
     */
    public long getId()
    {
        return _id;
    }

    /**
     * Sets CellStub for PoolManager.
     */
    public synchronized void setPoolManagerStub(PoolManagerStub stub)
    {
        _poolManager = requireNonNull(stub, "PoolManager stub can't be null");
    }

    /**
     * Sets CellStub for pools.
     */
    public synchronized void setPoolStub(CellStub stub)
    {
        _poolStub = requireNonNull(stub, "Pool stub can't be null");
    }

    /**
     * Sets CellStub for Billing.
     */
    public synchronized void setBillingStub(CellStub stub)
    {
        _billing = requireNonNull(stub, "Billing stub can't be null");
    }

    public synchronized void setKafkaSender(Consumer<DoorRequestInfoMessage> kafkaSender)
    {
        _kafkaSender = kafkaSender;
    }

    public synchronized void
    setCheckStagePermission(CheckStagePermission checkStagePermission)
    {
        _checkStagePermission = checkStagePermission;
    }

    /**
     * Sets the current status of a transfer. May be null.
     */
    public synchronized void setStatus(String status)
    {
        if (status != null) {
            _log.debug("Status: {}", status);
        }
        _status = status;
    }

    /**
     * Sets the current status of a pool and clear the status once the given future
     * completes.
     */
    public void setStatusUntil(String status, ListenableFuture<?> future)
    {
        setStatus(status);
        future.addListener(() -> setStatus(null), MoreExecutors.directExecutor());
    }

    /**
     * Sets the current status of a pool. May be null.
     */
    @Nullable
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
     * The name space path of the file being transferred.
     */
    public synchronized String getTransferPath()
    {
        return _path.toString();
    }

    /**
     * The billable name space path of the file being transferred.
     */
    public synchronized String getBillingPath()
    {
        if (_fileAttributes.isDefined(STORAGEINFO) && _fileAttributes.getStorageInfo().getKey("path") != null) {
            return _fileAttributes.getStorageInfo().getKey("path");
        } else {
            return _path.toString();
        }
    }

    /**
     * Returns the PnfsId of the file to be transferred.
     */
    @Nullable
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
     * Sets whether this is an upload.
     */
    protected synchronized void setWrite(boolean isWrite)
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
        _hasMoverBeenCreated = (_moverId != null);
    }

    /**
     * Returns the ID of the mover of this transfer.
     */
    @Nullable
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
        return _hasMoverBeenCreated && !_hasMoverFinished;
    }

    /**
     * Sets the pool to use for this transfer.
     */
    public synchronized void setPool(Pool pool)
    {
        _pool = pool;
    }

    /**
     * Returns the pool to use for this transfer.
     */
    @Nullable
    public synchronized Pool getPool()
    {
        return _pool;
    }

    /**
     * Restrict pool selection to only online files, e.g. no stage or p2p
     * are allowed. Has no effect on upload.
     *
     * This option has no effect on stage protection. This means, that stage protection
     * still can reject staging, even if {@code onlineOnly} is {@code false}.
     *
     * @param onlineOnly True, is transfer should use on files directly accessible,
     */
    public synchronized void setOnlineFilesOnly(boolean onlineOnly) {
        if (onlineOnly) {
            _allowedRequestStates = RequestContainerV5.ONLINE_FILES_ONLY;
        } else {
            _allowedRequestStates = RequestContainerV5.allStates;
        }
    }

    public synchronized boolean getOnlineFilesOnly() {
        return _allowedRequestStates.equals(RequestContainerV5.ONLINE_FILES_ONLY);
    }

    public synchronized void setAllowStaging(boolean isAllowed)
    {
        if (isAllowed) {
            _allowedRequestStates.add(RequestState.ST_STAGE);
        } else {
            _allowedRequestStates.remove(RequestState.ST_STAGE);
        }
    }

    public synchronized void setAllowPool2Pool(boolean isAllowed)
    {
        if (isAllowed) {
            _allowedRequestStates.add(RequestState.ST_POOL_2_POOL);
        } else {
            _allowedRequestStates.remove(RequestState.ST_POOL_2_POOL);
        }
    }

    public synchronized void setTriedHosts(Set<String> tried)
    {
        _tried = tried;
    }

    /**
     * Initialises the session value in the cells diagnostic context
     * (CDC). The session value is attached to the thread.
     * <p>
     * The session key is pushed to the NDC for purposes of logging.
     * <p>
     * The format of the session value is chosen to be compatible with
     * the transaction ID format as found in the
     * InfoMessage.getTransaction method.
     *
     * @param isCellNameSiteUnique       True if the cell name is unique throughout this
     *                                   dCache site, that is, it is well known or derived
     *                                   from a well known name.
     * @param isCellNameTemporallyUnique True if the cell name is temporally unique,
     *                                   that is, two invocations of initSession will
     *                                   never have the same cell name.
     * @throws IllegalStateException when the thread is not already
     *                               associated with a cell through the CDC.
     */
    public static void initSession(boolean isCellNameSiteUnique, boolean isCellNameTemporallyUnique)
    {
        Object domainName = MDC.get(CDC.MDC_DOMAIN);
        Object cellName = MDC.get(CDC.MDC_CELL);
        checkState(domainName != null, "Missing domain name in MDC");
        checkState(cellName != null, "Missing cell name in MDC");

        StringBuilder session = new StringBuilder();
        session.append("door:").append(cellName);
        if (!isCellNameSiteUnique) {
            session.append('@').append(domainName);
        }
        if (!isCellNameTemporallyUnique) {
            session.append(':').append(SESSION_ENCODING.encode(Longs.toByteArray(_sessionCounter.next())));
        }
        String s = session.toString();
        CDC.setSession(s);
        NDC.push(s);
    }

    /**
     * The transaction uniquely (with a high probably) identifies this
     * transfer.
     */
    public synchronized String getTransaction()
    {
        if (_session != null) {
            return _session.toString() + ":" + _id;
        } else if (_cellAddress != null) {
            return "door:" + _cellAddress + ":" + _id;
        } else {
            return String.valueOf(_id);
        }
    }

    /**
     * Restore cells diagnostic context for the transfer's session.
     */
    public void restoreSession() {
        CDC.setSession(_session);
        NDC.push(_session);
    }

    /**
     * Signals that the mover of this transfer finished.
     */
    public synchronized void finished(CacheException error)
    {
        _hasMoverFinished = true;
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

    public synchronized void setCellAddress(CellAddressCore address)
    {
        _cellAddress = address;
    }

    /**
     * Returns the cell name of the door handling the transfer.
     */
    public synchronized String getCellName()
    {
        checkState(_cellAddress != null);
        return _cellAddress.getCellName();
    }

    /**
     * Returns the domain name of the door handling the transfer.
     */
    public synchronized String getDomainName()
    {
        checkState(_cellAddress != null);
        return _cellAddress.getCellDomainName();
    }

    /**
     * The client address is the socket address from which the
     * transfer was initiated.
     */
    public synchronized void setClientAddress(InetSocketAddress address)
    {
        _clientAddresses = Collections.singletonList(address);
    }

    /**
     * The client address(es) that initiated the request.  If the
     * protocol does not support reporting relayed requests then this is
     * a single entry.  If the protocol allows reporting of client
     * addresses then the list-order represents the clients that initiated
     * this request, starting with the client on the TCP connection.
     */
    public synchronized void setClientAddresses(List<InetSocketAddress> addresses)
    {
        checkArgument(!addresses.isEmpty(), "empty address list is not allowed");
        _clientAddresses = addresses;
    }

    /**
     * Report the address of the client that connected to dCache when
     * initiated this transfer.
     */
    @Nullable
    public synchronized InetSocketAddress getClientAddress()
    {
        return _clientAddresses == null ? null : _clientAddresses.get(0);
    }

    /**
     * Report all relays and the client that initiated this transfer.
     * The last item is the client that initiated the transfer; any addresses
     * earlier in the list represent relay clients.  The first item is the
     * client that directly connected to dCache.
     */
    @Nullable
    public synchronized List<InetSocketAddress> getClientAddresses()
    {
        return _clientAddresses;
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
     * @throws CacheException       if the mover failed
     * @throws InterruptedException if the thread is interrupted
     */
    public synchronized boolean waitForMover(long millis)
            throws CacheException, InterruptedException
    {
        long deadline = addWithInfinity(System.currentTimeMillis(), millis);
        while (!_hasMoverFinished && System.currentTimeMillis() < deadline) {
            wait(subWithInfinity(deadline, System.currentTimeMillis()));
        }

        if (_error != null) {
            throw _error;
        }

        return _hasMoverFinished;
    }

    /**
     * Returns an IoDoorEntry describing the transfer. This is
     * used by the "Active Transfer" view of the HTTP monitor.
     */
    public synchronized IoDoorEntry getIoDoorEntry()
    {
        return new IoDoorEntry(_id,
                               getPnfsId(),
                               getTransferPath(),
                               _subject,
                               _pool == null? "<unknown>" : _pool.getName(),
                               _status,
                               _startedAt,
                               _clientAddresses.get(0)
                                               .getAddress()
                                               .getHostAddress());
    }

    /**
     * Creates a new name space entry for the file to transfer. This
     * will fill in the PnfsId and StorageInfo of the file and mark
     * the transfer as an upload.
     * <p>
     * Will fail if the subject of the transfer doesn't have
     * permission to create the file.
     * <p>
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
            _pnfs.createDirectories(_path.parent());
            createNameSpaceEntry();
        }
    }

    /**
     * Creates a new name space entry for the file to transfer. This
     * will fill in the PnfsId and StorageInfo of the file and mark
     * the transfer as an upload.
     * <p>
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
            FileAttributes desiredAttributes = fileAttributesForNameSpace();
            PnfsCreateEntryMessage msg;
            try {
                msg = _pnfs.createPnfsEntry(_path.toString(),
                        desiredAttributes);
            } catch (FileExistsCacheException e) {
                /* REVISIT: This should be moved to PnfsManager with a
                 * flag in the PnfsCreateEntryMessage.
                 */
                if (!_isOverwriteAllowed) {
                    throw e;
                }
                _pnfs.deletePnfsEntry(_path.toString(), EnumSet.of(REGULAR));
                msg = _pnfs.createPnfsEntry(_path.toString(),
                        desiredAttributes);
            }

            FileAttributes attrs = msg.getFileAttributes();
            attrs.setChecksums(new HashSet<>());
            setFileAttributes(attrs);
            setWrite(true);
        } finally {
            setStatus(null);
        }
    }

    protected FileAttributes fileAttributesForNameSpace()
    {
        return FileAttributes.ofFileType(REGULAR);
    }

    /**
     * Reads the name space entry of the file to transfer. This will fill in the PnfsId
     * and FileAttributes of the file.
     * <p>
     * Changes the I/O mode from write to read if the file is not new.
     *
     * @throws PermissionDeniedCacheException if permission to read/write the file is denied
     * @throws NotFileCacheException          if the file is not a regular file
     * @throws FileIsNewCacheException        when attempting to download an incomplete file
     * @throws CacheException                 if reading the entry failed
     * @throws InterruptedException           if the thread is interrupted
     * @param allowWrite whether the file may be opened for writing
     */
    public final void readNameSpaceEntry(boolean allowWrite)
            throws CacheException, InterruptedException
    {
        try {
            getCancellable(readNameSpaceEntryAsync(allowWrite));
        } catch (NoRouteToCellException e) {
            throw new TimeoutCacheException(e.getMessage(), e);
        }
    }

    /**
     * Reads the name space entry of the file to transfer. This will fill in the PnfsId
     * and FileAttributes of the file.
     * <p>
     * Changes the I/O mode from write to read if the file is not new.
     *
     * @param allowWrite whether the file may be opened for writing
     */
    public ListenableFuture<Void> readNameSpaceEntryAsync(boolean allowWrite)
    {
        return readNameSpaceEntryAsync(allowWrite, _pnfs.getPnfsTimeout());
    }

    private ListenableFuture<Void> readNameSpaceEntryAsync(boolean allowWrite, long timeout)
    {
        Set<FileAttribute> attr = EnumSet.of(PNFSID, TYPE, STORAGEINFO, SIZE);
        attr.addAll(_additionalAttributes);
        attr.addAll(PoolMgrSelectReadPoolMsg.getRequiredAttributes());
        Set<AccessMask> mask;
        if (allowWrite) {
            mask = EnumSet.of(AccessMask.READ_DATA, AccessMask.WRITE_DATA);
        } else {
            mask = EnumSet.of(AccessMask.READ_DATA);
        }
        PnfsId pnfsId = getPnfsId();
        PnfsGetFileAttributes request;
        if (pnfsId != null) {
            request = new PnfsGetFileAttributes(pnfsId, attr);
            if (_path != null) {
                // Needed for restriction check.
                request.setPnfsPath(_path.toString());
            }
        } else {
            request = new PnfsGetFileAttributes(_path.toString(), attr);
        }
        request.setAccessMask(mask);
        request.setUpdateAtime(true);
        ListenableFuture<PnfsGetFileAttributes> reply = _pnfs.requestAsync(request, timeout);

        setStatusUntil("PnfsManager: Fetching storage info", reply);

        return CellStub.transformAsync(reply,
                                       msg -> {
                                           FileAttributes attributes = msg.getFileAttributes();
                                           /* We can only transfer regular files.
                                            */
                                           FileType type = attributes.getFileType();
                                           if (type == FileType.DIR || type == FileType.SPECIAL) {
                                               throw new NotFileCacheException("Not a regular file");
                                           }

                                           /* I/O mode must match completeness of the file.
                                            */
                                           if (!attributes.getStorageInfo().isCreatedOnly()) {
                                               setWrite(false);
                                           } else if (allowWrite) {
                                               setWrite(true);
                                           } else {
                                               throw new FileIsNewCacheException();
                                           }

                                           setFileAttributes(attributes);
                                           return immediateFuture(null);
                                       });
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
     *
     * @throws IllegalStateException if the length isn't known
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
        checkState(isWrite(), "Can only set length for uploads");
        // The following check is to catch bugs: the door should have already handled this situation
        checkArgument(!_maximumSize.isPresent() || _maximumSize.getAsLong() >= length,
                "file length is larger than the already specified maximum length");
        _fileAttributes.setSize(length);
    }

    /**
     * Sets checksum of the file to be uploaded. Can be called multiple times
     * with different checksums types. Only valid for uploads.
     *
     * @param checksum of the file
     * @throws CacheException if reading the entry failed
     */
    public void setChecksum(Checksum checksum) throws CacheException
    {
        if (!isWrite()) {
            throw new IllegalStateException("Can only set checksum for uploads");
        }

        try {
            setStatus("PnfsManager: Setting checksum");
            FileAttributes attr = FileAttributes.ofChecksum(checksum);
            _pnfs.setFileAttributes(_path, attr);
            synchronized (this) {
                _fileAttributes.getChecksums().add(checksum);
            }
        } finally {
            setStatus(null);
        }
    }

    /**
     * Sets the size of the preallocation to make.
     * <p>
     * Only affects uploads. If the upload is larger than the
     * preallocation, then the upload may fail.
     */
    public synchronized void setAllocation(long length)
    {
        _allocated = length;
    }

    /**
     * Set the maximum allowed size for an upload.  If the size of the file
     * is known in advance then {@link #setLength(long)} should be used instead.
     * @param length the maximum number of bytes for this transfer.
     */
    public synchronized void setMaximumLength(long length)
    {
        checkState(isWrite(), "Can only set maximum length for uploads");
        checkArgument(length > 0, "maximum length must be a positive number");
        // The following check is to catch bugs: the door should have already handled this situation
        checkArgument(!_fileAttributes.isDefined(SIZE) || _fileAttributes.getSize() <= length,
                "maximum file length is smaller than already specified file length");
        _maximumSize = OptionalLong.of(length);
    }

    /**
     * Sets the mover queue to use.
     */
    public synchronized void setIoQueue(String queue)
    {
        _ioQueue = queue;
    }

    /**
     * Returns the mover queue to be used.
     */
    @Nullable
    public synchronized String getIoQueue()
    {
        return _ioQueue;
    }

    /**
     * Returns the read pool selection context.
     */
    protected synchronized PoolMgrSelectReadPoolMsg.Context getReadPoolSelectionContext()
    {
        return _readPoolSelectionContext;
    }

    /**
     * Sets the previous read pool selection message. The message
     * contains state that is maintained across repeated pool
     * selections.
     */
    protected synchronized void setReadPoolSelectionContext(PoolMgrSelectReadPoolMsg.Context context)
    {
        _readPoolSelectionContext = context;
    }

    /**
     * Selects a pool suitable for the transfer.
     */
    public ListenableFuture<Void> selectPoolAsync(long timeout)
    {
        FileAttributes fileAttributes = getFileAttributes();

        ProtocolInfo protocolInfo = getProtocolInfoForPoolManager();
        ListenableFuture<? extends PoolMgrSelectPoolMsg> reply;
        if (isWrite()) {
            long allocated = _allocated;
            if (allocated == 0 && fileAttributes.isDefined(SIZE)) {
                allocated = fileAttributes.getSize();
            }
            PoolMgrSelectWritePoolMsg request =
                    new PoolMgrSelectWritePoolMsg(fileAttributes,
                                                  protocolInfo,
                                                  allocated);
            request.setId(_id);
            request.setSubject(_subject);
            request.setBillingPath(getBillingPath());
            request.setTransferPath(getTransferPath());
            request.setIoQueueName(getIoQueue());
            request.setExcludedHosts(_tried);

            reply = _poolManager.sendAsync(request, timeout);
        } else {
            EnumSet<RequestState> allowedStates = getAllowedRequestStates();
            try {
                if (allowedStates.contains(RequestState.ST_STAGE) &&
                        !_checkStagePermission.canPerformStaging(_subject,
                        fileAttributes, protocolInfo)) {
                    allowedStates.remove(RequestState.ST_STAGE);
                }
            } catch (IOException e) {
                return immediateFailedFuture(
                        new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage()));
            }

            PoolMgrSelectReadPoolMsg request =
                    new PoolMgrSelectReadPoolMsg(fileAttributes,
                                                 protocolInfo,
                                                 getReadPoolSelectionContext(),
                                                 allowedStates);
            request.setId(_id);
            request.setSubject(_subject);
            request.setBillingPath(getBillingPath());
            request.setTransferPath(getTransferPath());
            request.setIoQueueName(getIoQueue());
            request.setExcludedHosts(_tried);

            reply = Futures.transform(_poolManager.sendAsync(request, timeout),
                                      (PoolMgrSelectReadPoolMsg msg) -> {
                                          setReadPoolSelectionContext(msg.getContext());
                                          return msg;
                                      });
        }

        setStatusUntil("PoolManager: Selecting pool", reply);
        return CellStub.transform(reply,
                                  (PoolMgrSelectPoolMsg msg) -> {
                                      setPool(msg.getPool());
                                      setFileAttributes(msg.getFileAttributes());
                                      return null;
                                  });
    }

    /**
     * Creates a mover for the transfer.
     */
    public ListenableFuture<Void> startMoverAsync(long timeout)
    {
        FileAttributes fileAttributes = getFileAttributes();
        Pool pool = getPool();

        if (fileAttributes == null || pool == null) {
            throw new IllegalStateException("Need PNFS ID, file attributes and pool before a mover can be started");
        }

        ProtocolInfo protocolInfo = getProtocolInfoForPool();
        PoolIoFileMessage message;
        if (isWrite()) {
            long allocated = _allocated;
            if (allocated == 0 && fileAttributes.isDefined(SIZE)) {
                allocated = fileAttributes.getSize();
            }
            message =
                    new PoolAcceptFileMessage(pool.getName(), protocolInfo, fileAttributes, pool.getAssumption(), _maximumSize, allocated);
        } else {
            message =
                    new PoolDeliverFileMessage(pool.getName(), protocolInfo, fileAttributes, pool.getAssumption());
        }
        message.setBillingPath(getBillingPath());
        message.setTransferPath(getTransferPath());
        message.setIoQueueName(getIoQueue());
        message.setInitiator(getTransaction());
        message.setId(_id);
        message.setSubject(_subject);

        ListenableFuture<PoolIoFileMessage> reply = _poolManager.startAsync(pool.getAddress(), message, timeout);

        reply = catchingAsync(reply, NoRouteToCellException.class, x -> {
            // invalidate pool selection to let the door to start over
            setPool(null);
            return immediateFailedFuture(x);
        });

        setStatusUntil("Pool " + pool + ": Creating mover", reply);
        return CellStub.transformAsync(reply, msg -> {
            setMoverId(msg.getMoverId());
            return immediateFuture(null);
        });
    }


    /**
     * Kills the mover of the transfer.  Blocks until the mover has died or
     * until a timeout is reached.  An error is logged if the mover failed to
     * die or if the timeout was reached.
     * @param timeout the duration of the timeout
     * @param unit the time units of the duration
     * @param explanation short information why the transfer is killed
     */
    public final void killMover(long timeout, TimeUnit unit, String explanation)
    {
        killMover(unit.toMillis(timeout), explanation);
    }


    /**
     * Kills the mover of the transfer. Blocks until the mover has
     * died or until a timeout is reached. An error is logged if
     * the mover failed to die or if the timeout was reached.
     *
     * @param millis Timeout in milliseconds
     * @param explanation short information why the transfer is killed
     */
    public void killMover(long millis, String explanation)
    {
        if (!hasMover()) {
            return;
        }

        Integer moverId = getMoverId();
        Pool pool = getPool();
        setStatus("Mover " + pool + "/" + moverId + ": Killing mover");
        try {
            /* Kill the mover.
             */
            PoolMoverKillMessage message =
                    new PoolMoverKillMessage(pool.getName(), moverId, explanation);
            message.setReplyRequired(false);
            _poolStub.notify(new CellPath(pool.getAddress()), message);

            /* To reduce the risk of orphans when using PNFS, we wait
             * for the transfer confirmation.
             */
            if (millis > 0 && !waitForMover(millis)) {
                _log.error("Failed to kill mover {}/{}: Timeout", pool, moverId);
            }
        } catch (CacheException e) {
            // Not surprising that the pool reported a failure
            // when we killed the mover.
            _log.debug("Killed mover and pool reported: {}",
                       e.getMessage());
        } catch (InterruptedException e) {
            _log.warn("Failed to kill mover {}/{}: {}", pool, moverId, e.getMessage());
            Thread.currentThread().interrupt();
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

        try {
            return _poolStub.sendAndWait(new CellPath(getPool().getAddress()),
                                     "mover ls -binary " + getMoverId(),
                                     IoJobInfo.class);
        } catch (NoRouteToCellException e) {
            throw new TimeoutCacheException(e.getMessage(), e);
        }
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
                /*  A user may be authorised to upload data but not delete data.
                 *  During error recovery, a door may attempt to delete the
                 *  bad file, which (in general) such users are not authorised
                 *  to do.  Therefore, this delete operation is done with no
                 *  restriction.
                 */
                new PnfsHandler(_pnfs, Restrictions.none())
                        .deletePnfsEntry(pnfsId, _path.toString());
            } catch (FileNotFoundCacheException e) {
                _log.debug("Failed to delete file after failed upload: {} ({}): {}", _path, pnfsId, e.getMessage());
            } catch (CacheException e) {
                _log.error("Failed to delete file after failed upload: {} ({}): {}", _path, pnfsId, e.getMessage());
            } finally {
                setStatus(null);
            }
        }
    }

    /**
     * Sends billing information to the billing cell. Any invocation
     * beyond the first is ignored.
     *
     * @param code  The error code of the transfer; zero indicates success
     * @param error The error string of the transfer; may be empty
     */
    public synchronized void notifyBilling(int code, String error)
    {
        if (_isBillingNotified) {
            return;
        }

        DoorRequestInfoMessage msg = new DoorRequestInfoMessage(_cellAddress);
        msg.setSubject(_subject);
        msg.setBillingPath(getBillingPath());
        msg.setTransferPath(getTransferPath());
        msg.setTransactionDuration(System.currentTimeMillis() - _startedAt);
        msg.setTransaction(getTransaction());
        String chain = _clientAddresses.stream().
                map(InetSocketAddress::getAddress).
                map(InetAddress::getHostAddress).
                collect(Collectors.joining(","));
        msg.setClientChain(chain);
        msg.setClient(_clientAddresses.get(0).getAddress().getHostAddress());
        msg.setPnfsId(getPnfsId());
        if (_fileAttributes.isDefined(SIZE)) {
            msg.setFileSize(_fileAttributes.getSize());
        }
        msg.setResult(code, error);
        if (_fileAttributes.isDefined(STORAGEINFO)) {
            msg.setStorageInfo(_fileAttributes.getStorageInfo());
        }
        _billing.notify(msg);

        _isBillingNotified = true;
        _kafkaSender.accept(msg);
    }


    private static long getTimeoutFor(long deadline)
    {
        return subWithInfinity(deadline, System.currentTimeMillis());
    }

    private static long getTimeoutFor(CellStub stub, long deadline)
    {
        return Math.min(getTimeoutFor(deadline), stub.getTimeoutInMillis());
    }

    private static long getTimeoutFor(PnfsHandler pnfs, long deadline)
    {
        return Math.min(getTimeoutFor(deadline), pnfs.getPnfsTimeout());
    }

    /**
     * Select a pool and start a mover. Failed attempts are handled
     * according to the {@link TransferRetryPolicy}. Note, that there
     * will be no retries on uploads.
     *
     * @param policy to handle error cases
     * @throws CacheException
     * @throws InterruptedException
     */
    public void selectPoolAndStartMover(TransferRetryPolicy policy)
            throws CacheException, InterruptedException
    {
        try {
            getCancellable(selectPoolAndStartMoverAsync(policy));
        } catch (NoRouteToCellException e) {
            throw new TimeoutCacheException(e.getMessage(), e);
        }
    }

    public ListenableFuture<Void> selectPoolAndStartMoverAsync(TransferRetryPolicy policy)
    {

        long deadLine = addWithInfinity(System.currentTimeMillis(), policy.getTotalTimeOut());

        AsyncFunction<Void, Void> selectPool =
                ignored -> selectPoolAsync(getTimeoutFor(deadLine));
        AsyncFunction<Void, Void> startMover =
                ignored -> startMoverAsync(getTimeoutFor(deadLine));
        AsyncFunction<Void, Void> readNameSpaceEntry =
                ignored -> readNameSpaceEntryAsync(false, getTimeoutFor(_pnfs, deadLine));

        AsyncFunction<CacheException,Void> retry =
                new AsyncFunction<CacheException, Void>()
                {
                    private int count;

                    private long start = System.currentTimeMillis();

                    @Override
                    public ListenableFuture<Void> apply(CacheException t) throws Exception
                    {
                        count++;

                        switch (t.getRc()) {
                        case CacheException.TIMEOUT:
                            if (getPool() != null && isWrite()) {
                                return immediateFailedFuture(t);
                            }
                            break;
                        case CacheException.OUT_OF_DATE:
                        case CacheException.POOL_DISABLED:
                        case CacheException.FILE_NOT_IN_REPOSITORY:
                            _log.info("Retrying pool selection: {}", t.getMessage());
                            return retryWhen(immediateFuture(null));
                        case CacheException.FILE_IN_CACHE:
                        case CacheException.INVALID_ARGS:
                        case CacheException.FILE_NOT_FOUND:
                            return immediateFailedFuture(t);
                        case CacheException.NO_POOL_CONFIGURED:
                            _log.error(t.getMessage());
                            return immediateFailedFuture(t);
                        case CacheException.NO_POOL_ONLINE:
                        case CacheException.POOL_UNAVAILABLE:
                            _log.warn(t.getMessage());
                            break;
                        case CacheException.PERMISSION_DENIED:
                            _log.info("request rejected due to permission settings: {}", t.getMessage());
                            return immediateFailedFuture(t);
                        default:
                            _log.error(t.getMessage());
                            break;
                        }

                        if (count >= policy.getRetryCount()) {
                            return immediateFailedFuture(t);
                        }

                        /* We rate limit the retry loop: two consecutive
                         * iterations are separated by at least retryPeriod.
                         */
                        long now = System.currentTimeMillis();
                        long timeToSleep = Math.max(0, policy.getRetryPeriod() - (now - start));

                        if (subWithInfinity(deadLine, now) <= timeToSleep) {
                            return immediateFailedFuture(t);
                        }

                        ListenableScheduledFuture<Void> doneSleeping =
                                RETRY_EXECUTOR.schedule(() -> null, timeToSleep, TimeUnit.MILLISECONDS);

                        setStatusUntil("Sleeping (" + t.getMessage() + ")", doneSleeping);
                        return retryWhen(doneSleeping);
                    }

                    public ListenableFuture<Void> retryWhen(ListenableFuture<Void> future)
                    {
                        if (!isWrite()) {
                            future = transformAsync(future, readNameSpaceEntry);
                        }
                        start = System.currentTimeMillis();
                        return catchingAsync(transformAsync(transformAsync(future, selectPool), startMover), CacheException.class, this);
                    }
                };

        return catchingAsync(transformAsync(
                selectPoolAsync(getTimeoutFor(deadLine)), startMover), CacheException.class, retry);
    }

    /**
     * Returns the result of {@link Future#get()} as if by {@link CellStub#get}, but
     * cancels {@code future} if the calling thread is interrupted.
     */
    protected static <T> T getCancellable(ListenableFuture<T> future) throws CacheException, InterruptedException, NoRouteToCellException
    {
        try {
            return CellStub.get(future);
        } catch (InterruptedException e) {
            future.cancel(true);
            throw e;
        }
    }

    /**
     * Get timestamp when this transfer was created.
     * @return timestamp, when transfer was created.
     */
    public long getCreationTime()
    {
        return _startedAt;
    }
}
