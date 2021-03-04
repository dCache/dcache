/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 - 2020 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.xrootd.door;

import com.google.common.base.Splitter;
import com.google.common.collect.Range;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.PnfsCancelUpload;
import diskCacheV111.vehicles.PnfsCommitUpload;
import diskCacheV111.vehicles.PnfsCreateUploadPath;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.login.LoginManagerChildrenInfo;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.security.auth.Subject;
import org.dcache.acl.enums.AccessType;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Activity;
import org.dcache.auth.attributes.Restriction;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import org.dcache.namespace.ACLPermissionHandler;
import org.dcache.namespace.ChainedPermissionHandler;
import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.PermissionHandler;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.poolmanager.PoolManagerStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.util.Args;
import org.dcache.util.Checksum;
import org.dcache.util.FireAndForgetTask;
import org.dcache.util.PingMoversTask;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicies;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsListDirectoryMessage;
import org.dcache.vehicles.XrootdDoorAdressInfoMessage;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.tpc.XrootdTpcInfo;
import org.dcache.xrootd.tpc.XrootdTpcInfoCleanerTask;
import org.dcache.xrootd.util.FileStatus;
import org.dcache.xrootd.util.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.kafka.core.KafkaTemplate;

import static diskCacheV111.util.MissingResourceCacheException.checkResourceNotMissing;
import static java.util.Objects.requireNonNull;
import static org.dcache.namespace.FileAttribute.CHECKSUM;
import static org.dcache.namespace.FileAttribute.MODIFICATION_TIME;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.SIZE;
import static org.dcache.namespace.FileAttribute.STORAGEINFO;
import static org.dcache.namespace.FileAttribute.TYPE;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_isDir;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_offline;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_other;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_poscpend;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_readable;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_writable;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_xset;

/**
 * Shared cell component used to interface with the rest of
 * dCache.
 *
 * Current implementation is more or less a copy of the old xrootd
 * code. Should be replaced by the equivalent component developed by
 * Tatjana and Tigran.
 */
public class XrootdDoor
    extends AbstractCellComponent
    implements CellMessageReceiver,
               CellCommandListener, CellInfoProvider
{
    public static final String XROOTD_PROTOCOL_STRING = "Xrootd";
    public static final String XROOTD_PROTOCOL_VERSION =
        String.format("%d.%d",
                      XrootdProtocol.PROTOCOL_VERSION_MAJOR,
                      XrootdProtocol.PROTOCOL_VERSION_MINOR);

    private static final String TPC_PLACEMENT = "tpc-placement";

    private static final Logger _log =
        LoggerFactory.getLogger(XrootdDoor.class);

    private static final AtomicInteger _handleCounter = new AtomicInteger();
    private static final AtomicInteger _tpcPlaceholder = new AtomicInteger();

    private static final long PING_DELAY = 300000;
    private static final long TPC_EVICT_DELAY = TimeUnit.MINUTES.toMillis(2);

    private static final TransferRetryPolicy RETRY_POLICY =
        TransferRetryPolicies.tryOncePolicy(Long.MAX_VALUE);

    private List<FsPath> _readPaths = Collections.singletonList(FsPath.ROOT);
    private List<FsPath> _writePaths = Collections.singletonList(FsPath.ROOT);

    private CellStub _pnfsStub;
    private CellStub _poolStub;
    private PoolManagerStub _poolManagerStub;
    private CellStub _billingStub;

    private PoolMonitor _poolMonitor;

    private final PermissionHandler _pdp = new ChainedPermissionHandler (
                                new ACLPermissionHandler(),
                                new PosixPermissionHandler());

    private int _moverTimeout = 180000;
    private TimeUnit _moverTimeoutUnit = TimeUnit.MILLISECONDS;

    private PnfsHandler _pnfs;

    private String _ioQueue;

    private Map<UUID, DirlistRequestHandler> _requestHandlers =
        new ConcurrentHashMap<>();

    private ScheduledExecutorService _scheduledExecutor;

    private Consumer<DoorRequestInfoMessage> _kafkaSender = (s) -> {};


    /**
     * Current xrootd transfers. The key is the xrootd file handle.
     */
    private final Map<Integer,XrootdTransfer> _transfers =
        new ConcurrentHashMap<>();

    private boolean triedHostsEnabled;

    @Autowired(required = false)
    private void setKafkaTemplate(
                    @Qualifier("billing-template") KafkaTemplate kafkaTemplate )
    {
        _kafkaSender = kafkaTemplate::sendDefault;
    }

    private final Map<String, XrootdTpcInfo> _tpcInfo     = new HashMap<>();
    private final Map<Integer, String>       _tpcFdIndex  = new HashMap<>();

    @Required
    public void setPnfsStub(CellStub pnfsStub) {
        _pnfsStub = pnfsStub;
    }

    @Required
    public void setPoolStub(CellStub stub)
    {
        _poolStub = stub;
    }

    @Required
    public void setPoolManagerStub(PoolManagerStub stub)
    {
        _poolManagerStub = stub;
    }

    @Required
    public void setBillingStub(CellStub stub)
    {
        _billingStub = stub;
    }

    @Required
    public void setPoolMonitor(PoolMonitor poolMonitor)
    {
        _poolMonitor = poolMonitor;
    }

    /**
     * Converts a colon separated list of paths to a List of FsPath.
     */
    private List<FsPath> toFsPaths(String s)
    {
        List<FsPath> list = new ArrayList<>();
        for (String path: Splitter.on(":").omitEmptyStrings().split(s)) {
            list.add(FsPath.create(path));
        }
        return list;
    }

    /**
     * The list of paths which are authorized for xrootd write access.
     */
    @Required
    public void setWritePaths(String s)
    {
        _writePaths = toFsPaths(s);
    }

    /**
     * Returns the list of write paths.
     *
     * Notice that the getter uses a different property name than the
     * setter. This is because the getter returns a different type
     * than set by the setter, and hence we must not use the same
     * property name (otherwise Spring complains).
     */
    @Required
    public List<FsPath> getWritePathsList()
    {
        return _writePaths;
    }

    /**
     * The list of paths which are authorized for xrootd write access.
     */
    @Required
    public void setReadPaths(String s)
    {
        _readPaths = toFsPaths(s);
    }

    /**
     * Returns the list of read paths.
     *
     * Notice that the getter uses a different property name than the
     * setter. This is because the getter returns a different type
     * than set by the setter, and hence we must not use the same
     * property name (otherwise Spring complains).
     */
    public List<FsPath> getReadPathsList()
    {
        return _readPaths;
    }

    @Required
    public void setPnfsHandler(PnfsHandler pnfs)
    {
        _pnfs = pnfs;
    }

    /**
     * The actual mover queue on the pool onto which this request gets
     * scheduled.
     */
    @Required
    public void setIoQueue(String ioQueue)
    {
        _ioQueue = ioQueue;
    }

    public String getIoQueue()
    {
        return _ioQueue;
    }

    /**
     * Returns the mover timeout in milliseconds.
     */
    public int getMoverTimeout()
    {
        return _moverTimeout;
    }

    /**
     * The mover timeout is the time we wait for the mover to start
     * after having been enqueued.
     *
     * @param timeout The mover timeout in milliseconds
     */
    @Required
    public void setMoverTimeout(int timeout)
    {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be positive");
        }
        _moverTimeout = timeout;
    }

    public void setMoverTimeoutUnit(TimeUnit unit)
    {
        _moverTimeoutUnit = requireNonNull(unit);
    }

    @Required
    public void setTriedHostsEnabled(boolean triedHostsEnabled)
    {
        this.triedHostsEnabled = triedHostsEnabled;
    }

    public TimeUnit getMoverTimeoutUnit()
    {
        return _moverTimeoutUnit;
    }

    /**
     * Sets the ScheduledExecutorService used for periodic tasks.
     */
    @Required
    public void setExecutor(ScheduledExecutorService executor)
    {
        _scheduledExecutor = executor;
        executor.scheduleAtFixedRate(new FireAndForgetTask(new PingMoversTask<>(_transfers.values())),
                                     PING_DELAY, PING_DELAY,
                                     TimeUnit.MILLISECONDS);
        executor.scheduleAtFixedRate(new FireAndForgetTask(new XrootdTpcInfoCleanerTask(_tpcInfo,
                                                                                        _tpcFdIndex)),
                                     TPC_EVICT_DELAY, TPC_EVICT_DELAY,
                                     TimeUnit.MILLISECONDS);
    }

    public boolean isTriedHostsEnabled()
    {
        return triedHostsEnabled;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println(String.format("Protocol Version %d.%d",
                                 XrootdProtocol.PROTOCOL_VERSION_MAJOR,
                                 XrootdProtocol.PROTOCOL_VERSION_MINOR));
    }

    private void uploadDone(Subject subject, Restriction restriction,
            FsPath path, FsPath uploadPath, boolean createDir,
            boolean overwrite)
            throws CacheException {
        try {
            EnumSet<CreateOption> options = EnumSet.noneOf(CreateOption.class);
            if (overwrite) {
                options.add(CreateOption.OVERWRITE_EXISTING);
            }
            PnfsCommitUpload msg
                    = new PnfsCommitUpload(subject,
                            restriction,
                            uploadPath,
                            path,
                            options,
                            EnumSet.of(PNFSID, SIZE, STORAGEINFO));
            msg = _pnfsStub.sendAndWait(msg);
        } catch (InterruptedException ex) {
            throw new CacheException("Operation interrupted", ex);
        } catch (NoRouteToCellException ex) {
            throw new CacheException("Internal communication failure", ex);
        }
    }

    private void abortUpload(Subject subject, Restriction restriction,
            FsPath path, FsPath uploadPath, String reason)
            throws CacheException {
        try {
            PnfsCancelUpload msg = new PnfsCancelUpload(subject, restriction,
                    uploadPath, path,
                    EnumSet.noneOf(FileAttribute.class),
                    "XROOTD upload aborted: " + reason);
            _pnfsStub.sendAndWait(msg);
        } catch (InterruptedException ex) {
            throw new CacheException("Operation interrupted", ex);
        } catch (NoRouteToCellException ex) {
            throw new CacheException("Internal communication failure", ex);
        }
    }

    private XrootdTransfer
            createUploadTransfer(InetSocketAddress client, FsPath path, Set<String> tried,
                    String ioQueue, UUID uuid, InetSocketAddress local,
                    Subject subject, Restriction restriction, boolean createDir,
                    boolean overwrite, Long size, FsPath uploadPath,
                    Map<String,String> opaque) throws ParseException {

        XrootdTransfer transfer = new XrootdTransfer(_pnfs, subject, restriction,
                uploadPath, opaque) {
            @Override
            public synchronized void finished(CacheException error) {
                try {
                    super.finished(error);

                    _transfers.remove(getFileHandle());
                    if (error == null) {
                        uploadDone(subject, restriction, path, uploadPath,
                                createDir, overwrite);

                        notifyBilling(0, "");
                        _log.info("Transfer {}@{} finished",
                                getPnfsId(), getPool());
                    } else {
                        int rc = error.getRc();
                        String message = error.getMessage();
                        abortUpload(subject, restriction, path, uploadPath, message);
                        notifyBilling(rc, message);
                        _log.warn("Transfer {}@{} failed: {} (error code={})",
                                getPnfsId(), getPool(), message, rc);
                    }
                } catch (CacheException ex) {
                    String message = ex.getMessage();
                    int rc = ex.getRc();
                    notifyBilling(rc, message);
                    _log.warn("Post upload operation failed: {} (error code={})",
                            message, rc);
                }
            }
        };
        transfer.setCellAddress(getCellAddress());
        transfer.setPoolManagerStub(_poolManagerStub);
        transfer.setPoolStub(_poolStub);
        transfer.setBillingStub(_billingStub);
        transfer.setClientAddress(client);
        transfer.setUUID(uuid);
        transfer.setDoorAddress(local);
        transfer.setIoQueue(ioQueue == null ? _ioQueue : ioQueue);
        transfer.setFileHandle(_handleCounter.getAndIncrement());
        transfer.setKafkaSender(_kafkaSender);
        transfer.setTriedHosts(tried);
        return transfer;
    }

    private XrootdTransfer
        createTransfer(InetSocketAddress client, FsPath path, Set<String> tried,
                       String ioQueue, UUID uuid, InetSocketAddress local, Subject subject,
                       Restriction restriction,
                       Map<String,String> opaque) throws ParseException
    {
        XrootdTransfer transfer =
            new XrootdTransfer(_pnfs, subject, restriction, path, opaque) {
                @Override
                public synchronized void finished(CacheException error)
                {
                    super.finished(error);

                    _transfers.remove(getFileHandle());

                    if (error == null) {
                        notifyBilling(0, "");
                        _log.info("Transfer {}@{} finished",
                                  getPnfsId(), getPool());
                    } else {
                        int rc = error.getRc();
                        String message = error.getMessage();
                        notifyBilling(rc, message);
                        _log.info("Transfer {}@{} failed: {} (error code={})", getPnfsId(), getPool(), message, rc);
                    }
                }
            };
        transfer.setCellAddress(getCellAddress());
        transfer.setPoolManagerStub(_poolManagerStub);
        transfer.setPoolStub(_poolStub);
        transfer.setBillingStub(_billingStub);
        transfer.setClientAddress(client);
        transfer.setUUID(uuid);
        transfer.setDoorAddress(local);
        transfer.setIoQueue(ioQueue == null ? _ioQueue : ioQueue);
        transfer.setFileHandle(_handleCounter.getAndIncrement());
        transfer.setKafkaSender(_kafkaSender);
        transfer.setTriedHosts(tried);
        return transfer;
    }

    public XrootdTransfer
        read(InetSocketAddress client, FsPath path, Set<String> tried,
             String ioQueue, UUID uuid, InetSocketAddress local,
             Subject subject, Restriction restriction, Map<String,String> opaque)
        throws CacheException, InterruptedException, ParseException
    {
        if (!isReadAllowed(path)) {
            throw new PermissionDeniedCacheException("Read permission denied");
        }

        XrootdTransfer transfer = createTransfer(client, path, tried, ioQueue,
                uuid, local, subject, restriction, opaque);
        int handle = transfer.getFileHandle();

        InetSocketAddress address = null;
        _transfers.put(handle, transfer);
        String explanation = "unspecified problem";
        try {
            transfer.readNameSpaceEntry(false);
            transfer.selectPoolAndStartMover(RETRY_POLICY);
            address = transfer.waitForRedirect(_moverTimeout, _moverTimeoutUnit);
            if (address == null) {
                throw new CacheException(transfer.getPool() + " failed to open TCP socket");
            }

            transfer.setStatus("Mover " + transfer.getPool() + "/" +
                               transfer.getMoverId() + ": Sending");
        } catch (CacheException e) {
            explanation = e.getMessage();
            transfer.notifyBilling(e.getRc(), e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            explanation = "transfer interrupted";
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   "Transfer interrupted");
            throw e;
        } catch (RuntimeException e) {
            explanation = "bug found: " + e.toString();
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.toString());
            throw e;
        } finally {
            if (address == null) {
                transfer.killMover(0, "killed by door: " + explanation);
                _transfers.remove(handle);
            }
        }
        return transfer;
    }

    private FsPath getUploadPath(Subject subject, Restriction restriction,
            boolean createDir, boolean overwrite, Long size, FsPath path,
            FsPath rootPath)
            throws CacheException, InterruptedException {
        try {
            EnumSet<CreateOption> options = EnumSet.noneOf(CreateOption.class);
            if (overwrite) {
                options.add(CreateOption.OVERWRITE_EXISTING);
            }
            if (createDir) {
                options.add(CreateOption.CREATE_PARENTS);
            }
            PnfsCreateUploadPath msg = new PnfsCreateUploadPath(subject,
                    restriction, path, rootPath, size, null, null, null,
                    options);
            msg = _pnfsStub.sendAndWait(msg);
            return msg.getUploadPath();
        } catch (NoRouteToCellException ex) {
            throw new CacheException("Internal communication failure", ex);
        }
    }

    public XrootdTransfer
            write(InetSocketAddress client, FsPath path, Set<String> tried,
                    String ioQueue, UUID uuid, boolean createDir,
                  boolean overwrite, Long size, OptionalLong maxUploadSize,
                    InetSocketAddress local, Subject subject, Restriction restriction,
                    boolean persistOnSuccessfulClose, FsPath rootPath,
                    Serializable delegatedProxy, Map<String,String> opaque)
                    throws CacheException, InterruptedException, ParseException {

        if (!isWriteAllowed(path)) {
            throw new PermissionDeniedCacheException("Write permission denied");
        }

        XrootdTransfer transfer;
        if (persistOnSuccessfulClose) {
            FsPath uploadPath = getUploadPath(subject, restriction, createDir,
                                              overwrite, size, path, rootPath);
            transfer = createUploadTransfer(client, path, tried, ioQueue, uuid,
                                            local, subject, restriction,
                                            createDir, overwrite, size,
                                            uploadPath, opaque);
        } else {
            transfer = createTransfer(client, path, tried, ioQueue, uuid, local,
                                      subject, restriction, opaque);
        }
        transfer.setOverwriteAllowed(overwrite);
        /*
         *  If this is a destination door/server and the session
         *  does not contain a proxy, eventually fail downstream.
         */
        transfer.setDelegatedCredential(delegatedProxy);
        int handle = transfer.getFileHandle();
        InetSocketAddress address = null;
        _transfers.put(handle, transfer);
        String explanation = "problem within door";
        try {
            try {
                if (createDir) {
                    transfer.createNameSpaceEntryWithParents();
                } else {
                    transfer.createNameSpaceEntry();
                }
            } catch (FileExistsCacheException e) {
                transfer.readNameSpaceEntry(true);
                if (transfer.getFileAttributes().getStorageInfo().isCreatedOnly()) {
                    transfer.setOverwriteAllowed(true);
                    transfer.createNameSpaceEntry();
                } else {
                    throw e;
                }
            }
            maxUploadSize.ifPresent(transfer::setMaximumLength);
            if (size != null) {
                checkResourceNotMissing(!maxUploadSize.isPresent() || size
                                                        <= maxUploadSize.getAsLong(),
                                        "File exceeds maximum upload size");
                transfer.setLength(size);
            }
            try {
                transfer.selectPoolAndStartMover(RETRY_POLICY);

                address = transfer.waitForRedirect(_moverTimeout,
                                                   _moverTimeoutUnit);
                if (address == null) {
                    throw new CacheException(transfer.getPool()
                                                             + " failed to open TCP socket");
                }

                transfer.setStatus("Mover " + transfer.getPool() + "/"
                                                   + transfer.getMoverId()
                                                   + ": Receiving");
            } finally {
                if (address == null) {
                    transfer.deleteNameSpaceEntry();
                }
            }
        } catch (CacheException e) {
            explanation = e.getMessage();
            transfer.notifyBilling(e.getRc(), e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            explanation = "transfer interrupted";
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   "Transfer interrupted");
            throw e;
        } catch (RuntimeException e) {
            explanation = "bug found: " + e.toString();
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.toString());
            throw e;
        } finally {
            if (address == null) {
                transfer.killMover(0, "killed by door: " + explanation);
                _transfers.remove(handle);
            }
        }
        return transfer;
    }

    /**
     * Delete the file denoted by path from the namespace
     *
     * @param path The path of the file that is going to be deleted
     * @throws CacheException Deletion of the file failed
     * @throws PermissionDeniedCacheException Caller does not have permission to delete the file
     */
    public void deleteFile(FsPath path, Subject subject, Restriction restriction)
        throws PermissionDeniedCacheException, CacheException
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject, restriction);

        if (!isWriteAllowed(path)) {
            throw new PermissionDeniedCacheException("Write permission denied");
        }

        Set<FileType> allowedSet = EnumSet.of(FileType.REGULAR);
        PnfsId pnfsId = pnfsHandler.deletePnfsEntry(path.toString(), allowedSet);
        sendRemoveInfoToBilling(pnfsId, path, subject);
    }

    private void sendRemoveInfoToBilling(PnfsId pnfsId, FsPath path, Subject subject)
    {
        DoorRequestInfoMessage infoRemove =
                new DoorRequestInfoMessage(getCellAddress(), "remove");
        infoRemove.setSubject(subject);
        infoRemove.setBillingPath(path.toString());
        infoRemove.setPnfsId(pnfsId);
        Origin origin = Subjects.getOrigin(subject);
        if (origin != null) {
            infoRemove.setClient(origin.getAddress().getHostAddress());
        }
        _billingStub.notify(infoRemove);

        _kafkaSender.accept(infoRemove);
    }

    /**
     * Delete the directory denoted by path from the namespace
     *
     * @param path The path of the directory that is going to be deleted
     * @throws CacheException
     */
    public void deleteDirectory(FsPath path, Subject subject,
            Restriction restriction) throws CacheException
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject, restriction);

        if (!isWriteAllowed(path)) {
            throw new PermissionDeniedCacheException("Write permission denied");
        }

        Set<FileType> allowedSet = EnumSet.of(FileType.DIR);
        pnfsHandler.deletePnfsEntry(path.toString(), allowedSet);
    }

    /**
     * Create the directory denoted by path in the namespace.
     *
     * @param path The path of the directory that is going to be created.
     * @param createParents Indicates whether the parent directories of the
     *        directory should be created automatically if they do not yet
     *        exist.
     * @throws CacheException Creation of the directory failed.
     */
    public void createDirectory(FsPath path,
                                boolean createParents,
                                Subject subject,
                                Restriction restriction) throws CacheException
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject, restriction);

        if (!isWriteAllowed(path)) {
            throw new PermissionDeniedCacheException("Write permission denied");
        }

        if (createParents) {
            pnfsHandler.createDirectories(path);
        } else {
            pnfsHandler.createPnfsDirectory(path.toString());
        }
    }

    /**
     * Emulate a file-move-operation by renaming sourcePath to targetPath in
     * the namespace
     * @param sourcePath the original path of the file that should be moved
     * @param targetPath the path to which the file should be moved
     * @throws CacheException
     */
    public void moveFile(FsPath sourcePath,
                         FsPath targetPath,
                         Subject subject,
                         Restriction restriction) throws CacheException
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject, restriction);

        if (!isWriteAllowed(sourcePath)) {
            throw new PermissionDeniedCacheException("No write permission on" +
                                                     " source path!");
        }

        if (!isWriteAllowed(targetPath)) {
            throw new PermissionDeniedCacheException("No write permission on" +
                                                     " target path!");
        }

        pnfsHandler.renameEntry(sourcePath.toString(),
                                targetPath.toString(),
                                false);
    }

    /**
     * List the contents of a path, usually a directory. In order to make
     * fragmented responses, as supported by the xroot protocol, possible and
     * not block the processing thread in the door, this will register the
     * passed callback along with the UUID of the message that is sent to
     * PNFS-manager.
     *
     * Once PNFS-manager replies to the message, that callback is retrieved and
     * the response is processed by the callback.
     *
     * @param path The path that is listed
     * @param restriction The Restriction in effect
     * @param subject Representation of user that request listing
     * @param callback The callback that will process the response
     */
    public void listPath(FsPath path,
                         Subject subject,
                         Restriction restriction,
                         MessageCallback<PnfsListDirectoryMessage> callback,
                         EnumSet<FileAttribute> attributes)
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject, restriction);

        PnfsListDirectoryMessage msg =
            new PnfsListDirectoryMessage(
                    path.toString(),
                    null,
                    Range.<Integer>all(),
                    attributes);
        UUID uuid = msg.getUUID();

        try {
            DirlistRequestHandler requestHandler =
                new DirlistRequestHandler(uuid,
                                          pnfsHandler.getPnfsTimeout(),
                                          callback);
            _requestHandlers.put(uuid, requestHandler);
            pnfsHandler.send(msg);
            requestHandler.resetTimeout();
        } catch (RejectedExecutionException ree) {
            _requestHandlers.remove(uuid);
            callback.failure(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                             ree.getMessage());
        }
    }

    /**
     * Encapsulate the list directory callback into a handler that manages the
     * scheduled executor service for the timeout handling.
     *
     */
    private class DirlistRequestHandler {
        private ScheduledFuture<?> _executionInstance;
        private final long _timeout;
        private final UUID _uuid;
        private final MessageCallback<PnfsListDirectoryMessage> _callback;

        public DirlistRequestHandler(UUID uuid,
                              long responseTimeout,
                              MessageCallback<PnfsListDirectoryMessage> callback) {
            _uuid = uuid;
            _timeout = responseTimeout;
            _callback = callback;
        }

        /**
         * Final listing result. Report back via callback and cancel
         * the timeout handler.
         * @param msg The reply containing the listing result.
         */
        public synchronized void success(PnfsListDirectoryMessage msg) {
            if (_requestHandlers.remove(_uuid) == this) {
                cancelTimeout();
                _callback.setReply(msg);
                _callback.success();
            }
        }

        /**
         * Partial listing result, report that back to the callback. Also,
         * reset the timeout timer in anticipation of further listing results.
         * @param msg The reply containing the partial directory listing.
         */
        public synchronized void continueListing(PnfsListDirectoryMessage msg) {
            _callback.setReply(msg);
            try {
                _callback.success();
                resetTimeout();
            } catch (RejectedExecutionException ree) {
                _requestHandlers.remove(_uuid);
                _callback.failure(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                  ree.getMessage());
            }
        }

        /**
         * Remove the request handler from the list, report a failure to the
         * callback and cancel the timeout timer.
         * @param msg The reply received from PNFS
         */
        public synchronized void failure(PnfsListDirectoryMessage msg) {
            if (_requestHandlers.remove(_uuid) == this) {
                cancelTimeout();
                _callback.setReply(msg);
                _callback.failure(msg.getReturnCode(), msg.getErrorObject());
            }
        }

        /**
         * Reschedule the timeout task with the same timeout as initially.
         * Rescheduling means cancelling the old task and submitting a new one.
         * @throws RejectedExecutionException
         */
        public synchronized void resetTimeout()
            throws RejectedExecutionException {
            Runnable target = () -> {
                if (_requestHandlers.remove(_uuid)
                        == DirlistRequestHandler.this) {
                    _callback.timeout(null);
                }
            };

            if (_executionInstance != null) {
                _executionInstance.cancel(false);
            }

            _executionInstance =
                _scheduledExecutor.schedule(target,
                                                 _timeout,
                                                 TimeUnit.MILLISECONDS);
        }

        public synchronized void cancelTimeout() {
            if (_executionInstance != null) {
                _executionInstance.cancel(false);
            }
        }
    }

    /**
     * Check whether the given path matches against a list of allowed
     * write paths.
     *
     * @param path the path which is going to be checked
     */
    private boolean isWriteAllowed(FsPath path)
    {
        for (FsPath prefix: _writePaths) {
            if (path.hasPrefix(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check whether the given path matches against a list of allowed
     * read paths.
     *
     * Package visibility because needed by redirect handler in
     * case of a third-party source request.
     *
     * @param path the path which is going to be checked
     */
    boolean isReadAllowed(FsPath path)
    {
        for (FsPath prefix: _readPaths) {
            if (path.hasPrefix(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Requests to start movers are processed synchronously by the
     * Transfer class. This message handler will only ever receive
     * replies for those requests for which the Transfer class timed
     * out or interrupted.
     *
     * To avoid that orphaned movers fill a transfer slot on the pool,
     * we kill it right away.
     */
    public void messageArrived(PoolIoFileMessage message)
    {
        if (message.getReturnCode() == 0) {
            String pool = message.getPoolName();
            _poolStub.notify(new CellPath(pool), new PoolMoverKillMessage(pool,
                    message.getMoverId(), "door timed out before pool"));
        }
    }

    public void messageArrived(XrootdDoorAdressInfoMessage msg)
    {
        _log.debug("Received redirect msg from mover");
        XrootdTransfer transfer = _transfers.get(msg.getXrootdFileHandle());
        if (transfer != null) {
            transfer.redirect(msg.getSocketAddress());
        }
    }

    public void messageArrived(DoorTransferFinishedMessage msg)
    {
        if ((msg.getProtocolInfo() instanceof XrootdProtocolInfo)) {
            XrootdProtocolInfo info =
                (XrootdProtocolInfo) msg.getProtocolInfo();
            XrootdTransfer transfer =
                _transfers.get(info.getXrootdFileHandle());
            if (transfer != null) {
                transfer.finished(msg);
            }
        } else {
            _log.warn("Ignoring unknown protocol info {} from pool {}",
                      msg.getProtocolInfo(), msg.getPoolName());
        }
    }

    /**
     * Try to find callback registered in listPath(...) and process the
     * response there
     * @param msg The reply to a PnfsListDirectoryMessage sent earlier.
     */
    public void messageArrived(PnfsListDirectoryMessage msg)
    {
        UUID uuid = msg.getUUID();
        DirlistRequestHandler request = _requestHandlers.get(uuid);

        if (request == null) {
            _log.info("Did not find the callback for directory listing " +
                      "message with UUID {}.", uuid);
            return;
        }

        if (msg.getReturnCode() == 0 && msg.isFinal()) {
            request.success(msg);
        } else if (msg.getReturnCode() == 0) {
            request.continueListing(msg);
        } else {
            request.failure(msg);
        }
    }

    private int getFileStatusFlags(Subject subject, Restriction restriction,
            FsPath path, FileAttributes attributes)
    {
        int flags = 0;
        switch (attributes.getFileType()) {
        case DIR:
            boolean canListDir =
                    _pdp.canListDir(subject, attributes) == AccessType.ACCESS_ALLOWED
                        && !restriction.isRestricted(Activity.LIST, path);
            boolean canLookup =
                    _pdp.canLookup(subject, attributes) == AccessType.ACCESS_ALLOWED
                        && !restriction.isRestricted(Activity.READ_METADATA, path);
            boolean canCreateFile =
                    _pdp.canCreateFile(subject, attributes) == AccessType.ACCESS_ALLOWED
                        && !restriction.isRestricted(Activity.UPLOAD, path);
            boolean canCreateDir =
                    _pdp.canCreateSubDir(subject, attributes) == AccessType.ACCESS_ALLOWED
                        && !restriction.isRestricted(Activity.MANAGE, path);
            flags |= kXR_isDir;
            if (canLookup) {
                flags |= kXR_xset;
            }
            if (canCreateFile || canCreateDir) {
                flags |= kXR_writable;
            }
            if (canListDir) {
                flags |= kXR_readable;
            }
            break;
        case REGULAR:
            boolean canReadFile =
                    _pdp.canReadFile(subject, attributes)== AccessType.ACCESS_ALLOWED
                        && !restriction.isRestricted(Activity.DOWNLOAD, path);
            boolean canWriteFile =
                    _pdp.canWriteFile(subject, attributes)== AccessType.ACCESS_ALLOWED
                        && !restriction.isRestricted(Activity.UPLOAD, path);
            if (canWriteFile) {
                flags |= kXR_writable;
            }
            if (canReadFile) {
                flags |= kXR_readable;
            }
            if (attributes.getStorageInfo().isCreatedOnly()) {
                flags |= kXR_poscpend;
            }
            break;
        default:
            flags |= kXR_other;
            break;
        }
        return flags;
    }

    public Set<Checksum> getChecksums(FsPath fullPath, Subject subject, Restriction restriction) throws CacheException
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject, restriction);
        Set<FileAttribute> requestedAttributes = EnumSet.of(CHECKSUM);
        FileAttributes attributes =
                pnfsHandler.getFileAttributes(fullPath.toString(), requestedAttributes);
        return attributes.getChecksums();
    }

    public FileStatus getFileStatus(FsPath fullPath, Subject subject,
            Restriction restriction, String clientHost) throws CacheException
    {
        /* Fetch file attributes.
         */
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject, restriction);
        Set<FileAttribute> requestedAttributes = getRequiredAttributesForFileStatus();
        FileAttributes attributes = pnfsHandler.getFileAttributes(fullPath.toString(), requestedAttributes);
        return getFileStatus(subject, restriction, fullPath, clientHost, attributes);
    }

    public EnumSet<FileAttribute> getRequiredAttributesForFileStatus()
    {
        EnumSet<FileAttribute> requestedAttributes = EnumSet.of(TYPE, SIZE, MODIFICATION_TIME, STORAGEINFO);
        requestedAttributes.addAll(PoolMonitorV5.getRequiredAttributesForFileLocality());
        requestedAttributes.addAll(_pdp.getRequiredAttributes());
        return requestedAttributes;
    }

    public FileStatus getFileStatus(Subject subject, Restriction restriction,
            FsPath fullPath, String clientHost, FileAttributes attributes)
    {
        int flags = getFileStatusFlags(subject, restriction, fullPath, attributes);

        /* Determine file locality.
         */
        if (attributes.getFileType() != FileType.DIR) {
            FileLocality locality =
                    _poolMonitor.getFileLocality(attributes, clientHost);
            switch (locality) {
            case NEARLINE:
            case LOST:
            case UNAVAILABLE:
                flags |= kXR_offline;
            }
        }

        return new FileStatus(0, attributes.getSizeIfPresent().orElse(0L), flags, attributes.getModificationTime() / 1000);
    }

    public int[] getMultipleFileStatuses(FsPath[] allPaths, Subject subject,
            Restriction restriction) throws CacheException
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject, restriction);
        int[] flags = new int[allPaths.length];
        // TODO: Use SpreadAndWait
        for (int i = 0; i < allPaths.length; i++) {
            try {
                Set<FileAttribute> requestedAttributes = EnumSet.of(TYPE);
                requestedAttributes.addAll(_pdp.getRequiredAttributes());
                FileAttributes attributes =
                        pnfsHandler.getFileAttributes(allPaths[i].toString(), requestedAttributes);
                flags[i] = getFileStatusFlags(subject, restriction, allPaths[i], attributes);
            } catch (CacheException e) {
                if (e.getRc() != CacheException.FILE_NOT_FOUND &&
                        e.getRc() != CacheException.NOT_IN_TRASH) {
                    throw e;
                }
                flags[i] = kXR_other;
            }
        }
        return flags;
    }

    public int nextTpcPlaceholder()
    {
        synchronized (_tpcFdIndex) {
            Integer next = _tpcPlaceholder.getAndIncrement();
            _tpcFdIndex.put(next, TPC_PLACEMENT);
            _log.debug("Added fhandle {} without key.", next);
            return next;
        }
    }

    public XrootdTpcInfo createOrGetRendezvousInfo(String key)
    {
        synchronized (_tpcFdIndex) {
            XrootdTpcInfo info = _tpcInfo.get(key);
            if (info == null) {
                info = new XrootdTpcInfo(key);
                _tpcInfo.put(key, info);
                Integer next = _tpcPlaceholder.getAndIncrement();
                _tpcFdIndex.put(next, key);
                info.setFd(next);
                _log.debug("Added fhandle {} for key {}.", next, key);
            }
            _log.debug("info {} for key {}.", info, key);
            return info;
        }
    }

    public boolean removeTpcPlaceholder(int fd)
    {
        synchronized (_tpcFdIndex) {
            String tpc = _tpcFdIndex.remove(fd);
            if (tpc != null) {
                if (!tpc.equals(TPC_PLACEMENT)) {
                    _tpcInfo.remove(tpc);
                    _log.debug("fhandle {} was removed.", fd);
                }
                return true;
            }
            _log.debug("fhandle {} is invalid.", fd);
            return false;
        }
    }

    /**
     * To allow the transfer monitoring in the httpd cell to recognize us
     * as a door, we have to emulate LoginManager.  To emulate
     * LoginManager we list ourselves as our child.
     */
    public static final String hh_get_children = "[-binary]";
    public Object ac_get_children(Args args)
    {
        boolean binary = args.hasOption("binary");
        if (binary) {
            String [] list = new String[] { getCellName() };
            return new LoginManagerChildrenInfo(getCellName(), getCellDomainName(), list);
        } else {
            return getCellName();
        }
    }

    public static final String hh_get_door_info = "[-binary]";
    public static final String fh_get_door_info =
        "Provides information about the door and current transfers";
    public Object ac_get_door_info(Args args)
    {
        List<IoDoorEntry> entries = new ArrayList<>();
        for (Transfer transfer: _transfers.values()) {
            entries.add(transfer.getIoDoorEntry());
        }

        IoDoorInfo doorInfo = new IoDoorInfo(getCellName(), getCellDomainName());
        doorInfo.setProtocol(XROOTD_PROTOCOL_STRING, XROOTD_PROTOCOL_VERSION);
        doorInfo.setOwner("");
        doorInfo.setProcess("");
        doorInfo.setIoDoorEntries(entries.toArray(IoDoorEntry[]::new));
        return args.hasOption("binary") ? doorInfo : doorInfo.toString();
    }

    public static final String hh_kill_mover =
        " <pool> <moverid> # kill transfer on the pool";
    public String ac_kill_mover_$_2(Args args) throws NumberFormatException
    {
        int mover = Integer.parseInt(args.argv(1));
        String pool = args.argv(0);

        for (Transfer transfer : _transfers.values()) {
            if (transfer.getMoverId() == mover && transfer.getPool() != null && transfer.getPool().getName().equals(pool)) {

                transfer.killMover(0, "killed by door 'kill mover' command");
                return "Kill request to the mover " + mover + " has been submitted";
            }
        }
        return "mover " + mover + " not found on the pool " + pool;
    }
}
