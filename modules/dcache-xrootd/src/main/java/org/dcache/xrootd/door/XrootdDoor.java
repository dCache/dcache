/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import diskCacheV111.movers.NetIFContainer;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMoverKillMessage;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.login.LoginManagerChildrenInfo;

import org.dcache.acl.enums.AccessType;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import org.dcache.namespace.ACLPermissionHandler;
import org.dcache.namespace.ChainedPermissionHandler;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.PermissionHandler;
import org.dcache.namespace.PosixPermissionHandler;
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
import org.dcache.xrootd.util.FileStatus;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.xrootd.protocol.XrootdProtocol.*;

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
               CellCommandListener
{
    public final static String XROOTD_PROTOCOL_STRING = "Xrootd";
    public final static int XROOTD_PROTOCOL_MAJOR_VERSION = 2;
    public final static int XROOTD_PROTOCOL_MINOR_VERSION = 7;
    public final static String XROOTD_PROTOCOL_VERSION =
        String.format("%d.%d",
                      XROOTD_PROTOCOL_MAJOR_VERSION,
                      XROOTD_PROTOCOL_MINOR_VERSION);

    private final static Logger _log =
        LoggerFactory.getLogger(XrootdDoor.class);

    private final static AtomicInteger _handleCounter = new AtomicInteger();

    private final static long PING_DELAY = 300000;

    private final static TransferRetryPolicy RETRY_POLICY =
        TransferRetryPolicies.tryOncePolicy(Long.MAX_VALUE);

    private List<FsPath> _readPaths = Collections.singletonList(new FsPath());
    private List<FsPath> _writePaths = Collections.singletonList(new FsPath());

    private CellStub _poolStub;
    private CellStub _poolManagerStub;
    private CellStub _billingStub;

    private PoolMonitor _poolMonitor;

    private final PermissionHandler _pdp =
            new ChainedPermissionHandler
                    (
                            new ACLPermissionHandler(),
                            new PosixPermissionHandler()
                    );

    private int _moverTimeout = 180000;
    private TimeUnit _moverTimeoutUnit = TimeUnit.MILLISECONDS;

    private PnfsHandler _pnfs;

    private String _ioQueue;

    private Map<UUID, DirlistRequestHandler> _requestHandlers =
        new ConcurrentHashMap<>();

    private ScheduledExecutorService _dirlistTimeoutExecutor;

    /**
     * Current xrootd transfers. The key is the xrootd file handle.
     */
    private final Map<Integer,XrootdTransfer> _transfers =
        new ConcurrentHashMap<>();

    @Required
    public void setPoolStub(CellStub stub)
    {
        _poolStub = stub;
    }

    @Required
    public void setPoolManagerStub(CellStub stub)
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
            list.add(new FsPath(path));
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
        _moverTimeoutUnit = checkNotNull(unit);
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
        executor.scheduleAtFixedRate(new FireAndForgetTask(new PingMoversTask<>(_transfers.values())),
                                     PING_DELAY, PING_DELAY,
                                     TimeUnit.MILLISECONDS);
    }

    @Required
    public void setDirlistTimeoutExecutor(ScheduledExecutorService executor)
    {
        _dirlistTimeoutExecutor = executor;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println(String.format("Protocol Version %d.%d",
                                 XROOTD_PROTOCOL_MAJOR_VERSION,
                                 XROOTD_PROTOCOL_MINOR_VERSION));
    }

    private XrootdTransfer
        createTransfer(InetSocketAddress client, FsPath path,
                       UUID uuid, InetSocketAddress local, Subject subject)
    {
        XrootdTransfer transfer =
            new XrootdTransfer(_pnfs, subject, path) {
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
        transfer.setCellName(getCellName());
        transfer.setDomainName(getCellDomainName());
        transfer.setPoolManagerStub(_poolManagerStub);
        transfer.setPoolStub(_poolStub);
        transfer.setBillingStub(_billingStub);
        transfer.setClientAddress(client);
        transfer.setUUID(uuid);
        transfer.setDoorAddress(local);
        transfer.setFileHandle(_handleCounter.getAndIncrement());
        return transfer;
    }

    public XrootdTransfer
        read(InetSocketAddress client, FsPath path, UUID uuid,
             InetSocketAddress local, Subject subject)
        throws CacheException, InterruptedException
    {
        if (!isReadAllowed(path)) {
            throw new PermissionDeniedCacheException("Read permission denied");
        }

        XrootdTransfer transfer =
            createTransfer(client, path, uuid, local, subject);
        int handle = transfer.getFileHandle();

        InetSocketAddress address = null;
        _transfers.put(handle, transfer);
        try {
            transfer.readNameSpaceEntry();
            transfer.selectPoolAndStartMover(_ioQueue, RETRY_POLICY);
            address = transfer.waitForRedirect(_moverTimeout, _moverTimeoutUnit);
            if (address == null) {
                throw new CacheException(transfer.getPool() + " failed to open TCP socket");
            }

            transfer.setStatus("Mover " + transfer.getPool() + "/" +
                               transfer.getMoverId() + ": Sending");
        } catch (CacheException e) {
            transfer.notifyBilling(e.getRc(), e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   "Transfer interrupted");
            throw e;
        } catch (RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.toString());
            throw e;
        } finally {
            if (address == null) {
                transfer.killMover(0);
                _transfers.remove(handle);
            }
        }
        return transfer;
    }

    public XrootdTransfer
        write(InetSocketAddress client, FsPath path, UUID uuid,
              boolean createDir, boolean overwrite,
              InetSocketAddress local, Subject subject)
        throws CacheException, InterruptedException
    {
        if (!isWriteAllowed(path)) {
            throw new PermissionDeniedCacheException("Write permission denied");
        }

        XrootdTransfer transfer =
            createTransfer(client, path, uuid, local, subject);
        transfer.setOverwriteAllowed(overwrite);
        int handle = transfer.getFileHandle();
        InetSocketAddress address = null;
        _transfers.put(handle, transfer);
        try {
            if (createDir) {
                transfer.createNameSpaceEntryWithParents();
            } else {
                transfer.createNameSpaceEntry();
            }
            try {
                transfer.selectPoolAndStartMover(_ioQueue, RETRY_POLICY);

                address = transfer.waitForRedirect(_moverTimeout, _moverTimeoutUnit);
                if (address == null) {
                    throw new CacheException(transfer.getPool() + " failed to open TCP socket");
                }

                transfer.setStatus("Mover " + transfer.getPool() + "/" +
                                   transfer.getMoverId() + ": Receiving");
            } finally {
                if (address == null) {
                    transfer.deleteNameSpaceEntry();
                }
            }
        } catch (CacheException e) {
            transfer.notifyBilling(e.getRc(), e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   "Transfer interrupted");
            throw e;
        } catch (RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.toString());
            throw e;
        } finally {
            if (address == null) {
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
    public void deleteFile(FsPath path, Subject subject)
        throws PermissionDeniedCacheException, CacheException
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject);

        if (!isWriteAllowed(path)) {
            throw new PermissionDeniedCacheException("Write permission denied");
        }

        Set<FileType> allowedSet = EnumSet.of(FileType.REGULAR);
        pnfsHandler.deletePnfsEntry(path.toString(), allowedSet);
    }

    /**
     * Delete the directory denoted by path from the namespace
     *
     * @param path The path of the directory that is going to be deleted
     * @throws CacheException
     */
    public void deleteDirectory(FsPath path,
                                Subject subject) throws CacheException
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject);

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
                                Subject subject)
                                                    throws CacheException
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject);

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
                         Subject subject) throws CacheException
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject);

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
     * fragmented responses, as supported by the xrootd protocol, possible and
     * not block the processing thread in the door, this will register the
     * passed callback along with the UUID of the message that is sent to
     * PNFS-manager.
     *
     * Once PNFS-manager replies to the message, that callback is retrieved and
     * the response is processed by the callback.
     *
     * @param path The path that is listed
     * @param subject Representation of user that request listing
     * @param callback The callback that will process the response
     */
    public void listPath(FsPath path,
                         Subject subject,
                         MessageCallback<PnfsListDirectoryMessage> callback)
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject);

        PnfsListDirectoryMessage msg =
            new PnfsListDirectoryMessage(
                    path.toString(),
                    null,
                    Range.<Integer>all(),
                    EnumSet.noneOf(FileAttribute.class));
        UUID uuid = msg.getUUID();

        try {
            DirlistRequestHandler requestHandler =
                new DirlistRequestHandler(uuid,
                                          pnfsHandler.getPnfsTimeout(),
                                          callback);
            _requestHandlers.put(uuid, requestHandler);
            pnfsHandler.send(msg);
            requestHandler.resetTimeout();
        } catch (NoRouteToCellException e) {
            _requestHandlers.remove(uuid);
            callback.noroute(e.getDestinationPath());
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
            Runnable target = new Runnable() {
                @Override
                public void run() {
                    if (_requestHandlers.remove(_uuid)
                            == DirlistRequestHandler.this) {
                        _callback.timeout(null);
                    }
                }
            };

            if (_executionInstance != null) {
                _executionInstance.cancel(false);
            }

            _executionInstance =
                _dirlistTimeoutExecutor.schedule(target,
                                                 _timeout,
                                                 TimeUnit.MILLISECONDS);
        }

        public synchronized void cancelTimeout() {
            _executionInstance.cancel(false);
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
            if (path.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check whether the given path matches against a list of allowed
     * read paths.
     *
     * @param path the path which is going to be checked
     */
    private boolean isReadAllowed(FsPath path)
    {
        for (FsPath prefix: _readPaths) {
            if (path.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private Inet4Address getFirstIpv4(Collection<NetIFContainer> interfaces)
    {
        for (NetIFContainer container: interfaces) {
            for (Object ip: container.getInetAddresses()) {
                if (ip instanceof Inet4Address) {
                    return (Inet4Address) ip;
                }
            }
        }
        return null;
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
        String pool = message.getPoolName();
        int moverId = message.getMoverId();
        try {
            PoolMoverKillMessage killMessage =
                new PoolMoverKillMessage(pool, moverId);
            killMessage.setReplyRequired(false);
            _poolStub.notify(new CellPath(pool), killMessage);
        } catch (NoRouteToCellException e) {
            _log.error("Failed to kill mover {}/{}: {}", pool, moverId, e.getMessage());
        }
    }

    public void messageArrived(XrootdDoorAdressInfoMessage msg)
    {
        _log.trace("Received redirect msg from mover");
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

    private int getFileStatusFlags(Subject subject, FileAttributes attributes)
    {
        int flags = 0;
        switch (attributes.getFileType()) {
        case DIR:
            boolean canListDir =
                    _pdp.canListDir(subject, attributes) == AccessType.ACCESS_ALLOWED;
            boolean canLookup =
                    _pdp.canLookup(subject, attributes) == AccessType.ACCESS_ALLOWED;
            boolean canCreateFile =
                    _pdp.canCreateFile(subject, attributes) == AccessType.ACCESS_ALLOWED;
            boolean canCreateDir =
                    _pdp.canCreateSubDir(subject, attributes) == AccessType.ACCESS_ALLOWED;
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
                    _pdp.canReadFile(subject, attributes)== AccessType.ACCESS_ALLOWED;
            boolean canWriteFile =
                    _pdp.canWriteFile(subject, attributes)== AccessType.ACCESS_ALLOWED;
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

    public Set<Checksum> getChecksums(FsPath fullPath, Subject subject) throws CacheException
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject);
        Set<FileAttribute> requestedAttributes = EnumSet.of(CHECKSUM);
        FileAttributes attributes =
                pnfsHandler.getFileAttributes(fullPath.toString(), requestedAttributes);
        return attributes.getChecksums();
    }

    public FileStatus getFileStatus(FsPath fullPath, Subject subject, String clientHost) throws CacheException
    {
        /* Fetch file attributes.
         */
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject);
        Set<FileAttribute> requestedAttributes = EnumSet.of(TYPE, SIZE, MODIFICATION_TIME, STORAGEINFO);
        requestedAttributes.addAll(PoolMonitorV5.getRequiredAttributesForFileLocality());
        requestedAttributes.addAll(_pdp.getRequiredAttributes());

        FileAttributes attributes =
                pnfsHandler.getFileAttributes(fullPath.toString(), requestedAttributes);

        /* Determine file locality.
         */
        int flags = getFileStatusFlags(subject, attributes);
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

        return new FileStatus(0, attributes.getSize(), flags, attributes.getModificationTime() / 1000);
    }

    public int[] getMultipleFileStatuses(FsPath[] allPaths, Subject subject) throws CacheException
    {
        PnfsHandler pnfsHandler = new PnfsHandler(_pnfs, subject);
        int[] flags = new int[allPaths.length];
        // TODO: Use SpreadAndWait
        for (int i = 0; i < allPaths.length; i++) {
            try {
                Set<FileAttribute> requestedAttributes = EnumSet.of(TYPE);
                requestedAttributes.addAll(_pdp.getRequiredAttributes());
                FileAttributes attributes =
                        pnfsHandler.getFileAttributes(allPaths[i].toString(), requestedAttributes);
                flags[i] = getFileStatusFlags(subject, attributes);
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

    /**
     * To allow the transfer monitoring in the httpd cell to recognize us
     * as a door, we have to emulate LoginManager.  To emulate
     * LoginManager we list ourselves as our child.
     */
    public final static String hh_get_children = "[-binary]";
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

    public final static String hh_get_door_info = "[-binary]";
    public final static String fh_get_door_info =
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
        doorInfo.setIoDoorEntries(entries
                .toArray(new IoDoorEntry[entries.size()]));
        return args.hasOption("binary") ? doorInfo : doorInfo.toString();
    }

    public static final String hh_kill_mover =
        " <pool> <moverid> # kill transfer on the pool";
    public String ac_kill_mover_$_2(Args args) throws NumberFormatException
    {
        int mover = Integer.parseInt(args.argv(1));
        String pool = args.argv(0);

        for (Transfer transfer : _transfers.values()) {
            if (transfer.getMoverId() == mover && transfer.getPool() != null && transfer.getPool().equals(pool)) {

                transfer.killMover(0);
                return "Kill request to the mover " + mover + " has been submitted";
            }
        }
        return "mover " + mover + " not found on the pool " + pool;
    }
}
