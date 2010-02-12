package org.dcache.xrootd2.door;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.Collection;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.security.GeneralSecurityException;

import org.dcache.vehicles.XrootdDoorAdressInfoMessage;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.dcache.xrootd2.security.AbstractAuthorizationFactory;
import org.dcache.xrootd2.util.DoorRequestMsgWrapper;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.CellStub;

import diskCacheV111.movers.NetIFContainer;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.DirNotExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.FileMetaData.Permissions;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.CellVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    implements CellMessageReceiver
{
    private final static Logger _log = LoggerFactory.getLogger(XrootdDoor.class);

    private final static String XROOTD_PROTOCOL_STRING = "Xrootd";
    private final static int XROOTD_PROTOCOL_MAJOR_VERSION = 2;
    private final static int XROOTD_PROTOCOL_MINOR_VERSION = 7;
    private final static int RETRY_SLEEP = 500; // ms

    /**
     * Is set true only once (when plugin loading didn't succeed) to
     * avoid multiple trials.
     */
    private boolean _authzPluginLoadFailed = false;

    /**
     * File handle to port address mapping. Access is protected by
     * synchronization on the map.
     */
    private Set<Integer> _waitingForRedirect =
        new HashSet<Integer>();
    private Map<Integer,InetSocketAddress> _redirectTable =
        new HashMap<Integer,InetSocketAddress>();

    private AbstractAuthorizationFactory _authzFactory;

    private List<FsPath> _authorizedWritePaths = Collections.emptyList();

    private CellStub _poolStub;
    private CellStub _poolManagerStub;

    private PnfsHandler _pnfs;
    private String _transactionPrefix;
    private boolean _isReadOnly = false;
    private String _ioQueue;

    private FsPath _rootPath = new FsPath();

    public static CellVersion getStaticCellVersion()
    {
        return new CellVersion(diskCacheV111.util.Version.getVersion(),
                               "$Revision: 11646 $");
    }

    public void setPoolStub(CellStub stub)
    {
        _poolStub = stub;
    }

    public void setPoolManagerStub(CellStub stub)
    {
        _poolManagerStub = stub;
    }

    /**
     * The list of paths which are authorized for xrootd write access.
     */
    public void setAuthorizedWritePaths(String s)
    {
        List<FsPath> list = new ArrayList();
        for (String path: s.split(":")) {
            list.add(new FsPath(path));
        }
        _authorizedWritePaths = list;
        _log.info("allowed write paths: " + list);
    }

    /**
     * Returns the list of authorized write paths.
     *
     * Notice that the getter uses a different property name than the
     * setter. This is because the getter returns a different type
     * than set by the setter, and hence we must not use the same
     * property name (otherwise Spring complains).
     */
    public List<FsPath> getAuthorizedWritePathList()
    {
        return _authorizedWritePaths;
    }

    /**
     * Sets the root path.
     *
     * The root path forms the root of the name space of the xrootd
     * server. Xrootd paths are translated to full PNFS paths by
     * predending the root path.
     */
    public void setRootPath(String path)
    {
        _rootPath = new FsPath(path);
    }

    /**
     * Returns the root path.
     */
    public String getRootPath()
    {
        return _rootPath.toString();
    }

    /**
     *
     */
    public void setAuthorizationFactory(AbstractAuthorizationFactory factory)
    {
        _authzFactory = factory;
    }

    public AbstractAuthorizationFactory getAuthorizationFactory()
    {
        return _authzFactory;
    }

    public void setPnfsHandler(PnfsHandler pnfs)
    {
        _pnfs = pnfs;
    }

    /**
     * The prefix of the transaction string used for billing.
     */

    public void setTransactionPrefix(String prefix)
    {
        _transactionPrefix = prefix;
    }

    public String setTransactionPrefix()
    {
        return _transactionPrefix;
    }

    /**
     * Whether to only allow read access.
     */
    public void setReadOnly(boolean readonly)
    {
        _isReadOnly = readonly;
    }

    public boolean getReadOnly()
    {
        return _isReadOnly;
    }

    /**
     * The actual mover queue on the pool onto which this request gets
     * scheduled.
     */
    public void setIoQueue(String ioQueue)
    {
        _ioQueue = ioQueue;
    }

    public String getIoQueue()
    {
        return _ioQueue;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println(String.format("Protocol Version %d.%d",
                                 XROOTD_PROTOCOL_MAJOR_VERSION,
                                 XROOTD_PROTOCOL_MINOR_VERSION));
    }

    /**
     * Forms a full PNFS path. The path is created by concatenating
     * the root path and path. The root path is guaranteed to be a
     * prefix of the path returned.
     */
    private FsPath createFullPath(String path)
    {
        return new FsPath(_rootPath, new FsPath(path));
    }

    /**
     * Selects a pool and asks it to transfer a file.
     *
     * @param client      Address of client making the request
     * @param path        Path of the file
     * @param pnfsid      PNFS ID of file to transfer
     * @param storageInfo Storage info of file to transfer
     * @param token       Login token
     * @param fileHandle  xrootd file handle of file to transfer
     * @param checksum    checksum of open request
     * @param isWrite     true for uploads, false for downloads
     * @return The address on which the mover will accept the data connection
     */
    public InetSocketAddress
        transfer(InetSocketAddress client,
                 String path, PnfsId pnfsid, StorageInfo storageInfo,
                 int fileHandle, long checksum, boolean isWrite)
        throws CacheException, InterruptedException
    {
        XrootdProtocolInfo protocolInfo =
            new XrootdProtocolInfo(XROOTD_PROTOCOL_STRING ,
                                   XROOTD_PROTOCOL_MAJOR_VERSION,
                                   XROOTD_PROTOCOL_MINOR_VERSION,
                                   client.getAddress().getHostName(),
                                   client.getPort(),
                                   new CellPath(getCellName(), getCellDomainName()),
                                   pnfsid,
                                   fileHandle,
                                   checksum);

        protocolInfo.setPath(path);

        InetSocketAddress redirectAddress =
            createMover(pnfsid, storageInfo, protocolInfo, isWrite);
        while (redirectAddress == null) {
            Thread.sleep(RETRY_SLEEP);
            redirectAddress =
                createMover(pnfsid, storageInfo, protocolInfo, isWrite);
        }

        return redirectAddress;
    }

    private InetSocketAddress
        createMover(PnfsId pnfsid, StorageInfo storageInfo,
                    XrootdProtocolInfo protocolInfo, boolean isWrite)
        throws CacheException, InterruptedException
    {
        String pool = askForPool(pnfsid, storageInfo, protocolInfo, isWrite);
        try {
            return askForFile(pool, pnfsid, storageInfo, protocolInfo, isWrite);
        } catch (TimeoutCacheException e) {
            if (isWrite) {
                throw e;
            }
        } catch (CacheException e) {
            _log.warn("Pool error: " + e.getMessage());
        }
        return null;
    }

    public PnfsGetStorageInfoMessage getStorageInfo(String path)
        throws CacheException
    {
        FsPath fullPath = createFullPath(path);
        return _pnfs.getStorageInfoByPath(fullPath.toString());
    }

    /**
     * check wether the given path matches against a list of allowed paths
     * @param pathToOpen the path which is going to be checked
     * @param authorizedWritePathList the list of allowed paths
     * @return
     */
    private boolean matchWritePath(FsPath path)
    {
        if (_authorizedWritePaths.isEmpty()) {
            return true;
        }
        for (FsPath prefix: _authorizedWritePaths) {
            if (path.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    public PnfsCreateEntryMessage
        createNewPnfsEntry(String path, boolean createDir)
        throws CacheException
    {
        FsPath fullPath = createFullPath(path);

        // check if write access is restricted in general and whether
        // the path to open matches the whitelist
        if (!matchWritePath(fullPath)) {
            throw new PermissionDeniedCacheException("Write permission denied");
        }

        FsPath parentDir = fullPath.getParent();
        FileMetaData parentMD;

        try {
            parentMD = getFileMetaData(parentDir);
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.FILE_NOT_FOUND:
            case CacheException.NOT_IN_TRASH:
            case CacheException.DIR_NOT_EXISTS:
                if (!createDir) {
                    throw new DirNotExistsCacheException("Directory does not exist");
                }
                parentMD = makePnfsDir(parentDir);
                break;

            default:
                throw e;
            }
        }

        // at this point the parent directory exists, now we check
        // permissions
        if (!checkWritePermission(parentMD)) {
            throw new PermissionDeniedCacheException("No permission to create file");
        }

        // create the actual PNFS entry with parent uid:gid
        PnfsCreateEntryMessage msg =
            _pnfs.createPnfsEntry(fullPath.toString(),
                                  parentMD.getUid(),
                                  parentMD.getGid(),
                                  0644);

        // storageinfo must be available for the newly created PNFS entry
        if (msg.getStorageInfo() == null) {
            try {
                _log.error(String.format("Storage info is missing after create [%s].", fullPath));
                _pnfs.deletePnfsEntry(msg.getPnfsId(), fullPath.toString());
            } catch (FileNotFoundCacheException e) {
                // File was already gone, so never mind.
            } catch (CacheException e) {
                _log.error(String.format("Failed to delete partial entry [%s].", fullPath));
            }

            throw new CacheException("Failed to create file (failed to extract storage info)");
        }

        return msg;
    }

    public void deletePnfsEntry(PnfsId pnfsId)
        throws CacheException
    {
        _pnfs.deletePnfsEntry(pnfsId);
    }

    public InetSocketAddress askForFile(String pool, PnfsId pnfsId,
                                        StorageInfo storageInfo,
                                        XrootdProtocolInfo protocolInfo,
                                        boolean isWrite)
        throws CacheException, InterruptedException
    {
        _log.info("Trying pool " + pool + " for " + (isWrite ? "Write" : "Read"));
        PoolIoFileMessage poolMessage =
            isWrite
            ? (PoolIoFileMessage) new PoolAcceptFileMessage(pool, pnfsId, protocolInfo, storageInfo)
            : (PoolIoFileMessage) new PoolDeliverFileMessage(pool, pnfsId, protocolInfo, storageInfo);


        // specify the desired mover queue
        poolMessage.setIoQueueName(_ioQueue);

        // the transaction string will be used by the pool as
        // initiator (-> table join in Billing DB)
        poolMessage.setInitiator(_transactionPrefix + protocolInfo.getXrootdFileHandle());
        poolMessage.setReplyRequired(true);
        poolMessage.setId(protocolInfo.getXrootdFileHandle());

        // PoolManager must be on the path for return message
        // (DoorTransferFinished)
        CellPath path =
            (CellPath) _poolManagerStub.getDestinationPath().clone();
        path.add(pool);

        Integer key = protocolInfo.getXrootdFileHandle();
        synchronized (_redirectTable) {
            _waitingForRedirect.add(key);
        }
        try {
            PoolIoFileMessage poolReply =
                _poolStub.sendAndWait(path, poolMessage);

            _log.info("Pool " + pool +
                      (isWrite ? " will accept file" : " will deliver file ") +
                      pnfsId);

            synchronized (_redirectTable) {
                while (!_redirectTable.containsKey(key)) {
                    _log.info("waiting for redirect message from pool "+pool+
                              " (pnfsId="+pnfsId+" fileHandle="+key+")");
                    _redirectTable.wait();
                }

                /* null is a special value we added to the map when no
                 * valid interface was returned by the pool.
                 */
                InetSocketAddress newAddress = _redirectTable.get(key);
                if (newAddress == null) {
                    throw new CacheException("Pool failed to open TCP socket");
                }

                _log.info(String.format("Redirecting to %s@%s", pnfsId, pool));
                return newAddress;
            }
        } finally {
            synchronized (_redirectTable) {
                _redirectTable.remove(key);
                _waitingForRedirect.remove(key);
            }
        }
    }

    public String askForPool(PnfsId pnfsId, StorageInfo storageInfo,
                             ProtocolInfo protocolInfo, boolean isWrite)
        throws CacheException, InterruptedException
    {
        _log.debug("asking Poolmanager for "+ (isWrite ? "write" : "read") +
                   " pool for " + pnfsId);

        PoolMgrSelectPoolMsg request;
        if (isWrite) {
            request = new PoolMgrSelectWritePoolMsg(pnfsId, storageInfo, protocolInfo, 0L);
        } else {
            request = new PoolMgrSelectReadPoolMsg(pnfsId, storageInfo, protocolInfo, 0L);
        }

        return _poolManagerStub.sendAndWait(request).getPoolName();
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

    public void messageArrived(XrootdDoorAdressInfoMessage msg)
    {
        _log.info("received redirect msg from mover");
        synchronized (_redirectTable) {
            Integer key = msg.getXrootdFileHandle();
            if (_waitingForRedirect.contains(key)) {
                // pick the first IPv4 address from the collection at this
                // point, we can't determine, which of the pool
                // IP-addresses is the right one, so we select the first
                Collection<NetIFContainer> interfaces =
                    Collections.checkedCollection(msg.getNetworkInterfaces(),
                                                  NetIFContainer.class);
                Inet4Address ip = getFirstIpv4(interfaces);

                if (ip != null) {
                    InetSocketAddress address =
                        new InetSocketAddress(ip, msg.getServerPort());
                    _redirectTable.put(key, address);
                } else {
                    _log.warn("error: no valid IP-adress received from pool. Redirection not possible");

                    // we have to put a null address to at least notify the
                    // right xrootd thread about the failure
                    _redirectTable.put(key, null);
                }

                _redirectTable.notifyAll();
            }
        }
    }

    public void messageArrived(DoorTransferFinishedMessage msg)
    {
        if ((msg.getProtocolInfo() instanceof XrootdProtocolInfo)) {
            XrootdProtocolInfo info =
                (XrootdProtocolInfo) msg.getProtocolInfo();
            int rc = msg.getReturnCode();
            if (rc == 0) {
                _log.info(String.format("Transfer %s@%s finished",
                                        msg.getPnfsId(), msg.getPoolName()));
            } else {
                _log.info(String.format("Transfer %s@%s failed: %s (error code=%d)",
                                        msg.getPnfsId(), msg.getPoolName(),
                                        msg.getErrorObject(), rc));
            }

            synchronized (_redirectTable) {
                Integer key = info.getXrootdFileHandle();
                if (_waitingForRedirect.contains(key)) {
                    _redirectTable.put(key, null);
                }
                _redirectTable.notifyAll();
            }
        }
    }

    public boolean isReadOnly()
    {
        return _isReadOnly;
    }

    public FileMetaData makePnfsDir(String path) throws CacheException
    {
        return makePnfsDir(createFullPath(path));
    }

    private FileMetaData makePnfsDir(FsPath fullPath) throws CacheException
    {
        _log.info("about to create directory: " + fullPath);

        /* Get parent directory.
         */
        FsPath parentDir = fullPath.getParent();

        /* Check whether parent is a directory and has write permissions.
         *
         * FIXME: Use permission handler.
         */
        FileMetaData metadata;
        try {
            metadata = getFileMetaData(parentDir);
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.FILE_NOT_FOUND:
            case CacheException.NOT_IN_TRASH:
                _log.info("creating parent directory " + parentDir + " first");

                // create parent directory recursively
                metadata = makePnfsDir(parentDir);
                break;

            default:
                throw e;
            }
        }

        if (!checkWritePermission(metadata)) {
            throw new CacheException("No permission to create directory " +
                                     fullPath);
        }

        // at this point we assume that the parent directory exists
        // and has all necessary permissions

        /* Create the directory via PNFS.
         */
        PnfsCreateEntryMessage message =
            _pnfs.createPnfsDirectory(fullPath.toString(),
                                      metadata.getUid(),
                                      metadata.getGid(),
                                      0755);

        /* In case of incomplete create, delete the directory right
         * away.
         */
        if (message.getStorageInfo() == null) {
            _log.error("Error creating directory " + fullPath +
                       " (no storage info)");
            if (message.getPnfsId() != null) {
                try {
                    _pnfs.deletePnfsEntry(message.getPnfsId(),
                                          fullPath.toString());
                } catch (FileNotFoundCacheException e) {
                    // Already gone, so never mind
                } catch (CacheException e) {
                    _log.error(e.toString());
                }
            }

            throw new CacheException("Failed to create directory " + fullPath);
        }

        _log.info("Created directory " + fullPath);

        return message.getMetaData();
    }

    public boolean checkWritePermission(FileMetaData meta)
    {
        Permissions user = meta.getUserPermissions();
        // Permissions group = meta.getGroupPermissions();

        return meta.isDirectory() && user.canWrite() && user.canExecute();
        // return meta.isDirectory() && user.canWrite() && user.canExecute() && group.canWrite() && group.canExecute();
    }

    public FileMetaData getFileMetaData(String path) throws CacheException
    {
        return getFileMetaData(createFullPath(path));
    }

    private FileMetaData getFileMetaData(FsPath fullPath) throws CacheException
    {
        return _pnfs.getFileMetaDataByPath(fullPath.toString()).getMetaData();
    }

    public FileMetaData[] getMultipleFileMetaData(String[] allPaths)
        throws CacheException
    {
        FileMetaData[] allMetas = new FileMetaData[allPaths.length];

        // TODO: Use SpreadAndWait
        for (int i = 0; i < allPaths.length; i++) {
            try {
                allMetas[i] = getFileMetaData(allPaths[i]);
            } catch (CacheException e) {
                if (e.getRc() != CacheException.FILE_NOT_FOUND &&
                    e.getRc() != CacheException.NOT_IN_TRASH) {
                    throw e;
                }
            }
        }
        return allMetas;
    }

    public void sendBillingInfo(InetSocketAddress client,
                                DoorRequestMsgWrapper wrapper)
    {
        DoorRequestInfoMessage msg =
            new DoorRequestInfoMessage(getCellName() + "@" + getCellDomainName());
        msg.setClient(client.getHostName());
        msg.setTransactionTime(System.currentTimeMillis());

        msg.setPath(wrapper.getPath());

        if (wrapper.getUser() != null) {
            msg.setOwner(wrapper.getUser());
        }
        if (wrapper.getPnfsId() != null) {
            msg.setPnfsId(wrapper.getPnfsId());
        } else
            msg.setPnfsId(new PnfsId("000000000000000000000000"));

        if (wrapper.getErrorCode() != 0) {
            msg.setResult(wrapper.getErrorCode(), wrapper.getErrorMsg());
        }

        msg.setTrasaction(_transactionPrefix + wrapper.getFileHandle());

        try {
            sendMessage(new CellMessage(new CellPath("billing"), msg));
        } catch (NoRouteToCellException e){
            _log.warn("Could not send billing info: " + e);
        }

        _log.info(msg.toString());
    }
}
