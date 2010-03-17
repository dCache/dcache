package org.dcache.doors;

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
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.security.GeneralSecurityException;

import org.dcache.vehicles.XrootdDoorAdressInfoMessage;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.core.connection.PhysicalXrootdConnection;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.security.AbstractAuthorizationFactory;
import org.dcache.xrootd.util.DoorRequestMsgWrapper;

import org.dcache.cells.AbstractCell;
import org.dcache.cells.Option;

import diskCacheV111.movers.NetIFContainer;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
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
import dmg.util.Args;
import dmg.util.StreamEngine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XrootdDoor extends AbstractCell
{
    private final static Logger _log = LoggerFactory.getLogger(XrootdDoor.class);

    private final static String POOL_MANAGER = "PoolManager";

    private final static String PNFS_MANAGER = "PnfsManager";

    private final static String XROOTD_PROTOCOL_STRING = "Xrootd";
    private final static int XROOTD_PROTOCOL_MAJOR_VERSION = 2;
    private final static int XROOTD_PROTOCOL_MINOR_VERSION = 7;

    /**
     * The list of paths which are authorized for xrootd write access
     * (via dCacheSetup)
     */
    private static List<String> _authorizedWritePaths;

    /**
     * Holds the factory for the specific plugin. the plugin is loaded
     * only once.
     */
    private static AbstractAuthorizationFactory _authzFactory;

    /**
     * Is set true only once (when plugin loading didn't succeed) to
     * avoid multiple trials.
     */
    private static boolean _authzPluginLoadFailed = false;

    private static long _cellMessageTimeout = 3000;

    private PnfsHandler _pnfs;

    private Socket _doorSocket;

    /**
     * File handle to port address mapping. Access is protected by
     * synchronization on the map.
     */
    private Map<Integer,InetSocketAddress> _redirectTable = new HashMap();

    /**
     * File handle to Xrootd logical stream ID mapping.
     */
    private Map<Integer, Integer> _logicalStreamTable
        = Collections.synchronizedMap(new HashMap());

    private int _fileHandleCounter = 0;

    private XrootdDoorController _controller;

    private PhysicalXrootdConnection _physicalXrootdConnection;

    private boolean _closeInProgress = false;

    /**
     * Indicates for this xrootd connection whether authorization is
     * required.
     */
    private boolean _authzRequired = false;

    /**
     * Dirty hack: will be deprecated soon
     */
    private String _noStrongAuthz;

    /**
     * The prefix of the transaction string used for billing.
     */
    private String _transactionPrefix;

    /**
     * Forbid write access by default.
     */
    @Option(
        name = "isReadOnly",
        defaultValue = "false",
        description = "Whether to only allow read access"
    )
    private boolean _isReadOnly;

    /**
     * The actual mover queue on the pool onto which this request gets
     * scheduled.
     */
    @Option(
        name = "io-queue"
    )
    private String _ioQueue;

    /**
     * The number of max open files per physical xrootd connection.
     */
    @Option(
        name = "maxFileOpensPerLogin",
        description = "The maximum of open files per physical xrootd connection",
        defaultValue = "5000"
    )
    private int _maxFileOpens;

    public XrootdDoor(String name, StreamEngine engine, Args args)
    {
        super(name, args);

        try {
            _doorSocket = engine.getSocket();
            doInit();
        } catch (InterruptedException e) {
            // Super class has logged the incident
        } catch (ExecutionException e) {
            // Super class has logged the incident
        }
    }

    @Override
    protected void init()
        throws IOException
    {
        Args args = getArgs();

        _pnfs = new PnfsHandler(this, new CellPath(PNFS_MANAGER));

        // look for colon-seperated path list, which, if present, is
        // used to allow write access to these paths only
        String pathListString = args.getOpt("allowedPaths");
        if (!(pathListString == null || pathListString.length() == 0)
            && _authorizedWritePaths == null) {

            parseAllowedWritePaths(pathListString);
        }

        // try to load authorization plugin if required by batch file
        if (!(args.getOpt("authzPlugin") == null
              || args.getOpt("authzPlugin").length() == 0
              || "none".equals(args.getOpt("authzPlugin")))) {

            _authzRequired = true;
            loadAuthzPlugin(args);

            // dirty hack (about to be deprecated soon) for ALICE set
            // the authz logic during file open being less strict
            String noStrongAuthzString = args.getOpt("nostrongauthorization");
            if ("read".equalsIgnoreCase(noStrongAuthzString)
                || "write".equalsIgnoreCase(noStrongAuthzString)
                || "always".equalsIgnoreCase(noStrongAuthzString)) {

                _noStrongAuthz = noStrongAuthzString;
            }
        }

        _transactionPrefix =
            String.format("door:%s@%s:",
                          getNucleus().getCellName(),
                          getNucleus().getCellDomainName());

        // Create new XrootdConnection based on existing socket
        _physicalXrootdConnection =
            new PhysicalXrootdConnection(_doorSocket,
                                         XrootdProtocol.LOAD_BALANCER);

        // set controller for this connection to handle login, auth
        // and connection-specific settings
        _controller = new XrootdDoorController(this, _physicalXrootdConnection);
        _physicalXrootdConnection.setConnectionListener(_controller);
    }

    private void parseAllowedWritePaths(String pathListString)
    {
        String[] paths = pathListString.split(":");
        List<String> list = new ArrayList(paths.length);

        for (String path: paths) {
            if (!path.endsWith("/")) {
                path += "/";
            }

            list.add(path);
            _log.warn("allowed write path: "+path);
        }
        Collections.sort(list);
        _authorizedWritePaths = list;
    }

    public void cleanUp()
    {
        try {
            if (!isCloseInProgress()) {
                _controller.shutdownXrootd();
            }
        } finally {
            super.cleanUp();
        }
    }

    public void getInfo(PrintWriter pw)
    {
        pw.println("Xrootd Door");
        pw.println(String.format("Protocol Version %d.%d",
                                 XROOTD_PROTOCOL_MAJOR_VERSION,
                                 XROOTD_PROTOCOL_MINOR_VERSION));

        if (_physicalXrootdConnection.getStatus().isConnected()) {
            pw.println("Connected with "+_physicalXrootdConnection.getNetworkConnection().getSocket().getInetAddress());
        } else {
            pw.println("Not connected");
        }

        pw.println("number of open files: "+_logicalStreamTable.size());
        pw.println("number of logical streams: " + _physicalXrootdConnection.getStreamManager().getNumberOfStreams());
    }

    /**
     * Selects a pool and asks it to transfer a file.
     *
     * @param pnfsid      PNFS ID of file to transfer
     * @param storageInfo Storage info of file to transfer
     * @param fileHandle  xrootd file handle of file to transfer
     * @param checksum    checksum of open request
     * @param isWrite     true for uploads, false for downloads
     * @return The address on which the mover will accept the data connection
     */
    public InetSocketAddress
        transfer(PnfsId pnfsid, StorageInfo storageInfo,
                 int fileHandle, long checksum, boolean isWrite)
        throws CacheException, InterruptedException
    {
        InetSocketAddress client =
            _physicalXrootdConnection.getNetworkConnection().getClientSocketAddress();
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

        InetSocketAddress redirectAddress = null;
        do {
            // ask the Poolmanager for a pool
            String pool = askForPool(pnfsid,
                                     storageInfo,
                                     protocolInfo,
                                     isWrite);
            try {
                // ask the pool to prepare the transfer
                redirectAddress =
                    askForFile(pool,
                               pnfsid,
                               storageInfo,
                               protocolInfo,
                               isWrite);
            } catch (CacheException e) {
                _log.warn("Pool error: " + e.getMessage());
            }
        } while (redirectAddress == null);

        return redirectAddress;
    }

    public PnfsGetStorageInfoMessage getStorageInfo(String path)
        throws CacheException
    {
        return _pnfs.getStorageInfoByPath(path);
    }

    public PnfsCreateEntryMessage
        createNewPnfsEntry(String path, boolean createDir)
        throws CacheException
    {
        // get parent directory path by truncating filename
        String parentDir = path.substring(0, path.lastIndexOf("/"));
        FileMetaData parentMD;

        try {
            parentMD = getFileMetaData(parentDir);
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.FILE_NOT_FOUND:
            case CacheException.DIR_NOT_EXISTS:
                if (!createDir) {
                    throw new CacheException(CacheException.DIR_NOT_EXISTS,
                                             "Directory does not exist");
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
            throw new CacheException(CacheException.PERMISSION_DENIED,
                                     "No permission to create file");
        }

        // create the actual PNFS entry with parent uid:gid
        PnfsCreateEntryMessage msg =
            _pnfs.createPnfsEntry(path,
                                  parentMD.getUid(),
                                  parentMD.getGid(),
                                  0644);

        // storageinfo must be available for the newly created PNFS entry
        if (msg.getStorageInfo() == null) {
            try {
                _log.error(String.format("Storage info is missing after create [%s].", path));
                _pnfs.deletePnfsEntry(msg.getPnfsId(), path);
            } catch (CacheException e) {
                _log.error(String.format("Failed to delete partial entry [%s].", path));
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

    /**
     * Sends a message to a cell and waits for the reply. The reply is
     * expected to contain a message object of the same type as the
     * message object that was sent, and the return code of that
     * message is expected to be zero. If either is not the case,
     * Exception is thrown.
     *
     * @param  path    the path to the cell to which to send the message
     * @param  msg     the message object to send
     * @param  timeout timeout in milliseconds
     * @return         the message object from the reply
     * @throws InterruptedException If the thread is interrupted
     * @throws CacheException If the message could not be sent, a
     *       timeout occured, the object in the reply was of the wrong
     *       type, or the return code was non-zero.
     */
    private <T extends Message> T sendAndWait(CellPath path, T msg, long timeout)
        throws CacheException, InterruptedException
    {
        CellMessage replyMessage = null;
        try {
            replyMessage = sendAndWait(new CellMessage(path, msg), timeout);
        } catch (NoRouteToCellException e) {
            String errmsg =
                String.format("Cannot send message to %s. Got error: %s",
                              path, e.getMessage());
            _log.warn(errmsg.toString());

            /* We report this as a timeout, since from the callers
             * point of view a timeout due to a lost message or a
             * missing routing entry is pretty much the same.
             */
            throw new CacheException(CacheException.TIMEOUT, errmsg);
        }

        if (replyMessage == null) {
            String errmsg = String.format("Request to %s timed out.", path);
            throw new CacheException(CacheException.TIMEOUT, errmsg);
        }

        Object replyObject = replyMessage.getMessageObject();
        if (!(msg.getClass().isInstance(replyObject))) {
            String errmsg = "Got unexpected message of class " +
                            replyObject.getClass() + " from " +
                            replyMessage.getSourceAddress();
            _log.error(errmsg.toString());
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     errmsg);
        }

        T reply = (T)replyObject;
        if (reply.getReturnCode() != 0) {
            throw new CacheException(reply.getReturnCode(),
                                     String.format("Got error from %s: %s",
                                                   path,
                                                   reply.getErrorObject()));
        }

        return reply;
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

        // PoolManager must be on the path for return message
        // (DoorTransferFinished)
        CellPath path = new CellPath(POOL_MANAGER);
        path.add(pool);

        PoolIoFileMessage poolReply =
            sendAndWait(path, poolMessage, _cellMessageTimeout);

        _log.info("Pool " + pool +
                  (isWrite ? " will accept file" : " will deliver file ") +
                  pnfsId);

        Integer key = protocolInfo.getXrootdFileHandle();

        synchronized (_redirectTable) {
            while (!_redirectTable.containsKey(key)) {
                _log.info("waiting for redirect message from pool "+pool+
                          " (pnfsId="+pnfsId+" fileHandle="+key+")");
                _redirectTable.wait();
            }

            /* null is a special value we added to the map when no valid
             * interface was returned by the pool.
             */
            InetSocketAddress newAddress = _redirectTable.remove(key);
            if (newAddress == null) {
                throw new CacheException("Pool responded with invalid redirection address, transfer failed");
            }

            _log.info("got redirect message from pool "+pool+" (pnfsId="+
                      pnfsId+" fileHandle="+key+")");
            return newAddress;
        }
    }

    public String askForPool(PnfsId pnfsId, StorageInfo storageInfo,
                             ProtocolInfo protocolInfo, boolean isWrite)
        throws CacheException, InterruptedException
    {
        _log.info("asking Poolmanager for "+ (isWrite ? "write" : "read") +
                  "pool for PnfsId " + pnfsId);

        //
        // ask for a pool
        //
        PoolMgrSelectPoolMsg request;

        if (isWrite) {
            request = new PoolMgrSelectWritePoolMsg(pnfsId, storageInfo, protocolInfo, 0L);
        } else {
            request = new PoolMgrSelectReadPoolMsg(pnfsId, storageInfo, protocolInfo, 0L);
        }

        // Wait almost forever. Taking the PoolMgrSelectPoolMsg very
        // long could be caused by a restage from tape to pool OR the
        // request is suspended in PoolManager (can be checked by 'rc
        // ls' in admin interface)
        long poolMgrTimeout = Long.MAX_VALUE;	// timeout in ms
        request =
            sendAndWait(new CellPath(POOL_MANAGER), request, poolMgrTimeout);

        return request.getPoolName();
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
            _log.debug("got lock on _sync");

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
                _redirectTable.put(msg.getXrootdFileHandle(), address);
            } else {
                _log.warn("error: no valid IP-adress received from pool. Redirection not possible");

                // we have to put a null address to at least notify the
                // right xrootd thread about the failure
                _redirectTable.put(msg.getXrootdFileHandle(), null);
            }

            _redirectTable.notifyAll();
        }
    }

    public void messageArrived(DoorTransferFinishedMessage msg)
    {
        if ((msg.getProtocolInfo() instanceof XrootdProtocolInfo)) {
            XrootdProtocolInfo protoInfo =
                (XrootdProtocolInfo) msg.getProtocolInfo();
            int fileHandle = protoInfo.getXrootdFileHandle();

            Integer streamID = _logicalStreamTable.remove(fileHandle);
            if (streamID != null) {
                _log.info("received DoorTransferFinished-Message from mover, cleaning up (PnfsId="
                          + msg.getPnfsId() + " fileHandle="+fileHandle+")");

                _physicalXrootdConnection.getStreamManager().
                    getStreamByID(streamID).removeFile(fileHandle);
            }
        }
    }

    public void newFileOpen(int fileHandle, int streamID)
    {
        _logicalStreamTable.put(fileHandle, streamID);
    }

    public void clearOpenFiles()
    {
        _logicalStreamTable.clear();
    }

    public InetAddress getDoorHost()
    {
        return _doorSocket.getLocalAddress();
    }

    public int getDoorPort()
    {
        return _doorSocket.getLocalPort();
    }

    //	returns a filehandle unique within this Xrootd-Door instance
    public synchronized int getNewFileHandle()
    {
        return ++_fileHandleCounter;
    }

    public Socket getDoorSocket()
    {
        return _doorSocket;
    }

    public synchronized boolean isCloseInProgress()
    {
        if (_closeInProgress == false) {
            _closeInProgress = true;
            return false;
        }

        return true;
    }

    public boolean isReadOnly()
    {
        return _isReadOnly;
    }

    public boolean authzRequired()
    {
        return _authzRequired;
    }

    public AbstractAuthorizationFactory getAuthzFactory()
    {
        return _authzFactory;
    }

    private void loadAuthzPlugin(Args args)
    {
        if (getAuthzFactory() != null || // authorization plugin (static factory) already loaded ?
            _authzPluginLoadFailed) {	 // don't try again to load if it failed once
            return;
        }

        _log.info("trying to load authz plugin");

        try {
            AbstractAuthorizationFactory newAuthzFactory =
                AbstractAuthorizationFactory.getFactory(args.getOpt("authzPlugin"));

            _log.info("trying to find all options required by the plugin");

            // get names of all options required by the plugin
            String[] names = newAuthzFactory.getRequiredOptions();
            Map<String,String> options = new HashMap();

            // try to load that options from the batchfile
            for (String name: names) {
                String value = args.getOpt(name);
                if (value == null || value.equals("")) {
                    /* Maybe not the right exception. Logic should be
                     * restructured.
                     */
                    throw new NoSuchElementException("required option '"+name+
                                                     "' not found in batchfile");
                } else {
                    options.put(name, value);
                }
            }

            newAuthzFactory.initialize(options);

            _authzFactory = newAuthzFactory;
            _log.info("authorization plugin initialised successfully!");
        } catch (NoSuchElementException e) {
            _log.warn(e.getMessage());
            _authzPluginLoadFailed = true;
        } catch (GeneralSecurityException e) {
            _log.warn("error initializing the authorization plugin: "+ e);
            _authzPluginLoadFailed = true;
        } catch (ClassNotFoundException e) {
            _log.warn("Could not load authorization plugin " +
                      args.getOpt("authzPlugin")+" cause: "+e);
            _authzPluginLoadFailed = true;
        }

        if (_authzPluginLoadFailed) {
            _log.warn("Loading authorization plugin failed. All subsequent xrootd requests will fail due to this. Please change batch file configuration and restart xrootd door.");
        }
    }

    public FileMetaData makePnfsDir(String path) throws CacheException
    {
        _log.info("about to create directory: " + path);

        /* Get parent directory.
         */
        String parentDir = path;
        if (parentDir.endsWith("/")) {
            parentDir = parentDir.substring(0, parentDir.length() - 1);
        }
        parentDir = parentDir.substring(0, parentDir.lastIndexOf("/"));

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
                _log.info("creating parent directory " + parentDir + " first");

                // create parent directory recursively
                metadata = makePnfsDir(parentDir);
                break;

            default:
                throw e;
            }
        }

        if (!checkWritePermission(metadata)) {
            throw new CacheException("No permission to create directory "+path);
        }

        // at this point we assume that the parent directory exists
        // and has all necessary permissions

        /* Create the directory via PNFS.
         */
        PnfsCreateEntryMessage message =
            _pnfs.createPnfsDirectory(path,
                                      metadata.getUid(),
                                      metadata.getGid(),
                                      0755);

        /* In case of incomplete create, delete the directory right
         * away.
         */
        if (message.getStorageInfo() == null) {
            _log.error("Error creating directory " + path +
                       " (no storage info)");
            if (message.getPnfsId() != null) {
                try {
                    _pnfs.deletePnfsEntry(message.getPnfsId(), path);
                } catch (CacheException e) {
                    _log.error(e.toString());
                }
            }

            throw new CacheException("Cannot create directory " + path +
                                     " in PNFS");
        }

        _log.info("created directory " + path);

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
        return new FileMetaData(_pnfs.getFileAttributes(path, FileMetaData.getKnownFileAttributes()));
    }

    public FileMetaData[] getMultipleFileMetaData (String[] allPaths)
    {
        FileMetaData[] allMetas = new FileMetaData[allPaths.length];

        // TODO: Use SpreadAndWait
        for (int i = 0; i < allPaths.length; i++) {
            try {
                allMetas[i] = getFileMetaData(allPaths[i]);
            } catch (CacheException e) {
                // we just move on in case a single path was not found
                _log.info("failed to get meta data of " + allPaths[i]);
            }
        }
        return allMetas;
    }

    public String noStrongAuthorization()
    {
        return _noStrongAuthz;
    }

    public List<String> getAuthorizedWritePaths()
    {
        return _authorizedWritePaths;
    }

    public void sendBillingInfo(DoorRequestMsgWrapper wrapper)
    {
        DoorRequestInfoMessage msg =
            new DoorRequestInfoMessage(getNucleus().getCellName()+"@"+
                                       getNucleus().getCellDomainName());
        msg.setClient(getDoorSocket().getInetAddress().getHostName());
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

        msg.setTransaction(_transactionPrefix + wrapper.getFileHandle());

        try {
            sendMessage(new CellMessage(new CellPath("billing"), msg));
        } catch (NoRouteToCellException e){
            _log.warn("Could not send billing info: " + e);
        }

        _log.info(msg.toString());
    }

    public int getMaxFileOpens()
    {
        return _maxFileOpens;
    }
}
