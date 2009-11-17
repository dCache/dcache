package org.dcache.webdav;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Date;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.net.ServerSocket;
import java.net.InetAddress;
import javax.security.auth.Subject;
import java.security.AccessController;

import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.ResourceFactory;
import com.bradmcevoy.http.SecurityManager;
import com.bradmcevoy.http.XmlWriter;

import com.sun.security.auth.UserPrincipal;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.GFtpProtocolInfo;
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
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.DoorRequestInfoMessage;

import org.dcache.cells.CellStub;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.acl.Origin;
import org.dcache.auth.Subjects;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.ListDirectoryHandler;

import dmg.util.Args;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CollectionFactory;

import org.apache.log4j.Logger;

import static org.dcache.namespace.FileType.*;
import static org.dcache.namespace.FileAttribute.*;

/**
 * This ResourceFactory exposes the dCache name space through the
 * Milton WebDAV framework.
 */
public class DcacheResourceFactory
    extends AbstractCellComponent
    implements ResourceFactory, CellMessageReceiver
{
    private static final Logger _log =
        Logger.getLogger(DcacheResourceFactory.class);

    private static final Set<FileAttribute> REQUIRED_ATTRIBUTES =
        EnumSet.of(TYPE, PNFSID, CREATION_TIME, MODIFICATION_TIME, SIZE,
                   MODE, OWNER, OWNER_GROUP);

    private static final int FILE_UMASK = 0644;
    private static final int FILE_UMASK_ANONYMOUS = 0666;
    private static final int DIRECTORY_UMASK = 0755;
    private static final int DIRECTORY_UMASK_ANONYMOUS = 0777;

    private static final String PROTOCOL_INFO_NAME = "Http";
    private static final int PROTOCOL_INFO_MAJOR_VERSION = 1;
    private static final int PROTOCOL_INFO_MINOR_VERSION = 1;
    private static final int PROTOCOL_INFO_UNKNOWN_PORT = 0;

    private static final String RELAY_PROTOCOL_INFO_NAME = "GFtp";
    private static final int RELAY_PROTOCOL_INFO_MAJOR_VERSION = 1;
    private static final int RELAY_PROTOCOL_INFO_MINOR_VERSION = 0;
    private static final int RELAY_PROTOCOL_INFO_STREAMS = 1;
    private static final int RELAY_PROTOCOL_INFO_BUFFERSIZE = 0;
    private static final int RELAY_PROTOCOL_INFO_OFFSET = 0;
    private static final int RELAY_PROTOCOL_INFO_SIZE = 0;


    /**
     * Map used to map pool redirect messages to blocking queues used
     * to hand over the redirection URL to the proper transfer
     * request. All access must be synchronized on the monitor of the
     * map.
     */
    private final Map<PnfsId,List<LinkedBlockingQueue<String>>> _redirectTable =
        CollectionFactory.newHashMap();

    /**
     * In progress write transfers.
     */
    private final Set<Transfer> _transfers =
        new ConcurrentSkipListSet<Transfer>();

    /**
     * In progress write transfers with a mover.
     */
    private final Map<PnfsId,Transfer> _movers =
        CollectionFactory.newConcurrentHashMap();

    private ListDirectoryHandler _list;

    private long _killTimeout = 1000;
    private long _transferConfirmationTimeout = 60000;
    private int _bufferSize = 65536;
    private CellStub _poolStub;
    private CellStub _poolManagerStub;
    private CellStub _billingStub;
    private PnfsHandler _pnfs;
    private SecurityManager _securityManager;
    private String _ioQueue;
    private String _cellName;
    private String _domainName;
    private FsPath _rootPath = new FsPath();
    private List<FsPath> _allowedPaths =
        Collections.singletonList(new FsPath());

    public DcacheResourceFactory()
    {
        _securityManager = new NullSecurityManager();
    }

    public long getKillTimeout()
    {
        return _killTimeout;
    }

    public void setKillTimeout(long timeout)
    {
        _killTimeout = timeout;
    }

    public long getTransferConfirmationTimeout()
    {
        return _transferConfirmationTimeout;
    }

    public void setTransferConfirmationTimeout(long timeout)
    {
        _transferConfirmationTimeout = timeout;
    }

    public int getBufferSize()
    {
        return _bufferSize;
    }

    public void setBufferSize(int bufferSize)
    {
        _bufferSize = bufferSize;
    }

    /**
     * Returns the root path.
     */
    public String getRootPath()
    {
        return _rootPath.toString();
    }

    /**
     * Sets the root path of the WebDAV server. This path forms the
     * root of the WebDAV share. All WebDAV access will be relative to
     * this path.
     */
    public void setRootPath(String path)
    {
        _rootPath = new FsPath(path);
    }

    /**
     * Set the list of paths for which we allow access. Paths are
     * separated by a colon. This paths are relative to the root path.
     */
    public void setAllowedPaths(String s)
    {
        List<FsPath> list = new ArrayList();
        for (String path: s.split(":")) {
            list.add(new FsPath(path));
        }
        _allowedPaths = list;
    }

    /**
     * Returns the list of allowed paths.
     */
    public String getAllowedPaths()
    {
        StringBuilder s = new StringBuilder();
        for (FsPath path: _allowedPaths) {
            if (s.length() > 0) {
                s.append(':');
            }
            s.append(path);
        }
        return s.toString();
    }

    /**
     * Return the pool IO queue to use for WebDAV transfers.
     */
    public String getIoQueue()
    {
        return (_ioQueue == null) ? "" : _ioQueue;
    }

    /**
     * Sets the pool IO queue to use for WebDAV transfers.
     */
    public void setIoQueue(String ioQueue)
    {
        _ioQueue = (ioQueue != null && !ioQueue.isEmpty()) ? ioQueue : null;
    }

    /**
     * Sets the cell stub for PnfsManager communication.
     */
    public void setPnfsStub(CellStub stub)
    {
        _pnfs = new PnfsHandler(stub);
    }

    /**
     * Sets the cell stub for pool communication.
     */
    public void setPoolStub(CellStub stub)
    {
        _poolStub = stub;
    }

    /**
     * Sets the cell stub for PoolManager communication.
     */
    public void setPoolManagerStub(CellStub stub)
    {
        _poolManagerStub = stub;
    }

    /**
     * Sets the cell stub for billing communication.
     */
    public void setBillingStub(CellStub stub)
    {
        _billingStub = stub;
    }

    /**
     * Sets the ListDirectoryHandler used for directory listing.
     */
    public void setListHandler(ListDirectoryHandler list)
    {
        _list = list;
    }

    public SecurityManager getSecurityManager()
    {
        return _securityManager;
    }

    public void setSecurityManager(SecurityManager securityManager)
    {
        _securityManager = securityManager;
    }

    /**
     * Performs component initialization. Must be called after all
     * dependencies have been injected.
     */
    public void init()
    {
        _cellName = getCellName();
        _domainName = getCellDomainName();
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("Root path    : " + getRootPath());
        pw.println("Allowed paths: " + getAllowedPaths());
        pw.println("IO queue     : " + getIoQueue());
    }

    @Override
    public Resource getResource(String host, String path)
    {
        if (_log.isDebugEnabled()) {
            _log.debug("Resolving http://" + host + "/" + path);
        }

        return getResource(getFullPath(path));
    }

    @Override
    public String getSupportedLevels()
    {
        return "1,2";
    }

    /**
     * Returns the resource object for a path.
     *
     * @param path The full path
     */
    public DcacheResource getResource(FsPath path)
    {
        if (!isAllowedPath(path)) {
            return null;
        }

        try {
            PnfsHandler pnfs = new PnfsHandler(_pnfs, getSubject());
            FileAttributes attributes =
                pnfs.getFileAttributes(path.toString(), REQUIRED_ATTRIBUTES);
            return getResource(path, attributes);
        } catch (FileNotFoundCacheException e) {
            return null;
        } catch (CacheException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the resource object for a path.
     *
     * @param path The full path
     * @param attributes The attributes of the object identified by the path
     */
    private DcacheResource getResource(FsPath path, FileAttributes attributes)
    {
        if (attributes.getFileType() == DIR) {
            return new DcacheDirectoryResource(this, path, attributes);
        } else {
            return new DcacheFileResource(this, path, attributes);
        }
    }

    /**
     * Creates a new file. The door will relay all data to a pool
     * using a GridFTP mode S data connection.
     */
    public DcacheResource
        createFile(FileAttributes parent, FsPath path,
                   InputStream inputStream, Long length)
        throws CacheException, InterruptedException, IOException
    {
        Subject subject = getSubject();

        WriteTransfer transfer =
            new WriteTransfer(subject, path, new PnfsHandler(_pnfs, subject));
        _transfers.add(transfer);
        try {
            boolean success = false;
            transfer.createNameSpaceEntry(parent);
            try {
                PnfsId pnfsid = transfer.getPnfsId();
                transfer.setLength(length);
                transfer.selectPool();
                transfer.openServerChannel();
                _movers.put(pnfsid, transfer);
                try {
                    transfer.startMover();
                    try {
                        transfer.relayData(inputStream);
                        if (!transfer.join(_transferConfirmationTimeout)) {
                            throw new CacheException("Missing transfer confirmation from pool");
                        }
                    } finally {
                        transfer.setStatus(null);
                        transfer.killMover();
                    }
                } finally {
                    _movers.remove(pnfsid);
                    transfer.closeServerChannel();
                }

                transfer.notifyBilling(0, "");
                success = true;
            } finally {
                if (!success) {
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
        } catch (IOException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.getMessage());
            throw e;
        } catch (RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.getMessage());
            throw e;
        } finally {
            _transfers.remove(transfer);
        }

        return getResource(path);
    }

    /**
     * Performs a directory listing returning a list of Resource
     * objects.
     */
    public List<DcacheResource> list(final FsPath path)
        throws InterruptedException, CacheException
    {
        final List<DcacheResource> result = new ArrayList<DcacheResource>();
        DirectoryListPrinter printer =
            new DirectoryListPrinter()
            {
                public Set<FileAttribute> getRequiredAttributes()
                {
                    return REQUIRED_ATTRIBUTES;
                }

                public void print(FileAttributes dirAttr, DirectoryEntry entry)
                {
                    result.add(getResource(new FsPath(path, entry.getName()),
                                           entry.getFileAttributes()));
                }
            };

        _list.printDirectory(getSubject(), printer,
                             new File(path.toString()), null, null);
        return result;
    }

    /**
     * Performs a directory listing, writing an HTML view to an output
     * stream.
     */
    public void list(FsPath path, OutputStream out)
        throws InterruptedException, CacheException
    {
        final XmlWriter w = new XmlWriter(out);
        DirectoryListPrinter printer =
            new DirectoryListPrinter()
            {
                public Set<FileAttribute> getRequiredAttributes()
                {
                    return EnumSet.of(MODIFICATION_TIME,TYPE);
                }
                public void print(FileAttributes dirAttr, DirectoryEntry entry)
                {
                    FileAttributes attr = entry.getFileAttributes();
                    Date mtime = new Date(attr.getModificationTime());
                    w.open("tr");
                    w.open("td");
                    if (attr.getFileType() == DIR) {
                        w.begin("a").writeAtt("href", entry.getName() + "/").open().writeText(entry.getName() + "/").close();
                    } else {
                        w.begin("a").writeAtt("href", entry.getName()).open().writeText(entry.getName()).close();
                    }
                    w.close("td");
                    w.begin("td").open().writeText(mtime.toString()).close();
                    w.close("tr");
                }
            };
        w.open("html");
        w.open("body");
        w.begin("h1").open().writeText(path.toString()).close();
        w.open("table");
        _list.printDirectory(getSubject(), printer,
                             new File(path.toString()), null, null);
        w.close("table");
        w.close("body");
        w.close("html");
        w.flush();
    }

    /**
     * Deletes a file.
     */
    public void deleteFile(PnfsId pnfsid, FsPath path)
        throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, getSubject());
        pnfs.deletePnfsEntry(pnfsid, path.toString(),
                             EnumSet.of(REGULAR, LINK));
    }

    /**
     * Deletes a directory.
     */
    public void deleteDirectory(PnfsId pnfsid, FsPath path)
        throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, getSubject());
        pnfs.deletePnfsEntry(pnfsid, path.toString(),
                             EnumSet.of(DIR));
    }

    /**
     * Create a new directory.
     */
    public DcacheDirectoryResource
        makeDirectory(FileAttributes parent, FsPath path)
        throws CacheException
    {
        Subject subject = getSubject();
        long[] uids = Subjects.getUids(subject);
        long[] gids = Subjects.getGids(subject);
        int uid = (uids.length > 0) ? (int) uids[0] : parent.getOwner();
        int gid = (gids.length > 0) ? (int) gids[0] : parent.getGroup();
        int umask =
            (uids.length > 0) ? DIRECTORY_UMASK : DIRECTORY_UMASK_ANONYMOUS;
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        PnfsCreateEntryMessage reply =
            pnfs.createPnfsDirectory(path.toString(), uid, gid,
                                     umask & parent.getMode());
        FileAttributes attributes =
            pnfs.getFileAttributes(reply.getPnfsId(), REQUIRED_ATTRIBUTES);

        return new DcacheDirectoryResource(this, path, attributes);
    }

    public void move(PnfsId pnfsId, FsPath newPath)
        throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, getSubject());
        pnfs.renameEntry(pnfsId, newPath.toString());
    }

    /**
     * Returns a read URL for a file.
     *
     * @param path The full path of the file.
     * @param pnfsid The PNFS ID of the file.
     */
    public String getReadUrl(FsPath path, PnfsId pnfsid)
        throws CacheException, InterruptedException
    {
        Subject subject = getSubject();
        PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
        StorageInfo storage_info =
            pnfs.getStorageInfoByPnfsId(pnfsid).getStorageInfo();
        Origin origin = Subjects.getOrigin(subject);
        if (origin == null) {
            throw new IllegalStateException("Origin is missing");
        }

        String address = origin.getAddress().getHostAddress();

        HttpProtocolInfo protocol_info =
            new HttpProtocolInfo(PROTOCOL_INFO_NAME,
                                 PROTOCOL_INFO_MAJOR_VERSION,
                                 PROTOCOL_INFO_MINOR_VERSION,
                                 address,
                                 PROTOCOL_INFO_UNKNOWN_PORT,
                                 _cellName, _domainName, path.toString());
        String pool = askForPool(pnfsid,
                                 storage_info,
                                 protocol_info,
                                 false); /* false for read */
        return askForFile(pool,
                          pnfsid,
                          storage_info,
                          protocol_info,
                          false); /* false for read */
    }

    /**
     * Message handler for redirect messages from the pools.
     */
    public void messageArrived(HttpDoorUrlInfoMessage message)
    {
        synchronized (_redirectTable) {
            List<LinkedBlockingQueue<String>> list =
                _redirectTable.get(new PnfsId(message.getPnfsId()));
            if (list != null) {
                assert !list.isEmpty();
                list.get(0).offer(message.getUrl());
            }
        }
    }

    /**
     * Message handler for transfer completion messages from the
     * pools.
     */
    public void messageArrived(DoorTransferFinishedMessage message)
    {
        Transfer transfer = _movers.get(message.getPnfsId());
        if (transfer != null) {
            transfer.finished(message);
        }
    }

    /**
     * Obtains a pool from the PoolManager suitable for transfering a
     * specific file.
     *
     * @param pnfsId The PNFS ID of the file
     * @param storageInfo The storage info of the file
     * @param protocolInof The protocol info for the transfer
     * @param isWrite True for write transfer, false otherwise
     */
    private String askForPool(PnfsId pnfsId, StorageInfo storageInfo,
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

    /**
     * Creates an HTTP mover on a specific pool and a specific file
     * and returns the redirect URL.
     *
     * @param pool The name of the pool
     * @param pnfsId The PNFS ID of the file
     * @param storageInfo The storage info of the file
     * @param protocolInfo The protocol info of the file
     * @param isWrite True for write transfers, false otherwise
     */
    private String askForFile(String pool, PnfsId pnfsId,
                              StorageInfo storageInfo,
                              HttpProtocolInfo protocolInfo,
                              boolean isWrite)
        throws CacheException, InterruptedException
    {
        _log.info("Trying pool " + pool + " for " + (isWrite ? "write" : "read"));
        PoolIoFileMessage poolMessage =
            isWrite
            ? (PoolIoFileMessage) new PoolAcceptFileMessage(pool, pnfsId, protocolInfo, storageInfo)
            : (PoolIoFileMessage) new PoolDeliverFileMessage(pool, pnfsId, protocolInfo, storageInfo);

        // specify the desired mover queue
        poolMessage.setIoQueueName(_ioQueue);

        // the transaction string will be used by the pool as
        // initiator (-> table join in Billing DB)
//         poolMessage.setInitiator(_transactionPrefix + protocolInfo.getXrootdFileHandle());

        // PoolManager must be on the path for return message
        // (DoorTransferFinished)
        CellPath path =
            (CellPath) _poolManagerStub.getDestinationPath().clone();
        path.add(pool);

        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();
        List<LinkedBlockingQueue<String>> list;
        synchronized (_redirectTable) {
            list = _redirectTable.get(pnfsId);
            if (list == null) {
                list = new ArrayList<LinkedBlockingQueue<String>>();
                _redirectTable.put(pnfsId, list);
            }
            list.add(queue);
        }

        try {
            _poolStub.sendAndWait(path, poolMessage);

            _log.info("Pool " + pool +
                      (isWrite ? " will accept file " : " will deliver file ") +
                      pnfsId);

            return queue.take();
        } finally {
            synchronized (_redirectTable) {
                list.remove(queue);
                if (list.isEmpty()) {
                    _redirectTable.remove(list);
                }
            }
        }
    }

    /**
     * Given a path relative to the root path, this method returns a
     * full PNFS path.
     */
    private FsPath getFullPath(String path)
    {
        return new FsPath(_rootPath, new FsPath(path));
    }

    /**
     * Returns true if access to path is allowed through the WebDAV
     * door, false otherwise.
     */
    private boolean isAllowedPath(FsPath path)
    {
        for (FsPath allowedPath: _allowedPaths) {
            if (path.startsWith(allowedPath)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the current Subject of the calling thread.
     */
    private Subject getSubject()
    {
        return Subject.getSubject(AccessController.getContext());
    }

    public final static String hh_get_door_info = "[-binary]";
    public Object ac_get_door_info(Args args)
    {
        IoDoorInfo doorInfo = new IoDoorInfo(_cellName, _domainName);
        doorInfo.setProtocol("HTTP", "1.1");
        doorInfo.setOwner("");
        doorInfo.setProcess("");
        doorInfo.setIoDoorEntries(_transfers.toArray(new IoDoorEntry[0]));
        return (args.getOpt("binary") != null) ? doorInfo : doorInfo.toString();
    }

    /**
     * Encapulates information about a transfer.
     */
    private class Transfer implements Comparable<Transfer>
    {
        protected final long _startedAt;
        protected final long _sessionId;
        protected final FsPath _path;
        protected final Subject _subject;
        private String _pool;
        private Integer _moverId;
        private PnfsId _pnfsid;
        private String _status;
        private CacheException _error;

        Transfer(Subject subject, FsPath path)
        {
            _subject = subject;
            _path = path;
            _startedAt = System.currentTimeMillis();
            _sessionId = 0; //FIXME
        }

        @Override
        public int compareTo(Transfer o)
        {
            return o.hashCode() - hashCode();
        }

        synchronized void setStatus(String status)
        {
            _status = status;
        }

        synchronized void setPnfsId(PnfsId pnfsid)
        {
            _pnfsid = pnfsid;
        }

        synchronized PnfsId getPnfsId()
        {
            return _pnfsid;
        }

        synchronized void setMoverId(Integer moverId)
        {
            _moverId = moverId;
        }

        synchronized Integer getMoverId()
        {
            return _moverId;
        }

        synchronized void setPool(String pool)
        {
            _pool = pool;
        }

        synchronized String getPool()
        {
            return _pool;
        }

        synchronized void finished(DoorTransferFinishedMessage msg)
        {
            _moverId = null;
            if (msg.getReturnCode() != 0) {
                _error = CacheExceptionFactory.exceptionOf(msg);
            }
            notifyAll();
        }

        synchronized boolean join(long millis)
            throws CacheException, InterruptedException
        {
            long deadline = System.currentTimeMillis() + millis;
            while (_moverId != null && System.currentTimeMillis() < deadline) {
                wait(deadline - System.currentTimeMillis());
            }

            if (_error != null) {
                throw _error;
            }

            return _moverId == null;
        }

        synchronized IoDoorEntry getIoDoorEntry()
        {
            Origin origin = Subjects.getOrigin(_subject);
            return new IoDoorEntry(_sessionId,
                                   _pnfsid,
                                   _pool,
                                   _status,
                                   _startedAt,
                                   origin.getAddress().getHostAddress());
        }
    }

    private class WriteTransfer extends Transfer
    {
        private final PnfsHandler _pnfs;
        private StorageInfo _storageInfo;
        private ServerSocketChannel _serverChannel;
        private int _uid;
        private int _gid;

        WriteTransfer(Subject subject, FsPath path, PnfsHandler pnfs)
        {
            super(subject, path);
            _pnfs = pnfs;
        }

        void createNameSpaceEntry(FileAttributes parent)
            throws CacheException
        {
            setStatus("PnfsManager: Creating name space entry");
            try {
                long[] uids = Subjects.getUids(_subject);
                long[] gids = Subjects.getGids(_subject);
                _uid = (uids.length > 0) ? (int) uids[0] : parent.getOwner();
                _gid = (gids.length > 0) ? (int) gids[0] : parent.getGroup();
                int umask =
                    (uids.length > 0) ? FILE_UMASK : FILE_UMASK_ANONYMOUS;
                PnfsCreateEntryMessage msg =
                    _pnfs.createPnfsEntry(_path.toString(), _uid, _gid,
                                          umask & parent.getMode());
                setPnfsId(msg.getPnfsId());
                _storageInfo = msg.getStorageInfo();
            } finally {
                setStatus(null);
            }
        }

        void setLength(Long length)
        {
            if (length != null) {
                _storageInfo.setFileSize(length);
            }
        }

        void selectPool()
            throws CacheException, InterruptedException
        {
            setStatus("PoolManager: Selecting pool");
            try {
                Origin origin = Subjects.getOrigin(_subject);
                String address = origin.getAddress().getHostAddress();
                HttpProtocolInfo protocolInfo =
                    new HttpProtocolInfo(PROTOCOL_INFO_NAME,
                                         PROTOCOL_INFO_MAJOR_VERSION,
                                         PROTOCOL_INFO_MINOR_VERSION,
                                         address,
                                         PROTOCOL_INFO_UNKNOWN_PORT,
                                         _cellName, _domainName,
                                         _path.toString());
                setPool(askForPool(getPnfsId(), _storageInfo, protocolInfo, true));
            } finally {
                setStatus(null);
            }
        }

        synchronized void openServerChannel()
            throws IOException
        {
            _serverChannel = ServerSocketChannel.open();
            try {
                _serverChannel.socket().bind(null);
            } catch (IOException e) {
                _serverChannel.close();
                _serverChannel = null;
                throw e;
            }
        }

        synchronized void closeServerChannel()
            throws IOException
        {
            if (_serverChannel != null) {
                _serverChannel.close();
                _serverChannel = null;
            }
        }

        synchronized ServerSocketChannel getServerChannel()
        {
            return _serverChannel;
        }

        void startMover()
            throws CacheException, InterruptedException
        {
            String pool = getPool();
            setStatus("Pool " + pool + ": Creating mover");
            try {
                /* For now we use GridFTP mode S to stream the data to
                 * the pool.
                 */
                ServerSocket socket = getServerChannel().socket();
                String address = socket.getInetAddress().getHostAddress();
                int port = socket.getLocalPort();
                GFtpProtocolInfo protocolInfo =
                    new GFtpProtocolInfo(RELAY_PROTOCOL_INFO_NAME,
                                         RELAY_PROTOCOL_INFO_MAJOR_VERSION,
                                         RELAY_PROTOCOL_INFO_MINOR_VERSION,
                                         address, port,
                                         RELAY_PROTOCOL_INFO_STREAMS,
                                         RELAY_PROTOCOL_INFO_STREAMS,
                                         RELAY_PROTOCOL_INFO_STREAMS,
                                         RELAY_PROTOCOL_INFO_BUFFERSIZE,
                                         RELAY_PROTOCOL_INFO_OFFSET,
                                         RELAY_PROTOCOL_INFO_SIZE, null);

                PoolIoFileMessage message =
                    new PoolAcceptFileMessage(pool, getPnfsId(),
                                              protocolInfo, _storageInfo);
                message.setIoQueueName(_ioQueue);

                /* As always, PoolIoFileMessage has to be sent via the
                 * PoolManager (which could be the SpaceManager).
                 */
                CellPath poolPath =
                    (CellPath) _poolManagerStub.getDestinationPath().clone();
                poolPath.add(pool);

                setMoverId(_poolStub.sendAndWait(poolPath, message).getMoverId());
            } finally {
                setStatus(null);
            }
        }

        void relayData(InputStream inputStream)
            throws IOException
        {
            setStatus("Mover " + getPool() + "/" + getMoverId() +
                      ": Waiting for data connection");
            try {
                SocketChannel channel = getServerChannel().accept();
                try {
                    closeServerChannel();

                    setStatus("Mover " + getPool() + "/" + getMoverId() +
                              ": Receiving data");

                    /* Relay the data to the pool.
                     */
                    byte[] buffer = new byte[_bufferSize];
                    int read;
                    while ((read = inputStream.read(buffer)) > -1) {
                        ByteBuffer outputBuffer =
                            ByteBuffer.wrap(buffer, 0, read);
                        while (outputBuffer.remaining() > 0) {
                            channel.write(outputBuffer);
                        }
                    }
                } finally {
                    channel.close();
                }
            } finally {
                setStatus(null);
            }
        }

        void killMover()
        {
            Integer moverId = getMoverId();
            if (moverId != null) {
                String pool = getPool();
                setStatus("Mover " + pool + "/" + moverId + ": Killing mover");
                try {
                    /* Kill the mover.
                     */
                    PoolMoverKillMessage message =
                        new PoolMoverKillMessage(pool, moverId);
                    message.setReplyRequired(false);
                    _poolStub.send(new CellPath(pool), message);

                    /* To reduce the risk of orphans when using PNFS, we wait
                     * for the transfer confirmation.
                     */
                    join(_killTimeout);
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
        }

        void deleteNameSpaceEntry()
        {
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

        synchronized void finished(DoorTransferFinishedMessage msg)
        {
            super.finished(msg);
            if (_serverChannel != null) {
                try {
                    _serverChannel.close();
                } catch (IOException e) {
                    _log.error("Failed to close pool connection: " +
                               e.getMessage());
                }
            }
        }

        void notifyBilling(int code, String s)
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
                    new DoorRequestInfoMessage(_cellName + "@" + _domainName);
                msg.setOwner(owner);
                msg.setGid(_uid);
                msg.setUid(_gid);
                msg.setPath(_path.toString());
                msg.setTransactionTime(_startedAt);
                msg.setClient(origin.getAddress().getHostAddress());
                msg.setPnfsId(getPnfsId());
                msg.setResult(code, s);
                msg.setStorageInfo(_storageInfo);
                _billingStub.send(msg);
            } catch (NoRouteToCellException e) {
                _log.error("Failed to register transfer in billing: " +
                           e.getMessage());
            }
        }
    }
}
