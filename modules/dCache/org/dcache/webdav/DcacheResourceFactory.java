package org.dcache.webdav;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Date;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.File;
import java.io.OutputStream;
import java.io.PrintWriter;
import javax.security.auth.Subject;
import java.security.AccessController;

import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.ResourceFactory;
import com.bradmcevoy.http.SecurityManager;
import com.bradmcevoy.http.XmlWriter;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolDeliverFileMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;

import org.dcache.cells.CellStub;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.acl.Origin;
import org.dcache.auth.Subjects;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.ListDirectoryHandler;

import dmg.cells.nucleus.CellPath;
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

    private static final Set<FileAttribute> FILE_ATTRIBUTES =
        EnumSet.of(TYPE, PNFSID, CREATION_TIME, MODIFICATION_TIME, SIZE);

    private final Map<PnfsId,List<LinkedBlockingQueue>> _redirectTable =
        CollectionFactory.newHashMap();

    private ListDirectoryHandler _list;

    private CellStub _poolStub;
    private CellStub _poolManagerStub;
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
                pnfs.getFileAttributes(path.toString(), FILE_ATTRIBUTES);
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
                    return FILE_ATTRIBUTES;
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
    public DcacheDirectoryResource makeDirectory(FsPath path)
        throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, getSubject());
        PnfsCreateEntryMessage reply =
            pnfs.createPnfsDirectory(path.toString());
        FileAttributes attributes =
            pnfs.getFileAttributes(reply.getPnfsId(), FILE_ATTRIBUTES);

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
            new HttpProtocolInfo("Http", 1, 1,
                                 address, 0,
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
     * Returns a write URL for a file.
     *
     * @param path The full path of the file.
     * @param pnfsid The PNFS ID of the file.
     */
    public String getWriteUrl(FsPath path, PnfsId pnfsid)
        throws CacheException, InterruptedException
    {
//         PnfsHandler pnfs = new PnfsHandler(_pnfs, subject);
//         StorageInfo storage_info =
//             pnfs.getStorageInfoByPnfsId(pnfsid).getStorageInfo();
//         Origin origin = Subjects.getOrigin(subject);

//         HttpProtocolInfo protocol_info =
//             new HttpProtocolInfo("Http", 1, 1, origin.getHostAddress(), 0, _cellName,
//                                  _domainName, path.toString());
//         String pool = askForPool(pnfsid,
//                                  storage_info,
//                                  protocol_info,
//                                  true); /* true for write */
//         return askForFile(pool,
//                           pnfsid,
//                           storage_info,
//                           protocol_info,
//                           true); /* true for write */
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Message handler for redirect messages from the pools.
     */
    public void messageArrived(HttpDoorUrlInfoMessage message)
    {
        synchronized (_redirectTable) {
            List<LinkedBlockingQueue> list =
                _redirectTable.get(new PnfsId(message.getPnfsId()));
            if (list != null) {
                assert !list.isEmpty();
                list.get(0).offer(message.getUrl());
            }
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
            ? (PoolIoFileMessage) new PoolAcceptFileMessage(pool, pnfsId.toString(), protocolInfo, storageInfo)
            : (PoolIoFileMessage) new PoolDeliverFileMessage(pool, pnfsId.toString(), protocolInfo, storageInfo);

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
        List<LinkedBlockingQueue> list;
        synchronized (_redirectTable) {
            list = _redirectTable.get(pnfsId);
            if (list == null) {
                list = new ArrayList<LinkedBlockingQueue>();
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
}