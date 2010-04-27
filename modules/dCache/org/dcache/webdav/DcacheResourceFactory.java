package org.dcache.webdav;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Date;
import java.util.Collections;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.AsynchronousCloseException;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.URL;
import java.net.HttpURLConnection;
import javax.security.auth.Subject;
import java.security.AccessController;

import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.HttpManager;
import com.bradmcevoy.http.ResourceFactory;
import com.bradmcevoy.http.XmlWriter;
import com.bradmcevoy.http.Range;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.GFtpProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;

import org.dcache.cells.CellStub;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import org.dcache.acl.Origin;
import org.dcache.auth.Subjects;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.Transfer;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.PingMoversTask;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.ListDirectoryHandler;

import dmg.util.Args;
import dmg.util.CollectionFactory;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.services.login.LoginManagerChildrenInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dcache.namespace.FileType.*;
import static org.dcache.namespace.FileAttribute.*;

/**
 * This ResourceFactory exposes the dCache name space through the
 * Milton WebDAV framework.
 */
public class DcacheResourceFactory
    extends AbstractCellComponent
    implements ResourceFactory, CellMessageReceiver, CellCommandListener
{
    private static final Logger _log =
        LoggerFactory.getLogger(DcacheResourceFactory.class);

    private static final Set<FileAttribute> REQUIRED_ATTRIBUTES =
        EnumSet.of(TYPE, PNFSID, CREATION_TIME, MODIFICATION_TIME, SIZE,
                   MODE, OWNER, OWNER_GROUP);

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

    private static final int DIRECTORY_UMASK = 0755;
    private static final int DIRECTORY_UMASK_ANONYMOUS = 0777;

    private static final long PING_DELAY = 300000;


    /**
     * Used to hand over the redirection URL to the proper transfer
     * request.
     */
    private final MultiMap<RedirectKey,HttpTransfer> _redirect =
        new ArrayHashMultiMap<RedirectKey,HttpTransfer>();

    /**
     * In progress write transfers.
     */
    private final Set<Transfer> _transfers =
        new ConcurrentSkipListSet<Transfer>();

    /**
     * Write transfers with a mover.
     */
    private final Map<PnfsId,Transfer> _uploads =
        CollectionFactory.newConcurrentHashMap();

    /**
     * Read transfers with a mover. The key is the session ID.
     */
    private final Map<Integer,Transfer> _downloads =
        CollectionFactory.newConcurrentHashMap();

    private ListDirectoryHandler _list;

    private ScheduledExecutorService _executor;

    private int _moverTimeout = 180000;
    private long _killTimeout = 1500;
    private long _transferConfirmationTimeout = 60000;
    private int _bufferSize = 65536;
    private CellStub _poolStub;
    private CellStub _poolManagerStub;
    private CellStub _billingStub;
    private PnfsHandler _pnfs;
    private String _ioQueue;
    private String _cellName;
    private String _domainName;
    private FsPath _rootPath = new FsPath();
    private List<FsPath> _allowedPaths =
        Collections.singletonList(new FsPath());
    private InetAddress _internalAddress;
    private String _logoPath;
    private String _webdavStylesheetPath;
    private String _iconDirPath;
    private String _iconFilePath;
    private String _path;
    private boolean _doRedirectOnRead = true;
    private boolean _isOverwriteAllowed = false;

    public DcacheResourceFactory()
        throws UnknownHostException
    {
        _internalAddress = InetAddress.getLocalHost();
    }

    /**
     * Returns the kill timeout in milliseconds.
     */
    public long getKillTimeout()
    {
        return _killTimeout;
    }

    /**
     * The kill timeout is the time we wait for a transfer to
     * terminate after we killed the mover.
     *
     * @param timeout The mover timeout in milliseconds
     */
    public void setKillTimeout(long timeout)
    {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be positive");
        }
        _killTimeout = timeout;
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
    public void setMoverTimeout(int timeout)
    {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be positive");
        }
        _moverTimeout = timeout;
    }

    /**
     * Returns the transfer confirmation timeout in milliseconds.
     */
    public long getTransferConfirmationTimeout()
    {
        return _transferConfirmationTimeout;
    }

    /**
     * The transfer confirmation timeout is the time we wait after we
     * know that an upload has finished and until we received the
     * transfer confirmation message from the pool.
     *
     * @param timeout The transfer confirmation timeout in milliseconds
     */
    public void setTransferConfirmationTimeout(long timeout)
    {
        _transferConfirmationTimeout = timeout;
    }

    /**
     * Returns the buffer size in bytes.
     */
    public int getBufferSize()
    {
        return _bufferSize;
    }

    /**
     * Sets the size of the buffer used when proxying uploads.
     *
     * @param bufferSize The buffer size in bytes
     */
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
     * Sets whether read requests are redirected to the pool. If not,
     * then the door will act as a proxy.
     */
    public void setRedirectOnReadEnabled(boolean redirect)
    {
        _doRedirectOnRead = redirect;
    }

    public boolean isRedirectOnReadEnabled()
    {
        return _doRedirectOnRead;
    }

    /**
     * Sets whether existing files may be overwritten.
     */
    public void setOverwriteAllowed(boolean allowed)
    {
        _isOverwriteAllowed = allowed;
    }

    public boolean isOverwriteAllowed()
    {
        return _isOverwriteAllowed;
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

    /**
     * @return the logoPath
     */
    public String getLogoPath() {
        return _logoPath;
    }

    /**
     * @param logoPath the the path to the logo icon (for example dCache logo).
     */
    public void setLogoPath(String logoPath) {
        this._logoPath = logoPath;
    }

    /**
     * @return the stylesheetPath
     */
    public String getStylesheetPath() {
        return _webdavStylesheetPath;
    }

    /**
     * @param webdavStylesheetPath: the path to the css-file.
     */
    public void setWebdavStylesheetPath(String webdavStylesheetPath) {
        this._webdavStylesheetPath = webdavStylesheetPath;
    }

    /**
     * @return the _iconDirPath
     */
    public String getIconDirPath() {
        return _iconDirPath;
    }

    /**
     * @param iconDirPath: the path to the directory icon.
     */
    public void setIconDirPath(String iconDirPath) {
        this._iconDirPath = iconDirPath;
    }

    /**
     * @return the _iconFilePath
     */
    public String getIconFilePath() {
        return _iconFilePath;
    }

    /**
     * @param iconFilePath: the path to the file icon.
     */
    public void setIconFilePath(String iconFilePath) {
        this._iconFilePath = iconFilePath;
    }

    /**
     * Sets the ScheduledExecutorService used for periodic tasks.
     */
    public void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
        _executor.scheduleAtFixedRate(new PingMoversTask(_transfers),
                                      PING_DELAY, PING_DELAY,
                                      TimeUnit.MILLISECONDS);
    }

    public void setInternalAddress(String host)
        throws UnknownHostException
    {
        if (host != null && !host.isEmpty()) {
            InetAddress address = InetAddress.getByName(host);
            if (address.isAnyLocalAddress()) {
                throw new IllegalArgumentException("Wildcard address is not allowed: " + host);
            }
            _internalAddress = address;
        } else {
            _internalAddress = InetAddress.getLocalHost();
        }
    }

    public String getInternalAddress()
    {
        return _internalAddress.getHostAddress();
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
            _log.debug("Resolving " + HttpManager.request().getAbsoluteUrl());
        }

        return getResource(getFullPath(path));
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
        } catch (PermissionDeniedCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, null);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, null);
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
            new WriteTransfer(_pnfs, subject, path);
        _transfers.add(transfer);
        try {
            boolean success = false;
            transfer.createNameSpaceEntry(parent);
            try {
                PnfsId pnfsid = transfer.getPnfsId();
                transfer.setLength(length);
                transfer.selectPool();
                transfer.openServerChannel();
                _uploads.put(pnfsid, transfer);
                try {
                    transfer.startMover(_ioQueue);
                    try {
                        transfer.relayData(inputStream);
                    } finally {
                        transfer.setStatus(null);
                        transfer.killMover(_killTimeout);
                    }
                } finally {
                    _uploads.remove(pnfsid);
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
                                   e.toString());
            throw e;
        } catch (RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.toString());
            throw e;
        } finally {
            _transfers.remove(transfer);
        }

        return getResource(path);
    }

    /**
     * Reads the content of a file. The door will relay all data from
     * a pool.
     */
    public void readFile(FsPath path, PnfsId pnfsid,
                         OutputStream outputStream, Range range)
        throws CacheException, InterruptedException, IOException
    {
        ReadTransfer transfer = beginRead(path, pnfsid);
        try {
            transfer.relayData(outputStream, range);
        } catch (CacheException e) {
            transfer.notifyBilling(e.getRc(), e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   "Transfer interrupted");
            throw e;
        } catch (IOException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.toString());
            throw e;
        } catch (RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.toString());
            throw e;
        } finally {
            _downloads.remove(transfer.getSessionId());
            _transfers.remove(transfer);
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
            throws InterruptedException, CacheException {
        final XmlWriter w = new XmlWriter(out);
        DirectoryListPrinter printer =
                new DirectoryListPrinter() {

                    public Set<FileAttribute> getRequiredAttributes() {
                        return EnumSet.of(MODIFICATION_TIME, TYPE);
                    }

                    public void print(FileAttributes dirAttr, DirectoryEntry entry) {
                        FileAttributes attr = entry.getFileAttributes();
                        Date mtime = new Date(attr.getModificationTime());
                        w.open("tr");
                        w.open("td");

                        if (attr.getFileType() == DIR) {

                            w.begin("div").writeAtt("class", "directoryicon").open();
                            w.begin("img").writeAtt("class", "scaled").writeAtt("src", _iconDirPath).writeAtt("alt", "").open().close();
                            w.close("div");
                            w.close("td");
                            w.open("td");
                            w.begin("a").writeAtt("href", _path.concat(entry.getName()) + "/").open().writeText(entry.getName() + "/").close();

                        } else {

                            w.begin("div").writeAtt("class", "fileicon").open();
                            w.begin("img").writeAtt("class", "scaled").writeAtt("src", _iconFilePath).writeAtt("alt", "").open().close();
                            w.close("div");
                            w.close("td");
                            w.open("td");
                            w.begin("a").writeAtt("href", _path.concat(entry.getName())).open().writeText(entry.getName()).close();

                        }
                        w.close("td");
                        w.begin("td").open().writeText(mtime.toString()).close();
                        w.close("tr");
                    }
                };

        w.open("html");
        w.open("head");
        w.begin("title").open().writeText("dCache.org - File System").close();

        w.begin("link").writeAtt("rel", "stylesheet").writeAtt("type", "text/css").writeAtt("href", _webdavStylesheetPath).open().close();

        w.close("head");
        w.open("body");
        w.begin("div").writeAtt("id", "navi").open();
        w.begin("div").writeAtt("class", "figure").open();
        w.begin("img").writeAtt("class", "scaled").writeAtt("src", _logoPath).open().close();
        w.close("div");

        if (getSubject() != null) {

            w.begin("p").open().writeText(getSubject().getPrincipals().toString()).close();

        } else {
            w.writeData("<br>");

        }

        Request request = HttpManager.request();
        String[] splitPath = request.getAbsolutePath().split("/");

        w.open("p");

        String constructLink = "";
        String link = "";

        if (request.getAbsolutePath().equals("/")) {

            w.begin("a").writeAtt("href", "/").open().writeText("/").close();

        } else {
            for (String i : splitPath) {

                if (i.equals("")) {
                    continue;
                }

                constructLink = constructLink.concat("/").concat(i);
                link = String.format(
                        "<a href=%s>/%s</a>", constructLink,
                        i);
                w.begin("a").writeAtt("href", constructLink).open().writeText("/").writeText(i).close();
            }
        }

        _path = constructLink.concat("/");

        w.close("p");
        w.close("div");
        w.begin("div").writeAtt("id", "wrap").open();
        w.begin("div").writeAtt("class", "content").open();
        w.begin("h1").open().writeText("File System").close();

        w.open("table");
        w.open("thead");
        w.open("tr");
        w.open("th");
        w.close("th");
        w.begin("th").open().writeText("Name").close();
        w.begin("th").open().writeText("Last Modified").close();
        w.close("tr");
        w.close("thead");
        w.open("tbody");
        _list.printDirectory(getSubject(), printer,
                new File(path.toString()), null, null);
        w.close("tbody");
        w.close("table");
        w.close("div");
        w.begin("div").writeAtt("id", "footer").open();
        w.writeText("www.dCache.org");
        w.close("div");
        w.close("div");
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
        return beginRead(path, pnfsid).getRedirect();
    }

    /**
     * Initiates a read operation.
     *
     * @param path The full path of the file.
     * @param pnfsid The PNFS ID of the file.
     * @return ReadTransfer encapsulating the read operation
     */
    private ReadTransfer beginRead(FsPath path, PnfsId pnfsid)
        throws CacheException, InterruptedException
    {
        Subject subject = getSubject();

        String uri = null;
        ReadTransfer transfer =
            new ReadTransfer(_pnfs, subject, path, pnfsid);
        _transfers.add(transfer);
        try {
            Integer sessionId = (int) transfer.getSessionId();
            transfer.setPnfsId(pnfsid);
            transfer.readNameSpaceEntry();
            transfer.selectPool();

            RedirectKey key = new RedirectKey(pnfsid, transfer.getPool());
            _redirect.put(key, transfer);
            try {
                _downloads.put(sessionId, transfer);
                transfer.startMover(_ioQueue);
                transfer.setStatus("Mover " + transfer.getPool() + "/" +
                                   transfer.getMoverId() + ": Waiting for URI");
                uri = transfer.waitForRedirect(_moverTimeout);
                if (uri == null) {
                    throw new TimeoutCacheException("Server is busy (internal timeout)");
                }
            } finally {
                transfer.setStatus(null);
                if (uri == null) {
                    _downloads.remove(sessionId);
                }
                _redirect.remove(key, transfer);
            }
            transfer.setStatus("Mover " + transfer.getPool() + "/" +
                               transfer.getMoverId() + ": Waiting for completion");
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
            if (uri == null) {
                _transfers.remove(transfer);
            }
        }
        return transfer;
    }

    /**
     * Message handler for redirect messages from the pools.
     */
    public void messageArrived(CellMessage envelope,
                               HttpDoorUrlInfoMessage message)
    {
        RedirectKey key =
            new RedirectKey(new PnfsId(message.getPnfsId()),
                            envelope.getSourceAddress().getCellName());
        HttpTransfer transfer = _redirect.remove(key);
        if (transfer != null) {
            transfer.redirect(message.getUrl());
        }
    }

    /**
     * Message handler for transfer completion messages from the
     * pools.
     */
    public void messageArrived(DoorTransferFinishedMessage message)
    {
        Transfer transfer = null;
        ProtocolInfo protocolInfo = message.getProtocolInfo();
        if (protocolInfo instanceof HttpProtocolInfo) {
            int sessionId =
                ((HttpProtocolInfo) protocolInfo).getSessionId();
            transfer = _downloads.get(sessionId);
        } else {
            transfer = _uploads.get(message.getPnfsId());
        }
        if (transfer != null) {
            transfer.finished(message);
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

    /**
     * To emulate LoginManager we list ourselves as our child.
     */
    public final static String hh_get_children = "[-binary]";
    public Object ac_get_children(Args args)
    {
        boolean binary = args.getOpt("binary") != null;
        if (binary) {
            String [] list = new String[] { _cellName };
            return new LoginManagerChildrenInfo(_cellName, _domainName, list);
        } else {
            return _cellName;
        }
    }

    /**
     * Provides information about the door and current transfers.
     */
    public final static String hh_get_door_info = "[-binary]";
    public Object ac_get_door_info(Args args)
    {
        List<IoDoorEntry> transfers = new ArrayList<IoDoorEntry>();
        for (Transfer transfer: _transfers) {
            transfers.add(transfer.getIoDoorEntry());
        }

        IoDoorInfo doorInfo = new IoDoorInfo(_cellName, _domainName);
        doorInfo.setProtocol("HTTP", "1.1");
        doorInfo.setOwner("");
        doorInfo.setProcess("");
        doorInfo.setIoDoorEntries(transfers.toArray(new IoDoorEntry[0]));
        return (args.getOpt("binary") != null) ? doorInfo : doorInfo.toString();
    }

    /**
     * Specialisation of the Transfer class for HTTP transfers.
     */
    private class HttpTransfer extends RedirectedTransfer<String>
    {

        public HttpTransfer(PnfsHandler pnfs, Subject subject, FsPath path)
        {
            super(pnfs, subject, path);
            setCellName(_cellName);
            setDomainName(_domainName);
            setPoolManagerStub(_poolManagerStub);
            setPoolStub(_poolStub);
            setBillingStub(_billingStub);
            setClientAddress(new InetSocketAddress(Subjects.getOrigin(subject).getAddress(),
                                                   PROTOCOL_INFO_UNKNOWN_PORT));
        }

        protected ProtocolInfo createProtocolInfo()
        {
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
            protocolInfo.setSessionId((int) getSessionId());
            return protocolInfo;
        }

        @Override
        protected ProtocolInfo createProtocolInfoForPoolManager()
        {
            return createProtocolInfo();
        }

        @Override
        protected ProtocolInfo createProtocolInfoForPool()
        {
            return createProtocolInfo();
        }
    }

    /**
     * Specialised HttpTransfer for downloads.
     */
    private class ReadTransfer extends HttpTransfer
    {
        public ReadTransfer(PnfsHandler pnfs, Subject subject,
                            FsPath path, PnfsId pnfsid)
        {
            super(pnfs, subject, path);
            setPnfsId(pnfsid);
        }

        public void relayData(OutputStream outputStream, Range range)
            throws IOException, CacheException, InterruptedException
        {
            setStatus("Mover " + getPool() + "/" + getMoverId() +
                      ": Opening data connection");
            URL url = new URL(getRedirect());
            HttpURLConnection connection =
                (HttpURLConnection) url.openConnection();
            try {
                if (range != null) {
                    connection.addRequestProperty("Range", String.format("bytes=%d-%d", range.getStart(), range.getFinish()));
                }

                connection.connect();

                InputStream inputStream = connection.getInputStream();
                try {
                    setStatus("Mover " + getPool() + "/" + getMoverId() +
                              ": Sending data");

                    byte[] buffer = new byte[_bufferSize];
                    int read;
                    while ((read = inputStream.read(buffer)) > -1) {
                        outputStream.write(buffer, 0, read);
                    }
                    outputStream.flush();
                } finally {
                    inputStream.close();
                }

                if (!waitForMover(_transferConfirmationTimeout)) {
                    throw new CacheException("Missing transfer confirmation from pool");
                }
            } catch (SocketTimeoutException e) {
                throw new TimeoutCacheException("Server is busy (internal timeout)");
            } finally {
                setStatus(null);
                connection.disconnect();
            }
        }


        @Override
        public synchronized void finished(CacheException error)
        {
            super.finished(error);

            _downloads.remove((int) getSessionId());
            _transfers.remove(this);

            if (error == null) {
                notifyBilling(0, "");
            } else {
                notifyBilling(error.getRc(), error.getMessage());
            }
        }
    }

    /**
     * Specialised HttpTransfer for uploads.
     */
    private class WriteTransfer extends HttpTransfer
    {
        private ServerSocketChannel _serverChannel;

        public WriteTransfer(PnfsHandler pnfs, Subject subject, FsPath path)
        {
            super(pnfs, subject, path);
            setOverwriteAllowed(_isOverwriteAllowed);
        }

        public synchronized void openServerChannel()
            throws IOException
        {
            _serverChannel = ServerSocketChannel.open();
            try {
                _serverChannel.socket().setSoTimeout(_moverTimeout);
               _serverChannel.socket().bind(new InetSocketAddress(_internalAddress, 0));
            } catch (IOException e) {
                _serverChannel.close();
                _serverChannel = null;
                throw e;
            }
        }

        public synchronized void closeServerChannel()
            throws IOException
        {
            if (_serverChannel != null) {
                _serverChannel.close();
                _serverChannel = null;
            }
        }

        public synchronized ServerSocketChannel getServerChannel()
        {
            return _serverChannel;
        }

        @Override
        protected ProtocolInfo createProtocolInfoForPool()
        {
            ServerSocket socket = getServerChannel().socket();
            String address = socket.getInetAddress().getHostAddress();
            int port = socket.getLocalPort();
            return new GFtpProtocolInfo(RELAY_PROTOCOL_INFO_NAME,
                                        RELAY_PROTOCOL_INFO_MAJOR_VERSION,
                                        RELAY_PROTOCOL_INFO_MINOR_VERSION,
                                        address, port,
                                        RELAY_PROTOCOL_INFO_STREAMS,
                                        RELAY_PROTOCOL_INFO_STREAMS,
                                        RELAY_PROTOCOL_INFO_STREAMS,
                                        RELAY_PROTOCOL_INFO_BUFFERSIZE,
                                        RELAY_PROTOCOL_INFO_OFFSET,
                                        RELAY_PROTOCOL_INFO_SIZE, null);
        }

        public void relayData(InputStream inputStream)
            throws IOException, CacheException, InterruptedException
        {
            setStatus("Mover " + getPool() + "/" + getMoverId() +
                      ": Waiting for data connection");
            try {
                ServerSocketChannel serverChannel = getServerChannel();
                if (serverChannel == null) {
                    throw new AsynchronousCloseException();
                }

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
                if (!waitForMover(_transferConfirmationTimeout)) {
                    throw new CacheException("Missing transfer confirmation from pool");
                }
            } catch (AsynchronousCloseException e) {
                /* Server socket closed because the mover reported an
                 * error rather than connection to us. The mover has
                 * likely a much more interesting error message than
                 * the asynchronous close.
                 */
                waitForMover(0);
                throw new IllegalStateException("Server channel is not open");
            } catch (SocketTimeoutException e) {
                throw new TimeoutCacheException("Server is busy (internal timeout)");
            } finally {
                setStatus(null);
            }
        }

        @Override
        public synchronized void finished(CacheException error)
        {
            super.finished(error);
            if (_serverChannel != null) {
                try {
                    _serverChannel.close();
                } catch (IOException e) {
                    _log.error("Failed to close pool connection: " +
                               e.getMessage());
                }
            }
        }

        /**
         * Sets the length of the file to be uploaded. The length is
         * optional and will be ignored if null.
         */
        public void setLength(Long length)
        {
            if (length != null) {
                super.setLength(length.longValue());
            }
        }
    }
}
