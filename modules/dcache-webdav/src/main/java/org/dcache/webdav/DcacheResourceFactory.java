package org.dcache.webdav;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.EnumSet;
import java.util.Date;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.AsynchronousCloseException;
import java.nio.charset.UnsupportedCharsetException;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.net.URL;
import java.net.URI;
import java.net.HttpURLConnection;
import javax.security.auth.Subject;
import java.security.AccessController;

import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.HttpManager;
import com.bradmcevoy.http.ResourceFactory;
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
import diskCacheV111.vehicles.DoorRequestInfoMessage;

import org.dcache.cells.CellStub;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import org.dcache.auth.Subjects;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.util.TransferRetryPolicies;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.PingMoversTask;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.ListDirectoryHandler;

import dmg.util.Args;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.login.LoginManagerChildrenInfo;
import org.dcache.auth.Origin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Ranges;
import com.google.common.base.Splitter;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.antlr.stringtemplate.AutoIndentWriter;
import org.antlr.stringtemplate.language.DefaultTemplateLexer;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import org.dcache.auth.SubjectWrapper;

import static org.dcache.namespace.FileType.*;
import static org.dcache.namespace.FileAttribute.*;

import org.dcache.auth.UserNamePrincipal;
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

    private static final long PING_DELAY = 300000;

    /**
     * Used to hand over the redirection URL to the proper transfer
     * request.
     */
    private final SetMultimap<PnfsId,HttpTransfer> _redirects =
        Multimaps.synchronizedSetMultimap(LinkedHashMultimap.<PnfsId,HttpTransfer>create());

    private static final Splitter PATH_SPLITTER =
        Splitter.on('/').omitEmptyStrings();

    /**
     * In progress transfers.
     */
    private final Map<Long,Transfer> _transfers =
        new ConcurrentHashMap<Long,Transfer>();

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
    private String _path;
    private boolean _doRedirectOnRead = true;
    private boolean _isOverwriteAllowed = false;
    private boolean _isAnonymousListingAllowed;

    private String _staticContentPath;
    private StringTemplateGroup _listingGroup;

    private TransferRetryPolicy _retryPolicy =
        TransferRetryPolicies.tryOncePolicy(_moverTimeout);

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
        _retryPolicy = TransferRetryPolicies.tryOncePolicy(_moverTimeout);
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

    public void setAnonymousListing(boolean isAllowed)
    {
        _isAnonymousListingAllowed = isAllowed;
    }

    public boolean isAnonymousListing()
    {
        return _isAnonymousListingAllowed;
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
     * Sets the resource containing the StringTemplateGroup for
     * directory listing.
     */
    public void setTemplateResource(org.springframework.core.io.Resource resource)
        throws IOException
    {
        InputStream in = resource.getInputStream();
        try {
            _listingGroup = new StringTemplateGroup(new InputStreamReader(in),
                                                    DefaultTemplateLexer.class);
        } finally {
            in.close();
        }
    }

    /**
     * Returns the static content path.
     */
    public String getStaticContentPath()
    {
        return _staticContentPath;
    }

    /**
     * The static content path is the path under which the service
     * exports the static content. This typically contains stylesheets
     * and image files.
     */
    public void setStaticContentPath(String path)
    {
        _staticContentPath = path;
    }

    /**
     * Sets the ScheduledExecutorService used for periodic tasks.
     */
    public void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
        _executor.scheduleAtFixedRate(new PingMoversTask(_transfers.values()),
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
            throw new UnauthorizedException(e.getMessage(), e, null);
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
        createFile(FsPath path, InputStream inputStream, Long length)
        throws CacheException, InterruptedException, IOException
    {
        Subject subject = getSubject();

        WriteTransfer transfer =
            new WriteTransfer(_pnfs, subject, path);
        _transfers.put(transfer.getSessionId(), transfer);
        try {
            boolean success = false;
            transfer.createNameSpaceEntry();
            try {
                PnfsId pnfsid = transfer.getPnfsId();
                transfer.setLength(length);
                transfer.openServerChannel();
                try {
                    transfer.selectPoolAndStartMover(_ioQueue, _retryPolicy);
                    transfer.relayData(inputStream);
                } finally {
                    transfer.killMover(_killTimeout);
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
            _transfers.remove(transfer.getSessionId());
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
            _transfers.remove(transfer.getSessionId());
        }
    }

    /**
     * Performs a directory listing returning a list of Resource
     * objects.
     */
    public List<DcacheResource> list(final FsPath path)
        throws InterruptedException, CacheException
    {
        if (!_isAnonymousListingAllowed && Subjects.isNobody(getSubject())) {
            throw new PermissionDeniedCacheException("Access denied");
        }

        final List<DcacheResource> result = new ArrayList<DcacheResource>();
        DirectoryListPrinter printer =
            new DirectoryListPrinter()
            {
                @Override
                public Set<FileAttribute> getRequiredAttributes()
                {
                    return REQUIRED_ATTRIBUTES;
                }

                @Override
                public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
                {
                    result.add(getResource(new FsPath(path, entry.getName()),
                                           entry.getFileAttributes()));
                }
            };

        _list.printDirectory(getSubject(), printer, path, null,
                             Ranges.<Integer>all());
        return result;
    }

    /**
     * Performs a directory listing, writing an HTML view to an output
     * stream.
     * @throws URISyntaxException
     */
    public void list(FsPath path, Writer out)
        throws InterruptedException, CacheException, IOException, URISyntaxException
    {
        if (!_isAnonymousListingAllowed && Subjects.isNobody(getSubject())) {
            throw new PermissionDeniedCacheException("Access denied");
        }

        Request request = HttpManager.request();
        String requestPath = new URI(request.getAbsoluteUrl()).getPath();
        String[] base =
            Iterables.toArray(PATH_SPLITTER.split(requestPath), String.class);
        final StringTemplate t = _listingGroup.getInstanceOf("page");
        t.setAttribute("path", UrlPathWrapper.forPaths(base));
        t.setAttribute("static", _staticContentPath);
        t.setAttribute("subject", new SubjectWrapper(getSubject()));
        t.setAttribute("base", UrlPathWrapper.forEmptyPath());

        DirectoryListPrinter printer =
                new DirectoryListPrinter() {

                    @Override
                    public Set<FileAttribute> getRequiredAttributes() {
                        return EnumSet.of(MODIFICATION_TIME, TYPE, SIZE);
                    }

                    @Override
                    public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry) {
                        FileAttributes attr = entry.getFileAttributes();
                        Date mtime = new Date(attr.getModificationTime());
                        UrlPathWrapper name =
                                UrlPathWrapper.forPath(entry.getName());
                        t.setAttribute("files.{name,isDirectory,mtime,size}",
                                       name,
                                       attr.getFileType() == DIR,
                                       mtime,
                                       attr.getSize());
                    }
                };
        _list.printDirectory(getSubject(), printer, path, null,
                             Ranges.<Integer>all());

        t.write(new AutoIndentWriter(out));
        out.flush();
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
        sendRemoveInfoToBilling(path);
    }

    private void sendRemoveInfoToBilling(FsPath path)
    {
        try {
            String cell = getCellName() + "@" + getCellDomainName();
            DoorRequestInfoMessage infoRemove =
                new DoorRequestInfoMessage(cell, "remove");
            Subject subject = getSubject();
            infoRemove.setSubject(subject);
            infoRemove.setPath(path.toString());
            infoRemove.setClient(Subjects.getOrigin(subject).getAddress().getHostAddress());
            _billingStub.send(infoRemove);
        } catch (NoRouteToCellException e) {
            _log.error("Cannot send remove message to billing: {}",
                       e.getMessage());
        }
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
        PnfsHandler pnfs = new PnfsHandler(_pnfs, getSubject());
        PnfsCreateEntryMessage reply =
            pnfs.createPnfsDirectory(path.toString());
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
        ReadTransfer transfer = new ReadTransfer(_pnfs, subject, path, pnfsid);
        _transfers.put(transfer.getSessionId(), transfer);
        try {
            transfer.readNameSpaceEntry();
            try {
                _redirects.put(pnfsid, transfer);
                transfer.selectPoolAndStartMover(_ioQueue, _retryPolicy);
                transfer.setStatus("Mover " + transfer.getPool() + "/" +
                                   transfer.getMoverId() + ": Waiting for URI");
                uri = transfer.waitForRedirect(_moverTimeout);
                if (uri == null) {
                    throw new TimeoutCacheException("Server is busy (internal timeout)");
                }
            } finally {
                _redirects.remove(pnfsid, transfer);
                transfer.setStatus(null);
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
                _transfers.remove(transfer.getSessionId());
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
        PnfsId pnfsId = new PnfsId(message.getPnfsId());
        String pool = envelope.getSourceAddress().getCellName();

        synchronized (_redirects) {
            Iterator<HttpTransfer> i = _redirects.get(pnfsId).iterator();
            while (i.hasNext()) {
                HttpTransfer transfer = i.next();
                if (pool.equals(transfer.getPool())) {
                    i.remove();
                    transfer.redirect(message.getUrl());
                    return;
                }
            }
        }
    }

    /**
     * Message handler for transfer completion messages from the
     * pools.
     */
    public void messageArrived(DoorTransferFinishedMessage message)
    {
        Transfer transfer = _transfers.get(message.getId());
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
        boolean binary = args.hasOption("binary");
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
        for (Transfer transfer: _transfers.values()) {
            transfers.add(transfer.getIoDoorEntry());
        }

        IoDoorInfo doorInfo = new IoDoorInfo(_cellName, _domainName);
        doorInfo.setProtocol("HTTP", "1.1");
        doorInfo.setOwner("");
        doorInfo.setProcess("");
        doorInfo.setIoDoorEntries(transfers.toArray(new IoDoorEntry[0]));
        return args.hasOption("binary") ? doorInfo : doorInfo.toString();
    }

    private void initializeTransfer(HttpTransfer transfer, Subject subject)
    {
        transfer.setCellName(_cellName);
        transfer.setDomainName(_domainName);
        transfer.setPoolManagerStub(_poolManagerStub);
        transfer.setPoolStub(_poolStub);
        transfer.setBillingStub(_billingStub);
        transfer.setClientAddress(new InetSocketAddress(Subjects.getOrigin(subject).getAddress(),
                                                        PROTOCOL_INFO_UNKNOWN_PORT));
        transfer.setOverwriteAllowed(_isOverwriteAllowed);
    }

    /**
     * Specialisation of the Transfer class for HTTP transfers.
     */
    private class HttpTransfer extends RedirectedTransfer<String>
    {
        public HttpTransfer(PnfsHandler pnfs, Subject subject, FsPath path)
        {
            super(pnfs, subject, path);
            initializeTransfer(this, subject);
        }

        protected ProtocolInfo createProtocolInfo()
        {
            Origin origin = Subjects.getOrigin(_subject);
            HttpProtocolInfo protocolInfo =
                new HttpProtocolInfo(PROTOCOL_INFO_NAME,
                                     PROTOCOL_INFO_MAJOR_VERSION,
                                     PROTOCOL_INFO_MINOR_VERSION,
                                     getClientAddress(),
                                     _cellName, _domainName,
                                     _path.toString());
            protocolInfo.setSessionId((int) getSessionId());
            return protocolInfo;
        }

        @Override
        protected ProtocolInfo getProtocolInfoForPoolManager()
        {
            return createProtocolInfo();
        }

        @Override
        protected ProtocolInfo getProtocolInfoForPool()
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
            try {
                URL url = new URL(getRedirect());
                HttpURLConnection connection =
                    (HttpURLConnection) url.openConnection();
                try {
                    connection.setRequestProperty("Connection", "Close");
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
                } finally {
                    connection.disconnect();
                }

                if (!waitForMover(_transferConfirmationTimeout)) {
                    throw new CacheException("Missing transfer confirmation from pool");
                }
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

            _transfers.remove(getSessionId());

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
        protected ProtocolInfo getProtocolInfoForPool()
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
                                        RELAY_PROTOCOL_INFO_SIZE);
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
