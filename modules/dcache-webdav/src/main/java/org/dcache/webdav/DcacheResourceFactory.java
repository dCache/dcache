package org.dcache.webdav;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.dcache.http.HttpPoolRequestHandler.REFERRER_QUERY_PARAM;
import static org.dcache.namespace.FileAttribute.ACCESS_LATENCY;
import static org.dcache.namespace.FileAttribute.CHECKSUM;
import static org.dcache.namespace.FileAttribute.CREATION_TIME;
import static org.dcache.namespace.FileAttribute.MODE;
import static org.dcache.namespace.FileAttribute.MODIFICATION_TIME;
import static org.dcache.namespace.FileAttribute.OWNER;
import static org.dcache.namespace.FileAttribute.OWNER_GROUP;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.RETENTION_POLICY;
import static org.dcache.namespace.FileAttribute.SIZE;
import static org.dcache.namespace.FileAttribute.TYPE;
import static org.dcache.namespace.FileAttribute.XATTR;
import static org.dcache.namespace.FileType.DIR;
import static org.dcache.namespace.FileType.LINK;
import static org.dcache.namespace.FileType.REGULAR;
import static org.dcache.util.ByteUnit.KiB;
import static org.dcache.util.TransferRetryPolicy.tryOnce;
import static org.dcache.webdav.InsufficientStorageException.checkStorageSufficient;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.net.InetAddresses;
import com.google.common.net.MediaType;
import com.google.common.net.PercentEscaper;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.SpaceException;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsReadExtendedAttributesMessage;
import diskCacheV111.vehicles.PnfsWriteExtendedAttributesMessage.Mode;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginManagerChildrenInfo;
import io.milton.http.HttpManager;
import io.milton.http.Request;
import io.milton.http.ResourceFactory;
import io.milton.http.ResponseStatus;
import io.milton.http.exceptions.BadRequestException;
import io.milton.resource.Resource;
import io.milton.servlet.ServletRequest;
import io.milton.servlet.ServletResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.QueryStringEncoder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.PostConstruct;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.dcache.auth.Origin;
import org.dcache.auth.RolePrincipal;
import org.dcache.auth.RolePrincipal.Role;
import org.dcache.auth.SubjectWrapper;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.LoginAttributes;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.http.AuthenticationHandler;
import org.dcache.http.PathMapper;
import org.dcache.missingfiles.AlwaysFailMissingFileStrategy;
import org.dcache.missingfiles.MissingFileStrategy;
import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.PoolManagerStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.util.Args;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.Checksums;
import org.dcache.util.Exceptions;
import org.dcache.util.NetworkUtils;
import org.dcache.util.PingMoversTask;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.util.Xattrs;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.dcache.webdav.owncloud.OwncloudClients;
import org.dcache.webdav.transfer.RemoteTransferHandler;
import org.eclipse.jetty.io.EofException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.stringtemplate.v4.AutoIndentWriter;
import org.stringtemplate.v4.ST;

/**
 * This ResourceFactory exposes the dCache name space through the Milton WebDAV framework.
 */
public class DcacheResourceFactory
      extends AbstractCellComponent
      implements ResourceFactory, CellMessageReceiver, CellCommandListener, CellInfoProvider {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(DcacheResourceFactory.class);

    private static final XMLOutputFactory XML_OUTPUT_FACTORY = XMLOutputFactory.newFactory();


    public static final String TRANSACTION_ATTRIBUTE = "org.dcache.transaction";

    private static final Set<FileAttribute> REQUIRED_ATTRIBUTES =
          EnumSet.of(TYPE, PNFSID, CREATION_TIME, MODIFICATION_TIME, SIZE,
                MODE, OWNER, OWNER_GROUP, XATTR);

    private static final String HTML_TEMPLATE_LISTING_NAME = "page";
    private static final String HTML_TEMPLATE_CLIENT_NAME = "client";

    // Additional attributes needed for PROPFIND requests; e.g., to supply
    // values for properties.
    private static final Set<FileAttribute> PROPFIND_ATTRIBUTES = Sets.union(
          EnumSet.of(CHECKSUM, ACCESS_LATENCY, RETENTION_POLICY),
          PoolMonitorV5.getRequiredAttributesForFileLocality());

    private static final String PROTOCOL_INFO_NAME = "Http";
    private static final String PROTOCOL_INFO_SSL_NAME = "Https";

    private static final int PROTOCOL_INFO_MAJOR_VERSION = 1;
    private static final int PROTOCOL_INFO_MINOR_VERSION = 1;
    private static final int PROTOCOL_INFO_UNKNOWN_PORT = 0;

    private static final long PING_DELAY = 300000;

    private static final Splitter PATH_SPLITTER =
          Splitter.on('/').omitEmptyStrings();

    private static final PercentEscaper METALINK_NAME_ESCAPER = new PercentEscaper("", false);

    // See https://www.iana.org/assignments/hash-function-text-names/hash-function-text-names.xhtml
    private static final Map<ChecksumType,String> METALINK_NAMES_FOR_CHECKSUMS = Map.of(
            ChecksumType.MD5_TYPE, "md5",
            ChecksumType.SHA1, "sha-1",
            ChecksumType.SHA256, "sha-256",
            ChecksumType.SHA512, "sha-512");

    /**
     * In progress transfers. The key of the map is the session id of the transfer.
     * <p>
     * Note that the session id is cast to an integer - this is because HttpProtocolInfo uses
     * integer ids. Casting the session ID increases the risk of collision due to wrapping of the
     * ID. However this can only happen if transfers are longer than 50 days.
     */
    private final Map<Integer, HttpTransfer> _transfers =
          Maps.newConcurrentMap();

    /**
     * Cache of the current status of space reservations.
     */
    private LoadingCache<String, Optional<Space>> _spaceLookupCache;
    private LoadingCache<FsPath, Optional<String>> _writeTokenCache;

    private ListDirectoryHandler _list;

    private ScheduledExecutorService _executor;

    private int _moverTimeout;
    private TimeUnit _moverTimeoutUnit;
    private long _killTimeout = 1500;
    private TimeUnit _killTimeoutUnit = MILLISECONDS;
    private long _transferConfirmationTimeout = 60000;
    private TimeUnit _transferConfirmationTimeoutUnit = MILLISECONDS;
    private int _bufferSize = KiB.toBytes(64);
    private CellStub _poolStub;
    private PoolManagerStub _poolManagerStub;
    private CellStub _billingStub;
    private PnfsHandler _pnfs;
    private String _ioQueue;
    private PathMapper _pathMapper;
    private List<FsPath> _allowedPaths =
          Collections.singletonList(FsPath.ROOT);
    private InetAddress _internalAddress;
    private String _path;
    private boolean _doRedirectOnRead = true;
    private boolean _doRedirectOnWrite = true;
    private boolean _impatientClientProxied = true;
    private boolean _isOverwriteAllowed;
    private boolean _isAnonymousListingAllowed;

    private String _staticContentPath;
    private ReloadableTemplate _template;
    private ImmutableMap<String, String> _templateConfig;
    private RemoteTransferHandler _remoteTransferHandler;

    private TransferRetryPolicy _retryPolicy;

    private MissingFileStrategy _missingFileStrategy =
          new AlwaysFailMissingFileStrategy();

    private PoolMonitor _poolMonitor;
    private boolean _redirectToHttps;

    private Consumer<DoorRequestInfoMessage> _kafkaSender = (s) -> {
    };

    public DcacheResourceFactory()
          throws UnknownHostException {
        _internalAddress = InetAddress.getLocalHost();
    }

    @Autowired(required = false)
    private void setTransferTemplate(KafkaTemplate kafkaTemplate) {
        _kafkaSender = kafkaTemplate::sendDefault;
    }

    @Required
    public void setSpaceLookupCache(LoadingCache<String, Optional<Space>> cache) {
        _spaceLookupCache = cache;
    }

    @Required
    public void setWriteTokenCache(LoadingCache<FsPath, Optional<String>> cache) {
        _writeTokenCache = cache;
    }

    @Required
    public void setPoolMonitor(PoolMonitor monitor) {
        _poolMonitor = monitor;
    }

    @Required
    public void setRedirectToHttps(boolean redirectToHttps) {
        _redirectToHttps = redirectToHttps;
    }

    @Required
    public void setRemoteTransferHandler(RemoteTransferHandler handler) {
        _remoteTransferHandler = requireNonNull(handler);
    }

    /**
     * Returns the kill timeout in milliseconds.
     */
    public long getKillTimeout() {
        return _killTimeout;
    }

    /**
     * The kill timeout is the time we wait for a transfer to terminate after we killed the mover.
     *
     * @param timeout The mover timeout in milliseconds
     */
    public void setKillTimeout(long timeout) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be positive");
        }
        _killTimeout = timeout;
    }

    public void setKillTimeoutUnit(TimeUnit unit) {
        _killTimeoutUnit = requireNonNull(unit);
    }

    public TimeUnit getKillTimeoutUnit() {
        return _killTimeoutUnit;
    }

    /**
     * Returns the mover timeout in milliseconds.
     */
    public int getMoverTimeout() {
        return _moverTimeout;
    }

    /**
     * The mover timeout is the time we wait for the mover to start after having been enqueued.
     *
     * @param timeout The mover timeout in milliseconds
     */
    @Required
    public void setMoverTimeout(int timeout) {
        checkState(_moverTimeout == 0, "mover timeout already set");
        checkArgument(timeout > 0, "mover timeout must be positive");
        _moverTimeout = timeout;
    }

    @Required
    public void setMoverTimeoutUnit(TimeUnit unit) {
        checkState(_moverTimeoutUnit == null, "mover timeout unit already set");
        _moverTimeoutUnit = requireNonNull(unit);
    }

    public TimeUnit getMoverTimeoutUnit() {
        return _moverTimeoutUnit;
    }

    /**
     * Returns the transfer confirmation timeout in milliseconds.
     */
    public long getTransferConfirmationTimeout() {
        return _transferConfirmationTimeout;
    }

    /**
     * The transfer confirmation timeout is the time we wait after we know that an upload has
     * finished and until we received the transfer confirmation message from the pool.
     *
     * @param timeout The transfer confirmation timeout in milliseconds
     */
    public void setTransferConfirmationTimeout(long timeout) {
        _transferConfirmationTimeout = timeout;
    }

    public void setTransferConfirmationTimeoutUnit(TimeUnit unit) {
        _transferConfirmationTimeoutUnit = requireNonNull(unit);
    }

    public TimeUnit getTransferConfirmationTimeoutUnit() {
        return _transferConfirmationTimeoutUnit;
    }

    /**
     * Returns the buffer size in bytes.
     */
    public int getBufferSize() {
        return _bufferSize;
    }

    /**
     * Sets the size of the buffer used when proxying uploads.
     *
     * @param bufferSize The buffer size in bytes
     */
    public void setBufferSize(int bufferSize) {
        _bufferSize = bufferSize;
    }

    /**
     * Provide the mapping between request path and dCache internal path.
     */
    @Required
    public void setPathMapper(PathMapper mapper) {
        _pathMapper = mapper;
    }

    /**
     * Set the list of paths for which we allow access. Paths are separated by a colon. This paths
     * are relative to the root path.
     */
    public void setAllowedPaths(String s) {
        List<FsPath> list = new ArrayList<>();
        for (String path : s.split(":")) {
            list.add(FsPath.create(path));
        }
        _allowedPaths = list;
    }

    /**
     * Returns the list of allowed paths.
     */
    public String getAllowedPaths() {
        StringBuilder s = new StringBuilder();
        for (FsPath path : _allowedPaths) {
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
    public String getIoQueue() {
        return (_ioQueue == null) ? "" : _ioQueue;
    }

    /**
     * Sets the pool IO queue to use for WebDAV transfers.
     */
    public void setIoQueue(String ioQueue) {
        _ioQueue = (ioQueue != null && !ioQueue.isEmpty()) ? ioQueue : null;
    }

    /**
     * Sets whether read requests are redirected to the pool. If not, then the door will act as a
     * proxy.
     */
    public void setRedirectOnReadEnabled(boolean redirect) {
        _doRedirectOnRead = redirect;
    }

    public boolean isRedirectOnReadEnabled() {
        return _doRedirectOnRead;
    }

    public void setRedirectOnWriteEnabled(boolean redirect) {
        _doRedirectOnWrite = redirect;
    }

    public boolean isRedirectOnWriteEnabled() {
        return _doRedirectOnWrite;
    }

    public void setImpatientClientProxied(boolean proxied) {
        _impatientClientProxied = proxied;
    }

    public boolean isImpatientClientProxied() {
        return _impatientClientProxied;
    }

    /**
     * Sets whether existing files may be overwritten.
     */
    public void setOverwriteAllowed(boolean allowed) {
        _isOverwriteAllowed = allowed;
    }

    public boolean isOverwriteAllowed() {
        return _isOverwriteAllowed;
    }

    public void setAnonymousListing(boolean isAllowed) {
        _isAnonymousListingAllowed = isAllowed;
    }

    public boolean isAnonymousListing() {
        return _isAnonymousListingAllowed;
    }

    /**
     * Sets the cell stub for PnfsManager communication.
     */
    public void setPnfsStub(CellStub stub) {
        _pnfs = new PnfsHandler(stub);
    }

    /**
     * Sets the cell stub for pool communication.
     */
    public void setPoolStub(CellStub stub) {
        _poolStub = stub;
    }

    /**
     * Sets the cell stub for PoolManager communication.
     *
     * @param stub
     */
    public void setPoolManagerStub(PoolManagerStub stub) {
        _poolManagerStub = stub;
    }

    /**
     * Sets the cell stub for billing communication.
     */
    public void setBillingStub(CellStub stub) {
        _billingStub = stub;
    }

    /**
     * Sets the behaviour of this door when the user requests a file that doesn't exist.
     */
    public void setMissingFileStrategy(MissingFileStrategy strategy) {
        _missingFileStrategy = strategy;
    }

    /**
     * Sets the ListDirectoryHandler used for directory listing.
     */
    public void setListHandler(ListDirectoryHandler list) {
        _list = list;
    }

    /**
     * Sets the resource containing the StringTemplateGroup for directory listing.
     */
    @Required
    public void setTemplate(ReloadableTemplate template) {
        _template = template;
    }

    @Required
    public void setTemplateConfig(ImmutableMap<String, String> config) {
        _templateConfig = config;
    }

    /**
     * Returns the static content path.
     */
    public String getStaticContentPath() {
        return _staticContentPath;
    }

    /**
     * The static content path is the path under which the service exports the static content. This
     * typically contains stylesheets and image files.
     */
    public void setStaticContentPath(String path) {
        _staticContentPath = path;
    }

    /**
     * Sets the ScheduledExecutorService used for periodic tasks.
     */
    public void setExecutor(ScheduledExecutorService executor) {
        _executor = executor;
        _executor.scheduleAtFixedRate(new PingMoversTask<>(_transfers.values()),
              PING_DELAY, PING_DELAY,
              MILLISECONDS);
    }

    public void setInternalAddress(String ipString)
          throws IllegalArgumentException, UnknownHostException {
        _internalAddress = NetworkUtils.getInternalAddress(ipString);
    }

    public String getInternalAddress() {
        return _internalAddress.getHostAddress();
    }

    @PostConstruct
    public void init() {
        checkState(_moverTimeout > 0, "Mover timeout not specified");
        checkState(_moverTimeoutUnit != null, "Mover timeout units not specified");
        _retryPolicy = tryOnce().timeoutAfter(_moverTimeout, _moverTimeoutUnit);
    }

    public boolean isDoorRoot(FsPath path) {
        return _pathMapper.isDoorRoot(ServletRequest.getRequest(), path);
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.println("Allowed paths: " + getAllowedPaths());
        pw.println("IO queue     : " + getIoQueue());
    }

    @Override
    public Resource getResource(String host, String requestPath) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Resolving {}", HttpManager.request().getAbsoluteUrl());
        }

        FsPath dCachePath = _pathMapper.asDcachePath(ServletRequest.getRequest(),
              requestPath, m -> new ForbiddenException(m, null));
        return getResource(dCachePath);
    }

    /**
     * Returns the resource object for a path.
     *
     * @param path The full path
     */
    public DcacheResource getResource(FsPath path) {
        checkPathAllowed(path);

        String requestPath = getRequestPath();
        boolean haveRetried = false;
        Subject subject = getSubject();

        try {
            while (true) {
                try {
                    PnfsHandler pnfs = roleAwarePnfsHandler();
                    Set<FileAttribute> requestedAttributes =
                          buildRequestedAttributes();
                    FileAttributes attributes =
                          pnfs.getFileAttributes(path.toString(), requestedAttributes);
                    return getResource(path, attributes);
                } catch (FileNotFoundCacheException e) {
                    if (haveRetried) {
                        return null;
                    } else {
                        switch (_missingFileStrategy.recommendedAction(subject,
                              path, requestPath)) {
                            case FAIL:
                                return null;
                            case RETRY:
                                haveRetried = true;
                                break;
                        }
                    }
                }
            }
        } catch (PermissionDeniedCacheException e) {
            throw WebDavExceptions.permissionDenied(e.getMessage(), e,
                  new InaccessibleResource(path));
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, new InaccessibleResource(path));
        }
    }

    /**
     * Returns the resource object for a path.
     *
     * @param path       The full path
     * @param attributes The attributes of the object identified by the path
     */
    private DcacheResource getResource(FsPath path, FileAttributes attributes) {
        if (attributes.getFileType() == DIR) {
            return new DcacheDirectoryResource(this, path, attributes);
        } else {
            return new DcacheFileResource(this, path, attributes);
        }
    }

    /**
     * Returns a boolean indicating if the request should be redirected to a pool.
     *
     * @param request a Request
     * @return a boolean indicating if the request should be redirected
     */
    public boolean shouldRedirect(Request request) {
        switch (request.getMethod()) {
            case GET:
                return isRedirectOnReadEnabled();
            case PUT:
                boolean expects100Continue =
                      Objects.equal(request.getExpectHeader(), HttpHeaders.Values.CONTINUE);

                /* Has the client started sending the entity for a PUT request that
                 * is using the expect-100 protocol.  RFC 7231 says:
                 *
                 *   o  A client that sends a 100-continue expectation is not required
                 *      to wait for any specific length of time; such a client MAY
                 *      proceed to send the message body even if it has not yet
                 *      received a response.  Furthermore, since 100 (Continue)
                 *      responses cannot be sent through an HTTP/1.0 intermediary, such
                 *      a client SHOULD NOT wait for an indefinite period before
                 *      sending the message body.
                 *
                 * By default, libcurl waits a maximum of one second for the
                 * "100 Continue" response before sending the content anyway,
                 * libwww-perl waits two seconds, Apache common httpclient waits
                 * three seconds.
                 *
                 * The redirection response could take longer than one second if
                 * the mover is queued on the pool, or the pool is busy and delays
                 * starting the mover, or ...
                 *
                 * If the client times out waiting for dCache's response and
                 * started sending the HTTP entity (the file's data) then the client
                 * will only notice the redirection after sending the entire fire.
                 * This forces the client to send the data twice.
                 *
                 * To avoid this, the door can proxy the data if the client has
                 * already started to send the entity.
                 */
                boolean proxyImpatientClient = expects100Continue
                      && isImpatientClientProxied()
                      && ((MiltonHandler.DcacheServletRequest) request).isClientSendingEntity();

                if (proxyImpatientClient) {
                    LOGGER.warn(
                          "Proxying transfer because client has already started sending data.");
                }

                return isRedirectOnWriteEnabled() && expects100Continue && !proxyImpatientClient;
            default:
                return false;
        }
    }

    /**
     * Creates a new file. The door will relay all data to the pool.
     */
    public DcacheResource createFile(FsPath path, InputStream inputStream, Long length)
          throws CacheException, InterruptedException, IOException,
          URISyntaxException, BadRequestException {
        Subject subject = getSubject();
        Restriction restriction = getRestriction();

        checkUploadSize(length);

        WriteTransfer transfer = new WriteTransfer(_pnfs, subject, restriction, path);
        _transfers.put((int) transfer.getId(), transfer);
        try {
            boolean success = false;
            transfer.setProxyTransfer(true);
            transfer.createNameSpaceEntry();
            try {
                transfer.setLength(length);
                try {
                    transfer.selectPoolAndStartMover(_retryPolicy);
                    String uri = transfer.waitForRedirect(_moverTimeout, _moverTimeoutUnit);
                    if (uri == null) {
                        throw new TimeoutCacheException("Server is busy (internal timeout)");
                    }
                    transfer.relayData(inputStream);
                } catch (EofException e) {
                    // REVISIT: do we wish to log diagnostic/forensic details of the transfer?
                    LOGGER.info("Proxied upload of {} failed: client disconnected", transfer.getPnfsId());

                    String explanation = "Client disconnected while proxying an upload.";
                    transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, explanation);
                    transfer.killMover(explanation);
                    throw new ClientDisconnectedException(explanation);
                } catch (IOException e) {
                    String message = Exceptions.messageOrClassName(e);
                    transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                          "Error relaying data: " + message);
                    transfer.killMover("door experienced error relaying data: " + message);
                    throw e;
                } catch (BadRequestException e) {
                    transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                          e.getMessage());
                    transfer.killMover("pool reported bad request: " + e.getMessage());
                    throw e;
                } catch (InsufficientStorageException e) {
                    transfer.notifyBilling(CacheException.RESOURCE, e.getMessage());
                    transfer.killMover("pool reported insufficient storage: " + e.getMessage());
                    throw e;
                } catch (CacheException e) {
                    transfer.notifyBilling(e.getRc(), e.getMessage());
                    transfer.killMover("door experienced internal error: " + e.getMessage());
                    throw e;
                } catch (InterruptedException e) {
                    transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                          "Shutting down");
                    transfer.killMover("door shutting down");
                    throw e;
                } catch (RuntimeException e) {
                    transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                          "Found bug: " + e);
                    transfer.killMover("door encountered a bug");
                    throw e;
                } finally {
                    transfer.killMover("door found orphaned mover");
                }
                success = true;
            } finally {
                if (!success) {
                    transfer.deleteNameSpaceEntry();
                }
            }
        } finally {
            _transfers.remove((int) transfer.getId());
        }

        return getResource(path);
    }

    public String getWriteUrl(FsPath path, Long length)
          throws CacheException, InterruptedException,
          URISyntaxException {
        Subject subject = getSubject();
        Restriction restriction = getRestriction();

        checkUploadSize(length);

        String uri = null;
        WriteTransfer transfer = new WriteTransfer(_pnfs, subject, restriction, path);
        transfer.setSSL(_redirectToHttps && ServletRequest.getRequest().isSecure());
        _transfers.put((int) transfer.getId(), transfer);
        try {
            transfer.createNameSpaceEntry();
            try {
                transfer.setLength(length);
                transfer.selectPoolAndStartMover(_retryPolicy);
                uri = transfer.waitForRedirect(_moverTimeout, _moverTimeoutUnit);
                if (uri == null) {
                    throw new TimeoutCacheException("Server is busy (internal timeout)");
                }

                transfer.setStatus("Mover " + transfer.getPool() + "/" +
                      transfer.getMoverId() + ": Waiting for completion");
            } catch (CacheException e) {
                transfer.notifyBilling(e.getRc(), e.getMessage());
                transfer.killMover("door experienced internal error: " + e.getMessage());
                throw e;
            } catch (InterruptedException e) {
                transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                      "Shutting down");
                transfer.killMover("door shutting down");
                throw e;
            } catch (RuntimeException e) {
                transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                      e.toString());
                transfer.killMover("door encountered a bug");
                throw e;
            } finally {
                if (uri == null) {
                    transfer.killMover("door found orphaned mover");
                    transfer.deleteNameSpaceEntry();
                }
            }
        } finally {
            if (uri == null) {
                _transfers.remove((int) transfer.getId());
            }
        }
        return appendReferrer(uri);
    }


    /**
     * Reads the content of a file. The door will relay all data from a pool.
     */
    public void readFile(FsPath path, PnfsId pnfsid,
          OutputStream outputStream, io.milton.http.Range range)
          throws CacheException, InterruptedException, IOException,
          URISyntaxException {
        ReadTransfer transfer = beginRead(path, pnfsid, true, null);

        try {
            transfer.relayData(outputStream, range);
        } catch (CacheException e) {
            transfer.notifyBilling(e.getRc(), e.getMessage());
            transfer.killMover("door experienced internal error: " + e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  "Shutting down");
            transfer.killMover("door shutting down");
            throw e;
        } catch (EofException e) {
            // REVISIT: do we wish to log diagnostic/forensic details of the transfer?
            LOGGER.info("Proxied download of {} failed: client disconnected", transfer.getPnfsId());

            String explanation = "Client disconnected while proxying a download.";
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, explanation);
            transfer.killMover(explanation);
            throw new ClientDisconnectedException(explanation);
        } catch (IOException e) {
            String message = Exceptions.messageOrClassName(e);
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  "Error relaying data: " + message);
            transfer.killMover("door experienced error relaying data: " + message);
            throw e;
        } catch (RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  "Found bug: " + e);
            transfer.killMover("door encountered a bug");
            throw e;
        } finally {
            transfer.killMover("door found orphaned mover");
            _transfers.remove((int) transfer.getId());
        }
    }

    /**
     * Performs a directory listing returning a list of Resource objects.
     */
    public List<DcacheResource> list(final FsPath path)
          throws InterruptedException, CacheException {
        if (!_isAnonymousListingAllowed && Subjects.isNobody(getSubject())) {
            throw new PermissionDeniedCacheException("Access denied");
        }

        final List<DcacheResource> result = new ArrayList<>();
        DirectoryListPrinter printer =
              new DirectoryListPrinter() {
                  @Override
                  public Set<FileAttribute> getRequiredAttributes() {
                      return buildRequestedAttributes();
                  }

                  @Override
                  public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry) {
                      result.add(getResource(path.child(entry.getName()),
                            entry.getFileAttributes()));
                  }
              };

        _list.printDirectory(getSubject(), getRestriction(), printer, path, null,
              Range.<Integer>all());
        return result;
    }

    private class FileLocalityWrapper {

        private final FileLocality _inner;

        FileLocalityWrapper(FileLocality inner) {
            _inner = inner;
        }

        public boolean isOnline() {
            return _inner == FileLocality.ONLINE;
        }

        public boolean isNearline() {
            return _inner == FileLocality.NEARLINE;
        }

        public boolean isOnlineAndNearline() {
            return _inner == FileLocality.ONLINE_AND_NEARLINE;
        }

        public boolean isLost() {
            return _inner == FileLocality.LOST;
        }

        public boolean isUnavailable() {
            return _inner == FileLocality.UNAVAILABLE;
        }
    }

    private String getRequestPath() {
        Request request = HttpManager.request();
        return URI.create(request.getAbsoluteUrl()).getPath();
    }

    private String getQueryString() {
        return ServletRequest.getRequest().getQueryString();
    }

    private String getRemoteAddr() {
        return HttpManager.request().getRemoteAddr();
    }

    private void addTemplateAttributes(ST template) {
        String requestPath = getRequestPath();
        String[] base =
              Iterables.toArray(PATH_SPLITTER.split(requestPath), String.class);

        String relPathOfRoot = Joiner.on("/").join(limit(cycle(".."), base.length));

        template.add("path", asList(UrlPathWrapper.forPaths(base)));
        template.add("static", _staticContentPath);
        template.add("subject", new SubjectWrapper(getSubject()));
        template.add("base", UrlPathWrapper.forEmptyPath());
        template.add("config", _templateConfig);
        template.add("root", relPathOfRoot);
        template.add("query", getQueryString());
    }

    /**
     * Deliver a client to the user in response to a GET request on a directory. No effort is made
     * to provide the client with the contents of this directory. It is expected that the client
     * obtains the information it needs by itself; e.g., by executing JavaScript that is supplied
     * within the HTML response.
     *
     * @return true if a client has been sent.
     */
    public boolean deliverClient(FsPath path, Writer out) throws IOException {
        final ST t = _template.getInstanceOfQuietly(HTML_TEMPLATE_CLIENT_NAME);

        if (t == null) {
            return false;
        }

        addTemplateAttributes(t);
        t.write(new AutoIndentWriter(out));
        return true;
    }

    /**
     * Performs a directory listing, writing an HTML view to an output stream.
     */
    public void list(FsPath path, Writer out)
          throws InterruptedException, CacheException, IOException {
        if (!_isAnonymousListingAllowed && Subjects.isNobody(getSubject())) {
            throw new PermissionDeniedCacheException("Access denied");
        }

        final ST t = _template.getInstanceOf(HTML_TEMPLATE_LISTING_NAME);

        if (t == null) {
            out.append(DcacheHtmlResponseHandler.templateNotFoundErrorPage(_template.getPath(),
                  HTML_TEMPLATE_LISTING_NAME));
            return;
        }

        addTemplateAttributes(t);

        DirectoryListPrinter printer =
              new DirectoryListPrinter() {
                  @Override
                  public Set<FileAttribute> getRequiredAttributes() {
                      return EnumSet.copyOf(
                            Sets.union(PoolMonitorV5.getRequiredAttributesForFileLocality(),
                                  EnumSet.of(MODIFICATION_TIME, TYPE, SIZE)));
                  }

                  private FileAttributes entryAttributes(FsPath dir, DirectoryEntry entry) {
                      FileAttributes attr = entry.getFileAttributes();
                      switch (attr.getFileType()) {
                          case LINK:
                              String entryPath = dir.child(entry.getName()).toString();
                              try {
                                  return _pnfs.getFileAttributes(entryPath,
                                        getRequiredAttributes());
                              } catch (CacheException e) {
                                  LOGGER.debug("Symlink lookup of {} failed with {}",
                                        entryPath, e.getMessage());
                                  return attr;
                              }

                          default:
                              return attr;
                      }
                  }

                  @Override
                  public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry) {
                      FileAttributes attr = entryAttributes(dir, entry);
                      Date mtime = new Date(attr.getModificationTime());
                      UrlPathWrapper name =
                            UrlPathWrapper.forPath(entry.getName());
                      /* FIXME: SIZE is defined if client specifies the
                       * file's size before uploading.
                       */
                      boolean isUploading = !attr.isDefined(SIZE);
                      FileLocality locality = _poolMonitor.getFileLocality(attr, getRemoteAddr());
                      t.addAggr(
                            "files.{name,isDirectory,showGhosted,mtime,size,isUploading,locality}",
                            name,
                            attr.getFileType() == DIR,
                            attr.getFileType() == LINK,
                            mtime,
                            attr.getSizeIfPresent().map(SizeWrapper::new).orElse(null),
                            isUploading,
                            new FileLocalityWrapper(locality));
                  }
              };
        _list.printDirectory(getSubject(), getRestriction(), printer, path, null,
              Range.<Integer>all());

        t.write(new AutoIndentWriter(out));
    }

    /**
     * Performs a directory listing, writing a metalink description.
     */
    public void metalink(FsPath path, Writer out, URI uri)
            throws InterruptedException, CacheException, IOException, XMLStreamException {
        if (!_isAnonymousListingAllowed && Subjects.isNobody(getSubject())) {
            throw new PermissionDeniedCacheException("Access denied");
        }

        XMLStreamWriter sw = XML_OUTPUT_FACTORY.createXMLStreamWriter(out);

        sw.writeStartDocument();

        DirectoryListPrinter printer =
              new DirectoryListPrinter() {
                  @Override
                  public Set<FileAttribute> getRequiredAttributes() {
                      return Set.of(MODIFICATION_TIME, TYPE, SIZE, CHECKSUM);
                  }

                  private FileAttributes entryAttributes(FsPath dir, DirectoryEntry entry) {
                      FileAttributes attr = entry.getFileAttributes();
                      switch (attr.getFileType()) {
                          case LINK:
                              String entryPath = dir.child(entry.getName()).toString();
                              try {
                                  return _pnfs.getFileAttributes(entryPath,
                                        getRequiredAttributes());
                              } catch (CacheException e) {
                                  LOGGER.debug("Symlink lookup of {} failed with {}",
                                        entryPath, e.getMessage());
                                  return attr;
                              }

                          default:
                              return attr;
                      }
                  }

                  @Override
                  public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry) {
                      FileAttributes attr = entryAttributes(dir, entry);
                      var mtime = Instant.ofEpochMilli(attr.getModificationTime());
                      String safeName = METALINK_NAME_ESCAPER.escape(entry.getName());
                      URI target = uri.resolve(safeName);

                      if (attr.getFileType() != REGULAR && attr.getFileType() != LINK) {
                          return;
                      }

                      /* FIXME: SIZE is defined if client specifies the
                       * file's size before uploading.
                       */
                      if (attr.getFileType() == REGULAR && !attr.isDefined(SIZE)) {
                          return;
                      }

                      try {
                          sw.writeStartElement("file");
                          try {
                              sw.writeAttribute("name", entry.getName());

                              if (attr.isDefined(SIZE)) {
                                  sw.writeStartElement("size");
                                  try {
                                      sw.writeCharacters(Long.toString(attr.getSize()));
                                  } finally {
                                      sw.writeEndElement();
                                  }
                              }
                              if (attr.isDefined(CHECKSUM)) {
                                  for (Checksum checksum : attr.getChecksums()) {
                                        String type = METALINK_NAMES_FOR_CHECKSUMS.get(checksum.getType());
                                        if (type == null) {
                                            continue;
                                        }
                                        sw.writeStartElement("hash");
                                        try {
                                            sw.writeAttribute("type", type);
                                            sw.writeCharacters(checksum.getValue());
                                        } finally {
                                            sw.writeEndElement();
                                        }
                                  }
                              }
                              sw.writeStartElement("url");
                              try {
                                  sw.writeCharacters(target.toASCIIString());
                              } finally {
                                  sw.writeEndElement();
                              }
                              sw.writeStartElement("updated");
                              try {
                                  sw.writeCharacters(mtime.toString());
                              } finally {
                                  sw.writeEndElement();
                              }
                          } finally {
                              sw.writeEndElement();
                          }
                      } catch (XMLStreamException e) {
                          LOGGER.warn("Failed to process directory item {}: {}", entry.getName(),
                              e.toString());
                      }
                  }
              };

        sw.writeStartElement("metalink");
        sw.writeDefaultNamespace("urn:ietf:params:xml:ns:metalink");
        try {
            _list.printDirectory(getSubject(), getRestriction(), printer, path, null,
                  Range.<Integer>all());
        } finally {
            sw.writeEndElement();
        }

        sw.writeEndDocument();
    }

    /**
     * Deletes a file.
     */
    public void deleteFile(FileAttributes attributes, FsPath path)
          throws CacheException {
        PnfsHandler pnfs = roleAwarePnfsHandler();
        pnfs.deletePnfsEntry(attributes.getPnfsId(), path.toString(),
              EnumSet.of(REGULAR, LINK), EnumSet.noneOf(FileAttribute.class));
        sendRemoveInfoToBilling(attributes, path);
    }

    private void sendRemoveInfoToBilling(FileAttributes attributes, FsPath path) {
        DoorRequestInfoMessage infoRemove =
              new DoorRequestInfoMessage(getCellAddress(), "remove");
        Subject subject = getSubject();
        infoRemove.setSubject(subject);
        infoRemove.setBillingPath(path.toString());
        infoRemove.setPnfsId(attributes.getPnfsId());
        infoRemove.setFileSize(attributes.getSizeIfPresent().orElse(0L));
        infoRemove.setClient(Subjects.getOrigin(subject).getAddress().getHostAddress());
        _billingStub.notify(infoRemove);

        try {
            _kafkaSender.accept(infoRemove);
        } catch (KafkaException | org.apache.kafka.common.KafkaException e) {
            LOGGER.warn("Failed to send message to kafka: {} ", Throwables.getRootCause(e).getMessage());
        }
    }

    /**
     * Deletes a directory.
     */
    public void deleteDirectory(PnfsId pnfsid, FsPath path)
          throws CacheException {
        PnfsHandler pnfs = roleAwarePnfsHandler();
        pnfs.deletePnfsEntry(pnfsid, path.toString(), EnumSet.of(DIR),
              EnumSet.noneOf(FileAttribute.class));
    }

    /**
     * Create a new directory.
     */
    public DcacheDirectoryResource
    makeDirectory(FileAttributes parent, FsPath path)
          throws CacheException {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, getSubject(), getRestriction());
        PnfsCreateEntryMessage reply =
              pnfs.createPnfsDirectory(path.toString(), REQUIRED_ATTRIBUTES);

        return new DcacheDirectoryResource(this, path, reply.getFileAttributes());
    }

    public void move(FsPath sourcePath, PnfsId pnfsId, FsPath newPath)
          throws CacheException {
        PnfsHandler pnfs = roleAwarePnfsHandler();
        pnfs.renameEntry(pnfsId, sourcePath.toString(), newPath.toString(), true);
    }

    private URI buildRequestUri() {
        HttpServletRequest request = ServletRequest.getRequest();

        try {
            return new URI(request.getScheme(), null, request.getServerName(),
                    request.getServerPort(), null, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to build request URI: " + e);
        }
    }

    private String appendReferrer(String locationWithoutReferrer) {
        URI target = buildRequestUri();

        QueryStringDecoder decoder = new QueryStringDecoder(locationWithoutReferrer);
        Map<String, List<String>> existingQuery = decoder.parameters();

        QueryStringEncoder encoder = new QueryStringEncoder("");
        for (Map.Entry<String,List<String>> e : existingQuery.entrySet()) {
            e.getValue().forEach(v -> encoder.addParam(e.getKey(), v));
        }
        encoder.addParam(REFERRER_QUERY_PARAM, target.toASCIIString());

        URI location = URI.create(locationWithoutReferrer);
        URI updatedLocation;
        try {
            updatedLocation = new URI(location.getScheme(), null, location.getHost(),
                    location.getPort(), location.getPath(), null, null);
        } catch (URISyntaxException e) {
            LOGGER.warn("Bad URI when creating updated URL: {}", e.getMessage());
            return locationWithoutReferrer;
        }

        return updatedLocation.toASCIIString() + encoder.toString();
    }

    /**
     * Returns a read URL for a file.
     *
     * @param path   The full path of the file.
     * @param pnfsid The PNFS ID of the file.
     */
    public String getReadUrl(FsPath path, PnfsId pnfsid,
          HttpProtocolInfo.Disposition disposition)
          throws CacheException, InterruptedException, URISyntaxException {
        String uri = beginRead(path, pnfsid, false, disposition).getRedirect();
        return appendReferrer(uri);
    }

    /**
     * Initiates a read operation.
     *
     * @param path            The full path of the file.
     * @param pnfsid          The PNFS ID of the file.
     * @param isProxyTransfer
     * @return ReadTransfer encapsulating the read operation
     */
    private ReadTransfer beginRead(FsPath path, PnfsId pnfsid, boolean isProxyTransfer,
          HttpProtocolInfo.Disposition disposition) throws CacheException,
          InterruptedException, URISyntaxException {
        Subject subject = roleAwareSubject();
        Restriction restriction = roleAwareRestriction();

        String uri = null;
        ReadTransfer transfer = new ReadTransfer(_pnfs, subject, restriction,
              path, pnfsid, disposition);
        transfer.setIsChecksumNeeded(isDigestRequested());
        transfer.setSSL(
              !isProxyTransfer && _redirectToHttps && ServletRequest.getRequest().isSecure());
        _transfers.put((int) transfer.getId(), transfer);
        try {
            transfer.setProxyTransfer(isProxyTransfer);
            transfer.readNameSpaceEntry(false);
            try {
                transfer.selectPoolAndStartMover(_retryPolicy);
                uri = transfer.waitForRedirect(_moverTimeout, _moverTimeoutUnit);
                if (uri == null) {
                    throw new TimeoutCacheException("Server is busy (internal timeout)");
                }
            } finally {
                transfer.setStatus(null);
            }
            transfer.setStatus("Mover " + transfer.getPool() + "/" +
                  transfer.getMoverId() + ": Waiting for completion");
        } catch (CacheException e) {
            transfer.notifyBilling(e.getRc(), e.getMessage());
            transfer.killMover("door experienced internal error: " + e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  "Shutting down");
            transfer.killMover("door shutting down");
            throw e;
        } catch (RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.toString());
            transfer.killMover("door encountered a bug");
            throw e;
        } finally {
            if (uri == null) {
                transfer.killMover("door found orphaned mover");
                _transfers.remove((int) transfer.getId());
            }
        }
        return transfer;
    }

    /**
     * Message handler for redirect messages from the pools.
     */
    public void messageArrived(HttpDoorUrlInfoMessage message) {
        HttpTransfer transfer = _transfers.get((int) message.getId());
        if (transfer != null) {
            transfer.redirect(message.getUrl());
        }
    }

    /**
     * Message handler for transfer completion messages from the pools.
     */
    public void messageArrived(DoorTransferFinishedMessage message) {
        Transfer transfer = _transfers.get((int) message.getId());
        if (transfer != null) {
            transfer.finished(message);
        }
    }

    /**
     * Fall back message handler for mover creation replies. We only receive these if the Transfer
     * timed out before the mover was created. Instead we kill the mover.
     */
    public void messageArrived(PoolIoFileMessage message) {
        if (message.getReturnCode() == 0) {
            String pool = message.getPoolName();
            _poolStub.notify(new CellPath(pool), new PoolMoverKillMessage(pool,
                  message.getMoverId(), "door timed out before pool"));
        }
    }

    /**
     * Returns true if access to path is allowed through the WebDAV door, false otherwise.
     */
    private void checkPathAllowed(FsPath path) throws UnauthorizedException, ForbiddenException {
        if (!_allowedPaths.stream().anyMatch(path::hasPrefix)) {
            Resource denied = new DcacheResource(this, path, FileAttributes.of().build());
            throw WebDavExceptions.permissionDenied(denied);
        }
    }

    /**
     * Returns the current Subject of the calling thread.
     */
    private static Subject getSubject() {
        return Subject.getSubject(AccessController.getContext());
    }

    private Restriction getRestriction() {
        HttpServletRequest servletRequest = ServletRequest.getRequest();
        return (Restriction) servletRequest.getAttribute(
              AuthenticationHandler.DCACHE_RESTRICTION_ATTRIBUTE);
    }

    private OptionalLong getMaxUploadSize() {
        HttpServletRequest servletRequest = ServletRequest.getRequest();
        Set<LoginAttribute> attributes = AuthenticationHandler.getLoginAttributes(servletRequest);
        return LoginAttributes.maximumUploadSize(attributes);
    }

    private void checkUploadSize(Long length) {
        OptionalLong maxUploadSize = getMaxUploadSize();
        checkStorageSufficient(!maxUploadSize.isPresent() || length == null
              || length <= maxUploadSize.getAsLong(), "Upload too large");
    }

    private boolean isAdmin() {
        return getSubject().getPrincipals().stream()
              .filter(p->p instanceof RolePrincipal)
              .anyMatch(p->((RolePrincipal) p).hasRole(Role.ADMIN));
    }

    private PnfsHandler roleAwarePnfsHandler() {
        boolean isAdmin = isAdmin();
        Subject user = isAdmin ? Subjects.ROOT : getSubject();
        Restriction restriction = isAdmin ? Restrictions.none() : getRestriction();
        return new PnfsHandler(_pnfs, user, restriction);
    }

    private Subject roleAwareSubject() {
        Subject subject = getSubject();

        if (isAdmin()) {
            Origin origin = Subjects.getOrigin(subject);

            if (origin == null) {
                return Subjects.ROOT;
            } else {
                Subject adminSubject = new Subject(false,
                      Subjects.ROOT.getPrincipals(),
                      Subjects.ROOT.getPublicCredentials(),
                      Subjects.ROOT.getPrivateCredentials());
                adminSubject.getPrincipals().add(origin);
                adminSubject.setReadOnly();
                return adminSubject;
            }
        } else {
            return subject;
        }
    }

    private Restriction roleAwareRestriction() {
        return isAdmin() ? Restrictions.none() : getRestriction();
    }

    /**
     * Returns request params.
     */
    private static Map<String, String> getXattrsFromRequest() {
        return Xattrs.from(ServletRequest.getRequest().getParameterMap());
    }

    /**
     * Returns the location URI of the current request. This is the full request URI excluding user
     * information, query and fragments.
     */
    private static URI getLocation() throws URISyntaxException {
        URI uri = new URI(HttpManager.request().getAbsoluteUrl());
        return new URI(uri.getScheme(), null, uri.getHost(),
              uri.getPort(), uri.getPath(), null, null);
    }

    /**
     * To emulate LoginManager we list ourselves as our child.
     */
    public static final String hh_get_children = "[-binary]";

    public Object ac_get_children(Args args) {
        boolean binary = args.hasOption("binary");
        if (binary) {
            String[] list = new String[]{getCellName()};
            return new LoginManagerChildrenInfo(getCellName(), getCellDomainName(), list);
        } else {
            return getCellName();
        }
    }

    /**
     * Provides information about the door and current transfers.
     */
    public static final String hh_get_door_info = "[-binary]";

    public Object ac_get_door_info(Args args) {
        List<IoDoorEntry> transfers = new ArrayList<>();
        _remoteTransferHandler.addTransfers(transfers);
        for (Transfer transfer : _transfers.values()) {
            transfers.add(transfer.getIoDoorEntry());
        }

        IoDoorInfo doorInfo = new IoDoorInfo(getCellName(), getCellDomainName());
        doorInfo.setProtocol("HTTP", "1.1");
        doorInfo.setOwner("");
        doorInfo.setProcess("");
        doorInfo.setIoDoorEntries(transfers.toArray(IoDoorEntry[]::new));
        return args.hasOption("binary") ? doorInfo : doorInfo.toString();
    }

    private void initializeTransfer(HttpTransfer transfer, Subject subject)
          throws URISyntaxException {
        transfer.setLocation(getLocation());
        transfer.setCellAddress(getCellAddress());
        transfer.setPoolManagerStub(_poolManagerStub);
        transfer.setPoolStub(_poolStub);
        transfer.setBillingStub(_billingStub);
        transfer.setIoQueue(_ioQueue);
        transfer.setClientAddresses(List.of(
              new InetSocketAddress(InetAddresses.forUriString(
                    ServletRequest.getRequest().getRemoteAddr()),  ServletRequest.getRequest().getRemotePort())
        ));
        transfer.setOverwriteAllowed(_isOverwriteAllowed);
        transfer.setKafkaSender(_kafkaSender);
    }

    private Set<FileAttribute> buildRequestedAttributes() {
        Set<FileAttribute> attributes = EnumSet.copyOf(REQUIRED_ATTRIBUTES);

        if (isDigestRequested()) {
            attributes.add(CHECKSUM);
        }

        if (isPropfindRequest()) {
            // FIXME: Unfortunately, Milton parses the request body after
            // requesting the Resource, so we cannot know which additional
            // attributes are being requested; therefore, we must request all
            // of them.
            attributes.addAll(PROPFIND_ATTRIBUTES);
        }

        return attributes;
    }

    /**
     * Return the RFC 3230 Want-Digest header value, if present.  If multiple headers are present
     * then return a single value obtained by taking the values and creating the equivalent
     * comma-separated list.
     *
     * @return an Optional containing the Want-Digest header value, if present.
     */
    public static Optional<String> wantDigest() {
        Enumeration<String> e = ServletRequest.getRequest().getHeaders("Want-Digest");
        if (e == null || !e.hasMoreElements()) {
            return Optional.empty();
        }
        StringBuilder sb = new StringBuilder();
        while (e.hasMoreElements()) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            String value = e.nextElement();
            if (!value.isEmpty()) {
                sb.append(value);
            }
        }
        return Optional.of(sb.toString());
    }

    private static boolean isDigestRequested() {
        switch (HttpManager.request().getMethod()) {
            case PUT:
            case HEAD:
            case GET:
                return wantDigest()
                      .flatMap(Checksums::parseWantDigest)
                      .isPresent();
            default:
                return false;
        }
    }

    private boolean isPropfindRequest() {
        return HttpManager.request().getMethod() == Request.Method.PROPFIND;
    }

    FileLocality calculateLocality(FileAttributes attributes, String clientIP) {
        return _poolMonitor.getFileLocality(attributes, clientIP);
    }

    private Optional<Space> lookupSpaceById(String id) {
        try {
            return _spaceLookupCache.get(id);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            Throwables.throwIfUnchecked(t);
            LOGGER.warn("Failed to fetch space statistics for {}: {}", id, t.toString());
            return Optional.empty();
        }
    }

    private Optional<String> lookupWriteToken(FsPath path) {
        try {
            return _writeTokenCache.get(path);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            Throwables.throwIfUnchecked(t);
            LOGGER.warn("Failed to query for WriteToken tag on {}: {}", path, t.toString());
            return Optional.empty();
        }
    }

    public Space spaceForPath(FsPath path) throws SpaceException {
        return lookupWriteToken(path)
              .flatMap(this::lookupSpaceById)
              .orElseThrow(() -> new SpaceException("Path not under space management"));
    }

    public boolean isSpaceManaged(FsPath path) {
        return lookupWriteToken(path)
              .flatMap(this::lookupSpaceById)
              .isPresent();
    }

    Set<String> listExtendedAttributes(FsPath path) throws CacheException {
        return _pnfs.listExtendedAttributes(path);
    }

    byte[] readExtendedAttribute(FsPath path, String name) throws CacheException {
        return _pnfs.readExtendedAttribute(path, name);
    }

    Map<String, byte[]> readExtendedAttributes(FsPath path, Collection<String> names)
          throws CacheException {
        PnfsReadExtendedAttributesMessage message =
              new PnfsReadExtendedAttributesMessage(path.toString());
        names.forEach(message::addName);
        return _pnfs.request(message).getAllValues();
    }

    void writeExtendedAttribute(FsPath path, String name, byte[] value) throws CacheException {
        _pnfs.writeExtendedAttribute(path, name, value, Mode.EITHER);
    }

    void removeExtendedAttribute(FsPath path, String name) throws CacheException {
        _pnfs.removeExtendedAttribute(path, name);
    }

    /**
     * Specialisation of the Transfer class for HTTP transfers.
     */
    private class HttpTransfer extends RedirectedTransfer<String> {

        private URI _location;
        private ChecksumType _wantedChecksum;
        private InetSocketAddress _clientAddressForPool;
        protected HttpProtocolInfo.Disposition _disposition;
        private boolean _isSSL;

        /**
         * The original request path that will be passed to pool for fall-back redirect.
         */
        private final String _requestPath;

        public HttpTransfer(PnfsHandler pnfs, Subject subject,
              Restriction restriction, FsPath path) throws URISyntaxException {
            super(pnfs, subject, restriction, path);
            initializeTransfer(this, subject);
            _clientAddressForPool = getClientAddress();

            var request = ServletRequest.getRequest();
            request.setAttribute(TRANSACTION_ATTRIBUTE, getTransaction());
            _requestPath = ServletRequest.stripToPath(request.getRequestURL().toString());
        }

        protected ProtocolInfo createProtocolInfo(InetSocketAddress address) {
            HttpProtocolInfo protocolInfo =
                  new HttpProtocolInfo(
                        _isSSL ? PROTOCOL_INFO_SSL_NAME : PROTOCOL_INFO_NAME,
                        PROTOCOL_INFO_MAJOR_VERSION,
                        PROTOCOL_INFO_MINOR_VERSION,
                        address,
                        getCellName(), getCellDomainName(),
                        _requestPath,
                        _location,
                        _disposition,
                        _wantedChecksum);
            protocolInfo.setSessionId((int) getId());
            return protocolInfo;
        }

        @Override
        protected ProtocolInfo getProtocolInfoForPoolManager() {
            return createProtocolInfo(getClientAddress());
        }

        @Override
        protected ProtocolInfo getProtocolInfoForPool() {
            return createProtocolInfo(_clientAddressForPool);
        }

        public void setLocation(URI location) {
            _location = location;
        }

        public void setWantedChecksum(ChecksumType type) {
            _wantedChecksum = type;
        }

        public void setProxyTransfer(boolean isProxyTransfer) {
            if (isProxyTransfer) {
                _clientAddressForPool = new InetSocketAddress(_internalAddress, 0);
            } else {
                _clientAddressForPool = getClientAddress();
            }
        }

        public void setSSL(boolean isSSL) {
            _isSSL = isSSL;
        }

        public void killMover(String explanation) {
            killMover(_killTimeout, _killTimeoutUnit, explanation);
        }
    }

    /**
     * Specialised HttpTransfer for downloads.
     */
    private class ReadTransfer extends HttpTransfer {

        public ReadTransfer(PnfsHandler pnfs, Subject subject,
              Restriction restriction, FsPath path, PnfsId pnfsid,
              HttpProtocolInfo.Disposition disposition)
              throws URISyntaxException {
            super(pnfs, subject, restriction, path);
            setPnfsId(pnfsid);
            _disposition = disposition;
        }

        public void setIsChecksumNeeded(boolean isChecksumNeeded) {
            if (isChecksumNeeded) {
                setAdditionalAttributes(Collections.singleton(CHECKSUM));
            } else {
                setAdditionalAttributes(Collections.<FileAttribute>emptySet());
            }
        }

        public void relayData(OutputStream outputStream, io.milton.http.Range range)
              throws IOException, CacheException, InterruptedException {
            setStatus("Mover " + getPool() + "/" + getMoverId() +
                  ": Opening data connection");
            try {
                URL url = new URL(getRedirect());
                HttpURLConnection connection =
                      (HttpURLConnection) url.openConnection();
                try {
                    connection.setRequestProperty("Connection", "Close");
                    if (range != null) {
                        connection.addRequestProperty("Range",
                              String.format("bytes=%d-%d", range.getStart(), range.getFinish()));
                    }

                    connection.connect();
                    try (InputStream inputStream = connection
                          .getInputStream()) {
                        setStatus("Mover " + getPool() + "/" + getMoverId() +
                              ": Sending data");
                        inputStream.transferTo(outputStream);
                        outputStream.flush();
                    }

                } finally {
                    connection.disconnect();
                }

                if (!waitForMover(_transferConfirmationTimeout, _transferConfirmationTimeoutUnit)) {
                    throw new CacheException("Missing transfer confirmation from pool");
                }
            } catch (SocketTimeoutException e) {
                throw new TimeoutCacheException("Server is busy (internal timeout)");
            } finally {
                setStatus(null);
            }
        }


        @Override
        public synchronized void finished(CacheException error) {
            super.finished(error);

            _transfers.remove((int) getId());

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
    private class WriteTransfer extends HttpTransfer {

        private final Optional<Instant> _mtime;
        private final Optional<Checksum> _contentMd5;

        public WriteTransfer(PnfsHandler pnfs, Subject subject,
              Restriction restriction, FsPath path) throws URISyntaxException {
            super(pnfs, subject, restriction, path);

            HttpServletRequest request = ServletRequest.getRequest();

            _mtime = OwncloudClients.parseMTime(request);

            wantDigest()
                  .flatMap(Checksums::parseWantDigest)
                  .ifPresent(this::setWantedChecksum);

            try {
                _contentMd5 = Optional.ofNullable(
                            ServletRequest.getRequest().getHeader("Content-MD5"))
                      .map(Checksums::parseContentMd5);
            } catch (IllegalArgumentException e) {
                throw new UncheckedBadRequestException("Bad Content-MD5 header: " + e.toString(),
                      null);
            }
        }

        @Override
        protected FileAttributes fileAttributesForNameSpace() {
            FileAttributes attributes = super.fileAttributesForNameSpace();
            _mtime.map(Instant::toEpochMilli).ifPresent(attributes::setModificationTime);

            /**
             * Add user provided extended attributes, which will be sent to the pool.
             */
            Map<String, String> xattr = getXattrsFromRequest();
            if (!xattr.isEmpty()) {
                attributes.setXattrs(xattr);
            }

            requestContentType().ifPresent(mt -> attributes.updateXattr("mime_type", mt));

            return attributes;
        }

        private Optional<String> requestContentType() {
            String mimeType = HttpManager.request().getContentTypeHeader();

            try {
                if (mimeType != null) {
                    String normalised = MediaType.parse(mimeType).toString();
                    return Optional.of(normalised);
                }
            } catch (IllegalArgumentException e) {
                // Badly formatted MIME type in Content-Type ... just ignore
                // the value.
            }

            return Optional.empty();
        }

        @Override
        public void createNameSpaceEntry() throws CacheException {
            super.createNameSpaceEntry();

            if (_mtime.isPresent()) {
                OwncloudClients.addMTimeAccepted(ServletResponse.getResponse());
            }

            if (_contentMd5.isPresent()) {
                setChecksum(_contentMd5.get());
            }

            getMaxUploadSize().ifPresent(this::setMaximumLength);
        }

        public void relayData(InputStream inputStream)
              throws IOException, CacheException, InterruptedException, BadRequestException {
            setStatus("Mover " + getPool() + "/" + getMoverId() +
                  ": Opening data connection");
            try {
                URL url = new URL(getRedirect());
                HttpURLConnection connection =
                      (HttpURLConnection) url.openConnection();
                try {
                    connection.setRequestMethod("PUT");
                    connection.setRequestProperty("Connection", "Close");
                    connection.setDoOutput(true);
                    if (getFileAttributes().isDefined(SIZE)) {
                        connection.setFixedLengthStreamingMode(getFileAttributes().getSize());
                    } else {
                        connection.setChunkedStreamingMode(KiB.toBytes(8));
                    }
                    connection.connect();
                    try (OutputStream outputStream = connection.getOutputStream()) {
                        setStatus("Mover " + getPool() + "/" + getMoverId() +
                              ": Receiving data");
                        try {
                            inputStream.transferTo(outputStream);
                            outputStream.flush();
                        } catch (IOException e) {
                            // Although we were unable to send all the data,
                            // the pool may have replied with a valid HTTP response
                            try {
                                connection.getResponseCode();
                            } catch (IOException ignored) {
                                // oh well, propagate the original exception
                                throw e;
                            }
                        }
                    }
                    switch (connection.getResponseCode()) {
                        case ResponseStatus.SC_CREATED:
                            break;
                        case ResponseStatus.SC_BAD_REQUEST:
                            throw new BadRequestException(connection.getResponseMessage());
                        case 507: // Insufficient Storage
                            throw new InsufficientStorageException(connection.getResponseMessage(),
                                  null);
                        case ResponseStatus.SC_INTERNAL_SERVER_ERROR:
                            throw new CacheException(
                                  "Pool error: " + connection.getResponseMessage());
                        default:
                            throw new CacheException(
                                  "Unexpected pool response: " + connection.getResponseCode() + " "
                                        + connection.getResponseMessage());
                    }
                } finally {
                    connection.disconnect();
                }

                if (!waitForMover(_transferConfirmationTimeout, _transferConfirmationTimeoutUnit)) {
                    throw new CacheException("Missing transfer confirmation from pool");
                }
            } catch (SocketTimeoutException e) {
                throw new TimeoutCacheException("Server is busy (internal timeout)");
            } finally {
                setStatus(null);
            }
        }

        /**
         * Sets the length of the file to be uploaded. The length is optional and will be ignored if
         * null.
         */
        public void setLength(Long length) {
            if (length != null) {
                super.setLength(length);
            }
        }

        @Override
        public synchronized void finished(CacheException error) {
            super.finished(error);

            _transfers.remove((int) getId());

            if (error == null) {
                notifyBilling(0, "");
            } else {
                notifyBilling(error.getRc(), error.getMessage());
            }
        }
    }
}
