/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 - 2022 Deutsches Elektronen-Synchrotron
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

import static org.dcache.xrootd.CacheExceptionMapper.xrootdErrorCode;
import static org.dcache.xrootd.CacheExceptionMapper.xrootdException;
import static org.dcache.xrootd.protocol.XrootdProtocol.FilePerm;
import static org.dcache.xrootd.protocol.XrootdProtocol.UUID_PREFIX;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ArgInvalid;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ArgMissing;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_FileNotOpen;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_IOError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_InvalidRequest;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ItExists;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_Qcksum;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_Qconfig;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ServerError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_async;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_compress;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_delete;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_force;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_gr;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_gw;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_gx;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_mkpath;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_new;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_open_apnd;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_open_read;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_open_updt;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_or;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ow;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ox;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_posc;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_readable;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_refresh;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_retstat;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ur;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_uw;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ux;
import static org.dcache.xrootd.util.TriedRc.ENOENT;
import static org.dcache.xrootd.util.TriedRc.IOERR;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.net.InetAddresses;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileIsNewCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.TimeoutCacheException;
import dmg.cells.nucleus.CellPath;
import io.netty.channel.ChannelHandlerContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.auth.LoginReply;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.LoginAttributes;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.Checksum;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.vehicles.PnfsListDirectoryMessage;
import org.dcache.xrootd.LoginTokens;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.core.XrootdSession;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.DirListRequest;
import org.dcache.xrootd.protocol.messages.DirListResponse;
import org.dcache.xrootd.protocol.messages.MkDirRequest;
import org.dcache.xrootd.protocol.messages.MvRequest;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.OpenResponse;
import org.dcache.xrootd.protocol.messages.PrepareRequest;
import org.dcache.xrootd.protocol.messages.QueryRequest;
import org.dcache.xrootd.protocol.messages.QueryResponse;
import org.dcache.xrootd.protocol.messages.RedirectResponse;
import org.dcache.xrootd.protocol.messages.RmDirRequest;
import org.dcache.xrootd.protocol.messages.RmRequest;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.StatResponse;
import org.dcache.xrootd.protocol.messages.StatxRequest;
import org.dcache.xrootd.protocol.messages.StatxResponse;
import org.dcache.xrootd.protocol.messages.WaitRetryResponse;
import org.dcache.xrootd.protocol.messages.XrootdResponse;
import org.dcache.xrootd.tpc.XrootdTpcInfo;
import org.dcache.xrootd.tpc.XrootdTpcInfo.Cgi;
import org.dcache.xrootd.tpc.XrootdTpcInfo.Status;
import org.dcache.xrootd.util.ChecksumInfo;
import org.dcache.xrootd.util.FileStatus;
import org.dcache.xrootd.util.OpaqueStringParser;
import org.dcache.xrootd.util.ParseException;
import org.dcache.xrootd.util.TriedRc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel handler which redirects all open requests to a pool.
 */
public class XrootdRedirectHandler extends ConcurrentXrootdRequestHandler {

    private static final Logger _log =
          LoggerFactory.getLogger(XrootdRedirectHandler.class);

    private class LoginSessionInfo {

        private final Subject subject;
        private final Restriction restriction;
        private final OptionalLong maximumUploadSize;
        private final FsPath userRootPath;
        private final boolean loggedIn;

        LoginSessionInfo(Restriction restriction) {
            subject = new Subject();
            this.restriction = restriction;
            maximumUploadSize = OptionalLong.empty();
            userRootPath = null;
            loggedIn = false;
        }

        LoginSessionInfo(LoginReply reply) {
            subject = reply.getSubject();
            restriction = computeRestriction(reply);
            userRootPath = reply.getLoginAttributes().stream()
                  .filter(RootDirectory.class::isInstance)
                  .findFirst()
                  .map(RootDirectory.class::cast)
                  .map(RootDirectory::getRoot)
                  .map(FsPath::create)
                  .orElse(FsPath.ROOT);
            maximumUploadSize = LoginAttributes.maximumUploadSize(reply.getLoginAttributes());
            loggedIn = true;
        }

        public Restriction getRestriction() {
            return restriction;
        }

        public Subject getSubject() {
            return subject;
        }

        public OptionalLong getMaximumUploadSize() {
            return maximumUploadSize;
        }

        public FsPath getUserRootPath() {
            return userRootPath;
        }

        public boolean isLoggedIn() {
            return loggedIn;
        }

        private Restriction computeRestriction(LoginReply reply) {
            if (!Subjects.isNobody(subject)) {
                return reply.getRestriction();
            }

            switch(_door.getAnonymousUserAccess()) {
                case READONLY:
                    return Restrictions.readOnly();
                case FULL:
                    return Restrictions.none();
                default:
                    return Restrictions.denyAll();
            }
        }
    }

    private final XrootdDoor _door;
    private final Map<String, String> _appIoQueues;
    private final LoginSessionInfo _defaultLoginSessionInfo;
    private final Deque<LoginSessionInfo> _logins;
    private final FsPath _rootPath;
    private final AtomicInteger openRetry = new AtomicInteger(0);

    /**
     * Custom entries for kXR_Qconfig requests.
     */
    private final Map<String, String> _queryConfig;

    /**
     * The thread associated with the open call.
     * This is held here in case an inactive event occurs on the channel
     * and the thread is waiting for the redirect.
     */
    private volatile Thread onOpenThread;

    public XrootdRedirectHandler(XrootdDoor door, FsPath rootPath, ExecutorService executor,
          Map<String, String> queryConfig,
          Map<String, String> appIoQueues) {
        super(executor);
        _door = door;
        _rootPath = rootPath;
        _queryConfig = queryConfig;
        _appIoQueues = appIoQueues;
        _defaultLoginSessionInfo = new LoginSessionInfo(Restrictions.denyAll());
        _logins = new ArrayDeque<>(2);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof LoginEvent) {
            loggedIn((LoginEvent) event);
        } else {
            super.userEventTriggered(ctx, event);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
        if (t instanceof ClosedChannelException) {
            _log.info("Connection unexpectedly closed on {}, cause {}.", ctx.channel(),
                  Throwables.getRootCause(t).toString());
        } else if (t instanceof RuntimeException || t instanceof Error) {
            Thread me = Thread.currentThread();
            me.getUncaughtExceptionHandler().uncaughtException(me, t);
        } else if (!isHealthCheck() || !(t instanceof IOException)) {
            _log.warn("exception caught on {}: {}, cause {}.", ctx.channel(), t.getMessage(),
                  Throwables.getRootCause(t).toString());
        } else {
            _log.info("IO exception caught during health check on {}: {}, cause {}.", ctx.channel(),
                  t.getMessage(), Throwables.getRootCause(t).toString());
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        _log.info("channel inactive event received on {}.", ctx.channel());

        /**
         * If the doOnOpen call has not yet returned, interrupt its thread.
         */
        interruptOnOpenThread();
        ctx.fireChannelInactive();
    }

    /**
     * For client-server read and write, the open, if successful, will always result in a redirect
     * response to the proper pool; hence no subsequent requests like sync, read, write or close are
     * expected at the door.
     * <p>
     * For third-party copy where dCache is the source, the interactions are as follows:
     * <p>
     * 1.  The client opens the file to check availability (the 'placement' stage).  An OK response
     * is followed by the client closing the file. 2. Full TPC:  The client opens the file again
     * with rendezvous metadata.  The client will close the file only when notified by the
     * destination server that the transfer has completed. If TPC Lite (delegation), #2 is skipped.
     * 3.  The destination server will open the file for the actual read.
     * <p>
     * The order of 2, 3 is not deterministic; hence the response here must provide for the
     * possibility that the destination server attempts an open before the client specifies a
     * time-to-live on the rendezvous point.
     * <p>
     * The strategy adopted is therefore as follows:  response to (1) is simply to check file
     * permissions.  No metadata is generated and a "dummy" file handle is returned.  For 2 and 3,
     * whichever occurs first will cause a metadata object to be stored.  If the destination server
     * open occurs first, a wait response will tell the server to try again in a maximum of 3
     * seconds; otherwise, if the request matches and occurs within the ttl, the mover will be
     * started and the destination redirected to the pool. Response to the client will carry a file
     * handle but will not actually open a mover.  The close from the client is handled at the door
     * by removing the rendezvous information.  All of this is skipped if the third-party client has
     * been delegated a credential, in which case it connects and is treated as if it were a normal
     * two-party read.
     * <p>
     * Third-party copy where dCache is the destination should proceed with the usual upload
     * transfer creation, but when the client is redirected to the pool and calls kXR_open there, a
     * third-party client will be started which does read requests from the source and then writes
     * the data to the mover channel.
     */
    @Override
    protected XrootdResponse<OpenRequest> doOnOpen(ChannelHandlerContext ctx, OpenRequest req) {
        /*
         * TODO
         *
         * We ought to process this asynchronously to not block the calling thread during
         * staging or queuing. We should also switch to an asynchronous reply model if
         * the request is nearline or is queued on a pool. The naive approach to always
         * use an asynchronous reply model doesn't work because the xrootd 3.x client
         * introduces an artificial 1 second delay when processing such a response.
         */

        /**
         * Register this thread, so that it can be interrupted.
         * (If and when the above suggestion is implemented, this will no longer be necessary.)
         */
        setOnOpenThread();

        InetSocketAddress remoteAddress = getSourceAddress();
        LoginSessionInfo loginSessionInfo = sessionInfo();

        Map<String, String> opaque;

        try {
            opaque = OpaqueStringParser.getOpaqueMap(req.getOpaque());
            if (opaque.isEmpty()) {
                /*
                 * create a new HashMap as empty opaque map is immutable
                 */
                opaque = new HashMap<>();
            }
        } catch (ParseException e) {
            _log.warn("Ignoring malformed open opaque {}: {}", req.getOpaque(),
                  e.getMessage());
            opaque = new HashMap<>();
        }

        try {
            FsPath path = createFullPath(req.getPath());

            XrootdResponse response
                  = conditionallyHandleThirdPartyRequest(ctx,
                  req,
                  loginSessionInfo,
                  opaque,
                  path,
                  remoteAddress.getHostName());
            if (response != null) {
                return response;
            }

            FilePerm neededPerm = req.getRequiredPermission();

            _log.info("Opening {} for {}", req.getPath(), neededPerm.xmlText());
            if (_log.isDebugEnabled()) {
                logDebugOnOpen(req);
            }

            String ioQueue = appSpecificQueue(req);

            Long size = null;
            try {
                String value = opaque.get("oss.asize");
                if (value != null) {
                    size = Long.valueOf(value);
                }
            } catch (NumberFormatException exception) {
                _log.warn("Ignoring malformed oss.asize: {}",
                      exception.getMessage());
            }

            _log.info("OPAQUE : {}", opaque);
            Set<String> triedHosts = extractTriedHosts(opaque);

            UUID uuid = UUID.randomUUID();
            opaque.put(UUID_PREFIX, uuid.toString());
            /*
             *  In case this is a third-party open as destination,
             *  pass the client information to the pool.
             */
            opaque.put("org.dcache.xrootd.client", getTpcClientId(req.getSession()));
            String opaqueString = OpaqueStringParser.buildOpaqueString(opaque);

            /*
             * Interact with core dCache to open the requested file.
             */
            XrootdTransfer transfer;
            if (neededPerm == FilePerm.WRITE) {
                /**
                 *  boolean createDir = req.isMkPath() has
                 *  been changed to default to true
                 *  so as to conform to the general expectations that this
                 *  behavior should not depend on the client.
                 */
                boolean overwrite = req.isDelete() && !req.isNew();
                boolean persistOnSuccessfulClose = (req.getOptions()
                      & XrootdProtocol.kXR_posc) == XrootdProtocol.kXR_posc;
                // TODO: replace with req.isPersistOnSuccessfulClose() with the latest xrootd4j
                transfer = _door.write(remoteAddress, path, triedHosts,
                      ioQueue, uuid, true, overwrite, size,
                      loginSessionInfo.getMaximumUploadSize(),
                      localAddress(),
                      loginSessionInfo.getSubject(),
                      loginSessionInfo.getRestriction(),
                      persistOnSuccessfulClose,
                      ((loginSessionInfo.isLoggedIn()) ?
                            loginSessionInfo.getUserRootPath() : _rootPath),
                      req.getSession().getDelegatedCredential(),
                      opaque);
            } else {
                /*
                 * If this is a tpc transfer, then dCache is source here.
                 *
                 * Since we accept (from the destination server) any
                 * valid form of authentication, but without requiring
                 * the associated user to be mapped, we can override
                 * file permission restrictions (since we possess the
                 * 'token' rendezvous key, and the client file permissions
                 * have been checked during its open request).
                 */
                Subject subject;

                if (opaque.get("tpc.key") == null) {
                    subject = loginSessionInfo.getSubject();
                } else {
                    subject = Subjects.ROOT;
                }

                transfer = _door.read(remoteAddress, path, triedHosts, ioQueue,
                      uuid, localAddress(), subject,
                      loginSessionInfo.getRestriction(), opaque);

                /*
                 * Again, if this is a tpc transfer, then dCache is source here.
                 * The transfer is initiated by the destination server
                 * (= current session).  However, we wish the doorinfo
                 * client in billing to reflect the original user connection,
                 * so we overwrite the transfer client address, which
                 * is unused by the mover.
                 */
                String client = opaque.get("tpc.org");
                if (client != null) {
                    int index = client.indexOf("@");
                    if (index != -1 && index < client.length() - 1) {
                        client = client.substring(index + 1);
                        transfer.setClientAddress(new InetSocketAddress(client,
                              0));
                    }
                }
            }

            /*
             * ok, open was successful
             */
            InetSocketAddress address = getRedirect(transfer);

            String token = LoginTokens.encodeToken(localAddress());

            return new RedirectResponse<>(req, address.getHostName(), address.getPort(),
                  opaqueString, token);
        } catch (ParseException e) {
            return withError(ctx, req, kXR_ArgInvalid, "Path arguments do not parse");
        } catch (FileNotFoundCacheException e) {
            return withError(ctx, req, xrootdErrorCode(e.getRc()), "No such file");
        } catch (FileExistsCacheException e) {
            return withError(ctx, req, kXR_ItExists, "File already exists");
        } catch (TimeoutCacheException e) {
            return withError(ctx, req, xrootdErrorCode(e.getRc()), "Internal timeout");
        } catch (PermissionDeniedCacheException e) {
            return withError(ctx, req, xrootdErrorCode(e.getRc()), e.getMessage());
        } catch (FileIsNewCacheException e) {
            return withError(ctx, req, xrootdErrorCode(e.getRc()), "File is locked by upload");
        } catch (NotFileCacheException e) {
            return withError(ctx, req, xrootdErrorCode(e.getRc()), "Not a file");
        } catch (CacheException e) {
            return withError(ctx, req, xrootdErrorCode(e.getRc()),
                  String.format("Failed to open file (%s [%d])",
                        e.getMessage(), e.getRc()));
        } catch (InterruptedException e) {
            /* Interrupt may be caused by cell shutdown or client
             * disconnect.  If the client disconnected, then the error
             * message will never reach the client, so saying that the
             * server shut down is okay.
             */
            return withError(ctx, req, kXR_ServerError, "Server shutdown");
        } catch (XrootdException e) {
            return withError(ctx, req, e.getError(), e.getMessage());
        } catch (IOException e) {
            return withError(ctx, req, kXR_IOError, e.getMessage());
        } finally {
            unsetOnOpenThread();
        }
    }

    /**
     * The door's endpoint to which a client may connect.
     */
    private InetSocketAddress localAddress() {
        /*
         * Use the advertised endpoint, if possble, otherwise fall back to the
         * address to which the client connected.
         */
        return _door.publicEndpoint().orElse(getDestinationAddress());
    }

    /**
     * @return address to which to redirect client.  This will be the pool address or the proxy
     * address on the door node.
     */
    private InetSocketAddress getRedirect(XrootdTransfer transfer) throws IOException {
        InetSocketAddress poolAddress = transfer.getRedirect();
        InetSocketAddress redirectAddress;

        if (_door.isProxied()) {
            redirectAddress = _door.createProxy(poolAddress)
                  .start(transfer.getClientAddress().getAddress());
        } else {
            redirectAddress = poolAddress;
        }

        /*
         *  Do not use the IP address as host name, as this will block
         *  TLS from working.
         *
         *  According to https://tools.ietf.org/html/rfc5280#section-4.2.1.6
         *  an IP is required to be in the list of Subject Alternative Names
         *  in the host certificate, but these are rarely added in practice.
         *  TLS enforces the RFC and this is a workaround.
         */
        String host = redirectAddress.getHostName();
        if (InetAddresses.isInetAddress(host)) {
            _log.warn("Unable to resolve IP address {} "
                  + "to a canonical name", host);
        }

        _log.info("Redirecting to {}, {}", host, redirectAddress.getPort());

        return redirectAddress;
    }

    /**
     * <p>Special handling of third-party requests. Distinguishes among
     * several different cases for the open and either returns a response directly to the caller or
     * proceeds with the usual mover open and redirect to the pool by returning <code>null</code>.
     * Also verifies the rendezvous information in the case of the destination server contacting
     * dCache as source.</p>
     *
     * <p>With the modified TPC lite (delegation) protocol, there is no
     * need to wait for the rendezvous destination check by comparing the open from the source.</p>
     *
     * <p>There is also the case where no delegated proxy exists but
     * a different authentication protocol (like ZTN/scitokens) is being used.  If --tpc delegate
     * only has been used, we allow rendezvous to take </p>
     */
    private XrootdResponse<OpenRequest>
    conditionallyHandleThirdPartyRequest(ChannelHandlerContext ctx,
          OpenRequest req,
          LoginSessionInfo loginSessionInfo,
          Map<String, String> opaque,
          FsPath fsPath,
          String remoteHost)
          throws CacheException, XrootdException, ParseException {
        if (!_door.isReadAllowed(fsPath)) {
            throw new PermissionDeniedCacheException(
                  "Read permission denied");
        }

        Subject subject = loginSessionInfo.getSubject();
        Restriction restriction = loginSessionInfo.getRestriction();

        if ("placement".equals(opaque.get("tpc.stage"))) {
            FileStatus status = _door.getFileStatus(fsPath,
                  subject,
                  restriction,
                  remoteHost);
            int fd = _door.nextTpcPlaceholder();
            _log.debug("placement response to {} sent to {} with fhandle {}.",
                  req, remoteHost, fd);
            return new OpenResponse(req, fd,
                  null, null,
                  status);
        }

        String tpcKey = opaque.get("tpc.key");
        if (tpcKey == null) {
            _log.debug("{} –– not a third-party request.", req);
            return null;  // proceed as usual with mover + redirect
        }

        /*
         * Check the session for the delegated credential first, to avoid hanging
         * in the case that tpc cgi have been passed anyway by the destination server
         * to the TPC client.
         */
        if (req.getSession().getDelegatedCredential() != null) {
            _log.debug("{} –– third-party request with delegation.", req);
            return null;  // proceed as usual with mover + redirect
        }

        enforceClientTlsIfDestinationRequiresItForTpc(opaque);

        String slfn = req.getPath();
        XrootdTpcInfo info = _door.createOrGetRendezvousInfo(tpcKey);

        /*
         *  The request originated from the destination TPC client.
         *  If the initiating client has not yet opened the file here,
         *  tells the destination to wait.  If the verification, including
         *  time to live, fails, the request is cancelled.  Otherwise,
         *  the destination is allowed to open the mover and get the
         *  normal redirect response.
         *
         *  Note that the tpc info is created by either the initiating client or the
         *  destination client, whichever gets here first.  Verification of the key
         *  itself is implicit (it has been found in the map); correctness is
         *  further satisfied by matching org, host and file name.
         */
        if (opaque.containsKey("tpc.org")) {
            if (opaque.containsKey(Cgi.AUTHZ.key())) {
                /*
                 * Since it possesses a bearer token, this means that --tpc delegate only
                 * was called, and therefore that the client will not do a second
                 * open with the tpcKey on the source. Thus we should
                 * remove the key and return immediately.
                 */
                _door.removeTpcPlaceholder(tpcKey);
                _log.debug("{} –– request contains authorization token.", req);
                return null;  // proceed as usual with mover + redirect
            }

            info.addInfoFromOpaque(slfn, opaque); /** updates the status **/
            switch (info.verify(remoteHost, slfn, opaque.get("tpc.org"))) {
                case READY:
                    _log.debug("Open request {} from destination server, info {}: "
                                + "OK to proceed.",
                          req, info);
                    /*
                     *  This means that the tpc client open arrived
                     *  second, the initiating client open succeeded with
                     *  the correct permissions; proceed as usual
                     *  with mover + redirect.
                     */
                    return null;
                case PENDING:
                    _log.debug("Open request {} from destination server, info {}: "
                                + "PENDING client open; sending WAIT-RETRY.",
                          req, info);
                    /*
                     *  This means that the tpc client open arrived
                     *  first, the initiating client open has not yet taken place;
                     *  tell the tpc client to wait and retry.
                     *
                     *  Keep track of the retries and fail after 10.
                     */
                    if (openRetry.incrementAndGet() < 10) {
                        return new WaitRetryResponse<>(req, 1);
                    }
                    /*  fall through to ERROR condition */
                case ERROR:
                    /*
                     *  This means that the destination server requested open
                     *  before the client did, and the client did not have
                     *  read permissions on this file.
                     */
                    String error = "invalid open request (file permissions).";
                    _log.warn("Open request {} from destination server, info {}: "
                                + "ERROR: {}.",
                          req, info, error);
                    _door.removeTpcPlaceholder(info.getFd());
                    return withError(ctx, req, kXR_InvalidRequest,
                          "tpc rendezvous for " + tpcKey
                                + ": " + error);
                case CANCELLED:
                    error = info.isExpired() ? "ttl expired" : "dst, path or org"
                          + " did not match";
                    _log.warn("Open request {} from destination server, info {}: "
                                + "CANCELLED: {}.",
                          req, info, error);
                    _door.removeTpcPlaceholder(info.getFd());
                    return withError(ctx, req, kXR_InvalidRequest,
                          "tpc rendezvous for " + tpcKey
                                + ": " + error);
            }
        }

        /*
         *  The request originated from the client, indicating that this door is the source.
         */
        if (opaque.containsKey("tpc.dst")) {
            _log.debug("Open request {} from client to door as source, "
                  + "info {}: OK.", req, info);
            FileStatus status = _door.getFileStatus(fsPath, subject, restriction, remoteHost);
            int flags = status.getFlags();

            if ((flags & kXR_readable) != kXR_readable) {
                /*
                 * Update the info with ERROR, so when the destination checks
                 * it, an error can be returned.
                 */
                info.setStatus(Status.ERROR);
                return withError(ctx, req, kXR_InvalidRequest,
                      "not allowed to read file.");
            }

            info.addInfoFromOpaque(slfn, opaque); /** updates the status **/
            return new OpenResponse(req, info.getFd(),
                  null, null,
                  status);
        }

        /*
         *  The request originated from the client, indicating that this door is the destination.
         *  There is no need for tpcInfo stored on the destination, so we remove it and
         *  allow the write mover to be started on the selected pool.
         */
        if (opaque.containsKey("tpc.src")) {
            _log.debug("Open request {} from client to door as destination: OK;"
                  + "removing info {}.", req, info);
            _door.removeTpcPlaceholder(info.getFd());
            return null; // proceed as usual with mover + redirect
        }

        /*
         *  Something went wrong.
         */
        String error = String.format("Request metadata is invalid: %s: %s, %s.",
              req, fsPath, remoteHost);
        throw new CacheException(CacheException.THIRD_PARTY_TRANSFER_FAILED, error);
    }

    /*
     *  There are six recognized error codes for host retries.  Our policy is the following:
     *
     *  (a) if tried hosts is disabled, we ignore the host list;
     *  (b) if there are no associated error codes (list is empty or undefined), we ingore the host list;
     *  (c) we include only hosts in the set returned whose error codes are enoent or ioerr.
     */
    private Set<String> extractTriedHosts(Map<String, String> opaque) {
        String tried = Strings.emptyToNull(opaque.get("tried"));
        String triedrc = Strings.emptyToNull(opaque.get("triedrc"));

        if (!_door.isTriedHostsEnabled()) {
            _log.debug("tried hosts option not enabled, ignoring 'tried={},triedrc={}'.",
                  tried, triedrc);
            return Collections.EMPTY_SET;
        }

        if (tried == null || triedrc == null) {
            _log.debug("tried {}, triedrc {}, ignoring.", tried, triedrc);
            return Collections.EMPTY_SET;
        }

        List<String> hostNames
              = Arrays.stream(tried.split(",")).map(String::trim).collect(Collectors.toList());
        List<String> errorCodes
              = Arrays.stream(triedrc.split(",")).map(String::trim).collect(Collectors.toList());
        Set<String> triedHosts = new HashSet<>();

        /*
         *  Assuming the comma-delimited lists are correspondingly ordered,
         *  the iteration can be bound by the length of the error codes list.
         *  Should the length of the error code list exceed that of the host list,
         *  this would actually constitute a client bug, but we treat it silently
         *  by then using the length of the host list as upper bound.
         */
        int len = Math.min(errorCodes.size(), hostNames.size());

        for (int i = 0; i < len; ++i) {
            String value = errorCodes.get(i).toUpperCase();
            if (value.equals(ENOENT.name()) || value.equals(IOERR.name())) {
                String host = hostNames.get(i);
                triedHosts.add(host);
                _log.debug("tried {}, triedrc {}, {}.",
                      host, value, TriedRc.valueOf(value).description());
            }
        }

        _log.debug("tried hosts : {}", triedHosts);
        return triedHosts;
    }

    private String getTpcClientId(XrootdSession session) {
        int pid = session.getPID();
        String uname = session.getUserName();

        String clientHost;
        SocketAddress remoteAddress = session.getChannel().remoteAddress();
        if (remoteAddress instanceof InetSocketAddress) {
            clientHost = ((InetSocketAddress) remoteAddress).getHostName();
        } else {
            clientHost = "localhost";
        }

        return uname + "." + pid + "@" + clientHost;
    }

    private String appSpecificQueue(OpenRequest req) {
        String ioqueue = null;
        String token = req.getSession().getToken();

        try {
            Map<String, String> attr = OpaqueStringParser.getOpaqueMap(token);
            ioqueue = _appIoQueues.get(attr.get("xrd.appname"));
        } catch (ParseException e) {
            _log.debug("Ignoring malformed login token {}: {}", token, e.getMessage());
        }

        return ioqueue;
    }

    /**
     * Will only occur on third-party-copy where dCache acts as source. The client closes here
     * (whereas the destination server has been redirected to the pool).
     */
    @Override
    protected XrootdResponse<CloseRequest> doOnClose(ChannelHandlerContext ctx, CloseRequest msg)
          throws XrootdException {
        int fd = msg.getFileHandle();
        _log.debug("doOnClose: removing tpc info for {}.", fd);
        if (_door.removeTpcPlaceholder(fd)) {
            return withOk(msg);
        } else {
            return withError(ctx, msg, kXR_FileNotOpen,
                  "Invalid file handle " + fd
                        + " for tpc source close.");
        }
    }

    @Override
    protected XrootdResponse<StatRequest> doOnStat(ChannelHandlerContext ctx, StatRequest req)
          throws XrootdException {
        try {
            String path = req.getPath();
            LoginSessionInfo loginSessionInfo = sessionInfo();
            InetSocketAddress client = getSourceAddress();

            return new StatResponse(req, _door.getFileStatus(createFullPath(path),
                  loginSessionInfo.getSubject(),
                  loginSessionInfo.getRestriction(),
                  client.getAddress().getHostAddress()));
        } catch (FileNotFoundCacheException e) {
            throw xrootdException(e.getRc(), "No such file");
        } catch (TimeoutCacheException e) {
            throw xrootdException(e.getRc(), "Internal timeout");
        } catch (PermissionDeniedCacheException e) {
            throw xrootdException(e);
        } catch (CacheException e) {
            throw xrootdException(e.getRc(),
                  String.format("Failed to open file (%s [%d])",
                        e.getMessage(), e.getRc()));
        }
    }

    @Override
    protected XrootdResponse<StatxRequest> doOnStatx(ChannelHandlerContext ctx, StatxRequest req)
          throws XrootdException {
        if (req.getPaths().length == 0) {
            throw new XrootdException(kXR_ArgMissing, "no paths specified");
        }
        try {
            FsPath[] paths = new FsPath[req.getPaths().length];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = createFullPath(req.getPaths()[i]);
            }
            LoginSessionInfo loginSessionInfo = sessionInfo();
            Subject subject = loginSessionInfo.getSubject();
            Restriction restriction = loginSessionInfo.getRestriction();
            return new StatxResponse(req,
                  _door.getMultipleFileStatuses(paths,
                        subject,
                        restriction));
        } catch (TimeoutCacheException e) {
            throw xrootdException(e.getRc(), "Internal timeout");
        } catch (PermissionDeniedCacheException e) {
            throw xrootdException(e);
        } catch (CacheException e) {
            throw xrootdException(e.getRc(),
                  String.format("Failed to open file (%s [%d])",
                        e.getMessage(), e.getRc()));
        }
    }


    @Override
    protected XrootdResponse<RmRequest> doOnRm(ChannelHandlerContext ctx, RmRequest req)
          throws XrootdException {
        if (req.getPath().isEmpty()) {
            throw new XrootdException(kXR_ArgMissing, "no path specified");
        }

        _log.info("Trying to delete {}", req.getPath());

        try {
            LoginSessionInfo loginSessionInfo = sessionInfo();
            _door.deleteFile(createFullPath(req.getPath()),
                  loginSessionInfo.getSubject(),
                  loginSessionInfo.getRestriction());
            return withOk(req);
        } catch (TimeoutCacheException e) {
            throw xrootdException(e.getRc(), "Internal timeout");
        } catch (PermissionDeniedCacheException e) {
            throw xrootdException(e);
        } catch (FileNotFoundCacheException e) {
            throw xrootdException(e.getRc(), "No such file");
        } catch (CacheException e) {
            throw xrootdException(e.getRc(),
                  String.format("Failed to delete file (%s [%d])",
                        e.getMessage(), e.getRc()));
        }
    }

    @Override
    protected XrootdResponse<RmDirRequest> doOnRmDir(ChannelHandlerContext ctx, RmDirRequest req)
          throws XrootdException {
        if (req.getPath().isEmpty()) {
            throw new XrootdException(kXR_ArgMissing, "no path specified");
        }

        _log.info("Trying to delete directory {}", req.getPath());

        try {
            LoginSessionInfo loginSessionInfo = sessionInfo();
            _door.deleteDirectory(createFullPath(req.getPath()),
                  loginSessionInfo.getSubject(),
                  loginSessionInfo.getRestriction());
            return withOk(req);
        } catch (TimeoutCacheException e) {
            throw xrootdException(e.getRc(), "Internal timeout");
        } catch (PermissionDeniedCacheException | FileNotFoundCacheException e) {
            throw xrootdException(e);
        } catch (CacheException e) {
            throw xrootdException(e.getRc(),
                  String.format("Failed to delete directory " +
                              "(%s [%d]).",
                        e.getMessage(), e.getRc()));
        }
    }

    @Override
    protected XrootdResponse<MkDirRequest> doOnMkDir(ChannelHandlerContext ctx, MkDirRequest req)
          throws XrootdException {
        if (req.getPath().isEmpty()) {
            throw new XrootdException(kXR_ArgMissing, "no path specified");
        }

        _log.info("Trying to create directory {}", req.getPath());

        try {
            LoginSessionInfo loginSessionInfo = sessionInfo();
            _door.createDirectory(createFullPath(req.getPath()),
                  req.shouldMkPath(),
                  loginSessionInfo.getSubject(),
                  loginSessionInfo.getRestriction());
            return withOk(req);
        } catch (TimeoutCacheException e) {
            throw xrootdException(e.getRc(), "Internal timeout");
        } catch (PermissionDeniedCacheException | FileNotFoundCacheException
              | FileExistsCacheException e) {
            throw xrootdException(e);
        } catch (CacheException e) {
            throw xrootdException(e.getRc(),
                  String.format("Failed to create directory " +
                              "(%s [%d]).",
                        e.getMessage(), e.getRc()));
        }
    }

    @Override
    protected XrootdResponse<MvRequest> doOnMv(ChannelHandlerContext ctx, MvRequest req)
          throws XrootdException {
        String sourcePath = req.getSourcePath();
        if (sourcePath.isEmpty()) {
            throw new XrootdException(kXR_ArgMissing, "no source path specified");
        }

        String targetPath = req.getTargetPath();
        if (targetPath.isEmpty()) {
            throw new XrootdException(kXR_ArgMissing, "no target path specified");
        }

        _log.info("Trying to rename {} to {}", req.getSourcePath(), req.getTargetPath());

        try {
            LoginSessionInfo loginSessionInfo = sessionInfo();
            _door.moveFile(createFullPath(req.getSourcePath()),
                  createFullPath(req.getTargetPath()),
                  loginSessionInfo.getSubject(),
                  loginSessionInfo.getRestriction());
            return withOk(req);
        } catch (TimeoutCacheException e) {
            throw xrootdException(e.getRc(), "Internal timeout");
        } catch (PermissionDeniedCacheException e) {
            throw xrootdException(e);
        } catch (FileNotFoundCacheException e) {
            throw xrootdException(e.getRc(),
                  String.format("Source file does not exist (%s) ",
                        e.getMessage()));
        } catch (FileExistsCacheException e) {
            throw xrootdException(e.getRc(),
                  String.format("Will not overwrite existing file " +
                        "(%s).", e.getMessage()));
        } catch (CacheException e) {
            throw xrootdException(e.getRc(),
                  String.format("Failed to move file " +
                              "(%s [%d]).",
                        e.getMessage(), e.getRc()));
        }
    }

    @Override
    protected XrootdResponse<QueryRequest> doOnQuery(ChannelHandlerContext ctx, QueryRequest msg)
          throws XrootdException {
        switch (msg.getReqcode()) {
            case kXR_Qconfig:
                StringBuilder s = new StringBuilder();
                for (String name : msg.getArgs().split(" ")) {
                    switch (name) {
                        case "bind_max":
                            s.append(0);
                            break;
                        case "csname":
                            /**
                             * xrdcp expects lower case names for checksum algorithms
                             * https://github.com/xrootd/xrootd/issues/459
                             * TODO: revert to upper case then above issue is addressed
                             */
                            s.append("1:adler32,2:md5");
                            break;
                        case "tpc":
                            /**
                             * Indicate support for third-party copy by responding
                             * with the protocol version.
                             */
                            s.append(XrootdProtocol.TPC_VERSION);
                            break;
                        case "tpcdlg":
                            s.append("gsi");
                            break;
                        default:
                            s.append(_queryConfig.getOrDefault(name, name));
                            break;
                    }
                    s.append('\n');
                }
                return new QueryResponse(msg, s.toString());

            case kXR_Qcksum:
                try {
                    ChecksumInfo checksumInfo = new ChecksumInfo(msg.getPath(),
                          msg.getOpaque());
                    LoginSessionInfo loginSessionInfo = sessionInfo();
                    Set<Checksum> checksums = _door.getChecksums(createFullPath(msg.getPath()),
                          loginSessionInfo.getSubject(),
                          loginSessionInfo.getRestriction());
                    return selectChecksum(checksumInfo, checksums, msg);
                } catch (CacheException e) {
                    throw xrootdException(e);
                }
            default:
                return unsupported(ctx, msg);
        }
    }

    @Override
    protected XrootdResponse<DirListRequest> doOnDirList(ChannelHandlerContext ctx,
          DirListRequest request)
          throws XrootdException {
        try {
            String listPath = request.getPath();
            if (listPath.isEmpty()) {
                throw new XrootdException(kXR_ArgMissing, "no source path specified");
            }

            _log.info("Listing directory {}", listPath);
            FsPath fullListPath = createFullPath(listPath);

            if (!_door.isReadAllowed(fullListPath)) {
                throw new PermissionDeniedCacheException("Permission denied.");
            }
            LoginSessionInfo loginSessionInfo = sessionInfo();
            Subject subject = loginSessionInfo.getSubject();
            Restriction restriction = loginSessionInfo.getRestriction();
            if (request.isDirectoryStat()) {
                _door.listPath(fullListPath, subject, restriction,
                      new StatListCallback(request, subject, restriction, fullListPath, ctx),
                      _door.getRequiredAttributesForFileStatus());
            } else {
                _door.listPath(fullListPath, subject, restriction,
                      new ListCallback(request, ctx),
                      EnumSet.noneOf(FileAttribute.class));
            }
            return null;
        } catch (PermissionDeniedCacheException e) {
            throw xrootdException(e);
        }
    }

    @Override
    protected XrootdResponse<PrepareRequest> doOnPrepare(ChannelHandlerContext ctx,
          PrepareRequest msg)
          throws XrootdException {
        return withOk(msg);
    }

    private void logDebugOnOpen(OpenRequest req) {
        int options = req.getOptions();
        String openFlags =
              "options to apply for open path (raw=" + options + " ):";

        if ((options & kXR_async) == kXR_async) {
            openFlags += " kXR_async";
        }
        if ((options & kXR_compress) == kXR_compress) {
            openFlags += " kXR_compress";
        }
        if ((options & kXR_delete) == kXR_delete) {
            openFlags += " kXR_delete";
        }
        if ((options & kXR_force) == kXR_force) {
            openFlags += " kXR_force";
        }
        if ((options & kXR_new) == kXR_new) {
            openFlags += " kXR_new";
        }
        if ((options & kXR_open_read) == kXR_open_read) {
            openFlags += " kXR_open_read";
        }
        if ((options & kXR_open_updt) == kXR_open_updt) {
            openFlags += " kXR_open_updt";
        }
        if ((options & kXR_refresh) == kXR_refresh) {
            openFlags += " kXR_refresh";
        }
        if ((options & kXR_mkpath) == kXR_mkpath) {
            openFlags += " kXR_mkpath";
        }
        if ((options & kXR_open_apnd) == kXR_open_apnd) {
            openFlags += " kXR_open_apnd";
        }
        if ((options & kXR_retstat) == kXR_retstat) {
            openFlags += " kXR_retstat";
        }
        if ((options & kXR_posc) == kXR_posc) {
            openFlags += " kXR_posc";
        }

        _log.debug("open flags: {}", openFlags);

        int mode = req.getUMask();
        String s = "";

        if ((mode & kXR_ur) == kXR_ur) {
            s += "r";
        } else {
            s += "-";
        }
        if ((mode & kXR_uw) == kXR_uw) {
            s += "w";
        } else {
            s += "-";
        }
        if ((mode & kXR_ux) == kXR_ux) {
            s += "x";
        } else {
            s += "-";
        }

        s += " ";

        if ((mode & kXR_gr) == kXR_gr) {
            s += "r";
        } else {
            s += "-";
        }
        if ((mode & kXR_gw) == kXR_gw) {
            s += "w";
        } else {
            s += "-";
        }
        if ((mode & kXR_gx) == kXR_gx) {
            s += "x";
        } else {
            s += "-";
        }

        s += " ";

        if ((mode & kXR_or) == kXR_or) {
            s += "r";
        } else {
            s += "-";
        }
        if ((mode & kXR_ow) == kXR_ow) {
            s += "w";
        } else {
            s += "-";
        }
        if ((mode & kXR_ox) == kXR_ox) {
            s += "x";
        } else {
            s += "-";
        }

        _log.debug("mode to apply to open path: {}", s);
    }

    /**
     * Callback responding to client depending on the list directory messages it receives from Pnfs
     * via the door.
     *
     * @author tzangerl
     */
    private class ListCallback
          extends AbstractMessageCallback<PnfsListDirectoryMessage> {

        protected final DirListRequest _request;
        protected final ChannelHandlerContext _context;
        protected final DirListResponse.Builder _response;

        public ListCallback(DirListRequest request, ChannelHandlerContext context) {
            _request = request;
            _response = DirListResponse.builder(request);
            _context = context;
        }

        /**
         * Respond to client if message contains errors. Try to use meaningful status codes from the
         * xrootd-protocol to map the errors from PnfsManager.
         *
         * @param rc    The error code of the message
         * @param error Object describing the actual error that occurred
         */
        @Override
        public void failure(int rc, Object error) {
            String errorMessage;

            switch (rc) {
                case CacheException.TIMEOUT:
                    errorMessage = "Timeout when trying to list directory: "
                          + error.toString();
                    break;
                case CacheException.PERMISSION_DENIED:
                    errorMessage = "Permission to list that directory denied: "
                          + error.toString();
                    break;
                case CacheException.FILE_NOT_FOUND:
                    errorMessage = "Path not found: ";
                    break;
                default:
                    errorMessage = "Error when processing list response: "
                          + error.toString();
                    break;
            }

            respond(_context,
                  withError(_context, _request, xrootdErrorCode(rc), errorMessage));
        }

        /**
         * Reply to client if no route to PNFS manager was found.
         */
        @Override
        public void noroute(CellPath path) {
            respond(_context,
                  withError(_context, _request,
                        kXR_ServerError,
                        "Could not contact PNFS Manager."));
        }

        /**
         * In case of a listing success, inspect the message. If the message is the final listing
         * message, reply with kXR_ok and the full directory listing. If the message is not the
         * final message, reply with oksofar and the partial directory listing.
         *
         * @param message The PnfsListDirectoryMessage-reply as it was received from the
         *                PNFSManager.
         */
        @Override
        public void success(PnfsListDirectoryMessage message) {
            message.getEntries().stream().map(DirectoryEntry::getName).forEach(_response::add);
            if (message.isFinal()) {
                respond(_context, _response.buildFinal());
            } else {
                respond(_context, _response.buildPartial());
            }
        }

        /**
         * Respond to client in the case of a timeout.
         */
        @Override
        public void timeout(String error) {
            respond(_context,
                  withError(_context, _request,
                        kXR_ServerError,
                        "Timeout when trying to list directory!"));
        }
    }

    private class StatListCallback extends ListCallback {

        protected final FsPath _dirPath;
        private final String _client;
        private final Subject _subject;
        private final Restriction _restriction;

        public StatListCallback(DirListRequest request,
              Subject subject,
              Restriction restriction,
              FsPath dirPath,
              ChannelHandlerContext context) {
            super(request, context);
            _client = getSourceAddress().getAddress().getHostAddress();
            _dirPath = dirPath;
            _subject = subject;
            _restriction = restriction;
        }

        @Override
        public void success(PnfsListDirectoryMessage message) {
            message.getEntries().stream().forEach(
                  e -> _response.add(e.getName(), _door.getFileStatus(_subject,
                        _restriction,
                        _dirPath.child(e.getName()),
                        _client, e.getFileAttributes())));
            if (message.isFinal()) {
                respond(_context, _response.buildFinal());
            } else {
                respond(_context, _response.buildPartial());
            }
        }
    }

    /**
     * Execute login strategy to make an user authorization decision.
     */
    private void loggedIn(LoginEvent event) {
        if (_logins.size() > 1) {
            throw new IllegalStateException("Login called too many times; "
                  + "this is a bug.");
        }

        LoginReply reply = event.getLoginReply();
        LoginSessionInfo info = reply == null
              ? new LoginSessionInfo(Restrictions.none())
              : new LoginSessionInfo(reply);

        _logins.push(info);
    }

    /**
     * Forms a full PNFS path. The path is created by concatenating the root path and path. The root
     * path is guaranteed to be a prefix of the path returned.
     */
    private FsPath createFullPath(String path)
          throws PermissionDeniedCacheException {
        return _rootPath.chroot(path);
    }

    /**
     * Stack of maximum depth = 2.   The first object present is considered the main login info. The
     * second is valid only once and then should be discarded.  This is to allow for passing (or
     * not) multiple authorization tokens on the same session/connection.
     *
     * @return current info.
     */
    private LoginSessionInfo sessionInfo() {
        if (_logins.size() > 1) {
            return _logins.pop();
        }

        if (_logins.isEmpty()) {
            return _defaultLoginSessionInfo;
        }

        return _logins.peek();
    }

    private synchronized void setOnOpenThread() {
        onOpenThread = Thread.currentThread();
    }

    private synchronized void unsetOnOpenThread() {
        onOpenThread = null;
    }

    private synchronized void interruptOnOpenThread() {
        if (onOpenThread != null) {
            _log.info("{} called interruptOnOpenThread; interrupting {}.", Thread.currentThread(),
                  onOpenThread);
            onOpenThread.interrupt();
        }
    }
}
