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

import com.google.common.base.Strings;
import com.google.common.net.InetAddresses;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileIsNewCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.TimeoutCacheException;

import dmg.cells.nucleus.CellPath;

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
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.core.XrootdSession;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.XrootdProtocol.*;
import org.dcache.xrootd.protocol.messages.AwaitAsyncResponse;
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
import org.dcache.xrootd.protocol.messages.XrootdResponse;
import org.dcache.xrootd.tpc.XrootdTpcInfo;
import org.dcache.xrootd.tpc.XrootdTpcInfo.Status;
import org.dcache.xrootd.util.ChecksumInfo;
import org.dcache.xrootd.util.FileStatus;
import org.dcache.xrootd.util.OpaqueStringParser;
import org.dcache.xrootd.util.ParseException;

import static org.dcache.xrootd.CacheExceptionMapper.xrootdErrorCode;
import static org.dcache.xrootd.CacheExceptionMapper.xrootdException;
import static org.dcache.xrootd.protocol.XrootdProtocol.*;

/**
 * Channel handler which redirects all open requests to a pool.
 */
public class XrootdRedirectHandler extends ConcurrentXrootdRequestHandler
{
    private static final Logger _log =
        LoggerFactory.getLogger(XrootdRedirectHandler.class);

    private final XrootdDoor _door;

    private Restriction _authz = Restrictions.denyAll();
    private OptionalLong _maximumUploadSize = OptionalLong.empty();
    private final Map<String, String> _appIoQueues;

    private FsPath _rootPath;
    private FsPath _userRootPath;
    private boolean _isLoggedIn;

    /**
     * Custom entries for kXR_Qconfig requests.
     */
    private final Map<String,String> _queryConfig;

    public XrootdRedirectHandler(XrootdDoor door, FsPath rootPath, ExecutorService executor,
                                 Map<String, String> queryConfig,
                                 Map<String,String> appIoQueues)
    {
        super(executor);
        _door = door;
        _rootPath = rootPath;
        _queryConfig = queryConfig;
        _appIoQueues = appIoQueues;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception
    {
        if (event instanceof LoginEvent) {
            loggedIn((LoginEvent) event);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t)
    {
        if (t instanceof ClosedChannelException) {
            _log.info("Connection closed");
        } else if (t instanceof RuntimeException || t instanceof Error) {
            Thread me = Thread.currentThread();
            me.getUncaughtExceptionHandler().uncaughtException(me, t);
        } else if (!isHealthCheck() || !(t instanceof IOException)){
            _log.warn(t.toString());
        }
    }

    /**
     * For client-server read and write, the open, if successful, will always
     * result in a redirect response to the proper pool; hence no subsequent
     * requests like sync, read, write or close are expected at the door.
     *
     * For third-party copy where dCache is the source, the interactions are as
     * follows:
     *
     * 1.  The client opens the file to check availability (the 'placement'
     *     stage).  An OK response is followed by the client closing the file.
     * 2.  The client opens the file again with rendezvous metadata.  The
     *     client will close the file only when notified by the destination
     *     server that the transfer has completed.
     * 3.  The destination server will open the file for the actual read.
     *
     * The order of 2, 3 is not deterministic; hence the response here must
     * provide for the possibility that the destination server attempts an
     * open before the client specifies a time-to-live on the rendezvous
     * point.
     *
     * The strategy adopted is therefore as follows:  response to (1) is simply
     * to check file permissions.  No metadata is generated and a "dummy"
     * file handle is returned.  For 2 and 3, whichever occurs first
     * will cause a metadata object to be stored.  If the destination server
     * open occurs first, a wait response will tell the server to try again
     * in a maximum of 3 seconds; otherwise, if the request matches and occurs
     * within the ttl, the mover will be started and the destination
     * redirected to the pool. Response to the client will carry a file
     * handle but will not actually open a mover.  The close from the client
     * is handled at the door by removing the rendezvous information.
     *
     * Third-party copy where dCache is the destination should proceed with
     * the usual upload transfer creation, but when the client is redirected
     * to the pool and calls kXR_open there, a third-party client will
     * be started which does read requests from the source and then writes
     * the data to the mover channel.
     *
     * NOTE:  with the changed TPC Lite protocol, the client is not required
     * to open the source again during the copy phase (2) if delegation is being
     * used.
     */
    @Override
    protected XrootdResponse<OpenRequest> doOnOpen(ChannelHandlerContext ctx, OpenRequest req)
        throws XrootdException
    {
        /*
         * TODO
         *
         * We ought to process this asynchronously to not block the calling thread during
         * staging or queuing. We should also switch to an asynchronous reply model if
         * the request is nearline or is queued on a pool. The naive approach to always
         * use an asynchronous reply model doesn't work because the xrootd 3.x client
         * introduces an artificial 1 second delay when processing such a response.
         */

        InetSocketAddress localAddress = getDestinationAddress();
        InetSocketAddress remoteAddress = getSourceAddress();

        Map<String,String> opaque;

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
                = conditionallyHandleThirdPartyRequest(req,
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
            Set<String> triedHosts
                            = Arrays.stream(Strings.nullToEmpty(opaque.get("tried"))
                                                   .split(","))
                                    .collect(Collectors.toSet());
            _log.info("TRIED : {}", triedHosts);
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
                boolean createDir = req.isMkPath();
                boolean overwrite = req.isDelete() && !req.isNew();
                boolean persistOnSuccessfulClose = (req.getOptions()
                        & XrootdProtocol.kXR_posc) == XrootdProtocol.kXR_posc;
                // TODO: replace with req.isPersistOnSuccessfulClose() with the latest xrootd4j

                transfer = _door.write(remoteAddress, path, triedHosts,
                        ioQueue, uuid, createDir, overwrite, size, _maximumUploadSize,
                        localAddress, req.getSubject(), _authz, persistOnSuccessfulClose,
                        ((_isLoggedIn) ? _userRootPath : _rootPath),
                        req.getSession().getDelegatedCredential());
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
                    subject = req.getSubject();
                } else {
                    subject = Subjects.ROOT;
                }

                transfer = _door.read(remoteAddress, path, triedHosts, ioQueue,
                                uuid, localAddress, subject, _authz);

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
                    if (index != -1 && index < client.length()-1) {
                        client = client.substring(index+1);
                        transfer.setClientAddress(new InetSocketAddress(client,
                                                                        0));
                    }
                }
            }

            // ok, open was successful
            InetSocketAddress address = transfer.getRedirect();
            _log.info("Redirecting to {}", address);

            /* xrootd developers say that IPv6 addresses must always be URI quoted.
             * The spec doesn't require this, but clients depend on it.
             */
            return new RedirectResponse<>(
                    req, InetAddresses.toUriString(address.getAddress()),
                    address.getPort(), opaqueString, "");
        } catch (FileNotFoundCacheException e) {
            return withError(req, xrootdErrorCode(e.getRc()), "No such file");
        } catch (FileExistsCacheException e) {
            return withError(req, kXR_NotAuthorized, "File already exists");
        } catch (TimeoutCacheException e) {
            return withError(req, xrootdErrorCode(e.getRc()), "Internal timeout");
        } catch (PermissionDeniedCacheException e) {
            return withError(req, xrootdErrorCode(e.getRc()), e.getMessage());
        } catch (FileIsNewCacheException e) {
            return withError(req, xrootdErrorCode(e.getRc()), "File is locked by upload");
        } catch (NotFileCacheException e) {
            return withError(req, xrootdErrorCode(e.getRc()), "Not a file");
        } catch (CacheException e) {
            return withError(req, xrootdErrorCode(e.getRc()),
                             String.format("Failed to open file (%s [%d])",
                                           e.getMessage(), e.getRc()));
        } catch (InterruptedException e) {
            /* Interrupt may be caused by cell shutdown or client
             * disconnect.  If the client disconnected, then the error
             * message will never reach the client, so saying that the
             * server shut down is okay.
             */
            return withError(req, kXR_ServerError, "Server shutdown");
        }
    }

    /**
     * <p>Special handling of third-party requests. Distinguishes among
     * several different cases for the open and either returns a response
     * directly to the caller or proceeds with the usual mover open and
     * redirect to the pool by returning <code>null</code>.
     * Also verifies the rendezvous information in the case of the destination
     * server contacting dCache as source.</p>
     *
     * <p>With the modified TPC lite (delegation) protocol, there is no
     * need to wait for the rendezvous destination check by comparing
     * the open from the source.</p>
     */
    private XrootdResponse<OpenRequest>
        conditionallyHandleThirdPartyRequest(OpenRequest req,
                                                Map<String,String> opaque,
                                                FsPath fsPath,
                                                String remoteHost)
                    throws CacheException
    {
        if (!_door.isReadAllowed(fsPath)) {
            throw new PermissionDeniedCacheException(
                            "Read permission denied");
        }

        if ("placement".equals(opaque.get("tpc.stage"))) {
            FileStatus status = _door.getFileStatus(fsPath,
                                                    req.getSubject(),
                                                    _authz,
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
         * Check the session for the delegated credential to avoid hanging
         * in the case that tpc cgi have been passed by the destination
         * server even with TPC with delegation.
         */
        if (req.getSession().getDelegatedCredential() != null) {
            _log.debug("{} –– third-party request with delegation.", req);
            return null;  // proceed as usual with mover + redirect
        }

        String slfn = req.getPath();

        XrootdTpcInfo info = _door.createOrGetRendezvousInfo(tpcKey);

        /*
         *  The request originated from the TPC destination server.
         *  If the client has not yet opened the file here,
         *  tells the destination to wait.  If the verification, including
         *  time to live, fails, the request is cancelled.  Otherwise,
         *  the destination is allowed to open the mover and get the
         *  normal redirect response.
         *
         *  Note that the tpc info is created by either the client or the
         *  server, whichever gets here first.  Verification of the key
         *  itself is implicit (it has been found in the map); correctness is
         *  further satisfied by matching org, host and file name.
         */
        if (opaque.containsKey("tpc.org")) {
            info.addInfoFromOpaque(slfn, opaque);
            switch (info.verify(remoteHost, slfn, opaque.get("tpc.org"))) {
                case READY:
                    _log.debug("Open request {} from destination server, info {}: "
                                              + "OK to proceed.",
                              req, info);
                    /*
                     *  This means that the destination server open arrived
                     *  second, the client server open succeeded with
                     *  the correct permissions; proceed as usual
                     *  with mover + redirect.
                     */
                    return null;
                case PENDING:
                    _log.debug("Open request {} from destination server, info {}: "
                                              + "PENDING client open.",
                              req, info);
                    /*
                     *  This means that the destination server open arrived
                     *  first; return a wait-retry reply.
                     */
                    return new AwaitAsyncResponse<>(req, 3);
                case CANCELLED:
                    String error = info.isExpired() ? "ttl expired" : "dst, path or org"
                                    + " did not match";
                    _log.warn("Open request {} from destination server, info {}: "
                                              + "CANCELLED: {}.",
                              req, info, error);
                    _door.removeTpcPlaceholder(info.getFd());
                    return withError(req, kXR_InvalidRequest,
                                     "tpc rendezvous for " + tpcKey
                                                     + ": " + error);
                case ERROR:
                    /*
                     *  This means that the destination server requested open
                     *  before the client did, and the client did not have
                     *  read permissions on this file.
                     */
                    error = "invalid open request (file permissions).";
                    _log.warn("Open request {} from destination server, info {}: "
                                              + "ERROR: {}.",
                              req, info, error);
                    _door.removeTpcPlaceholder(info.getFd());
                    return withError(req, kXR_InvalidRequest,
                                     "tpc rendezvous for " + tpcKey
                                                     + ": " + error);
            }
        }

        /*
         *  The request originated from the TPC client, indicating dCache
         *  is the source.
         */
        if (opaque.containsKey("tpc.dst")) {
            _log.debug("Open request {} from client to door as source, "
                                      + "info {}: OK.", req, info);
            FileStatus status = _door.getFileStatus(fsPath,
                                                    req.getSubject(),
                                                    _authz,
                                                    remoteHost);
            int flags = status.getFlags();

            if ((flags & kXR_readable) != kXR_readable) {
                /*
                 * Update the info with ERROR, so when the destination checks
                 * it, an error can be returned.
                 */
                info.setStatus(Status.ERROR);
                return withError(req, kXR_InvalidRequest,
                                 "not allowed to read file.");
            }

            info.addInfoFromOpaque(slfn, opaque);
            return new OpenResponse(req, info.getFd(),
                                    null, null,
                                    status);
        }

        /*
         *  The request originated from the TPC client, indicating dCache
         *  is the destination.  Remove the rendezvous info (not needed),
         *  allow mover to start and redirect the client to the pool.
         *
         *  It is not necessary to delegate the tpc information through the
         *  protocol, particularly the rendezvous key, because it is part of
         *  the opaque data, and if any of the opaque tpc info is missing
         *  from redirected call to the pool, the transfer will fail.
         *
         *  However, the calling method will need to fetch a delegated
         *  proxy credential and add that to the protocol.
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

    private String appSpecificQueue(OpenRequest req)
    {
        String ioqueue = null;
        String token = req.getSession().getToken();

        try {
            Map<String,String> attr = OpaqueStringParser.getOpaqueMap(token);
            ioqueue = _appIoQueues.get(attr.get("xrd.appname"));
        } catch (ParseException e) {
            _log.debug("Ignoring malformed login token {}: {}", token, e.getMessage());
        }

        return ioqueue;
    }

    /**
     * Will only occur on third-party-copy where dCache acts as source. The
     * client closes here (whereas the destination server has been redirected
     * to the pool).
     */
    @Override
    protected XrootdResponse<CloseRequest> doOnClose(ChannelHandlerContext ctx, CloseRequest msg)
                    throws XrootdException
    {
        int fd = msg.getFileHandle();
        _log.debug("doOnClose: removing tpc info for {}.", fd);
        if (_door.removeTpcPlaceholder(fd)) {
            return withOk(msg);
        } else {
            return withError(msg, kXR_FileNotOpen,
                             "Invalid file handle " + fd
                                             + " for tpc source close.");
        }
    }

    @Override
    protected XrootdResponse<StatRequest> doOnStat(ChannelHandlerContext ctx, StatRequest req)
        throws XrootdException
    {
        String path = req.getPath();
        try {
            InetSocketAddress client = getSourceAddress();
            return new StatResponse(req, _door.getFileStatus(createFullPath(path), req.getSubject(), _authz,
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
        throws XrootdException
    {
        if (req.getPaths().length == 0) {
            throw new XrootdException(kXR_ArgMissing, "no paths specified");
        }
        try {
            FsPath[] paths = new FsPath[req.getPaths().length];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = createFullPath(req.getPaths()[i]);
            }
            return new StatxResponse(req, _door.getMultipleFileStatuses(paths, req.getSubject(), _authz));
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
        throws XrootdException
    {
        if (req.getPath().isEmpty()) {
            throw new XrootdException(kXR_ArgMissing, "no path specified");
        }

        _log.info("Trying to delete {}", req.getPath());

        try {
            _door.deleteFile(createFullPath(req.getPath()), req.getSubject(), _authz);
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
        throws XrootdException
    {
        if (req.getPath().isEmpty()) {
            throw new XrootdException(kXR_ArgMissing, "no path specified");
        }

        _log.info("Trying to delete directory {}", req.getPath());

        try {
            _door.deleteDirectory(createFullPath(req.getPath()), req.getSubject(), _authz);
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
        throws XrootdException
    {
        if (req.getPath().isEmpty()) {
            throw new XrootdException(kXR_ArgMissing, "no path specified");
        }

        _log.info("Trying to create directory {}", req.getPath());

        try {
            _door.createDirectory(createFullPath(req.getPath()),
                                  req.shouldMkPath(),
                                  req.getSubject(),
                                  _authz);
            return withOk(req);
        } catch (TimeoutCacheException e) {
            throw xrootdException(e.getRc(), "Internal timeout");
        } catch (PermissionDeniedCacheException |FileNotFoundCacheException
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
        throws XrootdException
    {
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
            _door.moveFile(createFullPath(req.getSourcePath()),
                           createFullPath(req.getTargetPath()),
                           req.getSubject(),
                           _authz);
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
    protected XrootdResponse<QueryRequest> doOnQuery(ChannelHandlerContext ctx, QueryRequest msg) throws XrootdException
    {
        switch (msg.getReqcode()) {
        case kXR_Qconfig:
            StringBuilder s = new StringBuilder();
            for (String name: msg.getArgs().split(" ")) {
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
                ChecksumInfo info = new ChecksumInfo(msg.getPath(),
                                                     msg.getOpaque());
                Set<Checksum> checksums = _door.getChecksums(createFullPath(msg.getPath()),
                                                             msg.getSubject(),
                                                             _authz);
                return selectChecksum(info, checksums, msg);
            } catch (CacheException e) {
                throw xrootdException(e);
            }
        default:
            return unsupported(ctx, msg);
        }
    }

    @Override
    protected XrootdResponse<DirListRequest> doOnDirList(ChannelHandlerContext ctx, DirListRequest request)
        throws XrootdException
    {
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

            if (request.isDirectoryStat()) {
                _door.listPath(fullListPath, request.getSubject(), _authz,
                               new StatListCallback(request, fullListPath, ctx),
                               _door.getRequiredAttributesForFileStatus());
            } else {
                _door.listPath(fullListPath, request.getSubject(), _authz,
                               new ListCallback(request, ctx),
                               EnumSet.noneOf(FileAttribute.class));
            }
            return null;
        } catch (PermissionDeniedCacheException e) {
            throw xrootdException(e);
        }
    }

    @Override
    protected XrootdResponse<PrepareRequest> doOnPrepare(ChannelHandlerContext ctx, PrepareRequest msg)
        throws XrootdException
    {
        return withOk(msg);
    }

    private void logDebugOnOpen(OpenRequest req)
    {
        int options = req.getOptions();
        String openFlags =
            "options to apply for open path (raw=" + options +" ):";

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
     * Callback responding to client depending on the list directory messages
     * it receives from Pnfs via the door.
     * @author tzangerl
     *
     */
    private class ListCallback
        extends AbstractMessageCallback<PnfsListDirectoryMessage>
    {
        protected final DirListRequest _request;
        protected final ChannelHandlerContext _context;
        protected final DirListResponse.Builder _response;

        public ListCallback(DirListRequest request, ChannelHandlerContext context)
        {
            _request = request;
            _response = DirListResponse.builder(request);
            _context = context;
        }

        /**
         * Respond to client if message contains errors. Try to use
         * meaningful status codes from the xrootd-protocol to map the errors
         * from PnfsManager.
         *
         * @param rc The error code of the message
         * @param error Object describing the actual error that occurred
         */
        @Override
        public void failure(int rc, Object error)
        {
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
                    withError(_request, xrootdErrorCode(rc), errorMessage));
        }

        /**
         * Reply to client if no route to PNFS manager was found.
         *
         */
        @Override
        public void noroute(CellPath path)
        {
            respond(_context,
                    withError(_request,
                              kXR_ServerError,
                              "Could not contact PNFS Manager."));
        }

        /**
         * In case of a listing success, inspect the message. If the message
         * is the final listing message, reply with kXR_ok and the full
         * directory listing. If the message is not the final message, reply
         * with oksofar and the partial directory listing.
         *
         * @param message The PnfsListDirectoryMessage-reply as it was received
         * from the PNFSManager.
         */
        @Override
        public void success(PnfsListDirectoryMessage message)
        {
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
                    withError(_request,
                              kXR_ServerError,
                              "Timeout when trying to list directory!"));
        }
    }

    private class StatListCallback extends ListCallback
    {
        private final String _client;
        protected final FsPath _dirPath;

        public StatListCallback(DirListRequest request, FsPath dirPath, ChannelHandlerContext context)
        {
            super(request, context);
            _client = getSourceAddress().getAddress().getHostAddress();
            _dirPath = dirPath;
        }

        @Override
        public void success(PnfsListDirectoryMessage message)
        {
            message.getEntries().stream().forEach(
                    e -> _response.add(e.getName(), _door.getFileStatus(_request.getSubject(),
                                                                        _authz,
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
        LoginReply reply = event.getLoginReply();
        _authz = Restrictions.none();
        if (reply != null) {
            _authz = reply.getRestriction();
            _isLoggedIn = true;
            _userRootPath = reply.getLoginAttributes().stream()
                    .filter(RootDirectory.class::isInstance)
                    .findFirst()
                    .map(RootDirectory.class::cast)
                    .map(RootDirectory::getRoot)
                    .map(FsPath::create)
                    .orElse(FsPath.ROOT);
            _maximumUploadSize = LoginAttributes.maximumUploadSize(reply.getLoginAttributes());
        } else {
            _isLoggedIn = false;
        }
    }

    /**
     * Forms a full PNFS path. The path is created by concatenating
     * the root path and path. The root path is guaranteed to be a
     * prefix of the path returned.
     */
    private FsPath createFullPath(String path)
            throws PermissionDeniedCacheException
    {
        return _rootPath.chroot(path);
    }
}
