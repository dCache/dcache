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

import com.google.common.net.InetAddresses;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

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
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.Checksum;
import org.dcache.util.Checksums;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.vehicles.PnfsListDirectoryMessage;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.protocol.messages.DirListRequest;
import org.dcache.xrootd.protocol.messages.DirListResponse;
import org.dcache.xrootd.protocol.messages.MkDirRequest;
import org.dcache.xrootd.protocol.messages.MvRequest;
import org.dcache.xrootd.protocol.messages.OpenRequest;
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
import org.dcache.xrootd.util.OpaqueStringParser;
import org.dcache.xrootd.util.ParseException;

import static org.dcache.xrootd.protocol.XrootdProtocol.*;

/**
 * Channel handler which redirects all open requests to a pool.
 */
public class XrootdRedirectHandler extends ConcurrentXrootdRequestHandler
{
    private static final Logger _log =
        LoggerFactory.getLogger(XrootdRedirectHandler.class);

    private final XrootdDoor _door;
    private final FsPath _rootPath;

    private Restriction _authz = Restrictions.denyAll();
    private final Map<String,String> _appIoQueues;

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
     * The open, if successful, will always result in a redirect
     * response to the proper pool, hence no subsequent requests like
     * sync, read, write or close are expected at the door.
     */
    @Override
    protected XrootdResponse<OpenRequest> doOnOpen(ChannelHandlerContext ctx, OpenRequest req)
        throws XrootdException
    {
        /* We ought to process this asynchronously to not block the calling thread during
         * staging or queuing. We should also switch to an asynchronous reply model if
         * the request is nearline or is queued on a pool. The naive approach to always
         * use an asynchronous reply model doesn't work because the xrootd 3.x client
         * introduces an artificial 1 second delay when processing such a response.
         */

        InetSocketAddress localAddress = getLocalAddress();
        InetSocketAddress remoteAddress = getRemoteAddress();

        FilePerm neededPerm = req.getRequiredPermission();

        _log.info("Opening {} for {}", req.getPath(), neededPerm.xmlText());
        if (_log.isDebugEnabled()) {
            logDebugOnOpen(req);
        }

        String ioQueue = appSpecificQueue(req);

        Long size = null;
        try {
            Map<String,String> opaque = OpaqueStringParser.getOpaqueMap(req.getOpaque());
            try {
                String value = opaque.get("oss.asize");
                if (value != null) {
                    size = Long.valueOf(value);
                }
            } catch (NumberFormatException exception) {
                _log.warn("Ignoring malformed oss.asize: {}", exception.getMessage());
            }
        } catch (ParseException e) {
            _log.warn("Ignoring malformed open opaque {}: {}", req.getOpaque(),
                    e.getMessage());
        }

        UUID uuid = UUID.randomUUID();
        String opaque = OpaqueStringParser.buildOpaqueString(UUID_PREFIX, uuid.toString());

        /* Interact with core dCache to open the requested file.
         */
        try {
            XrootdTransfer transfer;
            if (neededPerm == FilePerm.WRITE) {
                boolean createDir = req.isMkPath();
                boolean overwrite = req.isDelete();

                transfer =
                    _door.write(remoteAddress, createFullPath(req.getPath()), ioQueue,
                                uuid, createDir, overwrite, size, localAddress,
                                req.getSubject(), _authz);
            } else {
                transfer =
                    _door.read(remoteAddress, createFullPath(req.getPath()), ioQueue,
                               uuid, localAddress, req.getSubject(), _authz);
            }

            // ok, open was successful
            InetSocketAddress address = transfer.getRedirect();
            _log.info("Redirecting to {}", address);

            /* xrootd developers say that IPv6 addresses must always be URI quoted.
             * The spec doesn't require this, but clients depend on it.
             */
            return new RedirectResponse<>(
                    req, InetAddresses.toUriString(address.getAddress()), address.getPort(), opaque, "");
        } catch (FileNotFoundCacheException e) {
            return withError(req, kXR_NotFound, "No such file");
        } catch (FileExistsCacheException e) {
            return withError(req, kXR_Unsupported, "File already exists");
        } catch (TimeoutCacheException e) {
            return withError(req, kXR_ServerError, "Internal timeout");
        } catch (PermissionDeniedCacheException e) {
            return withError(req, kXR_NotAuthorized, e.getMessage());
        } catch (FileIsNewCacheException e) {
            return withError(req, kXR_FileLocked, "File is locked by upload");
        } catch (NotFileCacheException e) {
            return withError(req, kXR_NotFile, "Not a file");
        } catch (CacheException e) {
            return withError(req, kXR_ServerError,
                             String.format("Failed to open file (%s [%d])", e.getMessage(), e.getRc()));
        } catch (InterruptedException e) {
            /* Interrupt may be caused by cell shutdown or client
             * disconnect.  If the client disconnected, then the error
             * message will never reach the client, so saying that the
             * server shut down is okay.
             */
            return withError(req, kXR_ServerError, "Server shutdown");
        }
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

    @Override
    protected XrootdResponse<StatRequest> doOnStat(ChannelHandlerContext ctx, StatRequest req)
        throws XrootdException
    {
        String path = req.getPath();
        try {
            InetSocketAddress client = getRemoteAddress();
            return new StatResponse(req, _door.getFileStatus(createFullPath(path), req.getSubject(), _authz,
                                                             client.getAddress().getHostAddress()));
        } catch (FileNotFoundCacheException e) {
            throw new XrootdException(kXR_NotFound, "No such file");
        } catch (TimeoutCacheException e) {
            throw new XrootdException(kXR_ServerError, "Internal timeout");
        } catch (PermissionDeniedCacheException e) {
            throw new XrootdException(kXR_NotAuthorized, e.getMessage());
        } catch (CacheException e) {
            throw new XrootdException(kXR_ServerError,
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
            throw new XrootdException(kXR_ServerError, "Internal timeout");
        } catch (PermissionDeniedCacheException e) {
            throw new XrootdException(kXR_NotAuthorized, e.getMessage());
        } catch (CacheException e) {
            throw new XrootdException(kXR_ServerError,
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
            throw new XrootdException(kXR_ServerError, "Internal timeout");
        } catch (PermissionDeniedCacheException e) {
            throw new XrootdException(kXR_NotAuthorized, e.getMessage());
        } catch (FileNotFoundCacheException e) {
            throw new XrootdException(kXR_NotFound, "No such file");
        } catch (CacheException e) {
            throw new XrootdException(kXR_ServerError,
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
            throw new XrootdException(kXR_ServerError, "Internal timeout");
        } catch (PermissionDeniedCacheException e) {
            throw new XrootdException(kXR_NotAuthorized, e.getMessage());
        } catch (FileNotFoundCacheException e) {
            throw new XrootdException(kXR_NotFound, e.getMessage());
        } catch (CacheException e) {
            throw new XrootdException(kXR_ServerError,
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
            throw new XrootdException(kXR_ServerError, "Internal timeout");
        } catch (PermissionDeniedCacheException e) {
            throw new XrootdException(kXR_NotAuthorized, e.getMessage());
        } catch (FileNotFoundCacheException | FileExistsCacheException e) {
            throw new XrootdException(kXR_FSError, e.getMessage());
        } catch (CacheException e) {
            throw new XrootdException(kXR_ServerError,
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
            _door.moveFile(
                    createFullPath(req.getSourcePath()),
                    createFullPath(req.getTargetPath()),
                    req.getSubject(),
                    _authz);
            return withOk(req);
        } catch (TimeoutCacheException e) {
            throw new XrootdException(kXR_ServerError, "Internal timeout");
        } catch (PermissionDeniedCacheException e) {
            throw new XrootdException(kXR_NotAuthorized, e.getMessage());
        } catch (FileNotFoundCacheException e) {
            throw new XrootdException(kXR_NotFound,
                                      String.format("Source file does not exist (%s) ",
                                                    e.getMessage()));
        } catch (FileExistsCacheException e) {
            throw new XrootdException(kXR_FSError,
                                      String.format("Will not overwrite existing file " +
                                                    "(%s).", e.getMessage()));
        } catch (CacheException e) {
            throw new XrootdException(kXR_ServerError,
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
                    s.append("1:ADLER32,2:MD5");
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
                Set<Checksum> checksums = _door.getChecksums(createFullPath(msg.getArgs()),
                                                             msg.getSubject(),
                                                             _authz);
                if (!checksums.isEmpty()) {
                    Checksum checksum = Checksums.preferrredOrder().min(checksums);
                    return new QueryResponse(msg, checksum.getType().getName() + " " + checksum.getValue());
                }
            } catch (FileNotFoundCacheException e) {
                throw new XrootdException(kXR_NotFound, e.getMessage());
            } catch (PermissionDeniedCacheException e) {
                throw new XrootdException(kXR_NotAuthorized, e.getMessage());
            } catch (CacheException e) {
                throw new XrootdException(kXR_ServerError, e.getMessage());
            }
            throw new XrootdException(kXR_Unsupported, "No checksum available for this file.");

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
            throw new XrootdException(kXR_NotAuthorized, e.getMessage());
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

        _log.debug("open flags: "+openFlags);

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
            switch (rc) {
            case CacheException.TIMEOUT:
                respond(_context,
                        withError(_request,
                                  kXR_ServerError,
                                  "Timeout when trying to list directory: " +
                                  error.toString()));
                break;
            case CacheException.PERMISSION_DENIED:
                respond(_context,
                        withError(_request,
                                  kXR_NotAuthorized,
                                  "Permission to list that directory denied: " +
                                  error.toString()));
                break;
            case CacheException.FILE_NOT_FOUND:
                respond(_context,
                        withError(_request, kXR_NotFound, "Path not found"));
                break;
            default:
                respond(_context,
                        withError(_request,
                                  kXR_ServerError,
                                  "Error when processing list response: " +
                                  error.toString()));
                break;
            }
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
            _client = getRemoteAddress().getAddress().getHostAddress();
            _dirPath = dirPath;
        }

        @Override
        public void success(PnfsListDirectoryMessage message)
        {
            message.getEntries().stream().forEach(
                    e -> _response.add(e.getName(), _door.getFileStatus(_request.getSubject(), _authz, _dirPath.child(e.getName()), _client, e.getFileAttributes())));
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
    private void loggedIn(LoginEvent event)
    {
        LoginReply reply = event.getLoginReply();
        _authz = Restrictions.none();
        if (reply != null) {
            _authz = reply.getRestriction();
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
