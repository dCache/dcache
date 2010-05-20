package org.dcache.xrootd2.door;

import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.nio.channels.ClosedChannelException;
import java.security.SecureRandom;

import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.dcache.xrootd2.protocol.messages.AuthorizableRequestMessage;
import org.dcache.xrootd2.protocol.messages.CloseRequest;
import org.dcache.xrootd2.protocol.messages.OpenRequest;
import org.dcache.xrootd2.protocol.messages.RedirectResponse;
import org.dcache.xrootd2.protocol.messages.StatRequest;
import org.dcache.xrootd2.protocol.messages.StatResponse;
import org.dcache.xrootd2.protocol.messages.StatxRequest;
import org.dcache.xrootd2.protocol.messages.StatxResponse;
import org.dcache.xrootd2.security.AuthorizationHandler;
import org.dcache.xrootd2.util.FileStatus;
import org.dcache.xrootd2.util.ParseException;
import org.dcache.xrootd2.core.XrootdRequestHandler;
import org.dcache.xrootd2.protocol.messages.*;
import static org.dcache.xrootd2.protocol.XrootdProtocol.*;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DirNotExistsCacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.util.FileMetaData.Permissions;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel handler which redirects all open requests to a
 * pool. Besides open, the handler implements stat and statx. All
 * other requests return an error to the client.
 *
 * Most of the code is copied from the old xrootd implementation.
 *
 * Should possibly be renamed as only open requests are
 * redirected. Other requests can be handled locally.
 */
@ChannelPipelineCoverage("one")
public class XrootdRedirectHandler extends XrootdRequestHandler
{
    private final static Logger _log =
        LoggerFactory.getLogger(XrootdRedirectHandler.class);

    /**
     * Secure random number generator used for making login tokens.
     */
    private final static SecureRandom _random = new SecureRandom();

    private final XrootdDoor _door;

    /**
     * The set of threads which currently process an xrootd request
     * for this channel. They will be interrupted in case the channel
     * is disonnected.
     */
    private final Set<Thread> _threads =
        Collections.synchronizedSet(new HashSet());

    public XrootdRedirectHandler(XrootdDoor door)
    {
        _door = door;
    }

    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
    {
        Thread me = Thread.currentThread();
        _threads.add(me);
        try {
            super.messageReceived(ctx, e);
        } finally {
            _threads.remove(me);
        }
    }

    public void channelDisconnected(ChannelHandlerContext ctx,
                                    ChannelStateEvent e)
        throws Exception
    {
        synchronized (_threads) {
            for (Thread thread: _threads) {
                thread.interrupt();
            }
        }
        super.channelDisconnected(ctx, e);
    }

    public void exceptionCaught(ChannelHandlerContext ctx,
                                ExceptionEvent e)
    {
        Throwable t = e.getCause();
        if (t instanceof ClosedChannelException) {
            _log.info("Connection closed");
        } else if (t instanceof RuntimeException || t instanceof Error) {
            Thread me = Thread.currentThread();
            me.getUncaughtExceptionHandler().uncaughtException(me, t);
        } else {
            _log.warn(t.toString());
        }
        // TODO: If not already closed, we should probably close the
        // channel.
    }

    protected void doOnLogin(ChannelHandlerContext ctx, MessageEvent e, LoginRequest msg)
    {
        _log.debug("login attempt, access granted");
        respond(ctx, e, new OKResponse(msg.getStreamID()));
    }

    protected void doOnAuthentication(ChannelHandlerContext ctx, MessageEvent e, AuthenticationRequest msg)
    {
        _log.debug("authentication passed");
        respond(ctx, e, new OKResponse(msg.getStreamID()));
    }

    /**
     * The open, if successful, will always result in a redirect
     * response to the proper pool, hence no subsequent requests like
     * sync, read, write or close are expected at the door.
     */
    protected void doOnOpen(ChannelHandlerContext ctx, MessageEvent event,
                            OpenRequest req)
    {
        Channel channel = event.getChannel();
        InetSocketAddress localAddress =
            (InetSocketAddress) channel.getLocalAddress();
        InetSocketAddress remoteAddress =
            (InetSocketAddress) channel.getRemoteAddress();
        int options = req.getOptions();

        FilePerm neededPerm;

        if (req.isNew() || req.isReadWrite()) {
            neededPerm = FilePerm.WRITE;
        } else {
            neededPerm = FilePerm.READ;
        }

        _log.info("Opening {} for {}", req.getPath(), neededPerm.xmlText());
        if (_log.isDebugEnabled()) {
            logDebugOnOpen(req);
        }

        try {
           String authPath = checkOperationPermission(neededPerm, req,
                                                      localAddress);
            ////////////////////////////////////////////////////////////////
            // interact with core dCache to open the requested file
            long checksum = req.calcChecksum();
            XrootdTransfer transfer;
            if (neededPerm == FilePerm.WRITE) {
                boolean createDir = (options & XrootdProtocol.kXR_mkpath) ==
                    XrootdProtocol.kXR_mkpath;
                transfer =
                    _door.write(remoteAddress, authPath, checksum, createDir);
            } else {
                transfer =
                    _door.read(remoteAddress, authPath, checksum);
            }

            // ok, open was successful
            InetSocketAddress address = transfer.getRedirect();
            _log.info("Redirecting to {}", address);
            respond(ctx, event,
                    new RedirectResponse(req.getStreamID(),
                                         address.getHostName(),
                                         address.getPort()));
        } catch (FileNotFoundCacheException fnfex) {
            respondWithError(ctx, event, req,
                             XrootdProtocol.kXR_FileNotOpen,
                             "No such file");
        } catch (DirNotExistsCacheException dnex) {
            respondWithError(ctx, event, req,
                             XrootdProtocol.kXR_FileNotOpen,
                             "No such directory");
        } catch (FileExistsCacheException feex) {
            respondWithError(ctx, event, req,
                             XrootdProtocol.kXR_Unsupported,
                             "File already exists");
        } catch (TimeoutCacheException tex) {
            respondWithError(ctx, event, req,
                             XrootdProtocol.kXR_ServerError,
                             "Internal timeout");
        } catch (PermissionDeniedCacheException pdex) {
            respondWithError(ctx, event, req,
                             XrootdProtocol.kXR_NotAuthorized,
                             pdex.getMessage());
        } catch (CacheException ex) {
            respondWithError(ctx, event, req,
                             XrootdProtocol.kXR_ServerError,
                             String.format("Failed to open file (%s [%d])",
                                           ex.getMessage(), ex.getRc()));
        } catch (InterruptedException e) {
            /* Interrupt may be caused by cell shutdown or client
             * disconnect.  If the client disconnected, then the error
             * message will never reach the client, so saying that the
             * server shut down is okay.
             */
            respondWithError(ctx, event, req,
                             XrootdProtocol.kXR_ServerError,
                             "Server shutdown");
        } catch (RuntimeException e) {
            _log.error("Open failed due to a bug", e);
            respondWithError(ctx, event, req,
                             XrootdProtocol.kXR_ServerError,
                             String.format("Internal server error (%s)",
                                           e.getMessage()));
        }
    }

    protected void doOnStat(ChannelHandlerContext ctx, MessageEvent event,
                            StatRequest req)
    {
        String path = req.getPath();
        try {
            FileMetaData meta = _door.getFileMetaData(path);
            FileStatus fs = convertToFileStatus(meta); // FIXME
            respond(ctx, event, new StatResponse(req.getStreamID(), fs));
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.FILE_NOT_FOUND:
                _log.info("No PnfsId found for path: " + path);
                respond(ctx, event,
                        new StatResponse(req.getStreamID(),
                                         FileStatus.FILE_NOT_FOUND));
                break;

            case CacheException.TIMEOUT:
                respondWithError(ctx, event, req,
                                 XrootdProtocol.kXR_ServerError,
                                 "Internal timeout");
                break;

            case CacheException.PERMISSION_DENIED:
                respondWithError(ctx, event, req,
                                 XrootdProtocol.kXR_NotAuthorized,
                                 e.getMessage());
                break;

            default:
                respondWithError(ctx, event, req,
                                 XrootdProtocol.kXR_ServerError,
                                 String.format("Failed to open file (%s [%d])",
                                               e.getMessage(), e.getRc()));
            }
        } catch (RuntimeException e) {
            _log.error("Stat failed due to a bug", e);
            respondWithError(ctx, event, req,
                             XrootdProtocol.kXR_ServerError,
                             String.format("Internal server error (%s)",
                                           e.getMessage()));
        }
    }

    protected void doOnStatx(ChannelHandlerContext ctx, MessageEvent event,
                             StatxRequest req)
    {
        if (req.getPaths().length == 0) {
            respondWithError(ctx, event, req,
                             kXR_ArgMissing, "no paths specified");
            return;
        }

        try {
            String[] paths = req.getPaths();
            FileMetaData[] allMetas = _door.getMultipleFileMetaData(paths);

            int[] flags = new int[allMetas.length];
            for (int i =0; i < allMetas.length; i++) {
                if (allMetas[i] == null) {
                    flags[i] = kXR_other;
                } else {
                    flags[i] = getFileStatusFlags(allMetas[i]);
                }
            }

            respond(ctx, event, new StatxResponse(req.getStreamID(), flags));
        } catch (CacheException e) {
            switch (e.getRc()) {
            case CacheException.TIMEOUT:
                respondWithError(ctx, event, req,
                                 XrootdProtocol.kXR_ServerError,
                                 "Internal timeout");
                break;
            case CacheException.PERMISSION_DENIED:
                respondWithError(ctx, event, req,
                                 XrootdProtocol.kXR_NotAuthorized,
                                 e.getMessage());
                break;
            default:
                respondWithError(ctx, event, req,
                                 XrootdProtocol.kXR_ServerError,
                                 String.format("Failed to open file (%s [%d])",
                                               e.getMessage(), e.getRc()));
            }
        } catch (RuntimeException e) {
            _log.error("Statx failed due to a bug", e);
            respondWithError(ctx, event, req,
                             XrootdProtocol.kXR_ServerError,
                             String.format("Internal server error (%s)",
                                           e.getMessage()));
        }
    }


    @Override
    protected void doOnRm(ChannelHandlerContext ctx, MessageEvent e, RmRequest req)
    {
        Channel channel = e.getChannel();
        InetSocketAddress localAddress =
                            (InetSocketAddress) channel.getLocalAddress();
        if (req.getPath().isEmpty()) {
            respondWithError(ctx, e, req,
                             kXR_ArgMissing, "no path specified");
            return;
        }

        _log.info("Trying to delete {}", req.getPath());

        try {
            String authPath = checkOperationPermission(FilePerm.DELETE,
                                                       req, localAddress);

            _door.deleteFile(authPath);
            respond(ctx, e, new OKResponse(req.getStreamID()));
        } catch (TimeoutCacheException tce) {
                respondWithError(ctx, e, req,
                                 XrootdProtocol.kXR_ServerError,
                                 "Internal timeout");
        } catch (PermissionDeniedCacheException pdce) {
                respondWithError(ctx, e, req,
                                 XrootdProtocol.kXR_NotAuthorized,
                                 pdce.getMessage());
        } catch (CacheException ce) {
                respondWithError(ctx, e, req,
                                 XrootdProtocol.kXR_ServerError,
                                 String.format("Failed to delete file (%s [%d])",
                                               ce.getMessage(), ce.getRc()));
        } catch (RuntimeException exp) {
            _log.error("Rm failed due to a bug", exp);
            respondWithError(ctx, e, req,
                             XrootdProtocol.kXR_ServerError,
                             String.format("Internal server error (%s)",
                                           exp.getMessage()));
        }
    }

    @Override
    protected void doOnRmDir(ChannelHandlerContext ctx, MessageEvent e,
                             RmDirRequest req)
    {
        Channel channel = e.getChannel();
        InetSocketAddress localAddress =
                            (InetSocketAddress) channel.getLocalAddress();
        if (req.getPath().isEmpty()) {
            respondWithError(ctx, e, req,
                             kXR_ArgMissing, "no path specified");
            return;
        }

        _log.info("Trying to delete directory {}", req.getPath());

        try {
            String authPath = checkOperationPermission(FilePerm.DELETE,
                                                       req, localAddress);

            _door.deleteDirectory(authPath);
            respond(ctx, e, new OKResponse(req.getStreamID()));
        } catch (TimeoutCacheException tce) {
            respondWithError(ctx, e, req,
                             XrootdProtocol.kXR_ServerError,
                             "Internal timeout");
        } catch (PermissionDeniedCacheException pdce) {
            respondWithError(ctx, e, req,
                             XrootdProtocol.kXR_NotAuthorized,
                             pdce.getMessage());
        } catch (FileNotFoundCacheException fnfce) {
            respondWithError(ctx, e, req,
                             XrootdProtocol.kXR_FSError,
                             fnfce.getMessage());
        } catch (CacheException ce) {
                respondWithError(ctx, e, req,
                                 XrootdProtocol.kXR_ServerError,
                                 String.format("Failed to delete directory " +
                                               "(%s [%d]).",
                                               ce.getMessage(), ce.getRc()));
        } catch (RuntimeException exp) {
            _log.error("RmDir failed due to a bug", exp);
            respondWithError(ctx, e, req,
                             XrootdProtocol.kXR_ServerError,
                             String.format("Internal server error (%s)",
                                           exp.getMessage()));
        }
    }

    @Override
    protected void doOnMkDir(ChannelHandlerContext ctx, MessageEvent e,
                             MkDirRequest req) {
        Channel channel = e.getChannel();
        InetSocketAddress localAddress =
                            (InetSocketAddress) channel.getLocalAddress();
        if (req.getPath().isEmpty()) {
            respondWithError(ctx, e, req,
                             kXR_ArgMissing, "no path specified");
            return;
        }

        _log.info("Trying to create directory {}", req.getPath());

        try {
            String authPath = checkOperationPermission(FilePerm.WRITE,
                                                       req, localAddress);

            _door.createDirectory(authPath, req.shouldMkPath());
            respond(ctx, e, new OKResponse(req.getStreamID()));
        } catch (TimeoutCacheException tce) {
            respondWithError(ctx, e, req,
                             XrootdProtocol.kXR_ServerError,
                             "Internal timeout");
        } catch (PermissionDeniedCacheException pdce) {
            respondWithError(ctx, e, req,
                             XrootdProtocol.kXR_NotAuthorized,
                             pdce.getMessage());
        } catch (FileNotFoundCacheException fnfce) {
            respondWithError(ctx, e, req,
                             XrootdProtocol.kXR_FSError,
                             fnfce.getMessage());
        } catch (FileExistsCacheException fece) {
            respondWithError(ctx, e, req,
                             XrootdProtocol.kXR_FSError,
                             fece.getMessage());
        } catch (CacheException ce) {
                respondWithError(ctx, e, req,
                                 XrootdProtocol.kXR_ServerError,
                                 String.format("Failed to create directory " +
                                               "(%s [%d]).",
                                               ce.getMessage(), ce.getRc()));
        } catch (RuntimeException exp) {
            _log.error("MkDir failed due to a bug", exp);
            respondWithError(ctx, e, req,
                             XrootdProtocol.kXR_ServerError,
                             String.format("Internal server error (%s)",
                                           exp.getMessage()));
        }
    }

    protected void doOnClose(ChannelHandlerContext ctx, MessageEvent e, CloseRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    protected void doOnProtocolRequest(ChannelHandlerContext ctx, MessageEvent e, ProtocolRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    /**
     * Check if the permissions on the path sent along with the request message
     * satisfy the required permission level.
     * @param neededPerm The permission level that is required for the operation
     * @param req The actual request, containing path and authZ token
     * @param localAddress The local address, needed for token endpoint
     *        verification
     * @return The path referring to the LFN in the request, if the request
     *         contained a LFN. The path from the request, if the request
     *         contained an absolute path.
     * @throws PermissionDeniedCacheException The needed permissions are not
     *         present in the authZ token, the authZ token is not present or
     *         the format is corrupted.
     */
    private String checkOperationPermission(FilePerm neededPerm,
                                          AuthorizableRequestMessage req,
                                          InetSocketAddress localAddress)
                                 throws PermissionDeniedCacheException {
        AuthorizationHandler authzHandler =
            _door.getAuthorizationFactory().getAuthzHandler();
        String path = req.getPath();

        if (authzHandler != null) {
            // all information neccessary for checking authorization
            // is found in opaque
            Map<String, String> opaque;
            try {
                opaque = req.getOpaqueMap();
            } catch (ParseException e) {
                StringBuffer msg =
                    new StringBuffer("invalid opaque data: ");
                msg.append(e);
                String s = req.getOpaque();
                if (s != null) {
                    msg.append(" opaque=").append(s);
                }
                throw new PermissionDeniedCacheException(msg.toString());
            }

            boolean isAuthorized;

            try {
                isAuthorized =
                    authzHandler.checkAuthz(path, opaque, neededPerm,
                                            localAddress);
            } catch (GeneralSecurityException e) {
                throw new PermissionDeniedCacheException("authorization check"
                                                         + "failed: " +
                                                         e.getMessage());
            }

            if (!isAuthorized) {
                throw new PermissionDeniedCacheException("not authorized");
            }

            // In case of enabled authorization, the path in the open
            // request can refer to the lfn.  In this case the real
            // path is delivered by the authz plugin
            if (authzHandler.providesPFN()) {
                _log.info("access granted for LFN={} PFN={}",
                          path, authzHandler.getPFN());

                // get the real path (pfn) which we will open
                return authzHandler.getPFN();
            }
        }

        return path;
    }

    private void logDebugOnOpen(OpenRequest req)
    {
        int options = req.getOptions();
        String openFlags =
            "options to apply for open path (raw=" + options +" ):";

        if ((options & XrootdProtocol.kXR_async) == XrootdProtocol.kXR_async)
            openFlags += " kXR_async";
        if ((options & XrootdProtocol.kXR_compress) == XrootdProtocol.kXR_compress)
            openFlags += " kXR_compress";
        if ((options & XrootdProtocol.kXR_delete) == XrootdProtocol.kXR_delete)
            openFlags += " kXR_delete";
        if ((options & XrootdProtocol.kXR_force) == XrootdProtocol.kXR_force)
            openFlags += " kXR_force";
        if ((options & XrootdProtocol.kXR_new) == XrootdProtocol.kXR_new)
            openFlags += " kXR_new";
        if ((options & XrootdProtocol.kXR_open_read) == XrootdProtocol.kXR_open_read)
            openFlags += " kXR_open_read";
        if ((options & XrootdProtocol.kXR_open_updt) == XrootdProtocol.kXR_open_updt)
            openFlags += " kXR_open_updt";
        if ((options & XrootdProtocol.kXR_refresh) == XrootdProtocol.kXR_refresh)
            openFlags += " kXR_refresh";
        if ((options & XrootdProtocol.kXR_mkpath) == XrootdProtocol.kXR_mkpath)
            openFlags += " kXR_mkpath";
        if ((options & XrootdProtocol.kXR_open_apnd) == XrootdProtocol.kXR_open_apnd)
            openFlags += " kXR_open_apnd";
        if ((options & XrootdProtocol.kXR_retstat) == XrootdProtocol.kXR_retstat)
            openFlags += " kXR_retstat";

        _log.debug("open flags: "+openFlags);

        int mode = req.getUMask();
        String s = "";

        if ((mode & XrootdProtocol.kXR_ur) == XrootdProtocol.kXR_ur)
            s += "r";
        else
            s += "-";
        if ((mode & XrootdProtocol.kXR_uw) == XrootdProtocol.kXR_uw)
            s += "w";
        else
            s += "-";
        if ((mode & XrootdProtocol.kXR_ux) == XrootdProtocol.kXR_ux)
            s += "x";
        else
            s += "-";

        s += " ";

        if ((mode & XrootdProtocol.kXR_gr) == XrootdProtocol.kXR_gr)
            s += "r";
        else
            s += "-";
        if ((mode & XrootdProtocol.kXR_gw) == XrootdProtocol.kXR_gw)
            s += "w";
        else
            s += "-";
        if ((mode & XrootdProtocol.kXR_gx) == XrootdProtocol.kXR_gx)
            s += "x";
        else
            s += "-";

        s += " ";

        if ((mode & XrootdProtocol.kXR_or) == XrootdProtocol.kXR_or)
            s += "r";
        else
            s += "-";
        if ((mode & XrootdProtocol.kXR_ow) == XrootdProtocol.kXR_ow)
            s += "w";
        else
            s += "-";
        if ((mode & XrootdProtocol.kXR_ox) == XrootdProtocol.kXR_ox)
            s += "x";
        else
            s += "-";

        _log.debug("mode to apply to open path: " + s);
    }

    private int getFileStatusFlags(FileMetaData meta)
    {
        int flags = 0;
        if (meta.isDirectory())
            flags |= kXR_isDir;
        if (!meta.isRegularFile() && !meta.isDirectory())
            flags |= kXR_other;
        Permissions pm = meta.getWorldPermissions();
        if (pm.canExecute())
            flags |= kXR_xset;
        if (pm.canRead())
            flags |= kXR_readable;
        if (pm.canWrite())
            flags |= kXR_writable;
        return flags;
    }

    private FileStatus convertToFileStatus(FileMetaData meta)
    {
        return new FileStatus(0, meta.getFileSize(), getFileStatusFlags(meta),
                              meta.getLastModifiedTime());
    }
}