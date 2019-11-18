/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 - 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.xrootd.pool;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileCorruptedCacheException;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.movers.NettyTransferService;
import org.dcache.pool.repository.OutOfDiskException;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Version;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.AbstractXrootdRequestHandler;
import org.dcache.xrootd.CacheExceptionMapper;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.core.XrootdSessionIdentifier;
import org.dcache.xrootd.core.XrootdSigverDecoder;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.AuthenticationRequest;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.DirListRequest;
import org.dcache.xrootd.protocol.messages.EndSessionRequest;
import org.dcache.xrootd.protocol.messages.GenericReadRequestMessage.EmbeddedReadRequest;
import org.dcache.xrootd.protocol.messages.LoginRequest;
import org.dcache.xrootd.protocol.messages.LoginResponse;
import org.dcache.xrootd.protocol.messages.MkDirRequest;
import org.dcache.xrootd.protocol.messages.MvRequest;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.OpenResponse;
import org.dcache.xrootd.protocol.messages.QueryRequest;
import org.dcache.xrootd.protocol.messages.QueryResponse;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.ReadVRequest;
import org.dcache.xrootd.protocol.messages.ReadVResponse;
import org.dcache.xrootd.protocol.messages.RedirectResponse;
import org.dcache.xrootd.protocol.messages.RmDirRequest;
import org.dcache.xrootd.protocol.messages.RmRequest;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.StatResponse;
import org.dcache.xrootd.protocol.messages.StatxRequest;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;
import org.dcache.xrootd.protocol.messages.XrootdRequest;
import org.dcache.xrootd.protocol.messages.XrootdResponse;
import org.dcache.xrootd.tpc.TpcWriteDescriptor;
import org.dcache.xrootd.tpc.XrootdTpcInfo;
import org.dcache.xrootd.util.ChecksumInfo;
import org.dcache.xrootd.util.FileStatus;
import org.dcache.xrootd.util.OpaqueStringParser;
import org.dcache.xrootd.util.ParseException;

import static org.dcache.xrootd.protocol.XrootdProtocol.*;

/**
 * XrootdPoolRequestHandler is an xrootd request processor on the pool
 * side - it receives xrootd requests messages from the client.
 *
 * Upon an open request it retrieves a mover channel from
 * XrootdPoolNettyServer and passes all subsequent client requests on
 * to a file descriptor wrapping the mover channel.
 *
 * Synchronisation is currently not ensured by the handler; it relies
 * on the synchronization by the underlying channel execution handler.
 */
public class XrootdPoolRequestHandler extends AbstractXrootdRequestHandler
{
    private static final Logger _log =
        LoggerFactory.getLogger(XrootdPoolRequestHandler.class);

    public static final int DEFAULT_FILESTATUS_FLAGS = 0;
    private static final int DEFAULT_FILESTATUS_ID = 0;
    private static final int DEFAULT_FILESTATUS_MODTIME = 0;

    private static final int MAX_JAVA_ARRAY = Integer.MAX_VALUE - 5;
    private static final int READV_HEADER_LENGTH = 24;
    private static final int READV_ELEMENT_LENGTH = 12;
    private static final int READV_IOV_MAX = (MAX_JAVA_ARRAY - READV_HEADER_LENGTH) / READV_ELEMENT_LENGTH;

    /**
     * Store file descriptors of open files.
     */
    private final List<FileDescriptor> _descriptors =
            Collections.synchronizedList(new ArrayList<>());

    /**
     * Use for timeout handling - a handler is always newly instantiated in
     * the Netty ChannelPipeline, so okay to store stateful information.
     */
    private boolean _hasOpenedFiles;
    /**
     * Address of the door. Enables us to redirect the client back if an
     * operation should better be performed at the door.
     */
    private InetSocketAddress _redirectingDoor;

    /**
     * The server on which this request handler is running.
     */
    private XrootdTransferService _server;

    /**
     * Maximum size of frame used for xrootd replies.
     */
    private final int _maxFrameSize;

    /**
     * Custom entries for kXR_Qconfig requests.
     */
    private final Map<String,String> _queryConfig;

    public XrootdPoolRequestHandler(XrootdTransferService server,
                                    int maxFrameSize,
                                    Map<String, String> queryConfig)
    {
        _server = server;
        _maxFrameSize = maxFrameSize;
        _queryConfig = queryConfig;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception
    {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state() == IdleState.ALL_IDLE) {
                if (!_hasOpenedFiles) {
                    _log.info("Closing idling connection without opened files.");
                    ctx.close();
                }
            }
        }
    }

    /**
     * @throws IOException closing the server socket that handles the
     *                     connection fails
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx)
            throws Exception
    {
        /* close leftover descriptors */
        for (FileDescriptor descriptor : _descriptors) {
            if (descriptor != null) {
                if (descriptor.isPersistOnSuccessfulClose()) {
                    descriptor.getChannel().release(new FileCorruptedCacheException(
                            "File was opened with Persist On Successful Close and not closed."));
                } else if (descriptor.getChannel().getIoMode().contains(StandardOpenOption.WRITE)) {
                    descriptor.getChannel().release(new CacheException(
                            "Client disconnected without closing file."));
                } else {
                    descriptor.getChannel().release();
                }
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t)
    {
        if (t instanceof ClosedChannelException) {
            _log.info("Connection {} unexpectedly closed.", ctx.channel());
        } else if (t instanceof Exception) {
            for (FileDescriptor descriptor : _descriptors) {
                if (descriptor != null) {
                    if (descriptor.isPersistOnSuccessfulClose()) {
                        descriptor.getChannel().release(new FileCorruptedCacheException(
                                "File was opened with Persist On Successful Close and client was disconnected due to an error: " +
                                t.getMessage(), t));
                    } else {
                        descriptor.getChannel().release(t);
                    }

                    if (descriptor instanceof TpcWriteDescriptor) {
                        ((TpcWriteDescriptor)descriptor).fireDelayedSync(kXR_error,
                                                                         t.getMessage());
                    }
                }
            }
            _descriptors.clear();
            ctx.close();
        } else {
            Thread me = Thread.currentThread();
            me.getUncaughtExceptionHandler().uncaughtException(me, t);
            ctx.close();
        }
    }

    @Override
    protected XrootdResponse<LoginRequest> doOnLogin(ChannelHandlerContext ctx, LoginRequest msg)
    {
        XrootdSessionIdentifier sessionId = new XrootdSessionIdentifier();
        /*
         * It is only necessary to tell the client to observe the unix protocol
         * if security is on and signed hashes are being enforced.
         *
         * We also need to swap the decoder.
         */
        String sec;

        if (signingPolicy.isSigningOn() && signingPolicy.isForceSigning()) {
            sec = "&P=unix";
            ctx.pipeline().addAfter("decoder",
                                        "sigverDecoder",
                                        new XrootdSigverDecoder(signingPolicy,
                                                                null));
            ctx.pipeline().remove("decoder");
            _log.debug("swapped decoder for sigverDecoder.");
        } else {
            sec = "";
        }

        return new LoginResponse(msg, sessionId, sec);
    }

    @Override
    protected XrootdResponse<AuthenticationRequest> doOnAuthentication(ChannelHandlerContext ctx,
                                                                       AuthenticationRequest msg)
    {
        return withOk(msg);
    }

    /**
     * Obtains the right mover channel using an opaque token in the
     * request. The mover channel is wrapped by a file descriptor. The
     * file descriptor is stored for subsequent access.
     *
     * In the case that this is a write request as destination in a third party
     * copy, a third-party client is started.  The client issues login, open and
     * read requests to the source server, and writes the responses
     * to the file descriptor.
     *
     * The third-party client also sends a sync response back to the client
     * when the transfer has completed.
     */
    @Override
    protected XrootdResponse<OpenRequest> doOnOpen(ChannelHandlerContext ctx,
                                                   OpenRequest msg)
        throws XrootdException
    {
        try {
            Map<String, String> opaqueMap = getOpaqueMap(msg.getOpaque());
            UUID uuid = getUuid(opaqueMap);
            if (uuid == null) {
                _log.info("Request to open {} contains no UUID.", msg.getPath());
                throw new XrootdException(kXR_NotAuthorized, "Request lacks the "
                                + UUID_PREFIX + " property.");
            }

            NettyTransferService<XrootdProtocolInfo>.NettyMoverChannel file
                            = _server.openFile(uuid, false);
            if (file == null) {
                _log.info("No mover found for {} with UUID {}.", msg.getPath(), uuid);
                return redirectToDoor(ctx, msg, () ->
                {
                    throw new XrootdException(kXR_NotAuthorized, UUID_PREFIX
                                    + " is no longer valid.");
                });
            }

            try {
                FileDescriptor descriptor;
                boolean isWrite = file.getIoMode().contains(StandardOpenOption.WRITE);
                if (msg.isNew() && !isWrite) {
                    throw new XrootdException(kXR_FileNotOpen, "File exists.");
                } else if (msg.isDelete() && !isWrite) {
                    throw new XrootdException(kXR_Unsupported, "File exists.");
                } else if ((msg.isNew() || msg.isReadWrite()) && isWrite) {
                    boolean posc = (msg.getOptions() & kXR_posc) == kXR_posc ||
                                    file.getProtocolInfo().getFlags()
                                        .contains(XrootdProtocolInfo.Flags.POSC);
                    if (opaqueMap.containsKey("tpc.src")) {
                        _log.debug("Request to open {} is as third-party destination.", msg);

                        XrootdTpcInfo tpcInfo = new XrootdTpcInfo(opaqueMap);
                        tpcInfo.setDelegatedProxy(file.getProtocolInfo().getDelegatedCredential());

                        descriptor = new TpcWriteDescriptor(file, posc, ctx,
                                                            _server,
                                                            opaqueMap.get("org.dcache.xrootd.client"),
                                                            tpcInfo);
                    } else {
                        descriptor = new WriteDescriptor(file, posc);
                    }
                } else {
                    descriptor = new ReadDescriptor(file);
                }

                FileStatus stat = msg.isRetStat() ? stat(file) : null;

                int fd = getUnusedFileDescriptor();
                _descriptors.set(fd, descriptor);

                _redirectingDoor = file.getProtocolInfo().getDoorAddress();
                file = null;
                _hasOpenedFiles = true;

                return new OpenResponse(msg, fd, null, null, stat);
            } finally {
                if (file != null) {
                    file.release();
                }
            }
        }  catch (IOException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        }
    }

    private UUID getUuid(Map<String, String> opaque)  throws XrootdException
    {
        String uuidString = opaque.get(XrootdProtocol.UUID_PREFIX);
        if (uuidString == null) {
            return null;
        }

        UUID uuid;
        try {
            uuid = UUID.fromString(uuidString);
        } catch (IllegalArgumentException e) {
            _log.warn("Failed to parse UUID {}: {}", opaque, e.getMessage());
            throw new XrootdException(kXR_ArgInvalid,
                                      "Cannot parse " + uuidString + ": " + e.getMessage());
        }
        return uuid;
    }

    private  Map<String,String> getOpaqueMap(String opaque) throws XrootdException
    {
        Map<String,String> map;
        try {
            map = OpaqueStringParser.getOpaqueMap(opaque);
        } catch (ParseException e) {
            _log.warn("Could not parse the opaque information {}: {}",
                      opaque, e.getMessage());
            throw new XrootdException(kXR_ArgInvalid,
                                      "Cannot parse opaque data: " + e.getMessage());
        }

        return map;
    }

    /**
     * In third-party requests where dCache is the destination, the client
     * may ask for a size update.  This can be provided by checking
     * channel size.
     *
     * Otherwise, stat is not supported on the pool and should be issued to the door.
     *
     * @param ctx Received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected XrootdResponse<StatRequest> doOnStat(ChannelHandlerContext ctx, StatRequest msg)
            throws XrootdException
    {
        switch (msg.getTarget()) {
        case PATH:
            _log.debug("Request to stat {}; redirecting to door.", msg);
            return redirectToDoor(ctx, msg);

        case FHANDLE:
            int fd = msg.getFhandle();

            if (!isValidFileDescriptor(fd)) {
                _log.warn("Could not find a file descriptor for handle {}", fd);
                throw new XrootdException(kXR_FileNotOpen,
                                          "The file handle does not refer to an open " +
                                          "file.");
            }

            FileDescriptor descriptor = _descriptors.get(fd);
            if (descriptor instanceof TpcWriteDescriptor) {
                _log.debug("Request to stat {} is for third-party transfer.", msg);
                return ((TpcWriteDescriptor)descriptor).handleStat(msg);
            } else {
                try {
                    _log.debug("Request to stat open file fhandle={}", fd);
                    return new StatResponse(msg, stat(descriptor.getChannel()));
                } catch (IOException e) {
                    throw new XrootdException(kXR_IOError, e.getMessage());
                }
            }

        default:
            throw new XrootdException(kXR_NotFile, "Unexpected stat target");
        }
    }

    @Override
    protected XrootdResponse<DirListRequest> doOnDirList(ChannelHandlerContext ctx, DirListRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, msg);
    }

    @Override
    protected XrootdResponse<MvRequest> doOnMv(ChannelHandlerContext ctx, MvRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, msg);
    }

    @Override
    protected XrootdResponse<RmRequest> doOnRm(ChannelHandlerContext ctx, RmRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, msg);
    }

    @Override
    protected XrootdResponse<RmDirRequest> doOnRmDir(ChannelHandlerContext ctx, RmDirRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, msg);
    }

    @Override
    protected XrootdResponse<MkDirRequest> doOnMkDir(ChannelHandlerContext ctx, MkDirRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, msg);
    }

    @Override
    protected XrootdResponse<StatxRequest> doOnStatx(ChannelHandlerContext ctx, StatxRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, msg);
    }

    private <R extends XrootdRequest> XrootdResponse<R> redirectToDoor(ChannelHandlerContext ctx, R msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, msg, () -> unsupported(ctx, msg));
    }

    private <R extends XrootdRequest> XrootdResponse<R> redirectToDoor(ChannelHandlerContext ctx, R msg, Callable<XrootdResponse<R>> onError)
        throws XrootdException
    {
        if (_redirectingDoor == null) {
            try {
                return onError.call();
            } catch (XrootdException e) {
                throw e;
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        } else {
            return new RedirectResponse<>(msg,
                                          _redirectingDoor.getHostName(),
                                          _redirectingDoor.getPort());
        }
    }


    /**
     * Lookup the file descriptor and obtain a Reader from it. The
     * Reader will be placed in a queue from which it is taken when
     * sending data to the client.
     *
     * @param ctx Received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected Object doOnRead(ChannelHandlerContext ctx, ReadRequest msg)
        throws XrootdException
    {
        int fd = msg.getFileHandle();

        if (!isValidFileDescriptor(fd)) {
            _log.warn("Could not find a file descriptor for handle {}", fd);
            throw new XrootdException(kXR_FileNotOpen,
                                      "The file handle does not refer to an open " +
                                      "file.");
        }

        if (msg.bytesToRead() == 0) {
            return withOk(msg);
        } else {
            return new ChunkedFileDescriptorReadResponse(msg, _maxFrameSize, _descriptors.get(fd));
        }
    }

    /**
     * Vector reads consist of several embedded read requests, which
     * can even contain different file handles. All the descriptors
     * for the file handles are looked up and passed to a vector
     * reader.
     *
     * @param ctx received from the netty pipeline
     * @param msg The actual request.
     */
    @Override
    protected Object doOnReadV(ChannelHandlerContext ctx, ReadVRequest msg)
        throws XrootdException
    {
        EmbeddedReadRequest[] list = msg.getReadRequestList();

        if (list == null || list.length == 0) {
            throw new XrootdException(kXR_ArgMissing, "Request contains no vector");
        }

        for (EmbeddedReadRequest req : list) {
            int fd = req.getFileHandle();

            if (!isValidFileDescriptor(fd)) {
                _log.warn("Could not find file descriptor for handle {}", fd);
                throw new XrootdException(kXR_FileNotOpen,
                                          "Descriptor for the embedded read request "
                                          + "does not refer to an open file.");
            }

            int totalBytesToRead = req.BytesToRead() +
                ReadVResponse.READ_LIST_HEADER_SIZE;

            if (totalBytesToRead > _maxFrameSize) {
                _log.warn("Vector read of {} bytes requested, exceeds " +
                          "maximum frame size of {} bytes!", totalBytesToRead,
                          _maxFrameSize);
                throw new XrootdException(kXR_ArgTooLong, "Single readv transfer is too large.");
            }
        }

        return new ChunkedFileDescriptorReadvResponse(msg, _maxFrameSize, new ArrayList<>(_descriptors));
    }

    /**
     * Lookup the file descriptor and delegate message processing to
     * it.
     *
     * @param ctx received from the netty pipeline
     * @param msg the actual request
     */
    @Override
    protected XrootdResponse<WriteRequest> doOnWrite(ChannelHandlerContext ctx, WriteRequest msg)
        throws XrootdException
    {
        int fd = msg.getFileHandle();

        if ((!isValidFileDescriptor(fd))) {
            _log.warn("No file descriptor for file handle {}", fd);
            throw new XrootdException(kXR_FileNotOpen,
                                      "The file descriptor does not refer to " +
                                      "an open file.");
        }

        FileDescriptor descriptor = _descriptors.get(fd);
        if (!(descriptor instanceof WriteDescriptor)) {
            _log.warn("File descriptor for handle {} is read-only, user " +
                              "tried to write.", fd);
            throw new XrootdException(kXR_FileNotOpen,
                                      "Tried to write on read only file.");
        }

        try {
            descriptor.write(msg);
        } catch (OutOfDiskException e) {
            throw new XrootdException(kXR_NoSpace, e.getMessage());
        } catch (ClosedChannelException e) {
            throw new XrootdException(kXR_FileNotOpen,
                    "The file was forcefully closed by the server.");
        } catch (IOException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        }
        return withOk(msg);
    }

    /**
     * Lookup the file descriptor and invoke its sync operation.
     *
     * @param ctx received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected XrootdResponse<SyncRequest> doOnSync(ChannelHandlerContext ctx, SyncRequest msg)
        throws XrootdException
    {
        int fd = msg.getFileHandle();

        if (!isValidFileDescriptor(fd)) {
            _log.warn("Could not find file descriptor for handle {}", fd);
            throw new XrootdException(kXR_FileNotOpen,
                                      "The file descriptor does not refer to an " +
                                      "open file.");
        }

        FileDescriptor descriptor = _descriptors.get(fd);

        try {
            return descriptor.sync(msg);
        } catch (ClosedChannelException e) {
            throw new XrootdException(kXR_FileNotOpen,
                    "The file was forcefully closed by the server.");
        } catch (IOException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        } catch (InterruptedException e) {
            throw new XrootdException(kXR_ServerError,
                                      "The server was interrupted; sync "
                                                      + "could not complete.");
        }
    }

    /**
     * Lookup the file descriptor and invoke its close operation.
     *
     * @param ctx received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected XrootdResponse<CloseRequest> doOnClose(ChannelHandlerContext ctx, CloseRequest msg)
        throws XrootdException
    {
        int fd = msg.getFileHandle();

        if (!isValidFileDescriptor(fd)) {
            _log.warn("Could not find file descriptor for handle {}", fd);
            throw new XrootdException(kXR_FileNotOpen,
                             "The file descriptor does not refer to an " +
                             "open file.");
        }

        ListenableFuture<Void> future = _descriptors.get(fd).getChannel().release();
        future.addListener(() -> {
            try {
                Uninterruptibles.getUninterruptibly(future);
                respond(ctx, withOk(msg));
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof CacheException) {
                    int rc = ((CacheException)cause).getRc();
                    respond(ctx, withError(msg,
                                           CacheExceptionMapper.xrootdErrorCode(rc),
                                           cause.getMessage()));
                } else if (cause instanceof IOException) {
                    respond(ctx, withError(msg, kXR_IOError, cause.getMessage()));
                } else {
                    respond(ctx, withError(msg, kXR_ServerError, cause.toString()));
                }
            } finally {
                _descriptors.set(fd, null);
            }
        }, MoreExecutors.directExecutor());

        return null;
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
                case "readv_ior_max":
                    s.append(_maxFrameSize - ReadVResponse.READ_LIST_HEADER_SIZE);
                    break;
                case "readv_iov_max":
                    s.append(READV_IOV_MAX);
                    break;
                case "version":
                    s.append("dCache ").append(Version.of(XrootdPoolRequestHandler.class).getVersion());
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
            String opaque = msg.getOpaque();
            if (opaque == null) {
                return redirectToDoor(ctx, msg);
            }
            UUID uuid = getUuid(getOpaqueMap(opaque));
            if (uuid == null) {
                /* The spec isn't clear about whether the path includes the opaque information or not.
                 * Thus we cannot rely on there being a uuid and without the uuid we cannot lookup the
                 * file attributes in the pool.
                 */
                return redirectToDoor(ctx, msg);
            }
            FileAttributes attributes = _server.getFileAttributes(uuid);
            if (attributes == null) {
                return redirectToDoor(ctx, msg);
            }
            if (attributes.isUndefined(FileAttribute.CHECKSUM)) {
                throw new XrootdException(kXR_Unsupported, "No checksum available for this file.");
            }
            return selectChecksum(new ChecksumInfo(msg.getPath(), opaque),
                                  attributes.getChecksums(),
                                  msg);
        default:
            return unsupported(ctx, msg);
        }
    }

    @Override
    protected Object doOnEndSession(ChannelHandlerContext ctx, EndSessionRequest request) throws XrootdException
    {
        return withOk(request);
    }

    /**
     * Gets the number of an unused file descriptor.
     * @return Number of an unused file descriptor.
     */
    private int getUnusedFileDescriptor()
    {
       for (int i = 0; i < _descriptors.size(); i++) {
           if (_descriptors.get(i) == null) {
               return i;
           }
       }

       _descriptors.add(null);
       return _descriptors.size() - 1;
    }

    /**
     * Test if the file descriptor actually refers to a file descriptor that
     * is contained in the descriptor list
     * @param fd file descriptor number
     * @return true, if the descriptor number refers to a descriptor in the
     *               list, false otherwise
     */
    private boolean isValidFileDescriptor(int fd)
    {
        return fd >= 0 && fd < _descriptors.size() &&
            _descriptors.get(fd) != null;
    }

    private FileStatus stat(RepositoryChannel file)
        throws IOException
    {
        return new FileStatus(DEFAULT_FILESTATUS_ID,
                              file.size(),
                              DEFAULT_FILESTATUS_FLAGS,
                              DEFAULT_FILESTATUS_MODTIME);
    }
}
