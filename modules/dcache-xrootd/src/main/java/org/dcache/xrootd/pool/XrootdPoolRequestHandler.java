package org.dcache.xrootd.pool;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.core.XrootdRequestHandler;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.protocol.messages.AuthenticationRequest;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.DirListRequest;
import org.dcache.xrootd.protocol.messages.GenericReadRequestMessage.EmbeddedReadRequest;
import org.dcache.xrootd.protocol.messages.LoginRequest;
import org.dcache.xrootd.protocol.messages.MkDirRequest;
import org.dcache.xrootd.protocol.messages.MvRequest;
import org.dcache.xrootd.protocol.messages.OpenRequest;
import org.dcache.xrootd.protocol.messages.OpenResponse;
import org.dcache.xrootd.protocol.messages.ProtocolRequest;
import org.dcache.xrootd.protocol.messages.ProtocolResponse;
import org.dcache.xrootd.protocol.messages.ReadRequest;
import org.dcache.xrootd.protocol.messages.ReadResponse;
import org.dcache.xrootd.protocol.messages.ReadVRequest;
import org.dcache.xrootd.protocol.messages.RedirectResponse;
import org.dcache.xrootd.protocol.messages.RmDirRequest;
import org.dcache.xrootd.protocol.messages.RmRequest;
import org.dcache.xrootd.protocol.messages.StatRequest;
import org.dcache.xrootd.protocol.messages.StatxRequest;
import org.dcache.xrootd.protocol.messages.SyncRequest;
import org.dcache.xrootd.protocol.messages.WriteRequest;
import org.dcache.xrootd.protocol.messages.XrootdRequest;
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
public class XrootdPoolRequestHandler extends XrootdRequestHandler
{
    private final static Logger _log =
        LoggerFactory.getLogger(XrootdPoolRequestHandler.class);

    private static final int DEFAULT_FILESTATUS_ID = 0;
    private static final int DEFAULT_FILESTATUS_FLAGS = 0;
    private static final int DEFAULT_FILESTATUS_MODTIME = 0;

    /**
     * Maximum frame size of a read or readv reply. Does not include the size
     * of the frame header.
     */
    private static final int MAX_FRAME_SIZE = 2 << 20;

    /**
     * Store file descriptors of open files.
     */
    private final List<FileDescriptor> _descriptors =
        new ArrayList<>();

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
    private XrootdPoolNettyServer _server;

    public XrootdPoolRequestHandler(XrootdPoolNettyServer server) {
        _server = server;
    }

    /**
     * @throws IOException opening a server socket to handle the connection
     *                     fails
     */
    @Override
    public void channelOpen(ChannelHandlerContext ctx,
                            ChannelStateEvent event)
        throws IOException
    {
        _server.clientConnected();
    }

    @Override
    public void channelIdle(ChannelHandlerContext ctx,
                            IdleStateEvent event)
    {
        if (event.getState() == IdleState.ALL_IDLE) {
            if (!_hasOpenedFiles) {

                if (_log.isInfoEnabled()) {
                    long idleTime = System.currentTimeMillis() -
                        event.getLastActivityTimeMillis();
                    _log.info("Closing idling connection without opened files." +
                              " Connection has been idle for {} ms.", idleTime);
                }

                ctx.getChannel().close();
            }
        }
    }

    /**
     * @throws IOException closing the server socket that handles the
     *                     connection fails
     */
    @Override
    public void channelClosed(ChannelHandlerContext ctx,
                              ChannelStateEvent event)
        throws IOException
    {
        /* close leftover descriptors */
        for (FileDescriptor descriptor : _descriptors) {
            if (descriptor != null) {
                _server.close(descriptor.getChannel());
            }
        }

        _server.clientDisconnected();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,
                                ExceptionEvent e)
    {
        Throwable t = e.getCause();
        if (t instanceof ClosedChannelException) {
            _log.info("Connection unexpectedly closed");
        } else if (t instanceof RuntimeException || t instanceof Error) {
            Thread me = Thread.currentThread();
            me.getUncaughtExceptionHandler().uncaughtException(me, t);
        } else {
            _log.warn(t.toString());
        }
        // TODO: If not already closed, we should probably close the
        // channel.
    }

    @Override
    protected AbstractResponseMessage
        doOnLogin(ChannelHandlerContext ctx, MessageEvent event, LoginRequest msg)
    {
        return withOk(msg);
    }

    @Override
    protected AbstractResponseMessage
        doOnAuthentication(ChannelHandlerContext ctx,
                           MessageEvent event,
                           AuthenticationRequest msg)
    {
        return withOk(msg);
    }

    /**
     * Obtains the right mover channel using an opaque token in the
     * request. The mover channel is wrapper by a file descriptor. The
     * file descriptor is stored for subsequent access.
     */
    @Override
    protected AbstractResponseMessage
        doOnOpen(ChannelHandlerContext ctx, MessageEvent event,
                 OpenRequest msg)
        throws XrootdException
    {
        try {
            Map<String,String> opaque;
            try {
                opaque = OpaqueStringParser.getOpaqueMap(msg.getOpaque());
            } catch (ParseException e) {
                _log.warn("Could not parse the opaque information in {}: {}",
                        msg, e.getMessage());
                throw new XrootdException(kXR_NotAuthorized,
                        "Cannot parse opaque data: " + e.getMessage());
            }

            String uuidString = opaque.get(XrootdProtocol.UUID_PREFIX);
            if (uuidString == null) {
                _log.warn("Request contains no UUID: {}", msg);
                throw new XrootdException(kXR_NotAuthorized,
                        XrootdProtocol.UUID_PREFIX + " is missing. Contact redirector to obtain a new UUID.");
            }

            UUID uuid;
            try {
                uuid = UUID.fromString(uuidString);
            } catch (IllegalArgumentException e) {
                _log.warn("Failed to parse UUID in {}: {}", msg, e.getMessage());
                throw new XrootdException(kXR_NotAuthorized,
                        "Cannot parse " + uuidString + ": " + e.getMessage());
            }

            MoverChannel<XrootdProtocolInfo> file = _server.open(uuid, false);
            if (file == null) {
                _log.warn("No mover found for {}", msg);
                throw new XrootdException(kXR_NotAuthorized,
                        "Request UUID is no longer valid. Contact redirector to obtain a new UUID.");
            }

            try {
                FileDescriptor descriptor;
                IoMode mode = file.getIoMode();
                if (msg.isNew() && mode != IoMode.WRITE) {
                    throw new XrootdException(kXR_ArgInvalid, "File exists");
                } else if (msg.isDelete() && mode != IoMode.WRITE) {
                    throw new XrootdException(kXR_Unsupported, "File exists");
                } else if ((msg.isNew() || msg.isReadWrite()) && mode == IoMode.WRITE) {
                    descriptor = new WriteDescriptor(file);
                } else {
                    descriptor = new ReadDescriptor(file);
                }

                FileStatus stat = msg.isRetStat() ? stat(file) : null;

                int fd = getUnusedFileDescriptor();
                _descriptors.set(fd, descriptor);

                _redirectingDoor = file.getProtocolInfo().getDoorAddress();
                file = null;
                _hasOpenedFiles = true;

                return new OpenResponse(msg,
                                        fd,
                                        null,
                                        null,
                                        stat);
            } finally {
                if (file != null) {
                    _server.close(file);
                }
            }
        }  catch (IOException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        }
    }

    /**
     * Not supported on the pool - should be issued to the door.
     * @param ctx Received from the netty pipeline
     * @param event Received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected Object
        doOnStat(ChannelHandlerContext ctx, MessageEvent event, StatRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    @Override
    protected Object
        doOnDirList(ChannelHandlerContext ctx, MessageEvent event, DirListRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    @Override
    protected Object
        doOnMv(ChannelHandlerContext ctx, MessageEvent event, MvRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    @Override
    protected Object
        doOnRm(ChannelHandlerContext ctx, MessageEvent event, RmRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    @Override
    protected Object
        doOnRmDir(ChannelHandlerContext ctx, MessageEvent event, RmDirRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    @Override
    protected Object
        doOnMkDir(ChannelHandlerContext ctx, MessageEvent event, MkDirRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    @Override
    protected Object
        doOnStatx(ChannelHandlerContext ctx, MessageEvent event, StatxRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    private Object
        redirectToDoor(ChannelHandlerContext ctx, MessageEvent event,
                       XrootdRequest msg)
        throws XrootdException
    {
        if (_redirectingDoor == null) {
            return unsupported(ctx, event, msg);
        } else {
            return new RedirectResponse(msg,
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
     * @param event Received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected Object
        doOnRead(ChannelHandlerContext ctx, MessageEvent event, ReadRequest msg)
        throws XrootdException
    {
        int fd = msg.getFileHandle();

        if (!isValidFileDescriptor(fd)) {
            _log.error("Could not find a file descriptor for handle {}", fd);
            throw new XrootdException(kXR_FileNotOpen,
                                      "The file handle does not refer to an open " +
                                      "file.");
        }

        if (msg.bytesToRead() == 0) {
            return withOk(msg);
        } else {
            return new ChunkedFileDescriptorReadResponse(msg, MAX_FRAME_SIZE, _descriptors.get(fd));
        }
    }

    /**
     * Vector reads consist of several embedded read requests, which
     * can even contain different file handles. All the descriptors
     * for the file handles are looked up and passed to a vector
     * reader.
     *
     * @param ctx received from the netty pipeline
     * @param event received from the netty pipeline
     * @param msg The actual request.
     */
    @Override
    protected Object
        doOnReadV(ChannelHandlerContext ctx, MessageEvent event, ReadVRequest msg)
        throws XrootdException
    {
        EmbeddedReadRequest[] list = msg.getReadRequestList();

        if (list == null || list.length == 0) {
            throw new XrootdException(kXR_ArgMissing, "Request contains no vector");
        }

        for (EmbeddedReadRequest req : list) {
            int fd = req.getFileHandle();

            if (!isValidFileDescriptor(fd)) {
                _log.error("Could not find file descriptor for handle {}!", fd);
                throw new XrootdException(kXR_FileNotOpen,
                                          "Descriptor for the embedded read request "
                                          + "does not refer to an open file.");
            }

            int totalBytesToRead = req.BytesToRead() +
                ReadResponse.READ_LIST_HEADER_SIZE;

            if (totalBytesToRead > MAX_FRAME_SIZE) {
                _log.warn("Vector read of {} bytes requested, exceeds " +
                          "maximum frame size of {} bytes!", totalBytesToRead,
                          MAX_FRAME_SIZE);
                throw new XrootdException(kXR_ArgInvalid, "Single readv transfer is too large");
            }
        }

        return new ChunkedFileDescriptorReadvResponse(msg, MAX_FRAME_SIZE, new ArrayList<>(_descriptors));
    }

    /**
     * Lookup the file descriptor and delegate message processing to
     * it.
     *
     * @param ctx received from the netty pipeline
     * @param event received from the netty pipeline
     * @param msg the actual request
     */
    @Override
    protected AbstractResponseMessage
        doOnWrite(ChannelHandlerContext ctx, MessageEvent event, WriteRequest msg)
        throws XrootdException
    {
        int fd = msg.getFileHandle();

        if ((!isValidFileDescriptor(fd))) {
            _log.info("No file descriptor for file handle {}", fd);
            throw new XrootdException(kXR_FileNotOpen,
                                      "The file descriptor does not refer to " +
                                      "an open file.");
        }

        FileDescriptor descriptor = _descriptors.get(fd);
        if (!(descriptor instanceof WriteDescriptor)) {
            _log.info("File descriptor for handle {} is read-only, user " +
                      "tried to write.", fd);
            throw new XrootdException(kXR_IOError,
                                      "Tried to write on read only file.");
        }

        try {
            descriptor.write(msg);
        } catch (ClosedChannelException e) {
            throw new XrootdException(kXR_FileNotOpen,
                    "The file was forcefully closed by the server");
        } catch (IOException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        }
        return withOk(msg);
    }

    /**
     * Lookup the file descriptor and invoke its sync operation.
     *
     * @param ctx received from the netty pipeline
     * @param event received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected AbstractResponseMessage
        doOnSync(ChannelHandlerContext ctx, MessageEvent event, SyncRequest msg)
        throws XrootdException
    {
        int fd = msg.getFileHandle();

        if (!isValidFileDescriptor(fd)) {
            _log.error("Could not find file descriptor for handle {}", fd);
            throw new XrootdException(kXR_FileNotOpen,
                                      "The file descriptor does not refer to an " +
                                      "open file.");
        }

        FileDescriptor descriptor = _descriptors.get(fd);
        try {
            descriptor.sync(msg);
        } catch (ClosedChannelException e) {
            throw new XrootdException(kXR_FileNotOpen,
                    "The file was forcefully closed by the server");
        } catch (IOException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        }
        return withOk(msg);
    }

    /**
     * Lookup the file descriptor and invoke its close operation.
     *
     * @param ctx received from the netty pipeline
     * @param event received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected AbstractResponseMessage
        doOnClose(ChannelHandlerContext ctx, MessageEvent event, CloseRequest msg)
        throws XrootdException
    {
        int fd = msg.getFileHandle();

        if (!isValidFileDescriptor(fd)) {
            _log.error("Could not find file descriptor for handle {}", fd);
            throw new XrootdException(kXR_FileNotOpen,
                             "The file descriptor does not refer to an " +
                             "open file.");
        }

        _server.close(_descriptors.set(fd, null).getChannel());
        return withOk(msg);
    }

    @Override
    protected AbstractResponseMessage
        doOnProtocolRequest(ChannelHandlerContext ctx,
                            MessageEvent event, ProtocolRequest msg)
        throws XrootdException
    {
        return new ProtocolResponse(msg, XrootdProtocol.DATA_SERVER);
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
