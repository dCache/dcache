package org.dcache.xrootd.pool;

import static org.dcache.xrootd.protocol.XrootdProtocol.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.core.XrootdRequestHandler;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.XrootdRequest;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.protocol.messages.AuthenticationRequest;
import org.dcache.xrootd.protocol.messages.CloseRequest;
import org.dcache.xrootd.protocol.messages.DirListRequest;
import org.dcache.xrootd.protocol.messages.GenericReadRequestMessage.EmbeddedReadRequest;
import org.dcache.xrootd.protocol.messages.ErrorResponse;
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
import org.dcache.xrootd.util.FileStatus;
import org.dcache.xrootd.util.OpaqueStringParser;
import org.dcache.xrootd.util.ParseException;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        new ArrayList<FileDescriptor>();

    /** Used for reading pool information, passing it on to the client */
    private final Queue<Reader> _readers = new ArrayDeque<Reader>();

    /** Simplistic read ahead buffer.
     */
    private AbstractResponseMessage _block;

    /**
     * Use for timeout handling - a handler is always newly instantiated in
     * the Netty ChannelPipeline, so okay to store stateful information.
     */
    private boolean _hasOpenedFiles = false;
    /**
     * Address of the door. Enables us to redirect the client back if an
     * operation should better be performed at the door.
     */
    private InetSocketAddress _redirectingDoor = null;

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
        _readers.clear();
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
    public void channelInterestChanged(ChannelHandlerContext ctx,
                                       ChannelStateEvent e)
    {
        /* push out the next block */
       sendToClient(e.getChannel());
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
            String opaque = msg.getOpaque();
            Map<String, String> opaqueMap;
            UUID uuid;

            _log.debug("Received the following opaque information " +
                       "from the client: {}", opaque);

            try {
                opaqueMap = OpaqueStringParser.getOpaqueMap(opaque);
                String uuidString = opaqueMap.get(XrootdProtocol.UUID_PREFIX);
                uuid = UUID.fromString(uuidString);
            } catch (ParseException e) {
                _log.error("Could not parse the opaque information from the " +
                           "request. Need opaque UUID to open file: {}",
                           e);
                throw new XrootdException(kXR_NotAuthorized, "Invalid client redirect.");
            } catch (IllegalArgumentException e) {
                _log.error("Could not construct the required UUID from the " +
                           "UUID string: {}", e);
                throw new XrootdException(kXR_NotAuthorized, "Invalid client redirect.");
            }

            MoverChannel<XrootdProtocolInfo> file = _server.open(uuid, false);
            if (file == null) {
                _log.error("Could not find a file for UUID {}", uuid);
                throw new XrootdException(kXR_NotAuthorized, "Request is not/no longer valid.");
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

                return new OpenResponse(msg.getStreamId(),
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
    protected AbstractResponseMessage
        doOnStat(ChannelHandlerContext ctx, MessageEvent event, StatRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    @Override
    protected AbstractResponseMessage
        doOnDirList(ChannelHandlerContext ctx, MessageEvent event, DirListRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    @Override
    protected AbstractResponseMessage
        doOnMv(ChannelHandlerContext ctx, MessageEvent event, MvRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    @Override
    protected AbstractResponseMessage
        doOnRm(ChannelHandlerContext ctx, MessageEvent event, RmRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    @Override
    protected AbstractResponseMessage
        doOnRmDir(ChannelHandlerContext ctx, MessageEvent event, RmDirRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    @Override
    protected AbstractResponseMessage
        doOnMkDir(ChannelHandlerContext ctx, MessageEvent event, MkDirRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    @Override
    protected AbstractResponseMessage
        doOnStatx(ChannelHandlerContext ctx, MessageEvent event, StatxRequest msg)
        throws XrootdException
    {
        return redirectToDoor(ctx, event, msg);
    }

    private AbstractResponseMessage
        redirectToDoor(ChannelHandlerContext ctx, MessageEvent event,
                       XrootdRequest msg)
        throws XrootdException
    {
        if (_redirectingDoor == null) {
            return unsupported(ctx, event, msg);
        } else {
            return new RedirectResponse(msg.getStreamId(),
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
    protected AbstractResponseMessage
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

        ReadDescriptor descriptor = (ReadDescriptor) _descriptors.get(fd);

        try {
            _readers.add(descriptor.read(msg));
        } catch (IllegalStateException e) {
            _log.error("File with file descriptor {}: Could not be read", fd);
            throw new XrootdException(kXR_ServerError,
                                      "Descriptor error. File reported not open, even " +
                                      "though it should be.");
        }

        sendToClient(event.getChannel());
        return null;
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
    protected AbstractResponseMessage
        doOnReadV(ChannelHandlerContext ctx, MessageEvent event, ReadVRequest msg)
        throws XrootdException
    {
        EmbeddedReadRequest[] list = msg.getReadRequestList();

        if (list == null || list.length == 0) {
            throw new XrootdException(kXR_ArgMissing, "Request contains no vector");
        }

        List<FileDescriptor> vectorDescriptors =
            new ArrayList<FileDescriptor>();

        for (EmbeddedReadRequest req : list) {
            int fd = req.getFileHandle();

            if (!isValidFileDescriptor(fd)) {
                _log.error("Could not find file descriptor for handle {}!", fd);
                throw new XrootdException(kXR_FileNotOpen,
                                          "Descriptor for the embedded read request "
                                          + "does not refer to an open file.");
            }

            FileDescriptor descriptor = _descriptors.get(fd);

            int totalBytesToRead = req.BytesToRead() +
                ReadResponse.READ_LIST_HEADER_SIZE;

            if (totalBytesToRead > MAX_FRAME_SIZE) {
                _log.warn("Vector read of {} bytes requested, exceeds " +
                          "maximum frame size of {} bytes!", totalBytesToRead,
                          MAX_FRAME_SIZE);
                throw new XrootdException(kXR_ArgInvalid, "Single readv transfer is too large");
            }

            vectorDescriptors.add(fd, descriptor);
        }

        _readers.add(new VectorReader(msg.getStreamId(),
                                      vectorDescriptors,
                                      list));
        sendToClient(event.getChannel());
        return null;
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
                      "to write.", fd);
            throw new XrootdException(kXR_IOError,
                                      "Tried to write on read only file.");
        }

        try {
            descriptor.write(msg);
        } catch (InterruptedException e) {
            /* may also happen if client disconnects during space
             * allocation. However, trying to report that to a disconnected
             * client is of limited use.
             */
            throw new XrootdException(kXR_ServerError, "Server timeout/shutdown");
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
        } catch (IOException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        } catch (IllegalStateException e) {
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

        try {
            _server.close(_descriptors.set(fd, null).getChannel());
        } catch (IllegalStateException e) {
            throw new XrootdException(kXR_IOError, e.getMessage());
        }
        return withOk(msg);
    }

    @Override
    protected AbstractResponseMessage
        doOnProtocolRequest(ChannelHandlerContext ctx,
                            MessageEvent event, ProtocolRequest msg)
        throws XrootdException
    {
        return new ProtocolResponse(msg.getStreamId(), XrootdProtocol.DATA_SERVER);
    }

    /**
     * Reads a full response message from the Reader returned by
     * invoking read or readV on the descriptor.
     *
     * @return Response to read request
     */
    private AbstractResponseMessage readBlock(Channel channel)
    {
        try {
            while (_readers.peek() != null) {
                Reader reader = _readers.element();
                AbstractResponseMessage block =
                    reader.read(MAX_FRAME_SIZE);
                if (block != null) {
                    return block;
                }
                _readers.remove();
            }
            return null;
        }  catch (IOException e) {
            Reader reader = _readers.remove();
            return new ErrorResponse(reader.getStreamID(),
                                     kXR_IOError,
                                     (e.getMessage() == null)
                                     ? e.toString()
                                     : e.getMessage());
        }
    }

    /**
     * Sends the next read-ahead response message to the client, or
     * reads the next response message.
     *
     * @param channel Channel to the client.
     */
    private void sendToClient(Channel channel)
    {
        if (_block == null) {
            _block = readBlock(channel);
        }

        while (_block != null && channel.isWritable()) {
            channel.write(_block);
            _block =  readBlock(channel);
        }
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

    private FileStatus stat(MoverChannel<XrootdProtocolInfo> file)
        throws IOException
    {
        return new FileStatus(DEFAULT_FILESTATUS_ID,
                              file.size(),
                              DEFAULT_FILESTATUS_FLAGS,
                              DEFAULT_FILESTATUS_MODTIME);
    }
}
