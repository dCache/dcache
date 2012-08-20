package org.dcache.xrootd2.pool;

import static org.dcache.xrootd2.protocol.XrootdProtocol.*;


import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import org.dcache.pool.movers.AbstractNettyServer;
import org.dcache.xrootd2.core.XrootdRequestHandler;
import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.dcache.xrootd2.protocol.messages.AbstractRequestMessage;
import org.dcache.xrootd2.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd2.protocol.messages.AuthenticationRequest;
import org.dcache.xrootd2.protocol.messages.CloseRequest;
import org.dcache.xrootd2.protocol.messages.DirListRequest;
import org.dcache.xrootd2.protocol.messages.GenericReadRequestMessage.EmbeddedReadRequest;
import org.dcache.xrootd2.protocol.messages.ErrorResponse;
import org.dcache.xrootd2.protocol.messages.LoginRequest;
import org.dcache.xrootd2.protocol.messages.MkDirRequest;
import org.dcache.xrootd2.protocol.messages.MvRequest;
import org.dcache.xrootd2.protocol.messages.OKResponse;
import org.dcache.xrootd2.protocol.messages.OpenRequest;
import org.dcache.xrootd2.protocol.messages.OpenResponse;
import org.dcache.xrootd2.protocol.messages.ProtocolRequest;
import org.dcache.xrootd2.protocol.messages.ReadRequest;
import org.dcache.xrootd2.protocol.messages.ReadResponse;
import org.dcache.xrootd2.protocol.messages.ReadVRequest;
import org.dcache.xrootd2.protocol.messages.RedirectResponse;
import org.dcache.xrootd2.protocol.messages.RmDirRequest;
import org.dcache.xrootd2.protocol.messages.RmRequest;
import org.dcache.xrootd2.protocol.messages.StatRequest;
import org.dcache.xrootd2.protocol.messages.StatxRequest;
import org.dcache.xrootd2.protocol.messages.SyncRequest;
import org.dcache.xrootd2.protocol.messages.WriteRequest;
import org.dcache.xrootd2.util.FileStatus;
import org.dcache.xrootd2.util.OpaqueStringParser;
import org.dcache.xrootd2.util.ParseException;
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
 * XrootdPoolRequestHandler is an xrootd request processor on the pool side -
 * it receives xrootd requests messages from the client. Upon an open request
 * it retrieves a file descriptor from the mover and passes all subsequent
 * client requests on to the file descriptor(s).
 *
 * The file descriptors are also used to propagate information (like
 * the time of the last transfer and the number of bytes that are transferred)
 * back to the mover.
 *
 * Still, the mover that is used for all the operations must be the same one
 * as started by the door while the client was redirected.
 * Extracting the right mover happens via maps.
 * On open, the mover can be extracted using an opaque UUID in the
 * request, that is also sent by the door when starting the mover.
 * On subsequent requests, all operations are performed using the file
 * descriptor(s) obtained upon open.
 *
 * Synchronisation is currently not ensured by the handler; it relies on the
 * synchronization by the underlying channel execution handler.
 */
public class XrootdPoolRequestHandler extends XrootdRequestHandler
{
    private final static Logger _log =
        LoggerFactory.getLogger(XrootdPoolRequestHandler.class);

    /**
     * Store file descriptors returned by mover.
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
    private AbstractNettyServer<XrootdProtocol_3> _server;

    public XrootdPoolRequestHandler(AbstractNettyServer<XrootdProtocol_3> server) {
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
                descriptor.close();
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
    protected void doOnLogin(ChannelHandlerContext ctx, MessageEvent event,
                             LoginRequest msg)
    {
        respond(ctx, event, new OKResponse(msg.getStreamID()));
    }

    @Override
    protected void doOnAuthentication(ChannelHandlerContext ctx,
                                      MessageEvent event,
                                      AuthenticationRequest msg)
    {
        respond(ctx, event, new OKResponse(msg.getStreamID()));
    }

    /**
     * Obtains the right mover instance using an opaque token in the
     * request and instruct the mover to open the file in the request.
     * Associates the mover with the file-handle that is produced during
     * processing
     */
    @Override
    protected void doOnOpen(ChannelHandlerContext ctx, MessageEvent event,
                            OpenRequest msg)
    {
        try {
            Map<String,String> opaque;
            try {
                opaque = OpaqueStringParser.getOpaqueMap(msg.getOpaque());
            } catch (ParseException e) {
                _log.warn("Could not parse the opaque information in {}: {}",
                        msg, e.getMessage());
                respondWithError(ctx, event, msg, kXR_NotAuthorized,
                                 "Cannot parse opaque data: " + e.getMessage());
                return;
            }

            String uuidString = opaque.get(XrootdProtocol.UUID_PREFIX);
            if (uuidString == null) {
                _log.warn("Request contains no UUID: {}", msg);
                respondWithError(ctx, event, msg, kXR_NotAuthorized,
                                 UUID_PREFIX + " is missing. Contact redirector to obtain a new UUID.");
                return;
            }

            UUID uuid;
            try {
                uuid = UUID.fromString(uuidString);
            } catch (IllegalArgumentException e) {
                _log.warn("Failed to parse UUID in {}: {}", msg, e.getMessage());
                respondWithError(ctx, event, msg, kXR_NotAuthorized,
                                 "Cannot parse " + uuidString + ": " + e.getMessage());
                return;
            }

            XrootdProtocol_3 mover = _server.getMover(uuid);
            if (mover == null) {
                _log.warn("No mover found for {}",
                           msg);
                respondWithError(ctx, event, msg, kXR_NotAuthorized,
                                 "Request UUID is no longer valid. Contact redirector to obtain a new UUID.");
                return;
            }

            FileDescriptor descriptor = null;

            try {
                descriptor = mover.open(msg);
                FileStatus stat = null;

                if (msg.isRetStat()) {
                    stat = mover.stat();
                }

                int fd = getUnusedFileDescriptor();
                _descriptors.set(fd, descriptor);

                respond(ctx, event,
                        new OpenResponse(msg.getStreamID(),
                                         fd,
                                         null,
                                         null,
                                         stat));
                descriptor = null;
                _hasOpenedFiles = true;
                _redirectingDoor = mover.getDoorAddress();
            } finally {
                if (descriptor != null) {
                    descriptor.close();
                }
            }
        }  catch (IOException ioex) {
            respondWithError(ctx, event, msg, kXR_IOError, ioex.getMessage());
        } catch (RuntimeException rtex) {
            _log.error("open failed due to a bug", rtex);
            respondWithError(ctx, event, msg,
                             kXR_ServerError,
                             String.format("Internal server error (%s)",
                                           rtex.getMessage()));
        }
    }

    /**
     * Not supported on the pool - should be issued to the door.
     * @param ctx Received from the netty pipeline
     * @param event Received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected void doOnStat(ChannelHandlerContext ctx, MessageEvent event,
                            StatRequest msg)
    {
        redirectToDoor(ctx, event, msg);

    }

    @Override
    protected void doOnDirList(ChannelHandlerContext ctx, MessageEvent event,
                               DirListRequest msg)
    {
        redirectToDoor(ctx, event, msg);
    }

    @Override
    protected void doOnMv(ChannelHandlerContext ctx, MessageEvent event,
                          MvRequest msg)
    {
        redirectToDoor(ctx, event, msg);
    }

    @Override
    protected void doOnRm(ChannelHandlerContext ctx, MessageEvent event,
                          RmRequest msg)
    {
        redirectToDoor(ctx, event, msg);
    }

    @Override
    protected void doOnRmDir(ChannelHandlerContext ctx, MessageEvent event,
                          RmDirRequest msg)
    {
        redirectToDoor(ctx, event, msg);
    }

    @Override
    protected void doOnMkDir(ChannelHandlerContext ctx, MessageEvent event,
                             MkDirRequest msg)
    {
        redirectToDoor(ctx, event, msg);
    }

    private void redirectToDoor(ChannelHandlerContext ctx, MessageEvent event,
                                AbstractRequestMessage msg) {
        if (_redirectingDoor == null) {
            unsupported(ctx, event, msg);
        } else {
            respond(ctx, event,
                    new RedirectResponse(msg.getStreamID(),
                                         _redirectingDoor.getHostName(),
                                         _redirectingDoor.getPort()));
        }
    }


    @Override
    protected void doOnStatx(ChannelHandlerContext ctx, MessageEvent event,
                             StatxRequest msg)
    {
        redirectToDoor(ctx, event, msg);
    }

    /**
     * Use the file descriptor retrieved from the mover upon open and let it
     * obtain a reader object on the pool. The reader object will be placed
     * in a queue, from which it can be taken when sending read information
     * to the client.
     * @param ctx Received from the netty pipeline
     * @param event Received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected void doOnRead(ChannelHandlerContext ctx, MessageEvent event,
                            ReadRequest msg)
    {
        try {
            int fd = msg.getFileHandle();

            if (!isValidFileDescriptor(fd)) {
                _log.error("Could not find a file descriptor for handle {}", fd);
                respondWithError(ctx, event, msg, kXR_FileNotOpen,
                                 "The file handle does not refer to an open " +
                                 "file.");
                return;
            }

            ReadDescriptor descriptor = (ReadDescriptor) _descriptors.get(fd);

            try {
                _readers.add(descriptor.read(msg));
            } catch (IllegalStateException ise) {
                _log.error("File with file descriptor {}: Could not be read", fd);
                respondWithError(ctx, event, msg, kXR_ServerError,
                                 "Descriptor error. File reported not open, even " +
                                 "though it should be.");
                return;
            }

            sendToClient(event.getChannel());
        } catch (RuntimeException rtex) {
            _log.error("Read failed due to a bug", rtex);
            respondWithError(ctx, event, msg, kXR_ServerError,
                             String.format("Internal server error (%s)",
                                           rtex.getMessage()));
        }
    }

    /**
     * Vector reads consist of several embedded read requests, which can even
     * contain different file handles. All the descriptors for the file
     * handles are looked up and passed to a vector reader. The vector reader
     * will use the descriptors connection to the mover that "owns" them to
     * update the mover's meta-information such as the number of bytes
     * transferred or the time of the last update.
     *
     * @param ctx received from the netty pipeline
     * @param event received from the netty pipeline
     * @param msg The actual request.
     */
    @Override
    protected void doOnReadV(ChannelHandlerContext ctx, MessageEvent event,
                             ReadVRequest msg)
    {
        try {
            EmbeddedReadRequest[] list = msg.getReadRequestList();

            if (list == null || list.length == 0) {
                respondWithError(ctx, event, msg, kXR_ArgMissing,
                                 "Request contains no vector");
                return;
            }

            int maxFrameSize = XrootdProtocol_3.getMaxFrameSize();

            for (EmbeddedReadRequest req : list) {
                int fd = req.getFileHandle();

                if (!isValidFileDescriptor(fd)) {
                    _log.error("Could not find file descriptor for handle {}!", fd);
                    respondWithError(ctx, event, msg, kXR_FileNotOpen,
                                     "Descriptor for the embedded read request "
                                     + "does not refer to an open file.");
                    return;
                }

                int totalBytesToRead = req.BytesToRead() +
                    ReadResponse.READ_LIST_HEADER_SIZE;

                if (totalBytesToRead > maxFrameSize) {
                    _log.warn("Vector read of {} bytes requested, exceeds " +
                              "maximum frame size of {} bytes!", totalBytesToRead,
                              maxFrameSize);
                    respondWithError(ctx, event, msg, kXR_ArgInvalid,
                                     "Single readv transfer is too large");
                    return;
                }
            }

            _readers.add(new VectorReader(msg.getStreamID(),
                                          Lists.newArrayList(_descriptors),
                                          list));
            sendToClient(event.getChannel());
        } catch (RuntimeException rtex) {
            _log.error("Vector-read failed due to a bug", rtex);
            respondWithError(ctx, event, msg, kXR_ServerError,
                             String.format("Internal server error (%s)",
                                           rtex.getMessage()));
        }
    }

    /**
     * Retrieves the file descriptor obtained upon open and invokes
     * its write operation. The file descriptor will propagate necessary
     * function calls to the mover.
     *
     * @param ctx received from the netty pipeline
     * @param event received from the netty pipeline
     * @param msg the actual request
     */
    @Override
    protected void doOnWrite(ChannelHandlerContext ctx, MessageEvent event,
                             WriteRequest msg)
    {
        try {
            int fd = msg.getFileHandle();

            if ((!isValidFileDescriptor(fd))) {
                _log.info("No file descriptor for file handle {}", fd);
                respondWithError(ctx, event, msg, kXR_FileNotOpen,
                               "The file descriptor does not refer to " +
                               "an open file.");
                return;
            }

            FileDescriptor descriptor = _descriptors.get(fd);

            if (!(descriptor instanceof WriteDescriptor)) {
                respondWithError(ctx, event, msg, kXR_IOError,
                                 "Tried to write on read only file.");
                _log.info("File descriptor for handle {} is read-only, user " +
                          "to write.", fd);
                return;
            }

            try {
                descriptor.write(msg);
                respond(ctx, event, new OKResponse(msg.getStreamID()));
            } catch (ClosedChannelException e) {
                respondWithError(ctx, event, msg, kXR_FileNotOpen,
                                 "The file was forcefully closed by the server");
            } catch (IOException e) {
                respondWithError(ctx, event, msg, kXR_IOError, e.getMessage());
            }
        } catch (RuntimeException rtex) {
                _log.error("Write failed due to a bug", rtex);
                respondWithError(ctx, event, msg, kXR_ServerError,
                                 String.format("Internal server error (%s)",
                                               rtex.getMessage()));
        }
    }



    /**
     * Retrieves the right mover based on the request's file-handle and
     * invokes its sync-operation.
     *
     * @param ctx received from the netty pipeline
     * @param event received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected void doOnSync(ChannelHandlerContext ctx, MessageEvent event,
                            SyncRequest msg)
    {
        try {
            int fd = msg.getFileHandle();

            if (!isValidFileDescriptor(fd)) {
                _log.error("Could not find file descriptor for handle {}", fd);
                respondWithError(ctx, event, msg, kXR_FileNotOpen,
                                 "The file descriptor does not refer to an " +
                                 "open file.");
                return;
            }

            FileDescriptor descriptor = _descriptors.get(fd);

            try {
                descriptor.sync(msg);
                respond(ctx, event, new OKResponse(msg.getStreamID()));
            } catch (ClosedChannelException e) {
                respondWithError(ctx, event, msg, kXR_FileNotOpen,
                                 "The file was forcefully closed by the server");
            } catch (IOException isex) {
                respondWithError(ctx, event, msg, kXR_IOError, isex.getMessage());
            }
        } catch (RuntimeException rtex) {
            _log.error("Sync failed due to a bug", rtex);
            respondWithError(ctx, event, msg, kXR_ServerError,
                             String.format("Internal server error (%s)",
                                           rtex.getMessage()));
        }
    }

    /**
     * Retrieves the right descriptor based on the request's file-handle and
     * invokes its close information.
     *
     * @param ctx received from the netty pipeline
     * @param event received from the netty pipeline
     * @param msg The actual request
     */
    @Override
    protected void doOnClose(ChannelHandlerContext ctx, MessageEvent event,
                             CloseRequest msg)
    {
        int fd = msg.getFileHandle();

        if (!isValidFileDescriptor(fd)) {
            _log.error("Could not find file descriptor for handle {}", fd);
            respondWithError(ctx, event, msg, kXR_FileNotOpen,
                             "The file descriptor does not refer to an " +
                             "open file.");
            return;
        }

        try {
            _descriptors.set(fd, null).close();
            respond(ctx, event, new OKResponse(msg.getStreamID()));
        } catch (IllegalStateException isex) {
            respondWithError(ctx, event, msg, kXR_IOError, isex.getMessage());
        } catch (RuntimeException rtex) {
            _log.error("Close failed due to a bug", rtex);
            respondWithError(ctx, event, msg, kXR_ServerError,
                             String.format("Internal server error (%s)",
                                           rtex.getMessage()));
        }
    }

    @Override
    protected void doOnProtocolRequest(ChannelHandlerContext ctx,
                                       MessageEvent event, ProtocolRequest msg)
    {
        redirectToDoor(ctx, event, msg);
    }

    /**
     * Reads a full response message from the reader returned by invoking read
     * or readV on the pool. The full response message will also contain
     * callbacks that allow to update the information about the transferred
     * bytes and the last transfer time on the mover.
     * @return Response message with mover callbacks.
     */
    private AbstractResponseMessage readBlock(Channel channel) {
        try {
            while (_readers.peek() != null) {
                Reader reader = _readers.element();
                AbstractResponseMessage block =
                    reader.read(XrootdProtocol_3.getMaxFrameSize());
                if (block != null) {
                    return block;
                }
                _readers.remove();
            }
            return null;
        } catch (ClosedChannelException e) {
            Reader reader = _readers.remove();
            return new ErrorResponse(
                    reader.getStreamID(),
                    kXR_FileNotOpen,
                    "File was forcefully closed by the server");
        } catch (IOException e) {
            Reader reader = _readers.remove();
            return new ErrorResponse(reader.getStreamID(),
                                     kXR_IOError,
                                     (e.getMessage() == null)
                                     ? e.toString()
                                     : e.getMessage());
        }
    }

    /**
     * Sends the next read-ahead response message to the client, or reads the
     * next response message. Updates the last-transferred time on the mover.
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
}
