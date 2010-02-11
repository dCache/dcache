package org.dcache.xrootd2.pool;

import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.net.SocketException;
import java.util.List;
import java.util.Queue;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.repository.Allocator;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.vehicles.XrootdDoorAdressInfoMessage;
import org.dcache.util.PortRange;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.movers.NetIFContainer;

import static org.dcache.xrootd2.protocol.XrootdProtocol.*;
import org.dcache.xrootd2.core.XrootdEncoder;
import org.dcache.xrootd2.core.XrootdDecoder;
import org.dcache.xrootd2.core.XrootdHandshakeHandler;
import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.dcache.xrootd2.protocol.messages.*;
import org.dcache.xrootd2.core.XrootdRequestHandler;
import org.dcache.xrootd2.util.FileStatus;

import static org.jboss.netty.channel.Channels.*;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import org.apache.log4j.Logger;
import org.dcache.util.NetworkUtils;


/**
 * Netty based XROOTD data server disguised as a dCache mover.
 *
 * All mover instances use a shared Netty ChannelFactory and shared
 * thread pools. We add a ServerChannel per mover. The ServerChannel
 * listens on a previously unused port. This is the port send back to
 * the door. Upon the first connect on this port, a HangupHandler will
 * close the ServerChannel to prevent further connects on the same
 * port.
 *
 * The mover instance doubles as an XrootdRequestHandler and is part
 * of the ChannelPipeline for the client Channel. It handles all
 * Xrootd requests from the client. This part of the mover is single
 * threaded and we use an OrderedMemoryAwareThreadPoolExecutor to
 * ensure that Xrootd messages are processed in order. Xrootd request
 * processing may block on disk IO or on space allocation.
 *
 * The mover thread (the thread calling runIO) is inactive during the
 * transfer. It blocks on a latch opened by the HangupHandler once the
 * client channel closes.
 *
 * There are essentially two ways a mover can shut down.
 *
 * * If the client disconnects, then the HangupHandler will open the
 *   latch and the mover thread will wake up. The HangupHandler is not
 *   served by the same thread pool and the Xrootd requests. Hence the
 *   HangupHandler is served even if the XrootdRequestHandler is
 *   blocked on IO or space allocation. The mover thread will return
 *   and dCache will close the RandomAccessFile and the
 *   Allocator. Closing those will cause any threads stuck in IO or
 *   space allocation to break out.
 *
 * * If the mover is killed, then the mover thread will close the
 *   ServerChannel and the client channel. dCache will close the
 *   RandomAccessFile and the Allocator causing any blocked threads to
 *   break out.
 *
 *
 * Open issues:
 *
 * * Write calls blocked on space allocation may starve read
 *   calls. This is because both are served by the same thread
 *   pool. This should be changed such that separate thread pools are
 *   used (may fix later).
 *
 * * We use a port per connection; ideally the door would provide the
 *   client with a token which the client then has to present to the
 *   pool. The pool could then use that to match the request to the
 *   door. This is supported by the Xrootd protoocol, however the
 *   xrdcp client contains a bug in the code for parsing the
 *   token. Hence we cannot rely on it at the moment. Instead we may
 *   use the open request itself to match the request to the
 *   door. This would work as the door doesn't maintain state after it
 *   has started the mover (fix later).
 *
 * * Due to the one-port-connection design, the Channel open and close
 *   logic is messy and hard to follow. Will be resolved when we move
 *   away from this design (fix later).
 *
 * * Write messages are currently processed as one big message. If the
 *   client chooses to upload a huge file as a single write message,
 *   then the pool will run out of memory. We can fix this by breaking
 *   a write message into many small blocks. The old mover suffers
 *   from the same problem (may fix later).
 *
 * * Vector read breaks the architecture: The idea was that IO
 *   operations are delegated to a FileDescriptor, however vector
 *   reads can access multiple files. Therefore one cannot delegate it
 *   to a single FileDescriptor. The current implementation is
 *   therefore a hack and must be cleaned up.
 *
 * * At least for vector read, the behaviour when reading beyond the
 *   end of the file is wrong.
 */
@ChannelPipelineCoverage("one")
public class XrootdProtocol_3
    extends XrootdRequestHandler
    implements MoverProtocol
{
    private static final int DEFAULT_DISK_THREAD_POOL_SIZE = 20;
    private static final int DEFAULT_SOCKET_THREAD_POOL_SIZE = 5;
    private static final int DEFAULT_PER_CHANNEL_LIMIT = 16 * (1 << 20);
    private static final int DEFAULT_TOTAL_LIMIT = 64 * (1 << 20);
    private static final int[] DEFAULT_PORTRANGE = { 20000, 25000 };

    /**
     * Timeout in milliseconds for establishing a connection with the
     * client. When an xrootd client is redirected to a pool, we
     * expect the client to connect to the pool within this amount of
     * time.
     */
    private static final long CONNECT_TIMEOUT = 300000; // 5 min

    /**
     * The minimum number of bytes to increment the space
     * allocation.
     */
    private static final long SPACE_INC = 50 * (1 << 20);

    private static final Logger _log = Logger.getLogger(XrootdProtocol_3.class);

    private static final Logger _logSpaceAllocation =
        Logger.getLogger("logger.dev.org.dcache.poolspacemonitor." +
                         XrootdProtocol_3.class.getName());

    /**
     * Maximum frame size of a read or readv reply. Does not include
     * the size of the frame header.
     */
    private static int _maxFrameSize = 2 << 20;

    /**
     * Shared thread pool accepting TCP connections.
     */
    private static Executor _acceptExecutor;

    /**
     * Shared thread pool performing non-blocking socket IO.
     */
    private static Executor _socketExecutor;

    /**
     * Shared thread pool performing blocking disk IO.
     */
    private static Executor _diskExecutor;

    /**
     * Shared Netty channel factory.
     */
    private static ChannelFactory _channelFactory;

    /**
     * Communication endpoint.
     */
    private final CellEndpoint _endpoint;

    /**
     * Queue of read requests.
     */
    private final Queue<Reader> _readers = new ArrayDeque();

    /**
     * Files opened by the client.
     */
    private final List<FileDescriptor> _descriptors = new ArrayList();

    /**
     * Used to signal when the client TCP connection has been closed.
     */
    private final CountDownLatch _closeLatch = new CountDownLatch(1);

    /**
     * Hangup handler signals closure of the client connection.
     */
    private final HangupHandler _hangupHandler = new HangupHandler(_closeLatch);

    /**
     * Next response message used during read operations. Provides a
     * simplistic read-ahead buffer.
     */
    private AbstractResponseMessage _block;

    /**
     * The file served by this mover.
     */
    private RandomAccessFile _file;

    /**
     * Protocol specific information provided by the door.
     */
    private XrootdProtocolInfo _protocolInfo;

    /**
     * Space allocator provided by the pool.
     */
    private Allocator _allocator;

    /**
     * The number of bytes reserved in the space allocator.
     */
    protected long _reservedSpace;

    /**
     * True while the transfer is in progress. The field is only ever
     * updated from a single thread, but may be read from multiple
     * threads.
     */
    private volatile boolean _inProgress = false;

    /**
     * True if the transfer any data. The field is only ever updated
     * from a single thread, but may be read from multiple threads.
     */
    private volatile boolean _wasChanged = false;

    /**
     * Timestamp of when the transfer started. The field is only ever
     * updated from a single thread, but may be read from multiple
     * threads.
     */
    private volatile long _transferStarted;

    /**
     * Timestamp of when the last block was transferred. The field is
     * only ever updated from a single thread, but may be read from
     * multiple threads.
     */
    private volatile long _lastTransferred;

    /**
     * The number of bytes transferred. The field is only ever updated
     * from a single thread, but may be read from multiple threads.
     */
    private volatile long _bytesTransferred;

    /**
     * The Netty Channel to the client.  The field is only ever
     * updated from a single thread, but may be read from multiple
     * threads.
     */
    private volatile Channel _clientChannel;

    /**
     * Switch Netty to log4j for logging. Should be moved somewhere
     * else.
     */
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());
    }

    /* Initialises the shared resources like a thread pool and the
     * Netty ChannelFactory.
     */
    private static synchronized void
        initSharedResources(CellEndpoint endpoint)
    {
        /* The disk executor handles the Xrootd request
         * processing. This boils down to reading and writing from
         * disk.
         */
        if (_diskExecutor == null) {
            int threads;
            String s = endpoint.getArgs().getOpt("xrootd-mover-disk-threads");
            if (s != null && !s.isEmpty()) {
                threads = Integer.parseInt(s);
            } else {
                threads = DEFAULT_DISK_THREAD_POOL_SIZE;
            }

            int perChannelLimit;
            s = endpoint.getArgs().getOpt("xrootd-mover-max-memory-per-connection");
            if (s != null && !s.isEmpty()) {
                perChannelLimit = Integer.parseInt(s);
            } else {
                perChannelLimit = DEFAULT_PER_CHANNEL_LIMIT;
            }

            int totalLimit;
            s = endpoint.getArgs().getOpt("xrootd-mover-max-memory");
            if (s != null && !s.isEmpty()) {
                totalLimit = Integer.parseInt(s);
            } else {
                totalLimit = DEFAULT_TOTAL_LIMIT;
            }

            s = endpoint.getArgs().getOpt("xrootd-mover-max-channel-memory");

            _diskExecutor =
                new OrderedMemoryAwareThreadPoolExecutor(threads,
                                                         perChannelLimit,
                                                         totalLimit);

            s = endpoint.getArgs().getOpt("xrootd-mover-max-frame-size");
            if (s != null && !s.isEmpty()) {
                _maxFrameSize = Integer.parseInt(s);
            }

        }

        /* The accept executor is used for accepting TCP
         * connections. An accept task will be submitted per server
         * socket.
         */
        if (_acceptExecutor == null) {
            _acceptExecutor = Executors.newCachedThreadPool();
        }

        /* The socket executor handles socket IO. As netty performs
         * all socket IO asynchronously, a small thread pool ought to
         * be enough.
         */
        if (_socketExecutor == null) {
            int threads;
            String s = endpoint.getArgs().getOpt("xrootd-mover-socket-threads");
            if (s != null && !s.isEmpty()) {
                threads = Integer.parseInt(s);
            } else {
                threads = DEFAULT_SOCKET_THREAD_POOL_SIZE;
            }

            _socketExecutor = Executors.newFixedThreadPool(threads);
        }

        if (_channelFactory == null) {
            _channelFactory =
                new NioServerSocketChannelFactory(_acceptExecutor,
                                                  _socketExecutor);
        }
    }

    public XrootdProtocol_3(CellEndpoint endpoint)
    {
        _endpoint = endpoint;
        initSharedResources(_endpoint);
    }

    @Override
    public void runIO(RandomAccessFile diskFile,
                      ProtocolInfo protocol,
                      StorageInfo storage,
                      PnfsId pnfsId,
                      Allocator allocator,
                      int access)
        throws Exception
    {
        _file = diskFile;
        _protocolInfo = (XrootdProtocolInfo) protocol;
        _allocator = allocator;

	_transferStarted  = System.currentTimeMillis();
        _lastTransferred = _transferStarted;

        _log.debug("Starting xrootd server");
        Channel channel = startServer();
        try {
            _inProgress = true;

            InetSocketAddress address =
                (InetSocketAddress) channel.getLocalAddress();
            sendAddressToDoor(address.getPort());

            _log.debug("Waiting for server shutdown");

            /* When an xrootd client is redirected to a pool, we
             * expect the client to connect within a reasonable amount
             * of time. Otherwise we kill the mover.
             */
            if (!_closeLatch.await(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS)) {
                if (_clientChannel == null) {
                    throw new TimeoutCacheException("No connection from Xrootd client after " +
                                                    TimeUnit.MILLISECONDS.toSeconds(CONNECT_TIMEOUT) + " seconds. Giving up.");
                }
                _closeLatch.await();
            }
        } finally {
            _log.debug("Server is down");
            _inProgress = false;
            channel.close();

            Channel client = _clientChannel;
            if (client != null && client.isOpen()) {
                client.close();
            }
        }

        if (!isTransferSuccessful(access)) {
            _log.warn("Xrootd transfer failed");
            throw new CacheException("xrootd transfer failed");
        }

        _log.debug("Xrootd transfer completed");
    }

    /**
     * Starts the Xrootd server on a fresh port.
     */
    private Channel startServer()
        throws IOException
    {
        String portRange = System.getProperty("org.dcache.net.tcp.portrange");
        PortRange range;
        if (portRange != null) {
            range = PortRange.valueOf(portRange);
        } else {
            range = new PortRange(DEFAULT_PORTRANGE[0], DEFAULT_PORTRANGE[1]);
        }

        ServerBootstrap bootstrap = new ServerBootstrap(_channelFactory);
        bootstrap.setOption("child.tcpNoDelay", false);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                public ChannelPipeline getPipeline()
                {
                    ChannelPipeline pipeline = pipeline();
                    pipeline.addLast("encoder", new XrootdEncoder());
                    pipeline.addLast("decoder", new XrootdDecoder());
                    if (_log.isDebugEnabled()) {
                        pipeline.addLast("logger", new LoggingHandler(XrootdProtocol_3.class));
                    }
                    pipeline.addLast("handshake", new XrootdHandshakeHandler(XrootdProtocol.DATA_SERVER));
                    pipeline.addLast("hangup", _hangupHandler);
                    pipeline.addLast("executor", new ExecutionHandler(_diskExecutor));
                    pipeline.addLast("transfer", XrootdProtocol_3.this);
                    return pipeline;
                }
            });

        return range.bind(bootstrap);
    }

    /**
     * Sends our address to the door. Copied from old xrootd mover.
     */
    private void sendAddressToDoor(int port)
        throws UnknownHostException, SocketException,
               CacheException, NoRouteToCellException
    {
        Collection netifsCol = new ArrayList();

        // try to pick the ip address with corresponds to the
        // hostname (which is hopefully visible to the world)
        InetAddress localIP = NetworkUtils.getLocalAddressForClient(_protocolInfo.getHosts());

        if (localIP != null && !localIP.isLoopbackAddress()
            && localIP instanceof Inet4Address) {
            // the ip we got from the hostname is at least not
            // 127.0.01 and from the IP4-family
            Collection col = new ArrayList(1);
            col.add(localIP);
            netifsCol.add(new NetIFContainer("", col));
            _log.debug("sending ip-address derived from hostname to Xrootd-door: "+localIP+" port: "+port);
        } else {
            // the ip we got from the hostname seems to be bad,
            // let's loop through the network interfaces
            Enumeration ifList = NetworkInterface.getNetworkInterfaces();

            while (ifList.hasMoreElements()) {
                NetworkInterface netif =
                    (NetworkInterface) ifList.nextElement();

                Enumeration ips = netif.getInetAddresses();
                Collection ipsCol = new ArrayList(2);

                while (ips.hasMoreElements()) {
                    InetAddress addr = (InetAddress) ips.nextElement();

                    // check again each ip from each interface.
                    // WARNING: multiple ip addresses in case of
                    // multiple ifs could be selected, we can't
                    // determine the "correct" one
                    if (addr instanceof Inet4Address
                        && !addr.isLoopbackAddress()) {
                        ipsCol.add(addr);
                        _log.debug("sending ip-address derived from network-if to Xrootd-door: "+addr+" port: "+port);
                    }
                }

                if (ipsCol.size() > 0) {
                    netifsCol.add(new NetIFContainer(netif.getName(), ipsCol));
                } else {
                    throw new CacheException("Error: Cannot determine my ip address. Aborting transfer");
                }
            }
        }

        //
        // send message back to the door, containing the new
        // serverport and ip
        //
        CellPath cellpath = _protocolInfo.getXrootdDoorCellPath();
        XrootdDoorAdressInfoMessage doorMsg =
            new XrootdDoorAdressInfoMessage(_protocolInfo.getXrootdFileHandle(),
                                            port, netifsCol);
        _endpoint.sendMessage (new CellMessage(cellpath, doorMsg));

        _log.debug("sending redirect message to Xrootd-door "+ cellpath);
    }

    @Override
    public void setAttribute(String name, Object attribute)
    {
    }

    @Override
    public Object getAttribute(String name)
    {
        throw new IllegalArgumentException("Couldn't find " + name);
    }

    @Override
    public long getBytesTransferred()
    {
        return _bytesTransferred;
    }

    @Override
    public long getTransferTime()
    {
        return
            (_inProgress ? System.currentTimeMillis() : _lastTransferred)
            - _transferStarted;
    }

    @Override
    public long getLastTransferred()
    {
        return _lastTransferred;
    }

    @Override
    public boolean wasChanged()
    {
        return _wasChanged;
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx,
                            ChannelStateEvent event)
        throws Exception
    {
        _clientChannel = ctx.getChannel();
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx,
                              ChannelStateEvent event)
        throws Exception
    {
        _clientChannel = null;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event)
    {
        _lastTransferred = System.currentTimeMillis();
        super.messageReceived(ctx, event);
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
            _log.warn(t);
        }
        // TODO: If not already closed, we should probably close the
        // channel.
    }

    @Override
    public void channelInterestChanged(ChannelHandlerContext ctx,
                                       ChannelStateEvent e)
    {
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

    @Override
    protected void doOnOpen(ChannelHandlerContext ctx, MessageEvent event,
                            OpenRequest msg)
    {
        try {
            /* The OpenRequest has to be identical to the one issued to
             * the door.
             */
            if (_protocolInfo.getChecksum() != msg.calcChecksum()) {
                _log.error("OpenRequest checksums do not match");
                closeWithError(ctx, event, msg,
                               kXR_ArgInvalid,
                               "OpenRequest different from the one the redirector got");
                return;
            }

            int fd = getUnusedFileDescriptor();
            FileDescriptor handler = null;
            if (msg.isNew() || msg.isReadWrite()) {
                handler = new WriteDescriptor(_file);
            } else {
                handler = new ReadDescriptor(_file);
            }

            _descriptors.set(fd, handler);

            FileStatus stat = null;
            if (msg.isRetStat()) {
                stat = new FileStatus(0, _file.length(), 0, 0);
            }

            respond(ctx, event,
                    new OpenResponse(msg.getStreamID(), fd, null, null, stat));
        } catch (IOException e) {
            respondWithError(ctx, event, msg, kXR_IOError, e.getMessage());
        }
    }

    @Override
    protected void doOnStat(ChannelHandlerContext ctx, MessageEvent event,
                            StatRequest msg)
    {
        try {
            String path = msg.getPath();
            long size = _file.length();
            FileStatus stat =
                _protocolInfo.getPath().equals(path)
                ? new FileStatus(0, size, 0, 0)
                : FileStatus.FILE_NOT_FOUND;
            respond(ctx, event, new StatResponse(msg.getStreamID(), stat));
        } catch (IOException e) {
            respondWithError(ctx, event, msg, kXR_IOError, e.getMessage());
        }
    }

    @Override
    protected void doOnStatx(ChannelHandlerContext ctx, MessageEvent e,
                             StatxRequest msg)
    {
        unsupported(ctx, e, msg);
    }

    @Override
    protected void doOnRead(ChannelHandlerContext ctx, MessageEvent event,
                            ReadRequest msg)
    {
        int fd = msg.getFileHandle();

        if (!isValidFileDescriptor(fd)) {
            respondWithError(ctx, event, msg, kXR_FileNotOpen,
                             "Invalid file handle");
            return;
        }

        _readers.add(_descriptors.get(fd).read(msg));
        sendToClient(event.getChannel());
    }

    @Override
    protected void doOnReadV(ChannelHandlerContext ctx, MessageEvent event,
                             ReadVRequest msg)
    {
        GenericReadRequestMessage.EmbeddedReadRequest[] list =
            msg.getReadRequestList();

        if (list == null || list.length == 0) {
            respondWithError(ctx, event, msg, kXR_ArgMissing,
                             "Request contains no vector");
            return;
        }

        for (GenericReadRequestMessage.EmbeddedReadRequest req: list) {
            if (!isValidFileDescriptor(req.getFileHandle())) {
                respondWithError(ctx, event, msg, kXR_FileNotOpen,
                                 "Invalid file handle");
                return;
            }

            if (req.BytesToRead() + ReadResponse.READ_LIST_HEADER_SIZE > _maxFrameSize) {
                respondWithError(ctx, event, msg, kXR_NoMemory,
                                 "Single readv transfer is too large");
                return;
            }
        }

        _readers.add(new VectorReader(msg.getStreamID(), _descriptors, list));

        sendToClient(event.getChannel());
    }

    @Override
    protected void doOnWrite(ChannelHandlerContext ctx, MessageEvent event,
                             WriteRequest msg)
    {
        try {
            int fd = msg.getFileHandle();

            if (!isValidFileDescriptor(fd)) {
                respondWithError(ctx, event, msg, kXR_FileNotOpen,
                                 "Invalid file handle");
                return;
            }

            _bytesTransferred += msg.getDataLength();
            _wasChanged = true;

            /* Not ellegant to check the type of the file descriptor,
             * but we need to test this before allocating
             * space. Eventually this should be done by the
             * descriptor.
             */
            FileDescriptor descriptor = _descriptors.get(fd);
            if (!(descriptor instanceof WriteDescriptor)) {
                throw new IOException("File is read only");
            }

            preallocate(msg.getWriteOffset() + msg.getDataLength());
            descriptor.write(msg);

            respond(ctx, event, new OKResponse(msg.getStreamID()));
        } catch (InterruptedException e) {
            respondWithError(ctx, event, msg, kXR_ServerError,
                             "Server shutdown");
        } catch (IOException e) {
            respondWithError(ctx, event, msg, kXR_IOError, e.getMessage());
        }
    }

    @Override
    protected void doOnSync(ChannelHandlerContext ctx, MessageEvent event,
                            SyncRequest msg)
    {
        try {
            int fd = msg.getFileHandle();

            if (!isValidFileDescriptor(fd)) {
                respondWithError(ctx, event, msg, kXR_FileNotOpen,
                                 "Invalid file handle");
                return;
            }

            _descriptors.get(fd).sync(msg);
            respond(ctx, event, new OKResponse(msg.getStreamID()));
        } catch (IOException e) {
            respondWithError(ctx, event, msg, kXR_IOError, e.getMessage());
        }
    }

    @Override
    protected void doOnClose(ChannelHandlerContext ctx, MessageEvent event,
                             CloseRequest msg)
    {
        int fd = msg.getFileHandle();

        if (!isValidFileDescriptor(fd)) {
            respondWithError(ctx, event, msg, kXR_FileNotOpen,
                             "Invalid file handle");
            return;
        }

        _descriptors.set(fd, null).close();

        respond(ctx, event, new OKResponse(msg.getStreamID()));
    }

    @Override
    protected void doOnProtocolRequest(ChannelHandlerContext ctx, MessageEvent e, ProtocolRequest msg)
    {
        unsupported(ctx, e, msg);
    }


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

    private boolean isValidFileDescriptor(int fd)
    {
        return fd >= 0 && fd < _descriptors.size() &&
            _descriptors.get(fd) != null;
    }

    private AbstractResponseMessage readBlock()
    {
        try {
            while (_readers.peek() != null) {
                ReadResponse block = _readers.element().read(_maxFrameSize);
                if (block != null) {
                    _bytesTransferred += block.getDataLength();
                    return block;
                }
                _readers.remove();
            }
            return null;
        } catch (IOException e) {
            Reader reader = _readers.remove();
            return new ErrorResponse(reader.getStreamID(),
                                     kXR_IOError, e.getMessage());
        }
    }

    private void sendToClient(Channel channel)
    {
        if (_block == null) {
            _block = readBlock();
        }

        while (_block != null && channel.isWritable()) {
            _lastTransferred = System.currentTimeMillis();
            write(channel, _block);
            _block = readBlock();
        }
    }

    /**
     * We consider a transfer successful if all open files where
     * properly closed and at least one file was opened.
     */
    private boolean isTransferSuccessful(int access)
    {
        boolean isRead = (access & MoverProtocol.WRITE) == 0;

        for (FileDescriptor descriptor: _descriptors) {
            if (descriptor != null) {
                return false;
            }
        }

        return !_descriptors.isEmpty() || isRead;
    }

    /**
     * Ensures that we have allocated space up to the given position
     * in the file. May block if we run out of space.
     */
    private void preallocate(long position) throws InterruptedException
    {
        if (position < 0)
            throw new IllegalArgumentException("Position must be positive");

	if (position > _reservedSpace) {
	    long additional = Math.max(position - _reservedSpace, SPACE_INC);
            _logSpaceAllocation.debug("ALLOC: " + additional );
            _allocator.allocate(additional);
	    _reservedSpace += additional;
	}
    }
}