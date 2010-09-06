package org.dcache.xrootd2.pool;

import java.io.RandomAccessFile;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.net.SocketException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.repository.Allocator;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.vehicles.XrootdDoorAdressInfoMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.movers.NetIFContainer;

import org.dcache.xrootd2.protocol.messages.*;
import org.dcache.xrootd2.util.FileStatus;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.util.NetworkUtils;
import org.dcache.util.PortRange;


/**
 * xrootd mover.
 *
 * The xrootd mover contains a static method for constructing a netty server
 * that will listen for incoming connections on the xrootd port. The purpose
 * of the server is to relay xrootd requests on the pool to the right mover.
 * The first mover that is started by the door will start the netty server
 * that is handling the pool requests; if there are no client connections to
 * the server left and no mover is still active, the server is shut down again,
 * thus unblocking the port.
 *
 * The mover responsible for a client connection is selected based on an opaque
 * UUID included by the client in redirect from the door.
 *
 * Once the mover is invoked, it will wait (on a count down latch)
 * for incoming requests from the started netty server. If the no open
 * operation is performed within CONNECT_TIMEOUT, the thread will be killed
 * by a TimeoutCacheException.
 *
 * The mover thread (the thread calling runIO) is inactive during the
 * transfer. The close operation will count down the latch on which it waits
 * and terminate the mover. The mover thread will return and dCache will close
 * the RandomAccessFile and the Allocator. Closing those will cause any
 * threads stuck in IO or space allocation to break out.
 *
 * If the mover is killed, then the intertransfer-timeout will eventually
 * unblock the latch, which will remove the mover from the map
 * (the intertransfer-timeout-handler is served from a different thread pool
 * than the normal mover). The netty handler will return an error to the
 * client during the next operation and the client will disconnect, which will
 * shut down the server, once there are no active client connections left.
 *
 * A transfer is considered to have succeeded if at least one file was
 * opened and all opened files were closed again.
 *
 * Open issues:
 *
 * * Write calls blocked on space allocation may starve read
 *   calls. This is because both are served by the same thread
 *   pool. This should be changed such that separate thread pools are
 *   used (may fix later).
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

public class XrootdProtocol_3
    implements MoverProtocol
{
    private static final long DEFAULT_CLIENT_IDLE_TIMEOUT =
        TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

    private static final int DEFAULT_DISK_THREAD_POOL_SIZE = 20;
    private static final int DEFAULT_PER_CHANNEL_LIMIT = 16 * (1 << 20);
    private static final int DEFAULT_TOTAL_LIMIT = 64 * (1 << 20);
    private static final int DEFAULT_FILESTATUS_ID = 0;
    private static final int DEFAULT_FILESTATUS_FLAGS = 0;
    private static final int DEFAULT_FILESTATUS_MODTIME = 0;

    private static final int[] DEFAULT_PORTRANGE = { 20000, 25000 };

    /**
     * If the client connection dies without closing the file, we will have
     * a dangling mover. The pool request handler will notice that a client
     * connection is gone (channelClosed), but will not know which mover to
     * kill. Therefore a thread should check in regular intervals whether a
     * transfer happened within a certain time interval and if not, shut down
     * the mover. We can conveniently use the _lastTransferred property for
     * that.
     */
    private static final long CONNECT_TIMEOUT =
        TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

    /**
     * The minimum number of bytes to increment the space
     * allocation.
     */
    private static final long SPACE_INC = 50 * (1 << 20);

    private static final Logger _log =
        LoggerFactory.getLogger(XrootdProtocol_3.class);

    /**
     * Maximum frame size of a read or readv reply. Does not include
     * the size of the frame header.
     */
    private static int _maxFrameSize = 2 << 20;

    private static final Logger _logSpaceAllocation =
        LoggerFactory.getLogger("logger.dev.org.dcache.poolspacemonitor." +
                         XrootdProtocol_3.class.getName());

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
     * Used to generate channel-idle events for the pool handler
     */
    private static Timer _timer;
    private static long _clientIdleTimeout;

    /**
     * Shared Netty channel factory.
     */
    private static ChannelFactory _channelFactory;

    /**
     * Shared Netty server channel
     */
    private static Channel _serverChannel;

    /**
     * mapping from tokens (opaque information) to movers
     */
    private static final Map<UUID, XrootdProtocol_3> _moversPerToken =
        new ConcurrentHashMap<UUID, XrootdProtocol_3>();

    /**
     * Communication endpoint.
     */
    private final CellEndpoint _endpoint;

    /**
     * Used to signal when the client TCP connection has been closed.
     */
    private final CountDownLatch _closeLatch = new CountDownLatch(1);

    /**
     * The file served by this mover.
     */
    private RandomAccessFile _file;

    /**
     * Protocol specific information provided by the door.
     */
    private XrootdProtocolInfo _protocolInfo;
    private volatile InetSocketAddress _doorAddress;

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
     * Tells the mover that a descriptor was actually opened upon termination.
     * This field is only ever updated from a single thread, but may be read
     * from multiple threads.
     */
    private volatile boolean _hasOpenedDescriptor = false;

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
     * Timestamp of when the last block was transferred - also updated by
     * any thread invoking the callback.
     */
    private AtomicLong _lastTransferred = new AtomicLong();

    /**
     * The number of bytes transferred - also updated by any thread invoking
     * the callback.
     */
    private AtomicLong _bytesTransferred = new AtomicLong();

    /**
     * Keep track of opened descriptors
     */
    private Set<FileDescriptor> _openedDescriptors =
        Collections.synchronizedSet(new HashSet<FileDescriptor>());

    /**
     * If both the number of client connections as well as waiting movers are
     * 0, we can shut down the xrootd server. Use this variable to synchronize
     * that. The only accessor to this variable is synchronized.
     */
    private static int _endpoints = 0;

    /**
     * Switch Netty to slf4j for logging. Should be moved somewhere
     * else.
     */
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
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

        /* The socket executor handles socket IO. Netty submits a
         * number of workers to this executor and each worker is
         * assigned a share of the connections.
         */
        if (_socketExecutor == null) {
            _socketExecutor = Executors.newCachedThreadPool();
        }

        if (_channelFactory == null) {
            String s = endpoint.getArgs().getOpt("xrootd-mover-socket-threads");
            if (s != null && !s.isEmpty()) {
                _channelFactory =
                    new NioServerSocketChannelFactory(_acceptExecutor,
                                                      _socketExecutor,
                                                      Integer.parseInt(s));
            } else {
                _channelFactory =
                    new NioServerSocketChannelFactory(_acceptExecutor,
                                                      _socketExecutor);
            }
        }

        if (_timer == null) {
            String s = endpoint.getArgs().getOpt("xrootd-mover-idle-client-timeout");

            if (s != null && !s.isEmpty()) {

                try {
                    _clientIdleTimeout = Long.parseLong(s);
                } catch (NumberFormatException nfex) {
                    _log.warn("xrootd-mover-client-idle-timeout contains invalid " +
                              "value: {}", nfex);
                    _clientIdleTimeout = DEFAULT_CLIENT_IDLE_TIMEOUT;
                }
            } else {
                _clientIdleTimeout = DEFAULT_CLIENT_IDLE_TIMEOUT;
            }

            _timer = new HashedWheelTimer();
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
        _protocolInfo = (XrootdProtocolInfo) protocol;
        _doorAddress = _protocolInfo.getDoorAddress();

        UUID uuid = _protocolInfo.getUUID();
        _log.debug("Received opaque information {}", uuid);
        _moversPerToken.put(uuid, this);

        boolean transferSuccess;
        incrementEndpointsToggleServer();

        try {
            _file = diskFile;
            _allocator = allocator;
            _transferStarted  = System.currentTimeMillis();
            _lastTransferred.set(_transferStarted);

            InetSocketAddress address =
                (InetSocketAddress) _serverChannel.getLocalAddress();
            sendAddressToDoor(address.getPort());

            _log.debug("Starting xrootd server");

            _inProgress = true;

            if (!_closeLatch.await(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS)) {
                if (!_hasOpenedDescriptor) {
                    throw new TimeoutCacheException("No connection from Xrootd client after " +
                                                    TimeUnit.MILLISECONDS.toSeconds(CONNECT_TIMEOUT) +
                                                    " seconds. Giving up.");
                }

                _closeLatch.await();
            }
        } finally {
            _moversPerToken.remove(uuid);
            /* this effectively closes all file descriptors obtained via this
             * mover */
            _file = null;
            _allocator = null;
            _inProgress = false;

            transferSuccess = isTransferSuccessful(access);

            _openedDescriptors.clear();
            decrementEndpointsToggleServer();
        }

       if (!transferSuccess) {
            _log.warn("Xrootd transfer failed");
            throw new CacheException("xrootd transfer failed");
       }

        _log.debug("Xrootd transfer completed, transferred {} bytes.",
                   _bytesTransferred.get());
    }

    /**
     * Binds a new netty server to the xrootd port. This netty server will
     * translate xrootd protocol requests to mover operations.
     */
    private static void bindServerChannel() throws IOException
    {
        String portRange = System.getProperty("org.dcache.net.tcp.portrange");
        PortRange range;
        if (portRange != null) {
            range = PortRange.valueOf(portRange);
        } else {
            range = new PortRange(DEFAULT_PORTRANGE[0], DEFAULT_PORTRANGE[1]);
        }

        _log.info("Binding a new server channel.");
        ServerBootstrap bootstrap = new ServerBootstrap(_channelFactory);
        bootstrap.setOption("child.tcpNoDelay", false);
        bootstrap.setOption("child.keepAlive", true);

        bootstrap.setPipelineFactory(new XrootdPoolPipelineFactory(_timer,
                                                                   _diskExecutor,
                                                                   _clientIdleTimeout));

        _serverChannel= range.bind(bootstrap);
    }

    /**
     * Sends our address to the door. Copied from old xrootd mover.
     */
    private void sendAddressToDoor(int port)
        throws UnknownHostException, SocketException,
               CacheException, NoRouteToCellException
    {
        Collection<NetIFContainer> netifsCol = new ArrayList<NetIFContainer>();

        // try to pick the ip address with corresponds to the
        // hostname (which is hopefully visible to the world)
        InetAddress localIP =
            NetworkUtils.getLocalAddressForClient(_protocolInfo.getHosts());

        if (localIP != null && !localIP.isLoopbackAddress()
            && localIP instanceof Inet4Address) {
            // the ip we got from the hostname is at least not
            // 127.0.01 and from the IP4-family
            Collection<InetAddress> col = new ArrayList<InetAddress>(1);
            col.add(localIP);
            netifsCol.add(new NetIFContainer("", col));
            _log.debug("sending ip-address derived from hostname " +
                       "to Xrootd-door: "+localIP+" port: "+port);
        } else {
            // the ip we got from the hostname seems to be bad,
            // let's loop through the network interfaces
            Enumeration<NetworkInterface> ifList =
                NetworkInterface.getNetworkInterfaces();

            while (ifList.hasMoreElements()) {
                NetworkInterface netif =
                    ifList.nextElement();

                Enumeration<InetAddress> ips = netif.getInetAddresses();
                Collection<InetAddress> ipsCol = new ArrayList<InetAddress>(2);

                while (ips.hasMoreElements()) {
                    InetAddress addr = ips.nextElement();

                    // check again each ip from each interface.
                    // WARNING: multiple ip addresses in case of
                    // multiple ifs could be selected, we can't
                    // determine the "correct" one
                    if (addr instanceof Inet4Address
                        && !addr.isLoopbackAddress()) {
                        ipsCol.add(addr);
                        _log.debug("sending ip-address derived from " +
                                   "network-if to Xrootd-door: "+addr+
                                   " port: "+port);
                    }
                }

                if (ipsCol.size() > 0) {
                    netifsCol.add(new NetIFContainer(netif.getName(), ipsCol));
                }
            }

            if (netifsCol.isEmpty()) {
                throw new CacheException("Error: Cannot determine my ip" +
                                         "address. Aborting transfer");
            }
        }

        //
        // send message back to the door, containing the new
        // serverport and ip
        //
        CellPath cellpath = _protocolInfo.getXrootdDoorCellPath();
        boolean uuidEnabledPool = true;
        XrootdDoorAdressInfoMessage doorMsg =
            new XrootdDoorAdressInfoMessage(_protocolInfo.getXrootdFileHandle(),
                                            port, netifsCol, uuidEnabledPool);
        _endpoint.sendMessage (new CellMessage(cellpath, doorMsg));

        _log.debug("sending redirect message to Xrootd-door "+ cellpath);
    }

    @Override
    public void setAttribute(String name, Object attribute)
    {
        /* currently not implemented */
    }

    @Override
    public Object getAttribute(String name)
    {
        throw new IllegalArgumentException("Couldn't find " + name);
    }

    @Override
    public long getBytesTransferred()
    {
        return _bytesTransferred.get();
    }

    @Override
    public long getTransferTime()
    {
        return
            (_inProgress ? System.currentTimeMillis() : _lastTransferred.get())
            - _transferStarted;
    }

    @Override
    public long getLastTransferred()
    {
        return _lastTransferred.get();
    }

    @Override
    public boolean wasChanged()
    {
        return _wasChanged;
    }

    RandomAccessFile getFile() {
        return _file;
    }

    static XrootdProtocol_3 getMover(UUID uuid)
    {
        return _moversPerToken.get(uuid);
    }

    /**
     * Retrieve a new file descriptor from this mover.
     * @param msg Request as received by the netty server
     * @return the number of the file-descriptor
     * @throws IOException
     */
    synchronized FileDescriptor open(OpenRequest msg) throws IOException
    {
        /* mover is killed between map lookup and open operation */
        if (!_inProgress) {
            throw new IOException("Open request failed, please retry.");
        }

        _lastTransferred.set(System.currentTimeMillis());
        _hasOpenedDescriptor = true;

        FileDescriptor handler;

        if (msg.isNew() || msg.isReadWrite()) {
            handler = new WriteDescriptor(this);
        } else {
            handler = new ReadDescriptor(this);
        }

        _openedDescriptors.add(handler);
        return handler;
    }

    /**
     * Closes the file descriptor encapsulated in the request. If this is the
     * last file descriptor of this mover, the mover will also be shut down.
     *
     * @param msg Request as received by the netty server
     * @throws IllegalStateException File descriptor is not open.
     * @throws IOException File descriptor is not valid.
     */
    synchronized void close(FileDescriptor descriptor)
    {
        _lastTransferred.set(System.currentTimeMillis());
        _openedDescriptors.remove(descriptor);

        /* shutdown the mover if all descriptors were removed. Should not
         * race against adding in open due to the happens-before relationship
         * of synchronized collections. */
        if (_openedDescriptors.isEmpty()) {
            _inProgress = false;
            _closeLatch.countDown();
        }
    }

    /**
     * Return the default file status
     * @return FileStatus object
     * @throws IOException
     */
    FileStatus stat() throws IOException
    {
        _lastTransferred.set(System.currentTimeMillis());
        return new FileStatus(DEFAULT_FILESTATUS_ID,
                              _file.length(),
                              DEFAULT_FILESTATUS_FLAGS,
                              DEFAULT_FILESTATUS_MODTIME);
    }

    /**
     * Return the InetAddress of the door, as it was received as a part of
     * the protocol-info.
     * @return The InetAddress of the door
     */
    InetSocketAddress getDoorAddress()
    {
        return _doorAddress;
    }

    /**
     * For write access, at least one descriptor must have been opened to
     * regard the transfer as successul. In any case, all open descriptors
     * must have been closed.
     */
    private boolean isTransferSuccessful(int access)
    {
        boolean isRead = (access & MoverProtocol.WRITE) == 0;
        return _openedDescriptors.isEmpty() && (_hasOpenedDescriptor || isRead);
    }

    /**
     * Add length to the number of transferred bytes
     * @param length The number of bytes that should be added.
     */
    void addTransferredBytes(long length) {
        _bytesTransferred.getAndAdd(length);
    }

    /**
     * Update the timestamp of the last transfer
     */
    void updateLastTransferred() {
        _lastTransferred.set(System.currentTimeMillis());
    }

    /**
     * Update whether the file associated with this mover was changed
     * @param wasChanged
     */
    void setWasChanged(boolean wasChanged) {
        _wasChanged = wasChanged;
    }

    /**
     * Ensures that we have allocated space up to the given position
     * in the file. May block if we run out of space.
     */
    synchronized void preallocate(long position) throws InterruptedException
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

    static int getMaxFrameSize()
    {
        return _maxFrameSize;
    }

    /**
     * Decrement the number of endpoints. If they become zero, meaning that
     * neither the server endpoint nor the client endpoint is needing a
     * connection anymore, shut down the server listening for client requests.
     * @throws IOException If shutting down the server fails for some reason
     */
    static void decrementEndpointsToggleServer() throws IOException
    {
        changeEndpointsToggleServer(-1);
    }

    /**
     * Increment the number of endpoints by one. If the server is not yet
     * running, start it.
     * @throws IOException Starting the server fails for some reason
     */
    static void incrementEndpointsToggleServer() throws IOException
    {
        changeEndpointsToggleServer(1);
    }

    /**
     * Add delta to the number of active endpoints. If the number of endpoints
     * is 0 and the server channel is still open, close it. If the number of
     * endpoints is larger than zero and the server channel does not exist,
     * open a new server channel.
     * @param delta The delta by which the number of endpoints should be
     *              changed.
     * @throws IOException Starting or stopping the server fails for some
     *                     reason.
     */
    private synchronized static void changeEndpointsToggleServer(int delta)
        throws IOException
    {
        _endpoints += delta;

        if (_endpoints > 0 && _serverChannel == null) {
            bindServerChannel();
        } else if (_endpoints == 0 && _serverChannel != null) {
            _log.info("No open client channels or waiting movers, closing " +
                      "down the server.");
            _serverChannel.close();
            _serverChannel = null;
        }
    }
}