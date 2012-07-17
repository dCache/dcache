package org.dcache.xrootd2.pool;

import java.io.RandomAccessFile;


import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;

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
import org.dcache.pool.movers.IoMode;

import org.dcache.xrootd2.protocol.messages.*;
import org.dcache.xrootd2.util.FileStatus;

import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.util.NetworkUtils;


/**
 * xrootd mover.
 *
 * The xrootd mover contains a static method for constructing a netty server
 * that will listen for incoming connections on the xrootd port. The purpose
 * of the server is to relay xrootd requests on the pool to the right mover.
 *
 * The mover responsible for a client connection is selected based on an opaque
 * UUID included by the client in redirect from the door.
 * The mover will register itself with a netty server handling the client
 * connections after starting. The registration will also start the server,
 * if it is not yet running.
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
    private static final int DEFAULT_FILESTATUS_ID = 0;
    private static final int DEFAULT_FILESTATUS_FLAGS = 0;
    private static final int DEFAULT_FILESTATUS_MODTIME = 0;

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



    private static final Logger _logSpaceAllocation =
        LoggerFactory.getLogger("logger.dev.org.dcache.poolspacemonitor." +
                         XrootdProtocol_3.class.getName());

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
     * The netty server that will be used for serving client requests. In
     * order for clients to be able to communicate with this mover, it
     * must register itself with this server.
     */
    private static XrootdPoolNettyServer _server;

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
     * Maximum frame size of a read or readv reply. Does not include the size
     * of the frame header.
     */
    private static int _maxFrameSize = 2 << 20;

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
     * Switch Netty to slf4j for logging. Should be moved somewhere
     * else.
     */
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    private static synchronized void initSharedResources(Args args) {
        if (_server == null) {
            int threads = args.getIntOption("xrootd-mover-disk-threads");
            int perChannelLimit = args.getIntOption("xrootd-mover-max-memory-per-connection");
            int totalLimit = args.getIntOption("xrootd-mover-max-memory");

            int clientIdleTimeout = args.getIntOption("xrootd-mover-idle-client-timeout");

            String socketThreads = args.getOpt("xrootd-mover-socket-threads");

            if (socketThreads == null || socketThreads.isEmpty()) {
                _server = new XrootdPoolNettyServer(threads,
                                                    perChannelLimit,
                                                    totalLimit,
                                                    clientIdleTimeout);
            } else {
                _server = new XrootdPoolNettyServer(threads,
                                                      perChannelLimit,
                                                      totalLimit,
                                                      clientIdleTimeout,
                                                      Integer.parseInt(socketThreads));
            }
        }
    }

    public XrootdProtocol_3(CellEndpoint endpoint)
    {
        _endpoint = endpoint;
        initSharedResources(_endpoint.getArgs());
    }

    @Override
    public void runIO(RandomAccessFile diskFile,
                      ProtocolInfo protocol,
                      StorageInfo storage,
                      PnfsId pnfsId,
                      Allocator allocator,
                      IoMode access)
        throws Exception
    {
        _protocolInfo = (XrootdProtocolInfo) protocol;
        _doorAddress = _protocolInfo.getDoorAddress();

        UUID uuid = _protocolInfo.getUUID();

        _log.debug("Received opaque information {}", uuid);

        boolean transferSuccess;

        try {
            _server.register(uuid, this);

            _file = diskFile;
            _allocator = allocator;
            _transferStarted  = System.currentTimeMillis();
            _lastTransferred.set(_transferStarted);

            InetSocketAddress address = _server.getServerAddress();
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
            _server.unregister(uuid);
            /* this effectively closes all file descriptors obtained via this
             * mover */
            _file = null;
            _allocator = null;
            _inProgress = false;

            transferSuccess = isTransferSuccessful(access);

            _openedDescriptors.clear();
        }

       if (!transferSuccess) {
            _log.warn("Xrootd transfer failed");
            throw new CacheException("xrootd transfer failed");
       }

        _log.debug("Xrootd transfer completed, transferred {} bytes.",
                   _bytesTransferred.get());
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
            NetworkUtils.getLocalAddress(_protocolInfo.getSocketAddress().getAddress());

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

    RandomAccessFile getFile() throws ClosedChannelException
    {
        RandomAccessFile file = _file;
        if (file == null) {
            throw new ClosedChannelException();
        }
        return file;
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
    private boolean isTransferSuccessful(IoMode access)
    {
        boolean isRead = access == IoMode.READ;
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
    synchronized void preallocate(long position)
            throws IOException
    {
        try {
            if (position < 0)
                throw new IllegalArgumentException("Position must be positive");

            if (position > _reservedSpace) {
                long additional = Math.max(position - _reservedSpace, SPACE_INC);
                _logSpaceAllocation.debug("ALLOC: " + additional );
                _allocator.allocate(additional);
                _reservedSpace += additional;
            }
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        } catch (IllegalStateException e) {
            throw new ClosedChannelException();
        }
    }

    static int getMaxFrameSize()
    {
        return _maxFrameSize;
    }
}