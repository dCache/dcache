package org.dcache.http;

import java.io.IOException;

import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.repository.Allocator;
import org.dcache.util.ConfigurationUtil;
import org.dcache.util.NetworkUtils;
import org.jboss.netty.handler.stream.ChunkedInput;
import org.jboss.netty.handler.stream.ChunkedNioFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsFile;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.util.Args;
import org.dcache.pool.movers.IoMode;

/**
 * Netty-based HTTP-mover. The mover generates an UUID that identifies it upon
 * generation and sends it back as a part of the address information to the
 * door.
 *
 * This UUID has to be included in client requests to the netty server, so the
 * netty server can extract the right mover.
 *
 * The netty server that handles all client requests is started by any invoked
 * mover, if it isn't running yet and shut down by the "last" mover that
 * terminates. All requests are handled on the same port.
 *
 * HTTP-keep-alive is implemented using a second count-down latch. Once the
 * first client closes its connection, it counts down the connect latch. Now
 * the mover thread gets to the latch blocking on keep alive. This latch will
 * only be countdown if a client sends a packet with "Connection: close" in
 * it. If such a packet never arrives, the mover will stay running (unless
 * it is interrupted) as long as requests come in during the keep alive
 * period.
 *
 * @author tzangerl
 *
 */
public class HttpProtocol_2 implements MoverProtocol
{
    public static final String UUID_QUERY_PARAM = "dcache-http-uuid";
    private static final String QUERY_PREFIX = "?";
    private static final String QUERY_PARAM_ASSIGN = "=";
    private static final String PROTOCOL_HTTP = "http";

    private HttpProtocolInfo _protocolInfo;

    private RandomAccessFile _diskFile;


    private final CountDownLatch _firstConnectionCloseLatch = new CountDownLatch(1);
    private final CountDownLatch _keepAliveLatch = new CountDownLatch(1);

    /**
     * Timeout for the interval that the mover will stay alive waiting for
     * further requests to enable HTTP Keep Alive
     */
    private final long _keepAliveTimeout;

    /**
     * Timeout for the interval that the mover will wait for any client to
     * connect
     */
    private final long _connectTimeout;

    /**
     * The size of the the individual chunks for the chunked transmission of
     * files used by this mover.
     */
    private final int _chunkSize;

    /**
     * Communication endpoint.
     */
    private final CellEndpoint _endpoint;
    private static HttpPoolNettyServer _server;
    private long _transferStarted;

    private boolean _inProgress = false;
    private volatile boolean _wasChanged = false;

    private final HttpResourceMonitor _httpMonitor = new HttpResourceMonitor();

    private final static Logger _logger =
        LoggerFactory.getLogger(HttpProtocol_2.class);

    private static synchronized void
        initSharedResources(Args args) {

        if (_server == null) {
            int threads = ConfigurationUtil.getIntOption(args,
                                                         "http-mover-disk-threads");
            int perChannelLimit = ConfigurationUtil.getIntOption(args,
                                                                 "http-mover-max-memory-per-connection");
            int totalLimit = ConfigurationUtil.getIntOption(args,
                                                            "http-mover-max-memory");
            int maxChunkSize =  ConfigurationUtil.getIntOption(args,
                                                               "http-mover-max-chunk-size");

            String socketThreads = args.getOpt("http-mover-socket-threads");

            if (socketThreads == null || socketThreads.isEmpty()) {
                _server = new HttpPoolNettyServer(threads,
                                                  perChannelLimit,
                                                  totalLimit,
                                                  maxChunkSize);
            } else {
                _server = new HttpPoolNettyServer(threads,
                                                  perChannelLimit,
                                                  totalLimit,
                                                  maxChunkSize,
                                                  Integer.parseInt(socketThreads));
            }
        }
    }

    public HttpProtocol_2(CellEndpoint endpoint) {
        _endpoint = endpoint;

        Args args = _endpoint.getArgs();
        long keepAlive = ConfigurationUtil.getLongOption(args,
                                                         "http-mover-keep-alive-period");
        _keepAliveTimeout = keepAlive * 1000;
        long connect = ConfigurationUtil.getLongOption(args,
                                                       "http-mover-connect-timeout");
        _connectTimeout = connect*1000;

        _chunkSize = ConfigurationUtil.getIntOption(args, "http-mover-chunk-size");

        initSharedResources(args);
    }

    @Override
    public void runIO(RandomAccessFile diskFile,
                      ProtocolInfo protocol,
                      StorageInfo storage,
                      PnfsId pnfsId,
                      Allocator allocator,
                      IoMode access) throws Exception {
        _protocolInfo = (HttpProtocolInfo) protocol;

        _logger.debug("Starting xrootd server");

        UUID uuid = UUID.randomUUID();
        _server.register(uuid, this);

        try {
            InetSocketAddress address = _server.getServerAddress();
            sendAddressToDoor(address.getPort(), uuid,  pnfsId);
            _diskFile = diskFile;
            _transferStarted  = System.currentTimeMillis();
            _httpMonitor.updateLastTransferred();

            _inProgress = true;

            if (!_firstConnectionCloseLatch.await(_connectTimeout, TimeUnit.MILLISECONDS)) {

                if (!_httpMonitor.hasConnected()) {
                    _logger.warn("HTTP mover started, but no connection from HTTP client!");
                    throw new TimeoutCacheException("No connection from HTTP client after " +
                                                    TimeUnit.MILLISECONDS.toSeconds(_connectTimeout) +
                                                    " seconds. Giving up.");
                }
            }

            _firstConnectionCloseLatch.await();

            while ((System.currentTimeMillis() - _httpMonitor.getLastTransferred())
                    < _keepAliveTimeout) {

                _keepAliveLatch.await(_keepAliveTimeout, TimeUnit.MILLISECONDS);
            }

        } finally {
            _logger.debug("Shutting down the mover.");
            _server.unregister(uuid);
            _diskFile = null;
            _inProgress = false;
        }
    }

    /**
     * Send the network address of this mover to the door, along with the UUID
     * identifying it
     */
    private void sendAddressToDoor(int port, UUID uuid, PnfsId pnfsId)
        throws SocketException, UnknownHostException, CacheException,
               SerializationException, NoRouteToCellException, URISyntaxException {

        String path = _protocolInfo.getPath();

        if (!path.startsWith("/")) {
            path = "/" + path;
        }

        URI targetURI = null;
        // try to pick the ip address with corresponds to the
        // hostname (which is hopefully visible to the world)
        InetAddress localIP =
            NetworkUtils.getLocalAddressForClient(_protocolInfo.getHosts());

        if (localIP != null && !localIP.isLoopbackAddress()) {
            // the ip we got from the hostname is at least not
            // 127.0.01 and from the IP4-family
            targetURI = new URI(PROTOCOL_HTTP,
                                null,
                                localIP.getCanonicalHostName(),
                                port,
                                path,
                                UUID_QUERY_PARAM + QUERY_PARAM_ASSIGN + uuid.toString(),
                                null);
        } else {
            // let's loop through the network interfaces
            Enumeration<NetworkInterface> ifList =
                NetworkInterface.getNetworkInterfaces();

            while (ifList.hasMoreElements()) {
                NetworkInterface netif =
                    ifList.nextElement();

                Enumeration<InetAddress> ips = netif.getInetAddresses();

                while (ips.hasMoreElements()) {
                    InetAddress addr = ips.nextElement();

                    // check again each ip from each interface.
                    // WARNING: multiple ip addresses in case of
                    // multiple ifs could be selected, we can't
                    // determine the "correct" one
                    if (!addr.isLoopbackAddress()) {
                        targetURI = new URI(PROTOCOL_HTTP,
                                            null,
                                            localIP.getCanonicalHostName(),
                                            port,
                                            path,
                                            UUID_QUERY_PARAM + QUERY_PARAM_ASSIGN + uuid.toString(),
                                            null);
                    }
                }
            }

            if (targetURI == null) {
                throw new CacheException("Error: Cannot determine my ip" +
                                         "address. Aborting transfer");
            }
        }

        _logger.info("Sending the following address to the WebDAV-door: {}",
                     targetURI.toString());

        CellPath cellPath = new CellPath(_protocolInfo.getHttpDoorCellName(),
                                         _protocolInfo.getHttpDoorDomainName());
        HttpDoorUrlInfoMessage httpDoorMessage =
            new HttpDoorUrlInfoMessage(pnfsId.getId(), targetURI.toString());

        _endpoint.sendMessage(new CellMessage(cellPath, httpDoorMessage));

    }

    /**
     * Read the resources requested in HTTP-request from the pool. Return a
     * ChunkedInput pointing to the requested portions of the file.
     *
     * Renew the keep-alive heartbeat, meaning that the last transferred time
     * will be updated, resetting the keep-alive timeout.
     *
     * @param requestedPath the path of the requested file (for verification)
     * @param lowerRange The lower delimiter of the requested byte range of the
     *                   file
     * @param upperRange The upper delimiter of the requested byte range of the
     *                   file
     * @return ChunkedInput View upon the mover file suitable for sending with
     *         netty and representing the requested parts.
     * @throws IOException Accessing the mover file fails
     * @throws IllegalArgumentException Request is illegal
     */
    ChunkedInput read(String requestedPath, long lowerRange, long upperRange)
        throws IOException, IllegalArgumentException {

        _httpMonitor.updateLastTransferred();
        _httpMonitor.setHasConnected();

        checkRequestPath(requestedPath);
        ChunkedInput content = null;

        /* need to count position 0 as well */
        long length = (upperRange - lowerRange) + 1;
        content = new ReusableChunkedNioFile(_diskFile.getChannel(),
                                             lowerRange, length, _chunkSize);

        _httpMonitor.updateBytesTransferred(length);

        return content;

    }

    /**
     * @see #read(String, long, long)
     */
    ChunkedInput read(String requestedPath)
        throws IllegalArgumentException, IOException {

        return read(requestedPath, 0, _diskFile.length() - 1);
    }

    /**
     * @return the size of the file managed by this mover
     * @throws IOException file is closed or other problem with descriptor
     */
    long getFileSize() throws IOException {
        return _diskFile.length();
    }

    /**
     * With a normal ChunkedNioFile, netty at some point receives a
     * "connection closed by peer" signal and closes the file, trying to
     * release the resources. As this closes the disk-file, the mover becomes
     * useless despite keep-alive. To avoid this, we are using the following
     * extension to ChunkedNioFile, which turns close into a no-op.
     *
     */
    static class ReusableChunkedNioFile extends ChunkedNioFile {
        public ReusableChunkedNioFile(FileChannel channel,
                                          long offset,
                                          long length,
                                          int chunkSize) throws IOException {
            super(channel, offset, length, chunkSize);
        }

        @Override
        public void close() {
            /* we are going to close the backing stream ourselves */
        }
    }

    /**
     * Check whether the path in the request matches the file served by this
     * mover. This sanity check on the request is additional to the UUID.
     *
     * @param request The path requested by the client
     * @throws IllegalArgumentException path in request is illegal
     */
    private void checkRequestPath(String requestedPath)
        throws IllegalArgumentException {
        requestedPath = requestedPath.substring(0, requestedPath.indexOf(QUERY_PREFIX));
        String transferPath = _protocolInfo.getPath();

        PnfsFile requestedFile;
        PnfsFile transferFile;

        try {
            requestedFile = new PnfsFile(requestedPath);
            transferFile = new PnfsFile(transferPath);
        } catch (CacheException cex) {
            throw new IllegalArgumentException(cex);
        }

        if (!requestedFile.equals(transferFile)) {
            _logger.warn("Received an illegal request for file {}, while serving {}",
                         requestedFile,
                         transferFile);
            throw new IllegalArgumentException("The file you specified does " +
                                               "not match the UUID you specified!");
        }
    }

    /**
     * Count-down both connect and keep-alive latches. Will cause the mover
     * to terminate.
     */
    void close() {
        _firstConnectionCloseLatch.countDown();
        _keepAliveLatch.countDown();
    }

    /**
     * Count down the connect latch. This will cause the mover to wait on the
     * keep-alive latch, if the keep alive is defined to be longer than zero.
     * If not, the mover will terminate.
     */
    void keepAlive() {
        _firstConnectionCloseLatch.countDown();
    }

    @Override
    public void setAttribute(String name, Object attribute) {
    }

    @Override
    public Object getAttribute(String name) {
        return null;
    }

    @Override
    public long getBytesTransferred() {
        return _httpMonitor.getBytesTransferred();
    }

    @Override
    public long getTransferTime() {
        return (_inProgress ? System.currentTimeMillis() : _httpMonitor.getLastTransferred()
                - _transferStarted);
    }

    @Override
    public long getLastTransferred() {
        return _httpMonitor.getLastTransferred();
    }


    @Override
    public boolean wasChanged() {
        return _wasChanged;
    }

    /**
     * Monitor for access synchronization to mover resources that are accessed
     * by multiple threads simultaneously.
     *
     */
    private class HttpResourceMonitor {
        private long _lastTransferred;
        private long _bytesTransferred = 0;
        private boolean _hasConnected = false;

        synchronized boolean hasConnected() {
            return _hasConnected;
        }

        synchronized void setHasConnected() {
            _hasConnected = true;
        }

        synchronized void updateLastTransferred() {
            _lastTransferred = System.currentTimeMillis();
        }

        synchronized long getLastTransferred() {
            return _lastTransferred;
        }

        synchronized long getBytesTransferred() {
            return _bytesTransferred;
        }

        synchronized void updateBytesTransferred(long additional) {
            _bytesTransferred += additional;
        }
    }
}
