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
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.repository.Allocator;
import org.dcache.util.NetworkUtils;
import org.jboss.netty.handler.stream.ChunkedInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
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
    private static final String QUERY_PARAM_ASSIGN = "=";
    private static final String PROTOCOL_HTTP = "http";

    private HttpProtocolInfo _protocolInfo;

    private RandomAccessFile _diskFile;


    private final CountDownLatch _connectLatch = new CountDownLatch(1);

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

    private volatile boolean _wasChanged = false;

    private final HttpResourceMonitor _httpMonitor = new HttpResourceMonitor();

    private final static Logger _logger =
        LoggerFactory.getLogger(HttpProtocol_2.class);

    private static synchronized void
        initSharedResources(Args args) {

        if (_server == null) {
            int threads = args.getIntOption("http-mover-disk-threads");

            int perChannelLimit = args.getIntOption("http-mover-max-memory-per-connection");

            int totalLimit = args.getIntOption("http-mover-max-memory");
            int maxChunkSize =  args.getIntOption("http-mover-max-chunk-size");

            int timeoutInSeconds = args.getIntOption("http-mover-client-idle-timeout") ;
            int clientIdleTimeout = (int)TimeUnit.SECONDS.toMillis(timeoutInSeconds);

            String socketThreads = args.getOpt("http-mover-socket-threads");

            if (socketThreads == null || socketThreads.isEmpty()) {
                _server = new HttpPoolNettyServer(threads,
                                                  perChannelLimit,
                                                  totalLimit,
                                                  maxChunkSize,
                                                  clientIdleTimeout);
            } else {
                _server = new HttpPoolNettyServer(threads,
                                                  perChannelLimit,
                                                  totalLimit,
                                                  maxChunkSize,
                                                  clientIdleTimeout,
                                                  Integer.parseInt(socketThreads));
            }
        }
    }

    public HttpProtocol_2(CellEndpoint endpoint) {
        _endpoint = endpoint;

        Args args = _endpoint.getArgs();
        long connect = args.getLongOption("http-mover-connect-timeout");
        _connectTimeout = connect*1000;

        _chunkSize = args.getIntOption("http-mover-chunk-size");

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

        try {
            _httpMonitor.setInProgress(true);
            _server.register(uuid, this);

            InetSocketAddress address = _server.getServerAddress();
            sendAddressToDoor(address.getPort(), uuid,  pnfsId);
            _diskFile = diskFile;
            _transferStarted  = System.currentTimeMillis();
            _httpMonitor.updateLastTransferred();

            if (!_connectLatch.await(_connectTimeout, TimeUnit.MILLISECONDS)) {
                /* if connect timeout has elapsed, fail if no connection yet */
                _httpMonitor.guardHasConnected();
            }

            /*
             * shutdown the mover if
             *    - all attached server handlers called close
             *      (count reaches zero and await returns true)
             */
            while (!_connectLatch.await(_connectTimeout, TimeUnit.MILLISECONDS));
        } finally {
            _logger.debug("Shutting down the mover: {}", uuid);
            _server.unregister(uuid);
            _diskFile = null;
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
            NetworkUtils.getLocalAddress(_protocolInfo.getSocketAddress().getAddress());

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
     * @throws TimeoutCacheException Mover has shutdown.
     */
    ChunkedInput read(String requestedPath, long lowerRange, long upperRange)
        throws IOException, IllegalArgumentException, TimeoutCacheException {

        if (!_httpMonitor.isInProgess()) {
            throw new TimeoutCacheException("Can not read from mover as it has shut down.");
        }

        checkRequestPath(requestedPath);
        ChunkedInput content = null;

        /* need to count position 0 as well */
        long length = (upperRange - lowerRange) + 1;

        content = new ReusableChunkedNioFile(_diskFile.getChannel(),
                                             lowerRange, length, _chunkSize,
                                             this);
        return content;

    }

    void updateBytesTransferred(long length) {
        _httpMonitor.updateBytesTransferred(length);
    }

    void updateLastTransferred() {
        _httpMonitor.updateLastTransferred();
    }

    /**
     * @see #read(String, long, long)
     */
    ChunkedInput read(String requestedPath)
        throws IllegalArgumentException, IOException, TimeoutCacheException {

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
     * Check whether the path in the request matches the file served by this
     * mover. This sanity check on the request is additional to the UUID.
     *
     * @param request The path requested by the client
     * @throws IllegalArgumentException path in request is illegal
     */
    private void checkRequestPath(String requestedPath)
        throws IllegalArgumentException {

        FsPath requestedFile;
        FsPath transferFile;

        try {

            URI uri = new URI(requestedPath);

            requestedFile = new FsPath(uri.getPath());
            transferFile = new FsPath(_protocolInfo.getPath());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
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
     * Try to add the handler to the set of connected handlers. Set hasConnected
     * true, if it is the first handler. Fail if mover not running.
     * @throws TimeoutCacheException Mover has shutdown
     */
    void open(HttpPoolRequestHandler handler) throws TimeoutCacheException {
        _httpMonitor.connectHandler(handler);
    }

    /**
     * Count-down connect latch. Mover will terminate when count reaches
     * zero.
     */
    void close(HttpPoolRequestHandler handler) {
        _httpMonitor.disconnectHandlerToggleMover(handler);
    }

    @Override
    public long getBytesTransferred() {
        return _httpMonitor.getBytesTransferred();
    }

    @Override
    public long getTransferTime() {
        return (_httpMonitor.isInProgess() ? System.currentTimeMillis() : _httpMonitor.getLastTransferred()
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
        private final Set<HttpPoolRequestHandler> _connectedHandlers =
            new HashSet<HttpPoolRequestHandler>();
        private boolean _inProgress = false;

        /**
         * Set inProgress to false and fail if no client has connected.
         */
        synchronized void guardHasConnected() throws TimeoutCacheException {
            if (!_hasConnected) {
                _inProgress = false;
                _logger.warn("HTTP mover started, but no connection from HTTP client!");
                throw new TimeoutCacheException("No connection from HTTP client after " +
                                                TimeUnit.MILLISECONDS.toSeconds(_connectTimeout) +
                                                " seconds. Giving up.");
            }
        }

        synchronized void updateLastTransferred() {
            _lastTransferred = System.currentTimeMillis();
        }

        synchronized boolean isInProgess() {
            return _inProgress;
        }

        synchronized void setInProgress(boolean inProgress) {
            _inProgress = inProgress;
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

        synchronized void connectHandler(HttpPoolRequestHandler handler) throws TimeoutCacheException{
            if (!_inProgress) {
                throw new TimeoutCacheException("Can not open mover, as it is shutting down.");
            }

            _hasConnected = true;
            _connectedHandlers.add(handler);
        }

        synchronized void disconnectHandlerToggleMover(HttpPoolRequestHandler handler) {
            _connectedHandlers.remove(handler);

            if (_connectedHandlers.isEmpty()) {
                _inProgress = false;
                _connectLatch.countDown();
            }
        }
    }
}
