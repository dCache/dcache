package org.dcache.http;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.ChecksumFactory;
import org.dcache.pool.movers.ChecksumChannel;
import org.dcache.pool.movers.ChecksumMover;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.repository.Allocator;
import org.dcache.util.Checksum;
import org.dcache.util.NetworkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;
import org.dcache.pool.repository.RepositoryChannel;

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
public class HttpProtocol_2 implements MoverProtocol, ChecksumMover
{
    private final static Logger _logger =
        LoggerFactory.getLogger(HttpProtocol_2.class);

    public static final String UUID_QUERY_PARAM = "dcache-http-uuid";
    private static final String QUERY_PARAM_ASSIGN = "=";
    private static final String PROTOCOL_HTTP = "http";

    private static HttpPoolNettyServer _server;

    /**
     * Timeout for the interval that the mover will wait for any client to
     * connect
     */
    private final long _connectTimeout;

    /**
     * Communication endpoint.
     */
    private final CellEndpoint _endpoint;

    private MoverChannel<HttpProtocolInfo> _wrappedChannel;

    /**
     * Wrapper around RepositoryChannel used when computing a
     * digest on the fly.
     */
    private ChecksumChannel _checksumChannel;

    private HttpProtocolInfo _protocolInfo;

    /**
     * ChecksumFactory to be used for creating a digest during upload.
     */
    private ChecksumFactory _checksumFactory;

    protected static synchronized void
        initSharedResources(Args args) {

        if (_server == null) {
            int threads = args.getIntOption("http-mover-disk-threads");

            int perChannelLimit = args.getIntOption("http-mover-max-memory-per-connection");

            int totalLimit = args.getIntOption("http-mover-max-memory");
            int chunkSize = args.getIntOption("http-mover-chunk-size");

            int timeoutInSeconds = args.getIntOption("http-mover-client-idle-timeout") ;
            long clientIdleTimeout = TimeUnit.SECONDS.toMillis(timeoutInSeconds);

            String socketThreads = args.getOpt("http-mover-socket-threads");

            if (socketThreads == null || socketThreads.isEmpty()) {
                _server = new HttpPoolNettyServer(threads,
                                                  perChannelLimit,
                                                  totalLimit,
                                                  chunkSize,
                                                  clientIdleTimeout);
            } else {
                _server = new HttpPoolNettyServer(threads,
                                                  perChannelLimit,
                                                  totalLimit,
                                                  chunkSize,
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

        initSharedResources(args);
    }

    @Override
    public void runIO(RepositoryChannel fileChannel,
                      ProtocolInfo protocol,
                      StorageInfo storage,
                      PnfsId pnfsId,
                      Allocator allocator,
                      IoMode access)
        throws Exception
    {
        _protocolInfo = (HttpProtocolInfo) protocol;

        RepositoryChannel channel;
        if (_checksumFactory != null) {
            _checksumChannel =
                    new ChecksumChannel(fileChannel, _checksumFactory);
            channel = _checksumChannel;
        } else {
            channel = fileChannel;
        }
        _wrappedChannel =
            new MoverChannel<HttpProtocolInfo>(access, _protocolInfo, channel, allocator);

        try {
            UUID uuid = _server.register(_wrappedChannel);
            InetSocketAddress address = _server.getServerAddress();
            sendAddressToDoor(address.getPort(), uuid,  pnfsId);
            _server.await(_wrappedChannel, _connectTimeout);
        } finally {
            _logger.debug("Shutting down mover");
            _server.unregister(_wrappedChannel);
        }
    }

    /**
     * Send the network address of this mover to the door, along with the UUID
     * identifying it
     */
    private void sendAddressToDoor(int port, UUID uuid, PnfsId pnfsId)
        throws SocketException, CacheException,
               NoRouteToCellException, URISyntaxException
    {
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
                     targetURI.toASCIIString());

        CellPath cellPath = new CellPath(_protocolInfo.getHttpDoorCellName(),
                                         _protocolInfo.getHttpDoorDomainName());
        HttpDoorUrlInfoMessage httpDoorMessage =
            new HttpDoorUrlInfoMessage(pnfsId.getId(), targetURI.toASCIIString());

        httpDoorMessage.setId(_protocolInfo.getSessionId());

        _endpoint.sendMessage(new CellMessage(cellPath, httpDoorMessage));

    }

    @Override
    public long getBytesTransferred() {
        return (_wrappedChannel == null) ? 0 : _wrappedChannel.getBytesTransferred();
    }

    @Override
    public long getTransferTime() {
        return (_wrappedChannel == null) ? 0 : _wrappedChannel.getTransferTime();
    }

    @Override
    public long getLastTransferred() {
        return (_wrappedChannel == null) ? 0 : _wrappedChannel.getLastTransferred();
    }

    @Override
    public boolean wasChanged() {
        return (_wrappedChannel != null) && _wrappedChannel.wasChanged();
    }

    @Override
    public ChecksumFactory getChecksumFactory(ProtocolInfo protocolInfo)
    {
        return null;
    }

    @Override
    public void setDigest(ChecksumFactory checksumFactory)
    {
        _checksumFactory = checksumFactory;
    }

    @Override
    public Checksum getClientChecksum()
    {
        return null;
    }

    @Override
    public Checksum getTransferChecksum()
    {
        return (_checksumChannel == null) ? null : _checksumChannel.getChecksum();
    }
}
