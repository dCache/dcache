package org.dcache.xrootd.pool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.net.SocketException;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;

import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.NetworkUtils;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.vehicles.XrootdDoorAdressInfoMessage;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.movers.NetIFContainer;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * * At least for vector read, the behaviour when reading beyond the
 *   end of the file is wrong.
 */
public class XrootdProtocol_3
    implements MoverProtocol
{
    private static final long CONNECT_TIMEOUT =
        TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

    private static final Logger _log =
        LoggerFactory.getLogger(XrootdProtocol_3.class);

    /**
     * Communication endpoint.
     */
    private final CellEndpoint _endpoint;

    /**
     * The file served by this mover.
     */
    private MoverChannel _wrappedChannel;

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
    public void runIO(RepositoryChannel fileChannel,
                      ProtocolInfo protocol,
                      StorageInfo storage,
                      PnfsId pnfsId,
                      Allocator allocator,
                      IoMode access)
        throws Exception
    {
        _protocolInfo = (XrootdProtocolInfo) protocol;

        UUID uuid = _protocolInfo.getUUID();

        _log.debug("Received opaque information {}", uuid);

        _wrappedChannel =
            new MoverChannel(access, _protocolInfo, fileChannel, allocator);
        try {
            _server.register(_wrappedChannel, uuid);

            InetSocketAddress address = _server.getServerAddress();
            sendAddressToDoor(address.getPort());

            _server.await(_wrappedChannel, CONNECT_TIMEOUT);
        } finally {
            _server.unregister(_wrappedChannel);
        }

        _log.debug("Xrootd transfer completed, transferred {} bytes.",
                   getBytesTransferred());
    }

    /**
     * Sends our address to the door. Copied from old xrootd mover.
     */
    private void sendAddressToDoor(int port)
        throws SocketException,
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
        return (_wrappedChannel == null) ? false : _wrappedChannel.wasChanged();
    }
}
