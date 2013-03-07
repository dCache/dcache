package org.dcache.xrootd.pool;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import diskCacheV111.movers.NetIFContainer;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Formats;
import dmg.util.Replaceable;

import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.NetworkUtils;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.XrootdDoorAdressInfoMessage;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.plugins.ChannelHandlerProvider;

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

    private final static ServiceLoader<ChannelHandlerProvider> _channelHandlerProviders =
            ServiceLoader.load(ChannelHandlerProvider.class);

    /**
     * Communication endpoint.
     */
    private final CellEndpoint _endpoint;

    /**
     * The file served by this mover.
     */
    private MoverChannel<XrootdProtocolInfo> _wrappedChannel;

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

    private static List<ChannelHandlerFactory> createPluginFactories(
            Properties properties)
            throws Exception
    {
        List<ChannelHandlerFactory> factories = Lists.newArrayList();
        String plugins = properties.getProperty("xrootdPlugins");
        for (String plugin: Splitter.on(",").omitEmptyStrings().split(plugins)) {
            factories.add(createChannelHandlerFactory(properties, plugin));
        }
        return factories;
    }

    private static ChannelHandlerFactory createChannelHandlerFactory(
            Properties properties, String plugin)
            throws Exception
    {
        for (ChannelHandlerProvider provider: _channelHandlerProviders) {
            ChannelHandlerFactory factory =
                    provider.createFactory(plugin, properties);
            if (factory != null) {
                return factory;
            }
        }
        throw new IllegalArgumentException("Channel handler plugin not found: " + plugin);
    }

    private static synchronized void initSharedResources(CellEndpoint endpoint)
            throws Exception
    {
        if (_server == null) {
            Properties properties = getConfiguration(endpoint);

            int threads = Integer.parseInt(properties.getProperty("xrootd-mover-disk-threads"));
            int perChannelLimit = Integer.parseInt(properties
                    .getProperty("xrootd-mover-max-memory-per-connection"));
            int totalLimit = Integer.parseInt(properties
                    .getProperty("xrootd-mover-max-memory"));
            int clientIdleTimeout = Integer.parseInt(properties
                    .getProperty("xrootd-mover-idle-client-timeout"));
            String socketThreads = properties.getProperty("xrootd-mover-socket-threads");
            List<ChannelHandlerFactory> plugins = createPluginFactories(properties);

            if (socketThreads == null || socketThreads.isEmpty()) {
                _server = new XrootdPoolNettyServer(
                        threads,
                        perChannelLimit,
                        totalLimit,
                        clientIdleTimeout,
                        plugins);
            } else {
                _server = new XrootdPoolNettyServer(
                        threads,
                        perChannelLimit,
                        totalLimit,
                        clientIdleTimeout,
                        plugins,
                        Integer.parseInt(socketThreads));
            }
        }
    }

    private static Properties getConfiguration(CellEndpoint endpoint)
    {
        try {
            /* REVISIT: UGLY hack to get to the environment. We should change
             * CellEndpoint to provide access to the configuration rather
             * just to the cell arguments.
             *
             * For now we do it like this because we need this code to be
             * backportable to 2.2 and 1.9.12.
             */
            Field field = endpoint.getClass().getDeclaredField("_environment");
            field.setAccessible(true);

            final Map<String,Object> env = (Map<String,Object>) field.get(endpoint);
            Replaceable replaceable = new Replaceable() {
                @Override
                public String getReplacement(String name)
                {
                    Object value =  env.get(name);
                    return (value == null) ? null : value.toString().trim();
                }
            };

            Properties properties = new Properties();
            for (Map.Entry<String,Object> e: env.entrySet()) {
                String key = e.getKey();
                String value = String.valueOf(e.getValue());
                properties.setProperty(key, Formats.replaceKeywords(value, replaceable));
            }

            for (Map.Entry<String,String> opt: endpoint.getArgs().optionsAsMap().entrySet()) {
                properties.setProperty(opt.getKey(), opt.getValue());
            }

            return properties;
        } catch (NoSuchFieldException e) {
            throw Throwables.propagate(e);
        } catch (IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    public XrootdProtocol_3(CellEndpoint endpoint) throws Exception
    {
        _endpoint = endpoint;
        initSharedResources(_endpoint);
    }

    @Override
    public void runIO(FileAttributes fileAttributes,
                      RepositoryChannel fileChannel,
                      ProtocolInfo protocol,
                      Allocator allocator,
                      IoMode access)
        throws Exception
    {
        _protocolInfo = (XrootdProtocolInfo) protocol;

        UUID uuid = _protocolInfo.getUUID();

        _log.trace("Received opaque information {}", uuid);

        _wrappedChannel =
            new MoverChannel<>(access, fileAttributes, _protocolInfo,
                fileChannel, allocator);
        try {
            _server.register(_wrappedChannel, uuid);

            InetSocketAddress address = _server.getServerAddress();
            sendAddressToDoor(address.getPort());

            _server.await(_wrappedChannel, CONNECT_TIMEOUT);
        } finally {
            _server.unregister(_wrappedChannel);
        }

        _log.trace("Xrootd transfer completed, transferred {} bytes.",
                   getBytesTransferred());
    }

    /**
     * Sends our address to the door. Copied from old xrootd mover.
     */
    private void sendAddressToDoor(int port)
        throws SocketException,
               CacheException, NoRouteToCellException
    {
        Collection<NetIFContainer> netifsCol = new ArrayList<>();

        // try to pick the ip address with corresponds to the
        // hostname (which is hopefully visible to the world)
        InetAddress localIP =
            NetworkUtils.getLocalAddress(_protocolInfo.getSocketAddress().getAddress());

        if (localIP != null && !localIP.isLoopbackAddress()
            && localIP instanceof Inet4Address) {
            // the ip we got from the hostname is at least not
            // 127.0.01 and from the IP4-family
            Collection<InetAddress> col = new ArrayList<>(1);
            col.add(localIP);
            netifsCol.add(new NetIFContainer("", col));
            _log.trace("sending ip-address derived from hostname " +
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
                Collection<InetAddress> ipsCol = new ArrayList<>(2);

                while (ips.hasMoreElements()) {
                    InetAddress addr = ips.nextElement();

                    // check again each ip from each interface.
                    // WARNING: multiple ip addresses in case of
                    // multiple ifs could be selected, we can't
                    // determine the "correct" one
                    if (addr instanceof Inet4Address
                        && !addr.isLoopbackAddress()) {
                        ipsCol.add(addr);
                        _log.trace("sending ip-address derived from " +
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

        _log.trace("sending redirect message to Xrootd-door "+ cellpath);
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
