/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.xdr;

import org.dcache.xdr.gss.GssProtocolFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.dcache.utils.net.InetSocketAddresses;
import org.dcache.xdr.gss.GssSessionManager;
import org.dcache.xdr.portmap.GenericPortmapClient;
import org.dcache.xdr.portmap.OncPortmapClient;
import org.dcache.xdr.portmap.OncRpcPortmap;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.PortRange;
import org.glassfish.grizzly.SocketBinder;
import org.glassfish.grizzly.Transport;
import org.glassfish.grizzly.filterchain.FilterChain;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.grizzly.nio.transport.UDPNIOTransport;
import org.glassfish.grizzly.nio.transport.UDPNIOTransportBuilder;
import static org.dcache.xdr.GrizzlyUtils.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OncRpcSvc {

    private final static Logger _log = LoggerFactory.getLogger(OncRpcSvc.class);

    private final static int BACKLOG = 4096;
    private final boolean _publish;
    private final PortRange _portRange;
    private final List<Transport> _transports = new ArrayList<Transport>();
    private final Set<Connection<InetSocketAddress>> _boundConnections =
            new HashSet<Connection<InetSocketAddress>>();

    /**
     * Handle RPCSEC_GSS
     */
    private GssSessionManager _gssSessionManager;

    /**
     * mapping of registered programs.
     */
    private final Map<OncRpcProgram, RpcDispatchable> _programs =
            new ConcurrentHashMap<OncRpcProgram, RpcDispatchable>();

    /**
     * Create a new server with default name. Bind to all supported protocols.
     *
     * @param port TCP/UDP port to which service will he bound.
     */
    public OncRpcSvc(int port) {
        this(port, IpProtocolType.TCP | IpProtocolType.UDP, true);
    }

    /**
     * Create a new server. Bind to all supported protocols.
     *
     * @param port TCP/UDP port to which service will he bound.
     * @param publish if true, register service by portmap
     */
    public OncRpcSvc(int port, boolean publish) {
        this(port, IpProtocolType.TCP | IpProtocolType.UDP, publish);
    }

    /**
     * Create a new server with given name, protocol and port number.
     *
     * @param port TCP/UDP port to which service will he bound.
     * @param protocol to bind (tcp or udp)
     */
    public OncRpcSvc(int port, int protocol, boolean publish) {
        this(new PortRange(port), protocol, publish);
    }

    /**
     * Create a new server with @{link PortRange} and name. If <code>publish</code>
     * is <code>true</code>, publish this service in a portmap.
     *
     * @param portRange to use.
     * @param publish this service
     */
    public OncRpcSvc(PortRange portRange, boolean publish) {
        this(portRange, IpProtocolType.TCP | IpProtocolType.UDP, publish);
    }

    /**
     * Create a new server with given @{link PortRange} and name. If <code>publish</code>
     * is <code>true</code>, publish this service in a portmap.
     *
     * @param {@link PortRange} of TCP/UDP ports to which service will he bound.
     * @param protocol to bind (tcp or udp).
     * @param publish this service.
     */
    public OncRpcSvc(PortRange portRange, int protocol, boolean publish) {
        _publish = publish;

        if ((protocol & (IpProtocolType.TCP | IpProtocolType.UDP)) == 0) {
            throw new IllegalArgumentException("TCP or UDP protocol have to be defined");
        }

        if ((protocol & IpProtocolType.TCP) != 0) {
            final TCPNIOTransport tcpTransport = TCPNIOTransportBuilder.newInstance().build();
            _transports.add(tcpTransport);
        }

        if ((protocol & IpProtocolType.UDP) != 0) {
            final UDPNIOTransport udpTransport = UDPNIOTransportBuilder.newInstance().build();
            _transports.add(udpTransport);
        }
        _portRange = portRange;
    }

    /**
     * Register services in portmap.
     *
     * @throws IOException
     * @throws UnknownHostException
     */
    private void publishToPortmap(Connection<InetSocketAddress> connection, Set<OncRpcProgram> programs) throws IOException {

        OncRpcClient rpcClient = new OncRpcClient(InetAddress.getLocalHost(),
                IpProtocolType.UDP, OncRpcPortmap.PORTMAP_PORT);
        XdrTransport transport = rpcClient.connect();
        OncPortmapClient portmapClient = new GenericPortmapClient(transport);

        try {
            String username = System.getProperty("user.name");
            Transport t = connection.getTransport();
            String uaddr = InetSocketAddresses.uaddrOf(connection.getLocalAddress());

            for (OncRpcProgram program : programs) {
                try {
                    if (t instanceof TCPNIOTransport) {
                        portmapClient.setPort(program.getNumber(), program.getVersion(),
                                "tcp", uaddr, username);
                    }
                    if (t instanceof UDPNIOTransport) {
                        portmapClient.setPort(program.getNumber(), program.getVersion(),
                                "udp", uaddr, username);
                    }
                } catch (OncRpcException ex) {
                    _log.warn("Failed to register program: {}", ex.toString());
                }
            }
        } finally {
            rpcClient.close();
        }
    }

    public void setGssSessionManager( GssSessionManager gssSessionManager) {
        _gssSessionManager = gssSessionManager;
    }
    /**
     * Start service.
     */
    public void start() throws IOException {

        for (Transport t : _transports) {

            FilterChainBuilder filterChain = FilterChainBuilder.stateless();
            filterChain.add(new TransportFilter());
            filterChain.add(rpcMessageReceiverFor(t));
            filterChain.add(new RpcProtocolFilter());
            // use GSS if configures
            if (_gssSessionManager != null) {
                filterChain.add(new GssProtocolFilter(_gssSessionManager));
            }
            filterChain.add(new RpcDispatcher(_programs));

            final FilterChain filters = filterChain.build();

            t.setProcessor(filters);
            Connection<InetSocketAddress> connection =
                    ((SocketBinder) t).bind("0.0.0.0", _portRange, BACKLOG);
            t.start();

            _boundConnections.add(connection);
            if (_publish) {
                publishToPortmap(connection, _programs.keySet());
            }
        }
    }

    /**
     * Stop service.
     */
    public void stop() throws IOException {
        for (Transport t : _transports) {
            t.stop();
        }
    }

    /**
     * Add programs to existing services.
     * @param services
     */
    public void setPrograms(Map<OncRpcProgram, RpcDispatchable> services) {
        _programs.putAll(services);
    }

    /**
     * Register a new PRC service. Existing registration will be overwritten.
     *
     * @param prog program number
     * @param handler RPC requests handler.
     */
    public void register(OncRpcProgram prog, RpcDispatchable handler) {
        _log.info( "Registering new program {} : {}", prog, handler);
        _programs.put(prog, handler);
    }

    /**
     * Unregister program.
     *
     * @param prog
     */
    public void unregister(OncRpcProgram prog) {
        _log.info( "Unregistering program {}", prog);
        _programs.remove(prog);
    }

    /**
     * Get number of maximal concurrent threads.
     * @return thread number
     */
    public int getThreadCount() {
        int max = 0;
        for (Transport t : _transports) {
            max = Math.max(max, t.getWorkerThreadPoolConfig().getMaxPoolSize());
        }
        return max;
    }

    /**
     * Set the maximal number of concurrent threads.
     * @param count
     */
    public void setThreadCount(int count) {
        for (Transport t : _transports) {
            t.getWorkerThreadPoolConfig().setMaxPoolSize(count);
        }
    }

    /**
     * Returns the address of the endpoint this service is bound to,
     * or <code>null<code> if it is not bound yet.
     * @param protocol
     * @return a {@link InetSocketAddress} representing the local endpoint of
     * this service, or <code>null</code> if it is not bound yet.
     */
    public InetSocketAddress getInetSocketAddress(int protocol) {
        Class< ? extends Transport> transportClass = transportFor(protocol);
        for (Connection<InetSocketAddress> connection: _boundConnections) {
            if(connection.getTransport().getClass() == transportClass)
                return connection.getLocalAddress();
        }
        return null;
    }
}
