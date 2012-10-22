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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.ConnectorHandler;
import org.glassfish.grizzly.Transport;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.grizzly.nio.transport.UDPNIOTransport;
import org.glassfish.grizzly.nio.transport.UDPNIOTransportBuilder;
import static org.dcache.xdr.GrizzlyUtils.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OncRpcClient {

    private final static Logger _log = LoggerFactory.getLogger(OncRpcClient.class);

    private final InetSocketAddress _socketAddress;
    private final Transport _transport;
    private Connection<InetSocketAddress> _connection;
    private final ReplyQueue<Integer, RpcReply> _replyQueue = new ReplyQueue<>();

    public OncRpcClient(InetAddress address, int protocol, int port) {

        _socketAddress = new InetSocketAddress(address, port);

        if (protocol == IpProtocolType.TCP) {
            final TCPNIOTransport tcpTransport = TCPNIOTransportBuilder.newInstance().build();
            _transport = tcpTransport;
        } else if (protocol == IpProtocolType.UDP) {
            final UDPNIOTransport udpTransport = UDPNIOTransportBuilder.newInstance().build();
            _transport = udpTransport;
        } else {
            throw new IllegalArgumentException("Unsupported protocol type: " + protocol);
        }

        FilterChainBuilder filterChain = FilterChainBuilder.stateless();
        filterChain.add(new TransportFilter());
        filterChain.add(rpcMessageReceiverFor(_transport));
        filterChain.add(new RpcProtocolFilter(_replyQueue));

        _transport.setProcessor(filterChain.build());
    }

     public XdrTransport connect() throws IOException {
        return connect(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
   }

    public XdrTransport connect(long timeout, TimeUnit timeUnit) throws IOException {

        _transport.start();
        Future<Connection> future = ((ConnectorHandler) _transport).connect(_socketAddress);

        try {
            _connection = future.get(timeout, timeUnit);
        } catch (TimeoutException e) {
           throw new IOException(e.toString(), e);
        } catch (InterruptedException e) {
            throw new IOException(e.toString(), e);
        } catch (ExecutionException e) {
            throw new IOException(e.toString(), e);
         }

        return new ClientTransport(_connection, _replyQueue);
     }

     public void close() throws IOException {
        _connection.close();
        _transport.stop();
    }
}
