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

import com.sun.grizzly.Context;
import com.sun.grizzly.filter.ReadFilter;
import com.sun.grizzly.util.OutputWriter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrizzlyXdrTransport implements XdrTransport {

    private final Context _context;
    private final InetSocketAddress _remote;
    private final InetSocketAddress _local;

    private final static Logger _log = LoggerFactory.getLogger(GrizzlyXdrTransport.class);

    public GrizzlyXdrTransport(Context context) {
        _context = context;
        switch(_context.getProtocol()) {
            case TCP:
                SocketChannel socketChannel = ((SocketChannel)context.getSelectionKey().channel());
                _local = (InetSocketAddress) socketChannel.socket().getLocalSocketAddress();
                _remote =(InetSocketAddress) socketChannel.socket().getRemoteSocketAddress();
                break;
            case UDP:
                _remote = (InetSocketAddress) _context.getAttribute(ReadFilter.UDP_SOCKETADDRESS);
                _local = null;
                break;
            default:
                _local = null;
                _remote = null;
                _log.error( "Unsupported protocol: {}", _context.getProtocol());

        }
        _log.debug("RPC call: remote/local: {}/{}", _remote,  _local );
    }


    @Override
    public void send(ByteBuffer data) throws IOException {

        _log.debug("reply sent: {}", data);
        SelectableChannel channel = _context.getSelectionKey().channel();
        switch(_context.getProtocol()) {
            case TCP:
                OutputWriter.flushChannel(channel, data);
                break;
            case UDP:
                DatagramChannel datagramChannel = (DatagramChannel) channel;
                SocketAddress address = (SocketAddress) _context.getAttribute(ReadFilter.UDP_SOCKETADDRESS);
                OutputWriter.flushChannel(datagramChannel, address, data);
                break;
            default:
                _log.error( "Unsupported protocol: {}", _context.getProtocol());
        }
    }


    public InetSocketAddress getLocalSocketAddress() {
        return _local;
    }

    public InetSocketAddress getRemoteSocketAddress() {
        return _remote;
    }

    public ReplyQueue<Integer, RpcReply> getReplyQueue() {
        return null;
    }
}
