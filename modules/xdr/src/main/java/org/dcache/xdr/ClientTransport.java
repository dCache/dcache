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
import java.net.InetSocketAddress;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientTransport implements XdrTransport {

    private final static Logger _log = LoggerFactory.getLogger(ClientTransport.class);

    private final Connection<InetSocketAddress> _connection;
    private final ReplyQueue<Integer, RpcReply> _replyQueue;

    public ClientTransport(Connection<InetSocketAddress> connection,
            ReplyQueue<Integer, RpcReply> replyQueue ) {
        _replyQueue = replyQueue;
        _connection = connection;
    }

    @Override
    public void send(Xdr data) throws IOException
    {
        Buffer buffer = data.body();
        _connection.write(buffer);
     }
    public InetSocketAddress getLocalSocketAddress() {
        return _connection.getLocalAddress();
    }

    public InetSocketAddress getRemoteSocketAddress() {
        return _connection.getPeerAddress();
    }

    public ReplyQueue<Integer, RpcReply> getReplyQueue() {
        return _replyQueue;
    }

}
