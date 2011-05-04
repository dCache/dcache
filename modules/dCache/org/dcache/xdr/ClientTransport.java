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

import com.sun.grizzly.ConnectorHandler;
import com.sun.grizzly.Controller;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientTransport implements XdrTransport {

    private final static Logger _log = Logger.getLogger(ClientTransport.class.getName());

    private final ConnectorHandler _connectorHandler;
    private final ReplyQueue<Integer, RpcReply> _replyQueue;
    private final InetSocketAddress _remote;

    public ClientTransport(InetSocketAddress remote, ConnectorHandler connectorHandler ,
            ReplyQueue<Integer, RpcReply> replyQueue ) {
        _replyQueue = replyQueue;
        _connectorHandler = connectorHandler;
        _remote = remote;
    }

    public void send(ByteBuffer data) throws IOException {
        if( _connectorHandler.protocol() == Controller.Protocol.UDP ) {
            // skip fragment marker
            data.getInt();
        }
            long n = _connectorHandler.write(data, true);
            _log.log(Level.FINEST, "Send {0} bytes", n);
    }

    public InetSocketAddress getLocalSocketAddress() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public InetSocketAddress getRemoteSocketAddress() {
        return _remote;
    }

    public ReplyQueue<Integer, RpcReply> getReplyQueue() {
        return _replyQueue;
    }

}
