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
import org.glassfish.grizzly.CompletionHandler;
import org.glassfish.grizzly.WriteResult;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.memory.ByteBufferWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrizzlyXdrTransport implements XdrTransport {

    private final FilterChainContext _context;

    private final static Logger _log = LoggerFactory.getLogger(GrizzlyXdrTransport.class);

    public GrizzlyXdrTransport(FilterChainContext context) {
        _context = context;
    }

    @Override
    public void send(Xdr xdr) throws IOException {
        DisposeXdrBuffer disposeXdr = new DisposeXdrBuffer(xdr);
        Buffer buffer = new ByteBufferWrapper(xdr.body());

         // pass destination address to handle UDP connections as well
        _context.write(_context.getAddress(), buffer, disposeXdr);
    }

    @Override
    public InetSocketAddress getLocalSocketAddress() {
        return (InetSocketAddress) _context.getConnection().getLocalAddress();
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress) _context.getAddress();
    }

    @Override
    public ReplyQueue<Integer, RpcReply> getReplyQueue() {
        return null;
    }

    private static class DisposeXdrBuffer implements CompletionHandler<WriteResult> {

        private final Xdr _xdr;

        public DisposeXdrBuffer(Xdr xdr) {
            _xdr = xdr;
        }

        @Override
        public void cancelled() {
            disposeXdr();
        }

        @Override
        public void completed(WriteResult e) {
            disposeXdr();
        }

        @Override
        public void failed(Throwable thrwbl) {
            disposeXdr();
        }

        @Override
        public void updated(WriteResult e) {
            disposeXdr();
        }

        private void disposeXdr() {
            _xdr.close();
        }
    }
}
