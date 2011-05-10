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

import com.sun.grizzly.Context;
import com.sun.grizzly.ProtocolFilter;
import com.sun.grizzly.ProtocolParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcProtocolFilter implements ProtocolFilter {

    public static final String RPC_CALL = "RPC_CALL";

    private final static Logger _log = LoggerFactory.getLogger(RpcProtocolFilter.class);
    private final ReplyQueue<Integer, RpcReply> _replyQueue;

    public RpcProtocolFilter() {
        this(null);
    }

    public RpcProtocolFilter(ReplyQueue<Integer, RpcReply> replyQueue) {
        _replyQueue = replyQueue;
    }

    @Override
    public boolean execute(Context context) throws IOException {
        Xdr  xdr = (Xdr) context.removeAttribute(ProtocolParser.MESSAGE);

        if (xdr == null) {
            _log.error( "Parser returns bad XDR");
            return false;
        }

        xdr.beginDecoding();

        RpcMessage message = new RpcMessage(xdr);
        XdrTransport transport = new GrizzlyXdrTransport(context);

        if (message.type() == RpcMessageType.CALL) {
            RpcCall call = new RpcCall(message.xid(), xdr, transport);
            try {
                call.accept();

               /*
                * pass RPC call to the next filter in the chain
                */

               context.setAttribute(RPC_CALL, call);

            }catch (RpcException e) {
                call.reject(e.getStatus(), e.getRpcReply());
                _log.info( "RPC request rejected: {}", e.getMessage());
                return false;
            }catch (OncRpcException e) {
                _log.info( "failed to process RPC request: {}", e.getMessage());
                return false;
            }
        } else {
            /*
             * For now I do not expect to receive a reply message over
             * the client connection. But it's definitely part of
             * bidirectional RPC calls.
             */
            try {
                RpcReply reply = new RpcReply(message.xid(), xdr, transport);
                if(_replyQueue != null ) {
                    _replyQueue.put(message.xid(), reply);
                }
            }catch(OncRpcException e) {
                _log.warn( "failed to decode reply:", e);
            }
        }

        return true;
    }

    @Override
    public boolean postExecute(Context context) throws IOException {

        /**
         * cleanup
         */
        context.removeAttribute(RPC_CALL);

        return true;
    }

}
