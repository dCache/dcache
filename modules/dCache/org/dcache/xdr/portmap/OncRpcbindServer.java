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

package org.dcache.xdr.portmap;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.RpcCall;
import org.dcache.xdr.RpcDispatchable;
import org.dcache.xdr.OncRpcSvc;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.XdrBoolean;
import org.dcache.xdr.XdrVoid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OncRpcbindServer implements RpcDispatchable {

    private final static Logger _log = LoggerFactory.getLogger(OncRpcbindServer.class);

    /**
     * Set of registered services.
     */
    private final Set<rpcb> _services = new HashSet<rpcb>();

    public OncRpcbindServer() {
        _services.add(new rpcb(OncRpcPortmap.PORTMAP_PROGRAMM, OncRpcPortmap.PORTMAP_V2, "tcp", "0.0.0.0.0.111", "superuser"));
        _services.add(new rpcb(OncRpcPortmap.PORTMAP_PROGRAMM, OncRpcPortmap.PORTMAP_V2, "udp", "0.0.0.0.0.111", "superuser"));
        //_services.add(new rpcb(100000, 4, "tcp", "0.0.0.0.0.111", "superuser"));
    }
    public void dispatchOncRpcCall(RpcCall call) throws OncRpcException, IOException {
        int version = call.getProgramVersion();

        switch(version) {
            case 2:
                processV2Call(call);
                break;
            case 3:
            case 4:
//                break;
            default:
                call.failProgramMismatch(2, 4);
        }
    }

    private void processV2Call(RpcCall call) throws OncRpcException, IOException {
        switch(call.getProcedure()) {
            case OncRpcPortmap.PMAPPROC_NULL:
                call.reply(XdrVoid.XDR_VOID);
                break;
            case OncRpcPortmap.PMAPPROC_SET:
                mapping newMapping = new mapping();
                call.retrieveCall(newMapping);
                // we sore every thing in v4 format
                rpcb rpcbMapping = new rpcb(newMapping);
                synchronized(_services) {
                    _services.add(rpcbMapping);
                }
                call.reply(XdrBoolean.True);
                break;
            case OncRpcPortmap.PMAPPROC_DUMP:
                pmaplist list = new pmaplist();
                pmaplist next = list;
                synchronized(_services) {
                    for(rpcb mapping: _services) {
                        next.setEntry(mapping.toMapping());
                        pmaplist n = new pmaplist();
                        next.setNext(n);
                        next = n;
                    }
                }
                call.reply(list);
                break;
            case OncRpcPortmap.PMAPPROC_GETPORT:
                mapping query = new mapping();
                call.retrieveCall(query);
                rpcb result = search(new rpcb(query));
                Port port;
                if(result == null) {
                    port = new Port(0);
                }else{
                    port = new Port( result.toMapping().getPort());
                }
                call.reply(port);
                break;
            default:
                call.failProcedureUnavailable();
        }
    }

    private rpcb search(rpcb query) {
        synchronized(_services) {
            for( rpcb e: _services) {
                if( e.match(query)) return e;
            }
        }
        return null;
    }

    public static void main(String[] args) throws Exception {

        if (args.length > 1) {
            System.err.println("Usage: SimpleRpcServer <port>");
            System.exit(1);
        }

        int port = OncRpcPortmap.PORTMAP_PORT;
        if (args.length == 1) {
            port = Integer.parseInt(args[0]);
        }


        RpcDispatchable rpcbind = new OncRpcbindServer();

        OncRpcSvc server  = new OncRpcSvc(port);
        server.register(new OncRpcProgram( OncRpcPortmap.PORTMAP_PROGRAMM,
                OncRpcPortmap.PORTMAP_V2), rpcbind);

        server.start();

    }
}
