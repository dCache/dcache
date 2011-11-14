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
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.dcache.xdr.IpProtocolType;
import org.dcache.xdr.OncRpcClient;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;
import org.dcache.xdr.OncRpcSvcBuilder;
import org.dcache.xdr.RpcAuth;
import org.dcache.xdr.RpcAuthTypeNone;
import org.dcache.xdr.RpcCall;
import org.dcache.xdr.XdrTransport;
import org.dcache.xdr.XdrVoid;


public class OncRpcEmbeddedPortmap {

    private static final RpcAuth _auth = new RpcAuthTypeNone();

    public  OncRpcEmbeddedPortmap() throws UnknownHostException, IOException {
        this(2000);
    }

    public  OncRpcEmbeddedPortmap(int timeout) throws UnknownHostException, IOException {

        // we start embedded portmap only if there no other one is running

        OncRpcClient rpcClient = null;
        boolean localPortmapperRunning = false;
        try {
            rpcClient = new OncRpcClient(InetAddress.getLocalHost(), IpProtocolType.UDP, OncRpcPortmap.PORTMAP_PORT);
            XdrTransport transport = rpcClient.connect();
            /* check for version 2, 3 and 4 */
            for (int i = 2; i < 5; i++) {
                RpcCall call = new RpcCall(OncRpcPortmap.PORTMAP_PROGRAMM,
                        i, _auth, transport);
                try {
                    call.call(0, XdrVoid.XDR_VOID, XdrVoid.XDR_VOID, 2000);
                } catch (OncRpcException ex) {}
                localPortmapperRunning = true;
            }
        } catch (IOException e) {
        } finally {
            if(rpcClient != null) rpcClient.close();
        }

        if(!localPortmapperRunning) {
            OncRpcSvc rpcbindServer = new OncRpcSvcBuilder()
                    .withPort(OncRpcPortmap.PORTMAP_PORT)
                    .withTCP()
                    .withUDP()
                    .withoutAutoPublish()
                    .build();
            rpcbindServer.register(new OncRpcProgram( OncRpcPortmap.PORTMAP_PROGRAMM, OncRpcPortmap.PORTMAP_V2), new OncRpcbindServer());
            rpcbindServer.start();
        }
    }

}
