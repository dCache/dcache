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
import org.dcache.xdr.RpcAuth;
import org.dcache.xdr.RpcAuthTypeNone;
import org.dcache.xdr.RpcCall;
import org.dcache.xdr.XdrTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericPortmapClient implements OncPortmapClient {

    private final static Logger _log = LoggerFactory.getLogger(GenericPortmapClient.class);
    private final RpcAuth _auth = new RpcAuthTypeNone();
    private final OncPortmapClient _portmapClient;

    public GenericPortmapClient(XdrTransport transport) {

       OncPortmapClient portmapClient = new RpcbindV4Client(new RpcCall(100000, 4,
               _auth, transport));
        if( !portmapClient.ping() ) {
            portmapClient = new PortmapV2Client( new RpcCall(100000, 2,
                    _auth, transport) );
            if(!portmapClient.ping()) {
                // FIXME: return correct exception
                throw new IllegalStateException("portmap service not available");
            }
            _log.debug("Using portmap V2");
        }
        _portmapClient = portmapClient;
    }

    @Override
    public void dump() throws OncRpcException, IOException {
        _portmapClient.dump();
    }

    @Override
    public boolean ping() {
        return _portmapClient.ping();
    }

    @Override
    public boolean setPort(int program, int version, String netid, String addr, String owner) throws OncRpcException, IOException {
        return _portmapClient.setPort(program, version, netid, addr, owner);
    }

    @Override
    public String getPort(int program, int version, String netid) throws OncRpcException, IOException {
        return _portmapClient.getPort(program, version, netid);
    }

    public static void main(String[] args) throws UnknownHostException,
                                                  IOException, OncRpcException {

        OncRpcClient rpcClient = new OncRpcClient(InetAddress.getByName("127.0.0.1"), IpProtocolType.UDP, 111);
        XdrTransport transport = rpcClient.connect();

        OncPortmapClient portmapClient = new GenericPortmapClient(transport);

        try {

            /*
             * check for V4
             */
            portmapClient.ping();
            portmapClient.setPort(100003, 4, "tcp", "127.0.0.2.8.4", System.getProperty("user.name"));
            portmapClient.dump();

            System.out.println("getport: " + portmapClient.getPort(100000, 4, "tcp"));
        } finally {
            rpcClient.close();
        }
    }
}
