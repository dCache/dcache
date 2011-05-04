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
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.dcache.utils.net.InetSocketAddresses;
import org.dcache.xdr.IpProtocolType;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.RpcCall;
import org.dcache.xdr.XdrBoolean;
import org.dcache.xdr.XdrInt;
import org.dcache.xdr.XdrVoid;
import org.dcache.xdr.netid;

public class PortmapV2Client implements OncPortmapClient {

    private final static Logger _log = Logger.getLogger(PortmapV2Client.class.getName());
    private final RpcCall _call;

    public PortmapV2Client(RpcCall call) {
        _call = call;
    }

    public void dump() throws OncRpcException, IOException {
        _log.log(Level.FINEST, "portmap dump");

        pmaplist list_reply = new pmaplist();
        _call.call(OncRpcPortmap.PMAPPROC_DUMP, XdrVoid.XDR_VOID, list_reply);

        System.out.println(list_reply);
    }

    public boolean ping() {
        _log.log(Level.FINEST, "portmap ping");
        boolean pong = false;
        try {
            _call.call(OncRpcPortmap.PMAPPROC_NULL, XdrVoid.XDR_VOID, XdrVoid.XDR_VOID, 2000);
            pong = true;
        }catch(OncRpcException e) {
        }catch(IOException e) {
        }

        return pong;
    }

    public boolean setPort(int program, int version, String netid, String addr, String owner) throws OncRpcException, IOException {
        _log.log(Level.FINEST, "portmap set port: prog: {0} vers: {1}, netid: {2} addr: {3}, owner: {4}",
                new Object[]{program, version, netid, addr, owner});

        InetSocketAddress address = org.dcache.xdr.netid.toInetSocketAddress(addr);
        // FIXME : nettype detection
        mapping m1 = new mapping(program, version, IpProtocolType.TCP, address.getPort());

        XdrBoolean isSet = new XdrBoolean();
        _call.call(OncRpcPortmap.PMAPPROC_SET, m1, isSet);

        return isSet.booleanValue();
    }

    @Override
    public String getPort(int program, int version, String nid) throws OncRpcException, IOException {

        mapping m = new mapping(program, version, netid.idOf(nid), 0);
        XdrInt port = new XdrInt();

        _call.call(OncRpcPortmap.PMAPPROC_GETPORT, m, port);
        return InetSocketAddresses.uaddrOf(_call.getTransport()
                .getRemoteSocketAddress()
                .getAddress()
                .getHostAddress()
            , port.intValue());
    }

}