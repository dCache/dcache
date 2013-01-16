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
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.RpcAuth;
import org.dcache.xdr.RpcAuthTypeNone;
import org.dcache.xdr.RpcCall;
import org.dcache.xdr.XdrBoolean;
import org.dcache.xdr.XdrString;
import org.dcache.xdr.XdrVoid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcbindV4Client implements OncPortmapClient {

    private final static Logger _log = LoggerFactory.getLogger(RpcbindV4Client.class);

    private final RpcAuth _auth = new RpcAuthTypeNone();
    private final RpcCall _call;

    public RpcbindV4Client(RpcCall call) {
        _call = call;
    }

    @Override
    public boolean ping() {

        _log.debug("portmap ping");
        boolean pong = false;

        try {
            _call.call(OncRpcPortmap.PMAPPROC_NULL, XdrVoid.XDR_VOID, XdrVoid.XDR_VOID, 2000);
            pong = true;
        }catch(OncRpcException | IOException e) {
        }

        return pong;
    }

    @Override
    public boolean setPort(int program, int version, String netid, String addr, String owner)
            throws OncRpcException, IOException {

        _log.debug("portmap set port: prog: {} vers: {}, netid: {} addr: {}, owner: {}", program, version, netid, addr,
                   owner);

        rpcb m1 = new rpcb(program, version, netid, addr, owner);

        XdrBoolean isSet = new XdrBoolean();
        _call.call(OncRpcPortmap.RPCBPROC_SET, m1, isSet);
        return isSet.booleanValue();

    }

    @Override
    public String getPort(int program, int version, String netid) throws OncRpcException, IOException {
        rpcb arg = new rpcb(program, version, netid, "", "");
        XdrString xdrString = new XdrString();
        _call.call(OncRpcPortmap.RPCBPROC_GETADDR, arg, xdrString);
        return xdrString.stringValue();
    }

    @Override
    public void dump() throws OncRpcException, IOException {

        _log.debug("portmap dump");

        rpcb_list rpcb_list_reply = new rpcb_list();
        _call.call(OncRpcPortmap.RPCBPROC_DUMP, XdrVoid.XDR_VOID, rpcb_list_reply);
    }
}
