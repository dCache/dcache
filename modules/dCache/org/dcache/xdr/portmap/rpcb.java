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
import org.dcache.xdr.IpProtocolType;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.XdrAble;
import org.dcache.xdr.XdrDecodingStream;
import org.dcache.xdr.XdrEncodingStream;
import org.dcache.xdr.netid;

/**
 *
 * A mapping of (program, version, network ID) to address.
 */
public class rpcb implements XdrAble {

    /**
     * program number
     */
    private int _prog;
    /**
     * version number
     */
    private int _vers;
    /**
     * network id
     */
    private String _netid;
    /**
     * universal address
     */
    private String _addr;
    /**
     * owner of this service
     */
    private String _owner;

    public rpcb() {}

    public rpcb(mapping old) {
        _prog = old.getProg();
        _vers = old.getVers();
        _netid = IpProtocolType.toString(old.getProt());
        _addr = netid.toString(old.getPort());
        _owner = "unspecified";



    }

    public rpcb(int prog, int vers, String netid, String addr, String owner) {
        _prog = prog;
        _vers = vers;
        _netid = netid;
        _addr = addr;
        _owner = owner;
    }

    public void xdrDecode(XdrDecodingStream xdr) throws OncRpcException, IOException {
        _prog = xdr.xdrDecodeInt();
        _vers = xdr.xdrDecodeInt();

        _netid = xdr.xdrDecodeString();
        _addr = xdr.xdrDecodeString();
        _owner = xdr.xdrDecodeString();

    }

    public void xdrEncode(XdrEncodingStream xdr) throws OncRpcException, IOException {
        xdr.xdrEncodeInt(_prog);
        xdr.xdrEncodeInt(_vers);

        xdr.xdrEncodeString(_netid);
        xdr.xdrEncodeString(_addr);
        xdr.xdrEncodeString(_owner);
    }

    @Override
    public String toString() {
        return String.format("prog: %d, vers: %d, netid: %s, addr: %s, owner: %s",
                _prog, _vers, _netid, _addr, _owner);
    }

    public mapping toMapping() {

        return new mapping(_prog, _vers, netid.idOf(_netid) , netid.getPort(_addr) );
    }

    boolean match(rpcb query) {
        return query._prog == _prog &&
                query._vers == _vers &&
                query._netid.equals(_netid);
    }
}
