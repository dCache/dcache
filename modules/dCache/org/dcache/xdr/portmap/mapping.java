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

public class mapping implements XdrAble {

    private int _prog;
    private int _vers;
    private int _prot;
    private int _port;

    public int getPort() {
        return _port;
    }

    public int getProg() {
        return _prog;
    }

    public int getProt() {
        return _prot;
    }

    public int getVers() {
        return _vers;
    }

    public mapping() {}

    public mapping(int prog, int vers, int prot, int port) {
        _prog = prog;
        _vers = vers;
        _prot = prot;
        _port = port;
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr) throws OncRpcException, IOException {
        _prog = xdr.xdrDecodeInt();
        _vers = xdr.xdrDecodeInt();
        _prot = xdr.xdrDecodeInt();
        _port = xdr.xdrDecodeInt();
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr) throws OncRpcException, IOException {
        xdr.xdrEncodeInt(_prog);
        xdr.xdrEncodeInt(_vers);
        xdr.xdrEncodeInt(_prot);
        xdr.xdrEncodeInt(_port);
    }

    @Override
    public String toString() {
        return String.format("prog: %d, vers: %d, prot: %s, port: %d",
                _prog, _vers, IpProtocolType.toString(_prot), _port);
    }
}
