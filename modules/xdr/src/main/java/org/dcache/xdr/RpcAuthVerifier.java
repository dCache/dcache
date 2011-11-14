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

/**
 * Authentication verifier. Depending of status of credentials the content may
 * change ( for example, in case of RPCGSS_SEC contains the checksum of RPC header).
 */
public class RpcAuthVerifier implements XdrAble {

    private int _type;
    private byte[] _body;

    public RpcAuthVerifier(int type, byte[] body) {
        _type = type;
        _body = body;
    }

    public RpcAuthVerifier(XdrDecodingStream xdr) throws OncRpcException, IOException {
        xdrDecode(xdr);
    }

    public int getType() {
        return _type;
    }

    public byte[] getBody() {
        return _body;
    }

    public void xdrDecode(XdrDecodingStream xdr) throws OncRpcException, IOException {
        _type = xdr.xdrDecodeInt();
        _body = xdr.xdrDecodeDynamicOpaque();
    }

    public void xdrEncode(XdrEncodingStream xdr) throws OncRpcException, IOException {
        xdr.xdrEncodeInt(_type);
        xdr.xdrEncodeDynamicOpaque(_body);
    }
}
