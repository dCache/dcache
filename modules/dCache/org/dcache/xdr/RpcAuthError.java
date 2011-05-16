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
 * Server rejects the identity of the caller (AUTH_ERROR).
 */
public class RpcAuthError implements XdrAble {

    private int _stat;

    public RpcAuthError() {
    }

    public RpcAuthError(int _stat) {
        this._stat = _stat;
    }

    public void xdrDecode(XdrDecodingStream xdr) throws OncRpcException, IOException {
        _stat = xdr.xdrDecodeInt();
    }

    public void xdrEncode(XdrEncodingStream xdr) throws OncRpcException, IOException {
        xdr.xdrEncodeInt(_stat);
    }

    public int getStat() {
        return _stat;
    }

    public void setStat(int _stat) {
        this._stat = _stat;
    }

}
