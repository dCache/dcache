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

public class RpcMessage implements XdrAble {

    private int _xid;
    private int _type;

    RpcMessage(XdrDecodingStream xdr) {
        this.xdrDecode(xdr);
    }

    RpcMessage(int xid, int type) {
        _xid = xid;
        _type = type;
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr) {
        _xid = xdr.xdrDecodeInt();
        _type = xdr.xdrDecodeInt();
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr) {
        xdr.xdrEncodeInt(_xid);
        xdr.xdrEncodeInt(_type);
    }

    public int xid() {
        return _xid;
    }

    public int type() {
        return _type;
    }
}
