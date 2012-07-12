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
import org.dcache.xdr.XdrAble;
import org.dcache.xdr.XdrDecodingStream;
import org.dcache.xdr.XdrEncodingStream;

public class pmaplist implements XdrAble {
    private mapping _mapping;
    private pmaplist _next;

    public pmaplist() {}

    public void setEntry(mapping mapping) {
        _mapping = mapping;
    }

    public void setNext(pmaplist next) {
        _next = next;
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr) throws OncRpcException, IOException {
         boolean hasMap = xdr.xdrDecodeBoolean();
         if(hasMap) {
             _mapping = new mapping();
             _mapping.xdrDecode(xdr);
             _next = new pmaplist();
             _next.xdrDecode(xdr);
         }else{
             _mapping = null;
         }
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr) throws OncRpcException, IOException {
        if(_mapping != null ) {
            xdr.xdrEncodeBoolean(true);
            _mapping.xdrEncode(xdr);
            if (_next != null) {
                _next.xdrEncode(xdr);
            } else {
                xdr.xdrEncodeBoolean(false);
            }
        }else{
             xdr.xdrEncodeBoolean(false);
        }
    }

    @Override
    public String toString() {
        return _mapping + "\n" + (_next != null ? _next : "");
    }
}
