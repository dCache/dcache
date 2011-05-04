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

public class MismatchInfo implements XdrAble {

    private int _min;
    private int _max;

    public MismatchInfo(int min, int max) {
        _min = min;
        _max = max;
    }

    public MismatchInfo() {}

    @Override
    public void xdrEncode(XdrEncodingStream xdr) {
        xdr.xdrEncodeInt(_min);
        xdr.xdrEncodeInt(_max);
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr) {
        _min = xdr.xdrDecodeInt();
        _max = xdr.xdrDecodeInt();
    }

    @Override
    public String toString() {
        return String.format("mismatch info: [%d, %d]", _min, _max);
    }

}
