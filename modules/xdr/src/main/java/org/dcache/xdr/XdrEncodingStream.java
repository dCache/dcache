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

import java.nio.ByteBuffer;

/**
 * Defines interface for encoding XDR stream. An encoding
 * XDR stream receives data in the form of Java data types and writes it to
 * a data sink (for instance, network or memory buffer) in the
 * platform-independent XDR format.
 */
public interface XdrEncodingStream {

    void beginEncoding();
    void endEncoding();
    void xdrEncodeInt(int value);
    void xdrEncodeIntVector(int[] ints);
    void xdrEncodeDynamicOpaque(byte [] opaque);
    void xdrEncodeOpaque(byte [] opaque, int len);
    void xdrEncodeOpaque(byte [] opaque, int offset, int len);
    void xdrEncodeBoolean(boolean bool);
    void xdrEncodeString(String str);
    void xdrEncodeLong(long value);
    void xdrEncodeByteBuffer(ByteBuffer buf);
    /*
     * Fake interface for compatibility with Remote Tea RPC library
     *
     */
}
