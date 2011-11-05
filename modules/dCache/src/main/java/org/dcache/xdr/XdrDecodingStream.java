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
 * Defines interface for decoding XDR stream. A decoding
 * XDR stream returns data in the form of Java data types which it reads
 * from a data source (for instance, network or memory buffer) in the
 * platform-independent XDR format.
 */
public interface XdrDecodingStream {


    void beginDecoding();
    void endDecoding();
    int xdrDecodeInt();
    int[] xdrDecodeIntVector();
    byte[] xdrDecodeDynamicOpaque();
    byte[] xdrDecodeOpaque(int size);
    void xdrDecodeOpaque(byte[] data, int offset, int len);
    boolean xdrDecodeBoolean();
    String xdrDecodeString();
    long xdrDecodeLong();
    ByteBuffer xdrDecodeByteBuffer();

    /*
     * Fake interface for compatibility with Remote Tea RPC library
     *
     */
}
