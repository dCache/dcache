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
 *
 * The same as Xdr, but does not reserve place
 * for the RPC fragment marker
 */
public class XdrBuffer extends Xdr {

    /**
     * Build a new Xdr object with a buffer of given size.
     *
     * @param size of the buffer in bytes
     */
    public XdrBuffer(int size) {
        this(ByteBuffer.allocate(size));
    }

    public XdrBuffer(ByteBuffer body) {
        super(body);
    }

    @Override
    public void beginDecoding() {
        _body.rewind();
    }

    @Override
    public void endDecoding() {
        // NOP
    }

    @Override
    public void beginEncoding() {
        _body.clear();
    }

    @Override
    public void endEncoding() {
        _body.flip();
    }



}
