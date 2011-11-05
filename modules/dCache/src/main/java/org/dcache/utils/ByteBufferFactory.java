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
package org.dcache.utils;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;

/**
 * A simple Cache for {@link ByteBuffer}. While in general the behavior is similar
 * to libc's malloc/free there are some small difference: If a client allocates
 * a buffer, but never returns it, garbage collector will take care of it. We don't
 * have any reference to returned objects. If a client returns a buffer which was
 * allocated outside of the factory we will take it and reuse it if possible.
 *
 * @author Tigran Mkrtchyan
 */
public class ByteBufferFactory {

    private static final int MAX_CACHED_SIZE = 512*1024;

    private final SoftReference<ByteBuffer>[] _buffers;

    public ByteBufferFactory(int size) {
        _buffers = new SoftReference[size];
    }

    /**
     * Allocate or reuse recycled {@link ByteBuffer}. The returned ByteBuffer may be
     * bigger than requested one. The position is zero and limit is equals to size.
     *
     * @param size minimal size of returned buffer.
     * @return ByteBuffer
     */
    public ByteBuffer allocate(int size) {

        if(size <= MAX_CACHED_SIZE ) {
            /*
             * do no scan cached entriles if requested size is greater than
             * we will cache.
             */
            synchronized (_buffers) {
                for (SoftReference<ByteBuffer> ref: _buffers) {
                    if (ref != null ) {
                        ByteBuffer b = ref.get();
                        if (b == null) continue;
                        if (size <= b.capacity()) {
                            ref.clear();
                            b.clear();
                            b.limit(size);
                            return b;
                        }
                    }
                }
            }
        }

        /*
         * no cached blocks available
         */
        return ByteBuffer.allocateDirect(size);
    }

    /**
     * Add the {@link ByteBuffer} for reuse. If all available slots for reusable
     * buffers are occupied, then the smallest one will be replaced.
     *
     * @param b buffer to recycle.
     */
    public void recycle(ByteBuffer b) {
        if(b.capacity() > MAX_CACHED_SIZE) return;

        synchronized(_buffers) {
            /*
             * fill empty slot if exists. If not replace smallest buffer
             */
            int indexOfSmallest = -1;
            int smallest = b.capacity();

            for (int i = 0; i < _buffers.length; i++) {

                if (_buffers[i] == null) {
                    _buffers[i] = new SoftReference<ByteBuffer>(b);
                    return;
                } else {
                    ByteBuffer old = _buffers[i].get();
                    if (old == null) {
                        _buffers[i] = new SoftReference<ByteBuffer>(b);
                        return;
                    } else if (old.capacity() < smallest) {
                        smallest = old.capacity();
                        indexOfSmallest = i;
                    }
                }
            }

             if(indexOfSmallest != -1) {
                 _buffers[indexOfSmallest] = new SoftReference<ByteBuffer>(b);
             }
        }
    }
}
