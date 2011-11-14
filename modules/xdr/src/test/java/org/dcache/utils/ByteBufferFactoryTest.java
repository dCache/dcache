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

import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author tigran
 */
public class ByteBufferFactoryTest {

    private ByteBufferFactory byteBufferFactory;

    @Before
    public void setUp() {
        byteBufferFactory = new ByteBufferFactory(5);
    }

    @Test
    public void testReAllocSame() {
        int size = 128;
        ByteBuffer b = byteBufferFactory.allocate(size);
        byteBufferFactory.recycle(b);

        assertSame("Not the cached buffer returned", b, byteBufferFactory.allocate(size));
    }

    @Test
    public void testReAllocSmaller() {
        int size = 128;
        int small = 64;
        ByteBuffer b = byteBufferFactory.allocate(size);
        byteBufferFactory.recycle(b);

        assertSame("Not the cached buffer returned", b, byteBufferFactory.allocate(small));
    }

    @Test
    public void testReAllocBigger() {
        int size = 128;
        int big = 256;
        ByteBuffer b = byteBufferFactory.allocate(size);
        byteBufferFactory.recycle(b);

        assertNotSame("cached buffer returned", b, byteBufferFactory.allocate(big));
    }
}