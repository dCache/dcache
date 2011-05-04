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
package org.dcache.chimera.nfs.v4;

import org.dcache.chimera.nfs.v4.xdr.uint32_t;

/**
 * An implementation of {@link StripingPattern} with round-robin algorithm.
 */
public class  RoundRobinStripingPattern<T> implements StripingPattern<T> {

    @Override
    public uint32_t[] getPattern(T[] data) {
        uint32_t[] stripeIndices = new uint32_t[data.length];
        for(int i = 0; i < data.length; i++) {
            stripeIndices[i] = new uint32_t(i);
        }
        return stripeIndices;
    }

}
