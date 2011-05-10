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
 * Interface to an algorithm of how data access is organized across
 * multiple data servers.
 * For example, if we have an array of data servers:
 *   [A, B, C, D],
 * then indeces [0, 1, 2, 3] will simply describes round-robin
 * pattern. As soon as last index is used client starts from beginning.
 * In example above, the fifth block will use data server A.
 *
 * @param <T>
 */
public interface StripingPattern<T> {

    /**
     * Get stripping pattern for a given data type.
     *
     * @param data
     * @return an array of data server indeces.
     */
    uint32_t[] getPattern(T[] data);
}
