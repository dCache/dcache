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
package org.dcache.chimera;

import java.util.UUID;

/**
 * @Threadsafe
 */
public class InodeId {

    /**
     * no instance allowed
     */
    private InodeId() { /**/ }

    /**
     *  generates new inode id
     *  format 0-3  - fsid
     *         4-35 - inode id
     * @param fsId
     * @return
     */
    public static String newID(int fsId) {

        UUID newId = UUID.randomUUID();

        StringBuilder idString = new StringBuilder(36);

        idString.append(digits((long) fsId >> 32, 4)).
                append(digits(newId.getMostSignificantBits() >> 32, 8)).
                append(digits(newId.getMostSignificantBits() >> 16, 4)).
                append(digits(newId.getMostSignificantBits(), 4)).
                append(digits(newId.getLeastSignificantBits() >> 48, 4)).
                append(digits(newId.getLeastSignificantBits(), 12));

        return idString.toString().toUpperCase();
    }

    /** Returns val represented by the specified number of hex digits. */
    private static String digits(long val, int digits) {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }
}
