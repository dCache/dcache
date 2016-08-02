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

        String idString = digits((long) fsId >> 32, 4) +
                          digits(newId.getMostSignificantBits() >> 32, 8) +
                          digits(newId.getMostSignificantBits() >> 16, 4) +
                          digits(newId.getMostSignificantBits(), 4) +
                          digits(newId.getLeastSignificantBits() >> 48, 4) +
                          digits(newId.getLeastSignificantBits(), 12);

        return idString.toUpperCase();
    }

    /** Returns val represented by the specified number of hex digits. */
    private static String digits(long val, int digits) {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }

    public static byte[] hexStringToByteArray(String id) {

        if (id.length() % 2 != 0) {
            throw new IllegalArgumentException("The string needs to be even-length: " + id);
        }

        int len = id.length() / 2;
        byte[] bytes = new byte[len];

        for (int i = 0; i < len; i++) {
            final int charIndex = i * 2;
            final int d0 = toDigit(id.charAt(charIndex));
            final int d1 = toDigit(id.charAt(charIndex + 1));
            bytes[i] = (byte) ((d0 << 4) + d1);
        }
        return bytes;
    }

    private static int toDigit(char ch) throws NumberFormatException {
        if (ch >= '0' && ch <= '9') {
            return ch - '0';
        }
        if (ch >= 'A' && ch <= 'F') {
            return ch - 'A' + 10;
        }
        if (ch >= 'a' && ch <= 'f') {
            return ch - 'a' + 10;
        }
        throw new NumberFormatException("illegal character '" + ch + '\'');
    }
}
