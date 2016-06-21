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
package org.dcache.util;

/**
 *
 * @since 0.0.3
 */
public final class Bytes {

    private Bytes() {
    }

    /**
     * Puts a big-endian representation of {@code value} into <code>bytes</code>
     * staring from <code>offset</code>.
     * @param bytes
     * @param offset
     * @param value
     * @throws IllegalArgumentException there is no enough room for 8 bytes.
     */
    public static void putLong(byte[] bytes, int offset, long value)
            throws IllegalArgumentException {

        if (bytes.length - offset < 8) {
            throw new IllegalArgumentException("not enough space to store long");
        }

        bytes[offset] = (byte) (value >> 56);
        bytes[offset + 1] = (byte) (value >> 48);
        bytes[offset + 2] = (byte) (value >> 40);
        bytes[offset + 3] = (byte) (value >> 32);
        bytes[offset + 4] = (byte) (value >> 24);
        bytes[offset + 5] = (byte) (value >> 16);
        bytes[offset + 6] = (byte) (value >> 8);
        bytes[offset + 7] = (byte) value;
    }

    /**
     * Puts a big-endian representation of {@code value} into <code>bytes</code>
     * staring from <code>offset</code>.
     * @param bytes
     * @param offset
     * @param value
     * @throws IllegalArgumentException there is no enough room for 4 bytes.
     */
    public static void putInt(byte[] bytes, int offset, int value)
            throws IllegalArgumentException {

        if (bytes.length - offset < 4) {
            throw new IllegalArgumentException("not enough space to store int");
        }

        bytes[offset] = (byte) (value >> 24);
        bytes[offset + 1] = (byte) (value >> 16);
        bytes[offset + 2] = (byte) (value >> 8);
        bytes[offset + 3] = (byte) value;
    }

    /**
     * Returns the big-endian {@code long} value whose byte representation is the 8
     * bytes of <code>bytes</code> staring <code>offset</code>.
     * @param bytes
     * @param offset
     * @return long value
     */
    public static long getLong(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFFL) << 56
                | (bytes[offset + 1] & 0xFFL) << 48
                | (bytes[offset + 2] & 0xFFL) << 40
                | (bytes[offset + 3] & 0xFFL) << 32
                | (bytes[offset + 4] & 0xFFL) << 24
                | (bytes[offset + 5] & 0xFFL) << 16
                | (bytes[offset + 6] & 0xFFL) << 8
                | (bytes[offset + 7] & 0xFFL);
    }

    /**
     * Returns the big-endian {@code int} value whose byte representation is the 4
     * bytes of <code>bytes</code> staring <code>offset</code>.
     * @param bytes
     * @param offset
     * @return int value
     */
    public static int getInt(byte[] bytes, int offset) {
        return (bytes[offset + 0] & 0xFF) << 24
                | (bytes[offset + 1] & 0xFF) << 16
                | (bytes[offset + 2] & 0xFF) << 8
                | (bytes[offset + 3] & 0xFF);
    }

    private static final char[] HEX =  new char[] {
            '0','1','2','3','4','5','6','7',
            '8','9','a','b','c','d','e','f'
        };

    /**
     * Returns a hexadecimal representation of given byte array.
     *
     * @param bytes whose string representation to return
     * @return a string representation of <tt>bytes</tt>
     */
    public static String toHexString(byte[] bytes) {

        char[] chars = new char[bytes.length*2];
        int p = 0;
        for(byte b : bytes) {
            int i = b & 0xff;
            chars[p++] = HEX[i / 16];
            chars[p++] = HEX[i%16];
        }
        return new String(chars);
    }
}
