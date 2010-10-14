/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.gsi.gssapi;

import java.io.InputStream;
import java.io.IOException;
import java.io.EOFException;

/**
 * A collection of SSL-protocol related functions.
 */
public class SSLUtil {

    /**
     * Reads some number of bytes from the input stream.
     * This function reads maximum data available on the 
     * stream.
     *
     * @param in the input stream to read the bytes from.
     * @param buf the buffer into which read the data is read.
     * @param off the start offset in array b at which the data is written.
     * @param len the maximum number of bytes to read.
     * @exception IOException if I/O error occurs.
     */
    public static int read(InputStream in, byte [] buf, int off, int len)
        throws IOException {
        int n = 0;
        while (n < len) {
            int count = in.read(buf, off + n, len - n);
            if (count < 0) {
                return count;
            }
            n += count;
        }
        return len;
    }
    
    /**
     * Reads some number of bytes from the input stream.
     * This function blocks until all data is read or an I/O 
     * error occurs.
     *
     * @param in the input stream to read the bytes from.
     * @param buf the buffer into which read the data is read.
     * @param off the start offset in array b at which the data is written.
     * @param len the maximum number of bytes to read.
     * @exception IOException if I/O error occurs.
     */
    public static void readFully(InputStream in, byte [] buf, int off, int len)
        throws IOException {
        int n = 0;
        while (n < len) {
            int count = in.read(buf, off + n, len - n);
            if (count < 0)
                throw new EOFException();
            n += count;
        }
    }

    /**
     * Reads an entire SSL message from the specified
     * input stream.
     *
     * @param in the input stream to read the SSL message
     *        from.
     * @return the byte array containing the SSL message
     * @exception IOException if I/O error occurs.
     */
    public static byte[] readSslMessage(InputStream in) 
        throws IOException {
        byte [] header = new byte[5];
        readFully(in, header, 0, header.length);
        if (!isSSLPacket(header)) {
            throw new IOException("Invalid SSL header");
        }
        int length = toShort(header[3], header[4]);
        byte [] inToken = new byte[header.length + length];
        System.arraycopy(header, 0, inToken, 0, header.length);
        readFully(in, inToken, header.length, length);
        return inToken;
    }
    
    /**
     * Determines if a given header is a SSL packet
     * (has a SSL header)
     *
     * @return true if the header is a SSL header. False, otherwise.
     */
    public static final boolean isSSLPacket(byte[] header) {
        return ( isSSLv3Packet(header) || isSSLv2HelloPacket(header) );
    }
    
    /**
     * Determines if a given header is a SSLv3 packet
     * (has a SSL header)
     *
     * @return true if the header is a SSLv3 header. False, otherwise.
     */
    public static final boolean isSSLv3Packet(byte[] header) {
        return ( (header[0] >= 20 && header[0] <= 26 &&
                  (header[1] == 3 && (header[2] == 0 || header[2] == 1) ||
                   header[1] == 2 && header[2] == 0)) );
    }
    
    /**
     * Determines if a given header is a SSLv2 client or server hello packet
     *
     * @return true if the header is such a SSLv2 client or server hello 
     *         packet. False, otherwise.
     */
    public static final boolean isSSLv2HelloPacket(byte[] header) {
        return ((header[0] & 0x80) != 0 && (header[2] == 1 || header[2] == 4));
    }

    /**
     * Converts 2 bytes to a <code>short</code>.
     *
     * @param a byte 1
     * @param b byte 2
     * @return the <code>short</code> value of the 2 bytes
     */
    public static short toShort(byte a, byte b) {
        return (short)((a << 8) | (b & 0xff));
    }

    /**
     * Converts 4 bytes to an <code>int</code> at 
     * the specified offset in the given byte array.
     *
     * @param buf the byte array containing the 4 bytes
     *        to be converted to an <code>int</code>.
     * @param off offset in the byte array 
     * @return the <code>int</code> value of the 4 bytes.
     */
    public static int toInt(byte[] buf, int off) {
        int lg = (buf[off] & 0xff) << 24;
        lg |= (buf[off+1] & 0xff) << 16;
        lg |= (buf[off+2] & 0xff) << 8;
        lg |= (buf[off+3] & 0xff);
        return lg;
    }

    /**
     * Converts the specified int value into
     * 4 bytes. The bytes are put into the
     * specified byte array at a given offset
     * location.
     *
     * @param v the int value to convert into 4 bytes.
     * @param buf the byte array to put the resulting
     *        4 bytes.
     * @param off offset in the byte array 
     */
    public static void writeInt(int v, byte[] buf, int off) {
        buf[off] = (byte)((v >>> 24) & 0xFF);
        buf[off+1] = (byte)((v >>> 16) & 0xFF);
        buf[off+2] = (byte)((v >>>  8) & 0xFF);
        buf[off+3] = (byte)((v >>>  0) & 0xFF);
    }
    
    /**
     * Converts 8 bytes to a <code>long</code> at the
     * specified offset in the given byte array.
     *
     * @param buf the byte array containing the 8 bytes
     *        to be converted to a <code>long</code>.
     * @param off offset in the byte array 
     * @return the <code>long</code> value of the 8 bytes.
     */
    public static long toLong(byte[]buf, int off) {
        return ((long)(toInt(buf, off)) << 32) + (toInt(buf, off+4) & 0xFFFFFFFFL);
    }

}
