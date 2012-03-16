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
import java.nio.ByteOrder;
import java.util.Arrays;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.memory.CompositeBuffer;
import org.glassfish.grizzly.memory.MemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Xdr implements XdrDecodingStream, XdrEncodingStream {

    private final static int SIZE_OF_LONG = Long.SIZE / 8;
    private final static int SIZE_OF_INT = Integer.SIZE / 8;

    /**
     * Maximal size of a XDR message.
     */
    public final static int MAX_XDR_SIZE = 512 * 1024;

    private final static Logger _log = LoggerFactory.getLogger(Xdr.class);

    /**
     * Byte buffer used by XDR record.
     */
    protected Buffer _body;

    /**
     * Create a new Xdr object with a buffer of given size.
     *
     * @param size of the buffer in bytes
     */
    public Xdr(int size) {
        this(MemoryManager.DEFAULT_MEMORY_MANAGER.allocate(size));
    }

    /**
     * Create a new XDR back ended with given {@link ByteBuffer}.
     * @param body buffer to use
     */
    public Xdr(Buffer body) {
        _body = body;
        _body.order(ByteOrder.BIG_ENDIAN);;
    }

    public void beginDecoding() {
        /*
         * Set potision to the beginning of this XDR in back end buffer.
         */
        _body.rewind();
    }

    public void endDecoding() {
        _body.rewind();
    }

    public void beginEncoding() {
        _body.clear();
    }

    public void endEncoding() {
        _body.flip();
    }

    /**
     * Decodes (aka "deserializes") a "XDR int" value received from a
     * XDR stream. A XDR int is 32 bits wide -- the same width Java's "int"
     * data type has. This method is one of the basic methods all other
     * methods can rely on. Because it's so basic, derived classes have to
     * implement it.
     *
     * @return The decoded int value.
     */
    public int xdrDecodeInt() {
        int val = _body.getInt();
        _log.debug("Decoding int {}", val);
        return val;
    }

    /**
     * Get next array of integers.
     *
     * @return the array on integers
     */
    public int[] xdrDecodeIntVector() {

        int len = xdrDecodeInt();
        _log.debug("Decoding int array with len = {}", len);

        int[] ints = new int[len];
        for (int i = 0; i < len; i++) {
            ints[i] = xdrDecodeInt();
        }
        return ints;
    }

    /**
     * Get next array of long.
     *
     * @return the array on integers
     */
    public long[] xdrDecodeLongVector() {

        int len = xdrDecodeInt();
        _log.debug("Decoding long array with len = {}", len);

        long[] longs = new long[len];
        for (int i = 0; i < len; i++) {
            longs[i] = xdrDecodeLong();
        }
        return longs;
    }

    /**
     * Get next opaque data.  The decoded data
     * is always padded to be a multiple of four.
     *
     * @param buf buffer where date have to be stored
     * @param offset in the buffer.
     * @param len number of bytes to read.
     */
    public void xdrDecodeOpaque(byte[] buf, int offset, int len) {
        int padding = (4 - (len & 3)) & 3;
        _log.debug("padding zeros: {}", padding);
        _body.get(buf, offset, len);
        _body.position(_body.position() + padding);
    }

    public void xdrDecodeOpaque(byte[] buf,  int len) {
        xdrDecodeOpaque(buf, 0, len);
    }

    public byte[] xdrDecodeOpaque(int len) {
        byte[] opaque = new byte[len];
        xdrDecodeOpaque(opaque, len);
        return opaque;
    }

    /**
     * Decodes (aka "deserializes") a XDR opaque value, which is represented
     * by a vector of byte values. The length of the opaque value to decode
     * is pulled off of the XDR stream, so the caller does not need to know
     * the exact length in advance. The decoded data is always padded to be
     * a multiple of four (because that's what the sender does).
     */
    public byte [] xdrDecodeDynamicOpaque() {
        int length = xdrDecodeInt();
        byte [] opaque = new byte[length];
        if ( length != 0 ) {
            xdrDecodeOpaque(opaque, 0, length);
        }
        return opaque;
    }

    /**
     * Get next String.
     *
     * @return decoded string
     */
    public String xdrDecodeString() {
        String ret;

        int len = xdrDecodeInt();
        _log.debug("Decoding string with len = {}", len);

        if (len > 0) {
            byte[] bytes = new byte[len];
            xdrDecodeOpaque(bytes, 0, len);
            ret = new String(bytes);
        } else {
            ret = "";
        }

        return ret;
    }

    public boolean xdrDecodeBoolean() {
        int bool = xdrDecodeInt();
        return bool != 0;
    }

    /**
     * Decodes (aka "deserializes") a long (which is called a "hyper" in XDR
     * babble and is 64&nbsp;bits wide) read from a XDR stream.
     *
     * @return Decoded long value.
     */
    public long xdrDecodeLong() {
        return _body.getLong();
    }

    public ByteBuffer xdrDecodeByteBuffer() {
        int len = this.xdrDecodeInt();
        int padding = (4 - (len & 3)) & 3;

       /*
        * as of grizzly 2.2.1 toByteBuffer returns a ByteBuffer view of
        * the backended heap. To be able to use rewind, flip and so on
        * we have to use slice of it.
        */
        ByteBuffer slice = _body.toByteBuffer().slice();
        slice.rewind();
        slice.limit(len);
        _body.position(_body.position() + len + padding);
        return slice;
    }
    ////////////////////////////////////////////////////////////////////////////
    //
    //         Encoder
    //
    ////////////////////////////////////////////////////////////////////////////

    /**
     * Encodes (aka "serializes") a "XDR int" value and writes it down a
     * XDR stream. A XDR int is 32 bits wide -- the same width Java's "int"
     * data type has. This method is one of the basic methods all other
     * methods can rely on.
     */
    public void xdrEncodeInt(int value) {
        _log.debug("Encode int {}", value);
        ensureCapacity(SIZE_OF_INT);
        _body.putInt(value);
    }

    public Buffer body() {
        return _body;
    }

    /**
     * Encodes (aka "serializes") a vector of ints and writes it down
     * this XDR stream.
     *
     * @param values int vector to be encoded.
     *
     */
    public void xdrEncodeIntVector(int[] values) {
        _log.debug("Encode int array {}", Arrays.toString(values));
        ensureCapacity(SIZE_OF_INT+SIZE_OF_INT*values.length);
        _body.putInt(values.length);
        for (int value: values) {
            _body.putInt( value );
        }
    }

    /**
     * Encodes (aka "serializes") a vector of longs and writes it down
     * this XDR stream.
     *
     * @param values long vector to be encoded.
     *
     */
    public void xdrEncodeLongVector(long[] values) {
        _log.debug("Encode int array {}", Arrays.toString(values));
        ensureCapacity(SIZE_OF_INT+SIZE_OF_LONG*values.length);
        _body.putInt(values.length);
        for (long value : values) {
            _body.putLong(value);
        }
    }

    /**
     * Encodes (aka "serializes") a string and writes it down this XDR stream.
     *
     */
    public void xdrEncodeString(String string) {
        _log.debug("Encode String:  {}", string);
        if( string == null ) string = "";
        xdrEncodeDynamicOpaque(string.getBytes());
    }

    private static final byte [] paddingZeros = { 0, 0, 0, 0 };

    /**
     * Encodes (aka "serializes") a XDR opaque value, which is represented
     * by a vector of byte values. Only the opaque value is encoded, but
     * no length indication is preceeding the opaque value, so the receiver
     * has to know how long the opaque value will be. The encoded data is
     * always padded to be a multiple of four. If the length of the given byte
     * vector is not a multiple of four, zero bytes will be used for padding.
     */
    public void xdrEncodeOpaque(byte[] bytes, int offset, int len) {
        _log.debug("Encode Opaque, len = {}", len);
        int padding = (4 - (len & 3)) & 3;
        ensureCapacity(len+padding);
        _body.put(bytes, offset, len);
        _body.put(paddingZeros, 0, padding);
    }

    public void xdrEncodeOpaque(byte[] bytes, int len) {
        xdrEncodeOpaque(bytes, 0, len);
    }

    /**
     * Encodes (aka "serializes") a XDR opaque value, which is represented
     * by a vector of byte values. The length of the opaque value is written
     * to the XDR stream, so the receiver does not need to know
     * the exact length in advance. The encoded data is always padded to be
     * a multiple of four to maintain XDR alignment.
     *
     */
    public void xdrEncodeDynamicOpaque(byte [] opaque) {
        xdrEncodeInt(opaque.length);
        xdrEncodeOpaque(opaque, 0, opaque.length);
    }

    public void xdrEncodeBoolean(boolean bool) {
        xdrEncodeInt( bool ? 1 : 0);
    }

    /**
     * Encodes (aka "serializes") a long (which is called a "hyper" in XDR
     * babble and is 64&nbsp;bits wide) and write it down this XDR stream.
     */
    public void xdrEncodeLong(long value) {
        ensureCapacity(SIZE_OF_LONG);
       _body.putLong(value);
    }

    public void xdrEncodeByteBuffer(ByteBuffer buf) {
        buf.flip();
        int len = buf.remaining();
        int padding = (4 - (len & 3)) & 3;
        xdrEncodeInt(len);
        ensureCapacity(len+padding);
        _body.put(buf);
        _body.position(_body.position() + padding);
    }

    public void close() {
        _body.tryDispose();
    }

    private void ensureCapacity(int size) {
        if (_body.remaining() < size) {
            int oldCapacity = _body.capacity();
            int newCapacity = Math.max((oldCapacity * 3) / 2 + 1, oldCapacity + size);
            if (_body instanceof CompositeBuffer) {
                Buffer addon = MemoryManager.DEFAULT_MEMORY_MANAGER.allocate(size - _body.capacity());
                ((CompositeBuffer) _body).append(addon);
            } else {
                _body = MemoryManager.DEFAULT_MEMORY_MANAGER.reallocate(_body, newCapacity);
            }
        }
    }
}
