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

import com.sun.grizzly.Controller;
import com.sun.grizzly.ProtocolParser;
import com.sun.grizzly.util.WorkerThread;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 *
 * Filter for parsing ONC RPC messages (RFC 1831).
 *
 * After parsing is done we got back complete RPC message even in case of
 * multiple fragment messages.
 */
public class RpcProtocolPaser implements ProtocolParser<Xdr> {

    private final static Logger _log = LoggerFactory.getLogger(RpcProtocolPaser.class);

    /*
     * RPC: 1831
     *     Remote Procedure Call Protocol Specification Version 2
     *
     * When RPC messages are passed on top of a byte stream transport
     * protocol (like TCP), it is necessary to delimit one message from
     * another in order to detect and possibly recover from protocol errors.
     * This is called record marking (RM).  One RPC message fits into one RM
     * record.
     *
     * A record is composed of one or more record fragments.  A record
     * fragment is a four-byte header followed by 0 to (2**31) - 1 bytes of
     * fragment data.  The bytes encode an unsigned binary number; as with
     * XDR integers, the byte order is from highest to lowest.  The number
     * encodes two values -- a boolean which indicates whether the fragment
     * is the last fragment of the record (bit value 1 implies the fragment
     * is the last fragment) and a 31-bit unsigned binary value which is the
     * length in bytes of the fragment's data.  The boolean value is the
     * highest-order bit of the header; the length is the 31 low-order bits.
     *
     */
    /** RPC fragment record marker mask */
    private final static int RPC_LAST_FRAG = 0x80000000;
    /** RPC fragment size mask */
    private final static int RPC_SIZE_MASK = 0x7fffffff;

    /**
     * Xdr which we try to construct.
     */
    private Xdr _xdr = null;

    /**
     * are we processing last fragment of RPC message?
     */
    private boolean _lastFragment = false;

    /** number of bytes which we we still have to read in the current fragment */
    private int _fragmentToRead = 0;

    /** position within buffer */
    private int _nextMessageStartPosition = 0;

    private ByteBuffer _buffer = null;
    private boolean _expectingMoreData;

    /**
     *
     * @see com.sun.grizzly.ProtocolParser#isExpectingMoreData()
     */
    @Override
    public boolean isExpectingMoreData() {
        _log.debug("isExpectingMoreData {}", _expectingMoreData);
        return _expectingMoreData;
    }

    /**
     *
     * @see com.sun.grizzly.ProtocolParser#hasMoreBytesToParse()
     */
    @Override
    public boolean hasMoreBytesToParse() {

        boolean rc = _buffer != null &&
            !_expectingMoreData &&
            _buffer.position() > _nextMessageStartPosition;

        _log.debug("hasMoreBytesToParse {}, buffer : {}, next read at: {}",
                new Object[]{rc, _buffer, _nextMessageStartPosition});
        return rc;
    }

    /**
     *
     * @see com.sun.grizzly.ProtocolParser#getNextMessage()
     */
    @Override
    public Xdr getNextMessage() {
        _log.debug("messate retrieved");
        _lastFragment = false;
        _fragmentToRead = 0;
        Xdr xdr = _xdr;
        _xdr = null;
        return xdr;
    }

    /**
     *
     * @see com.sun.grizzly.ProtocolParser#hasNextMessage()
     */
    @Override
    public boolean hasNextMessage() {

        /*
         * do we have some data to process?
         */
        if (_buffer == null) {
            _log.debug("hasNextMessage false");
            return false;
        }

        if( Thread.currentThread() instanceof WorkerThread ) {
            /*
             * we are runnig inside grizzly
             */
            Controller.Protocol protocol = (Controller.Protocol)((WorkerThread)Thread.currentThread()).getAttachment().getAttribute(ProtocolKeeperFilter.CONNECTION_PROTOCOL);
            if( protocol != null && protocol == Controller.Protocol.UDP ) {
                _log.debug("UDP XDR packet");
                /*
                 * UDP packets arriving in one go.
                 */
                ByteBuffer b = _buffer.duplicate();
                b.limit(_buffer.position());
                b.position(0);
                _nextMessageStartPosition = b.remaining();
                _xdr = new XdrBuffer(Xdr.MAX_XDR_SIZE);
                _xdr.fill(b);
                _expectingMoreData = false;
                return true;
            }
        }
        _expectingMoreData = true;
        ByteBuffer bytes = _buffer.duplicate();
        bytes.position(_nextMessageStartPosition);
        bytes.order(ByteOrder.BIG_ENDIAN);

        /*
         * It may happen that single buffer will contain multiple fragments.
         * Loop over the buffer content till we get complete message or buffer
         * has no more data.
         */
        while (_expectingMoreData ) {

            /*
             * do not go more that available data
             */
            bytes.limit( _buffer.position() );

            if( ! bytes.hasRemaining() ) break;

            if (_fragmentToRead == 0) {

                /*
                 * if it's a beginning of a message and  do we have at least 4 bytes
                 * for message size let's wait
                 */
                if (_xdr == null && bytes.remaining() < 4) {
                    _log.debug("hasNextMessage false (short read)");
                    return false;
                }

                _fragmentToRead = bytes.getInt();
                _nextMessageStartPosition += 4;
                _lastFragment = (_fragmentToRead & RPC_LAST_FRAG) != 0;
                _fragmentToRead &= RPC_SIZE_MASK;
                _log.debug("Fragment : lenght = {}, last = {}", _fragmentToRead, _lastFragment);
            }

            int n = Math.min(_fragmentToRead, bytes.remaining());
            _nextMessageStartPosition += n;

            bytes.limit(bytes.position() + n);
            if (_xdr == null) {
                _log.debug("allocating a new buffer for XDR message");
                _xdr = new Xdr(Xdr.MAX_XDR_SIZE);
            }
            _xdr.fill(bytes);

            _fragmentToRead -= n;

            /*
             * we are done with current XDR if all bytes of last fragment are
             * received
             */
            _expectingMoreData = !(_fragmentToRead == 0 && _lastFragment);
        }

        _log.debug("hasNextMessage {}", !_expectingMoreData);

        return !_expectingMoreData;
    }

    /**
     *
     * @see com.sun.grizzly.ProtocolParser#startBuffer(java.nio.ByteBuffer buffer)
     */
    @Override
    public void startBuffer(ByteBuffer buffer) {
        _log.debug("new buffer: {}. Next message position {}", buffer, _nextMessageStartPosition);
        _buffer = buffer;
        _buffer.order(ByteOrder.BIG_ENDIAN);
    }

    /**
     *
     * @see com.sun.grizzly.ProtocolParser#releaseBuffer()
     */
    @Override
    public boolean releaseBuffer() {

        /*
         * if there is no more data in the current buffer
         * return it back.
         *
         * the next buffer will be a fresh one and we will start
         * to process it from the beginning
         */
        if ( !hasMoreBytesToParse() ) {
            _nextMessageStartPosition = 0;
            _buffer.clear();
            _log.debug( "reseting buffer prior release: {}", _buffer );
            _buffer = null;
        }
        _log.debug("releaseBuffer: usesame = {}, current position = {}",
                _expectingMoreData, _nextMessageStartPosition);
        return _expectingMoreData;
    }

    @Override
    public String toString() {

        String str = String.format("hasMoreBytesToParse %s, expectingMoreData %s, pos %d, nextp %d",
            hasMoreBytesToParse(),
            _expectingMoreData,
            _buffer == null ? -1: _buffer.position(),
            _nextMessageStartPosition);

        return str;

    }
}
