package org.dcache.util;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.globus.gsi.gssapi.SSLUtil;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.MessageProp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple engine for processing the common operations needed to establish
 * GSI encrypted traffic.
 *
 * This class is similar to Sun's SSLEngine, but much simpler because it calls
 * JGlobus' GSSContext implementation for handshaking and wrap/unwrap operation.
 *
 * SimpleGSIEnginge checks whether all information needed as input to the
 * methods of GSSContext is present in the buffers. For that, it reads and
 * parses the SSL headers itself. If not all input is present, it reports
 * according status codes to the callers. Additionally it maintains the
 * handshake state for callers.
 *
 * Something that could be implemented in the future is a gathering wrap
 * operation, like SSLEngine provides. This would also be needed by
 * GSISelectChannelEndPoint.
 *
 * @see SSLEngine
 * @author tzangerl
 *
 */
public class SimpleGSIEngine {

    private final static Logger _logger =
        LoggerFactory.getLogger(SimpleGSIEngine.class);

    /** this is for SSLv3 large records (allow twice the payload) */
    private static final int SSLV3_MAX_PAYLOAD_SIZE = 32 * 1024;
    private static final int SSLV2_MAX_PAYLOAD_SIZE = 16 * 1024;

    /** 32MB - GSI payload size limit */
    private static final int GSI_MAX_PAYLOAD_SIZE = 32 * 1024 * 1024;

    public enum HandshakeStatus { CH_INITIAL,
                                   CH_NOTDONEYET,
                                   CH_ESTABLISHED,
                                   CH_CLOSED };

    public enum ResultStatus { OK,
                               BUFFER_UNDERFLOW,
                               BUFFER_OVERFLOW,
                               CLOSED };

    public enum GSIMode { SSL_MODE, GSI_MODE };

    private GSSContext _context;

    private volatile HandshakeStatus _handshakeStatus =
        HandshakeStatus.CH_INITIAL;
    private volatile GSIMode _currentMode = GSIMode.SSL_MODE;

    public SimpleGSIEngine(GSSContext context) {
        _context = context;
    }

    public void setMode(GSIMode mode) {
        _currentMode = mode;
    }

    public GSIMode getMode() {
        return _currentMode;
    }

    public HandshakeStatus getHandshakeStatus() {
        return _handshakeStatus;
    }

    public GSSContext getContext() {
        return _context;
    }

    /**
     * Disposes and deletes the GSS-context and set the handshake status to
     * CH_CLOSED.
     * @throws GSSException Disposing the context fails
     */
    public synchronized void close() throws GSSException {
        if (_context != null) {
            _context.dispose();
            _context = null;
            _handshakeStatus = HandshakeStatus.CH_CLOSED;
        }
    }

    /**
     * Result of a GSI engine operation, contains number of consumed/produced
     * bytes and a status indicating success or failure
     *
     */
    static class GSIEngineResult {
        private int _bytesConsumed;
        private int _bytesProduced;
        private ResultStatus _status;

        public GSIEngineResult(int bytesConsumed, int bytesWritten) {
            _bytesConsumed = bytesConsumed;
            _bytesProduced = bytesWritten;
            _status = ResultStatus.OK;
        }

        public GSIEngineResult(int bytesConsumed,
                               int bytesWritten,
                               ResultStatus status) {
            _bytesConsumed = bytesConsumed;
            _bytesProduced = bytesWritten;
            _status = status;
        }

        public int getBytesConsumed() {
            return _bytesConsumed;
        }

        public int getBytesProduced() {
            return _bytesProduced;
        }

        public ResultStatus getStatus() {
            return _status;
        }
    }

    /**
     * Perform a single SSL/GSI handshake step with the input in buffer src.
     *
     * @param src The buffer with handshake input from the client
     * @param dst buffer to which network data that should be sent to the client
     *            will be written.
     * @return number of bytes produced/consumed and status indicating success
     *         or failure
     * @throws GSSException No context upon which to operate/handshake fails
     * @throws IOException SSL/GSI payload to large or read/write failure
     */
    public synchronized GSIEngineResult handshake(ByteBuffer src, ByteBuffer dst)
        throws GSSException, IOException {

        if (_handshakeStatus == HandshakeStatus.CH_CLOSED) {
            return new GSIEngineResult(0, 0, ResultStatus.CLOSED);
        }

        if (_context == null) {
            throw new GSSException(GSSException.NO_CONTEXT);
        }

        int bytesProduced = 0;
        int bytesConsumed = 0;

        try {
            byte [] inToken;
            inToken = readToken(src);

            if (inToken == null || inToken.length == 0) {
                return new GSIEngineResult(0, 0, ResultStatus.BUFFER_UNDERFLOW);
            }

            bytesConsumed = inToken.length;

            byte [] outToken =
                _context.acceptSecContext(inToken, 0, inToken.length);

            if (outToken != null) {
                byte [] handshakeToken = outToken;

                if (_currentMode == GSIMode.GSI_MODE) {
                    handshakeToken = addGSIHeader(handshakeToken);
                }

                dst.put(handshakeToken);
                bytesProduced = handshakeToken.length;
            }

            if (_context.isEstablished()) {
                _handshakeStatus = HandshakeStatus.CH_ESTABLISHED;
            } else {
                _handshakeStatus = HandshakeStatus.CH_NOTDONEYET;
            }

            return new GSIEngineResult(bytesConsumed, bytesProduced);

        } catch (BufferUnderflowException buex) {
            return new GSIEngineResult(bytesConsumed,
                                       bytesProduced,
                                       ResultStatus.BUFFER_UNDERFLOW);
        } catch (BufferOverflowException boex) {
            return new GSIEngineResult(bytesConsumed,
                                       bytesProduced,
                                       ResultStatus.BUFFER_OVERFLOW);
        }
    }

    /**
     * Unwrap encrypted network data to unencrypted application data using
     * an established SSL/GSI context.
     *
     * NOTE: Synchronization of source and destination buffers must be
     * synchronized by the caller of this method
     *
     * @param src Input buffer from which to read encrypted network data
     * @param dst Output buffer to which to write unencrypted app data
     * @return Number of bytes produced/consumed and status indicating
     *         success or failure
     * @throws GSSException Context invalid or null
     * @throws IOException Packets too large or buffer read/write error
     */
    public GSIEngineResult unwrap(ByteBuffer src, ByteBuffer dst)
        throws GSSException, IOException
    {

        synchronized(this) {
            if (_handshakeStatus == HandshakeStatus.CH_CLOSED) {
                return new GSIEngineResult(0, 0, ResultStatus.CLOSED);
            }
        }

        if (_context == null || !_context.isEstablished()) {
            throw new GSSException(GSSException.NO_CONTEXT);
        }

        MessageProp ignoreProp = new MessageProp(true);

        int bytesConsumed = 0;
        int bytesProduced = 0;

        try {
            byte[] wrapped = readToken(src);

            if (wrapped == null || wrapped.length == 0) {
                return new GSIEngineResult(0,0, ResultStatus.BUFFER_UNDERFLOW);
            }

            bytesConsumed = wrapped.length;

            byte [] result =  _context.unwrap(wrapped,
                                              0,
                                              wrapped.length,
                                              ignoreProp);

            if (result.length == 0) {
                return new GSIEngineResult(bytesConsumed, 0);
            } else {
                dst.put(result);
                bytesProduced = result.length;
                return new GSIEngineResult(bytesConsumed, bytesProduced);
            }
        } catch (BufferUnderflowException buex) {
            return new GSIEngineResult(bytesConsumed,
                                       bytesProduced,
                                       ResultStatus.BUFFER_UNDERFLOW);
        } catch (BufferOverflowException boex) {
            return new GSIEngineResult(bytesConsumed,
                                       bytesProduced,
                                       ResultStatus.BUFFER_OVERFLOW);
        }
    }

    /**
     * Wrap application data using the established SSL/GSI session. Data
     * thusly obtained can be sent to the SSL authenticated peer. This
     * method can return without writing anything into the destination buffer,
     *
     * NOTE: synchronization of source and destination buffers must be
     * performed by the caller of this method
     *
     * @param src The buffer with the unencrypted application data.
     * @param dst The buffer into which the network ready data should be
     *            written
     * @return GSIEngineResult with number of bytes consumed/produced and
     *         the status indicating success of the operation
     * @throws GSSException invalid security context
     * @throws IOException payload to large/writing fails
     */
    public GSIEngineResult wrap(ByteBuffer src, ByteBuffer dst)
        throws GSSException
    {

        synchronized(this) {
            if (_handshakeStatus == HandshakeStatus.CH_CLOSED) {
                return new GSIEngineResult(0, 0, ResultStatus.CLOSED);
            }
        }

        if (_context == null || !_context.isEstablished()) {
            throw new GSSException(GSSException.NO_CONTEXT);
        }

        int bytesConsumed = 0;
        int bytesWritten = 0;

        try {
            MessageProp ignoreProp = new MessageProp(true);

            byte [] contents = new byte[src.remaining()];
            src.get(contents);
            bytesConsumed = contents.length;

            _logger.info("Length before wrapping {}", bytesConsumed);
            byte [] wrapped =  _context.wrap(contents,
                                             0,
                                             contents.length,
                                             ignoreProp);

            _logger.info("Length after wrapping {}", wrapped.length);

            if (wrapped.length == 0) {
                return new GSIEngineResult(bytesConsumed, 0);
            } else  {
                dst.put(wrapped);
                bytesWritten = wrapped.length;
                return new GSIEngineResult(bytesConsumed, bytesWritten);
            }
        } catch (BufferOverflowException boex) {
            return new GSIEngineResult(bytesConsumed,
                                       bytesWritten,
                                       ResultStatus.BUFFER_OVERFLOW);
        } catch (BufferUnderflowException buex) {
            return new GSIEngineResult(bytesConsumed,
                                       bytesWritten,
                                       ResultStatus.BUFFER_UNDERFLOW);
        }
    }

    /**
     * Synchronize whole block, accesses the engine
     * @param in ByteBuffer containing network data.
     * @return handshake token to be consumed by GSSContext handshake
     *         operation or null if not enough data to produce it.
     * @throws BufferUnderflowException If not all the bytes needed to
     *         decipher a handshake token are present
     * @throws IOException Packet is malformed
     */
    private synchronized byte[] readToken(ByteBuffer in)
       throws IOException {

        byte[] buf = null;
        byte [] header = new byte[5];

        /* we know the length that is needed to be read only after reading the
         * header. If the buffer underflows for the number of bytes we need
         * to read, we have to reset it's position to before the header in
         * order to be able to read the header again at the next call.
         */
        in.mark();
        try {
            in.get(header, 0, header.length -1);

            if (SSLUtil.isSSLv3Packet(header)) {
                _logger.debug("SimpleGSIEngine: Received SSLv3Packet");
                setMode(GSIMode.SSL_MODE);

                in.get(header,4,1);

                int len = SSLUtil.toShort(header[3], header[4]);

                /* can't read all now, return. maybe next time */
                if (len > SSLV3_MAX_PAYLOAD_SIZE) {
                    throw new IOException("Packet length according to header is " +
                                          len + " bytes, while SSLv3 allows a maximum of " +
                                          SSLV3_MAX_PAYLOAD_SIZE + " bytes.");
                } else if (len < 0) {
                    throw new IOException("Incorrect header format!");
                }

                buf = new byte[header.length + len];
                System.arraycopy(header, 0, buf, 0, header.length);

                in.get(buf, header.length, len);
            } else if (SSLUtil.isSSLv2HelloPacket(header)) {
                _logger.debug("SimpleGSIEngine: Received SSLv2HelloPacket");
                setMode(GSIMode.SSL_MODE);
                // SSLv2 - assume 2-byte header
                // read extra 2 bytes so subtract it from total len
                int len = (((header[0] & 0x7f) << 8) | (header[1] & 0xff)) - 2;
                buf = new byte[header.length-1 + len];
                System.arraycopy(header, 0, buf, 0, header.length-1);

                /* assume SSLv2Hello means SSLv2 encapsulated SSLv3, apply same
                 * maximum payload size
                 */
                if (len > SSLV2_MAX_PAYLOAD_SIZE) {
                    throw new IOException("Packet length according to header is " +
                                          len + " bytes, while SSLv2 allows a maximum of " +
                                          SSLV2_MAX_PAYLOAD_SIZE + "bytes.");
                } else if (len < 0) {
                    throw new IOException("Incorrect header format!");
                }

                in.get(buf, header.length -1, len);
            } else {
                _logger.debug("SimpleGSIEngine: Received GSI-packet");
                setMode(GSIMode.GSI_MODE);
                int len = SSLUtil.toInt(header, 0);

                if (len > GSI_MAX_PAYLOAD_SIZE) {
                    throw new IOException("Token length " + len + " > " + GSI_MAX_PAYLOAD_SIZE);
                } else if (len < 0) {
                    throw new IOException("Token length " + len + " < 0");
                }

                buf = new byte[len];
                in.get(buf);
            }
        } catch (BufferUnderflowException buex) {
            in.reset();
            throw buex;
        }

        return buf;
    }

    /**
     * Add additional GSI header information
     * @param content byte array with network data
     * @return byte array with GSI header + network data
     */
    private static byte [] addGSIHeader(final byte [] content) {
        byte [] header = new byte[4];
        SSLUtil.writeInt(content.length, header, 0);
        byte [] result = new byte[header.length + content.length];
        System.arraycopy(header, 0, result, 0, header.length);
        System.arraycopy(content, 0, result, header.length, content.length);
        return result;
    }
}

