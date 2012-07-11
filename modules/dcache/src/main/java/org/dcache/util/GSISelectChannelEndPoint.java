package org.dcache.util;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import org.dcache.util.SimpleGSIEngine.GSIEngineResult;
import org.dcache.util.SimpleGSIEngine.HandshakeStatus;
import org.dcache.util.SimpleGSIEngine.ResultStatus;
import org.eclipse.jetty.io.Buffers;
import org.eclipse.jetty.io.Buffer;
import org.eclipse.jetty.io.EofException;
import org.eclipse.jetty.io.nio.NIOBuffer;
import org.eclipse.jetty.io.nio.SelectChannelEndPoint;
import org.eclipse.jetty.io.nio.SelectorManager.SelectSet;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server side endpoint for asynchronous Jetty GSI connector. When filling
 * or flushing its buffers (which roughly corresponds to receiving network
 * data and sending network data), it uses the SimpleGSIEngine to either
 * complete SSL/GSI handshake or wrap/unwrap the data.
 *
 * The two important methods are fill and flush. Fill is called when there is
 * incoming network data from the client, in which case network data is
 * unwrapped and filled into the passed buffer. Jetty will make the unwrapped
 * data available to the application.
 *
 * Flush is called when the application needs to write data to the client. The
 * application data in the buffer will be wrapped and flushed into the passed
 * buffer, which jetty will send to the network peer.
 *
 * In both cases, a SSL/GSI handshake will be performed, if no secure context
 * has yet been established. For that, special in- and out-buffers are used
 * to send handshake data to the client and receive it from the client.
 * @see SimpleGSIEngine
 * @author tzangerl
 *
 */
public class GSISelectChannelEndPoint extends SelectChannelEndPoint
{
    private final static Logger _logger =
        LoggerFactory.getLogger(GSISelectChannelEndPoint.class);

    /**
     * Buffer pool for input and output buffers
     */
    private final Buffers _buffers;
    /** Manages context, provideds wrapping/unwrapping methods */
    private SimpleGSIEngine _engine;
    /** input buffer for reading encrypted content. */
    private volatile NIOBuffer _inNIOBuffer;
    /** buffer to write data back to the user */
    private volatile NIOBuffer _outNIOBuffer;

    private volatile boolean _closing = false;

    public GSISelectChannelEndPoint(Buffers buffers,
                                    SocketChannel channel,
                                    SelectSet selectSet,
                                    SelectionKey key,
                                    GSSContext context) throws IOException {
        super(channel, selectSet, key);
        _engine = new SimpleGSIEngine(context);
        _buffers = buffers;
    }

    /**
     * Fill the buffer by unwrapping encrypted network data received from
     * the client. Jetty will pass the data in the filled buffer to the
     * application.
     *
     * Handshake, if secure context not yet established.
     * Handshaking will not fill any data into the buffer.
     *
     * (copied from org.eclipse.jetty.io.EndPoint)
     * Fill the buffer from the current putIndex to it's capacity from whatever
     * byte source is backing the buffer. The putIndex is increased if bytes
     * filled. The buffer may chose to do a compact before filling.
     *
     * @param buffer to be filled by unwrapped network data.
     * @return an int value indicating the number of bytes filled or -1 if EOF
     * is reached.
     */
    @Override
    public int fill(Buffer buffer) throws IOException {
        ByteBuffer byteBuffer = extractInputBuffer(buffer);
        int totalFilled = 0;

        synchronized(byteBuffer) {
            int originalLength = buffer.length();
            needInBuffer();

            try {
                /* loop needs to run until handshake complete */
                loop: while(true) {
                    int filled = 0;

                    if (_inNIOBuffer.space() > 0) {
                        filled = fillWithEncryptedBytes();

                        if (!_inNIOBuffer.hasContent()) {
                            /* wait for more data to arrive */
                            break loop;
                        }
                    }

                    ByteBuffer inBuffer = _inNIOBuffer.getByteBuffer();
                    inBuffer.position(_inNIOBuffer.getIndex());
                    inBuffer.limit(_inNIOBuffer.putIndex());

                    GSIEngineResult result;

                    switch (_engine.getHandshakeStatus()) {

                    case CH_ESTABLISHED:

                        result = _engine.unwrap(inBuffer, byteBuffer);
                        break;

                    case CH_INITIAL:
                    case CH_NOTDONEYET:

                        needOutBuffer(inBuffer.capacity()*2);
                        ByteBuffer outBuffer = _outNIOBuffer.getByteBuffer();

                        /* this does not fill any bytes into the fill-buffer */
                        result = _engine.handshake(inBuffer, outBuffer);

                        /* synchronize the NIO buffer's read position with the
                         * data read by the byte buffer
                         */
                        _outNIOBuffer.setGetIndex(0);
                        _outNIOBuffer.setPutIndex(result.getBytesProduced());
                        /* push the data back to the client (if needed) */
                        flush();
                        freeOutBuffer();
                        break;

                    case CH_CLOSED:

                        _closing = true;
                        break loop;

                    default:
                        throw new IllegalStateException("Unknown handshake status " +
                                                        _engine.getHandshakeStatus());
                    }

                    /* synchronize the buffer with the backing byte-buffer
                     * upon which all read operations have happened
                     */
                    _inNIOBuffer.setGetIndex(inBuffer.position());
                    inBuffer.clear();

                    switch (result.getStatus()) {

                    case BUFFER_UNDERFLOW:

                        /* not enough bytes in buffer, no bytes added this
                         * iteration either.
                         * No point in continuing.
                         */
                        if (filled == 0) {
                            break loop;
                        }

                        if (!isOpen()) {
                            _logger.warn("Received incomplete SSL request and" +
                                         " channel is closed. Can not proceed.");
                            inBuffer.clear();
                            _inNIOBuffer.clear();

                            if (_outNIOBuffer != null) {
                                _outNIOBuffer.clear();
                            }

                            throw new EofException("Incomplete GSI request and " +
                                                   "connection is closed!");
                        }

                        break;

                    case BUFFER_OVERFLOW:

                        _logger.warn("Buffer overflow occurred when reading SSL package!");
                        throw new IOException("Problem writing back SSL package " +
                                              "to the channel!");

                    case CLOSED:

                        _closing = true;
                        break;

                    }
                } /* end try */
            } catch (GSSException gssex) {
                _logger.error("Got a GSSException while filling the buffer: {}",
                              gssex.getMessage());
                /* if a GSSException occurs once, the context will be unusable.
                 * Tear down everything.
                 */
                throw new IOException("Connection down due to GSSException",
                                      gssex);
            } finally {
                buffer.setPutIndex(byteBuffer.position());
                byteBuffer.position(0);
                freeInBuffer();
            }

            totalFilled = (buffer.length() - originalLength);
        } /* end synchronization */

        return totalFilled;
    }

    private synchronized void needOutBuffer(int size)
    {
        if (_outNIOBuffer==null || _outNIOBuffer.capacity() < size) {
            _outNIOBuffer = null;
            _logger.debug("Getting an output buffer with size {}", size);
            _outNIOBuffer=(NIOBuffer)_buffers.getBuffer(size);
        }
    }

    private synchronized void needInBuffer()
    {
        if(_inNIOBuffer==null) {
            _inNIOBuffer=(NIOBuffer)_buffers.getBuffer();
        }
    }

    /**
     * Check if there is any output in the out-buffer that needs to be flushed
     *
     * @return true, if there is output in the out-buffer
     */
    @Override
    public boolean isBufferingOutput()
    {
        final NIOBuffer b=_outNIOBuffer;
        return b==null?false:b.hasContent();
    }

    /**
     * Flush the application data in the passed buffers. Wrap the input data using
     * SimpleGSIEngine and flush it to the network.
     * Wraps are currently not done as gathering wraps, neither is writing the
     * output data.
     *
     * Handshake first, if necessary.
     *
     * (copied from org.eclipse.jetty.io.EndPoint)
     * Flush the buffer from the current getIndex to it's putIndex using whatever byte
     * sink is backing the buffer. The getIndex is updated with the number of bytes flushed.
     * Any mark set is cleared.
     * If the entire contents of the buffer are flushed, then an implicit empty() is done.
     * The passed header/trailer buffers are written before/after the contents of this buffer. This may be done
     * either as gather writes, as a poke into this buffer or as several writes. The implementation is free to
     * select the optimal mechanism.
     * @param header A buffer to write before flushing this buffer. This buffers getIndex is updated.
     * @param buffer The buffer to flush. This buffers getIndex is updated.
     * @param trailer A buffer to write after flushing this buffer. This buffers getIndex is updated.
     * @return the total number of bytes written.
     */
    @Override
    public int flush(Buffer header, Buffer buffer, Buffer trailer) throws IOException {
        int consumed=0;
        int available = (header == null) ? 0 : header.length();
        if (buffer != null) {
            available += buffer.length();
        }

        int outBufferSize = available +
            ((trailer == null)?0:trailer.length());
        outBufferSize *= 2;

        needOutBuffer(outBufferSize);

        try {
            loop: while (true) {
                if (_outNIOBuffer.length() > 0) {
                    flush();

                    if (isBufferingOutput()) {
                        break loop;
                    }
                }

                switch (_engine.getHandshakeStatus()) {

                case CH_INITIAL:
                case CH_NOTDONEYET:

                    needInBuffer();
                    ByteBuffer outBuffer = _outNIOBuffer.getByteBuffer();
                    ByteBuffer inBuffer = _inNIOBuffer.getByteBuffer();

                    /* this does not fill any bytes into the fill-buffer */
                    GSIEngineResult result =
                        _engine.handshake(inBuffer, outBuffer);
                    _logger.debug("handshake wrote {} bytes into the outbuffer",
                                 result.getBytesProduced());

                    switch (result.getStatus()) {

                    case OK:
                        /* synchronize the NIO buffer's read/write position with
                         * the byte buffer via which all read/write operations
                         * happened
                         */
                        _inNIOBuffer.setGetIndex(inBuffer.position());
                        _outNIOBuffer.setGetIndex(0);
                        _outNIOBuffer.setPutIndex(result.getBytesProduced());
                        inBuffer.clear();
                        /* push the data back to the client */
                        flush();
                        freeInBuffer();
                        break;

                    case BUFFER_UNDERFLOW:

                        /* need input data, call to fill */
                        freeInBuffer();
                        break loop;

                    case BUFFER_OVERFLOW:

                        freeInBuffer();
                        throw new IOException("Problem writing back SSL handshake package " +
                                              "to the channel!");

                    case CLOSED:

                        freeInBuffer();
                        _closing = true;
                        break loop;

                    }

                    break;

                case CH_ESTABLISHED:

                    int c = 0;


                    if (header!=null && header.length()>0) {
                        if (buffer!=null && buffer.length()>0) {
                            //c=wrap(header,buffer);
                            c=wrap(header);
                            flush();
                            c+=wrap(buffer);
                        } else {
                            c=wrap(header);
                        }
                    } else if (buffer != null && buffer.length() > 0) {
                        c=wrap(buffer);
                    }

                    /* my understanding is that jetty will never pass a trailer,
                     * maybe include an assert here?
                     */

                    _logger.debug("Wrapping body consumed {} bytes", c);

                    if (c > 0) {
                        consumed+=c;
                        available-=c;
                    } else {

                        if (consumed==0) {
                            consumed=-1;
                        }

                        break loop;
                    }


                    break;

                case CH_CLOSED:

                    if (_closing || available==0) {
                        if (consumed==0) {
                            consumed= -1;
                        }

                        break loop;
                    }

                    break;

                }

                flush();

                if (isBufferingOutput()) {
                    break loop;
                }
            }

        } catch (GSSException gssex) {
            _logger.error("Got a GSSException while flushing the buffer: {}",
                          gssex.getMessage());
            throw new IOException("Connection down due to GSSException.",
                                  gssex);
        }

        if (_outNIOBuffer != null) {
            freeOutBuffer();
        }

        return consumed;
    }

    /**
     * @see #flush(Buffer, Buffer, Buffer)
     */
    @Override
    public int flush(Buffer buffer) throws IOException
    {
        return flush(buffer, null, null);
    }

    /**
     * Use SelectChannelEndPoint's fill method to fill the _inNIOBuffer with
     * encrypted network bytes.
     * @throws IOException if we couldn't fill any network data into the
     *                     _inNIOBuffer and the connection is closed
     */
    private int fillWithEncryptedBytes() throws IOException {
        if (_inNIOBuffer.hasContent()) {
            _inNIOBuffer.compact();
        } else {
            _inNIOBuffer.clear();
        }

        int totalFilled=0;
        int lengthBeforeFill = _inNIOBuffer.length();

        // loop filling as much encrypted data as we can into the buffer
        while (_inNIOBuffer.space() > 0 && isOpen()) {
            try {
                int filled=super.fill(_inNIOBuffer);
                _logger.debug("Filled buffer with {} bytes from network",
                              filled);

                // break the loop if no progress is made (we have read everything
                // there is to read).
                if (filled <= 0) {
                    break;
                }

                totalFilled+=filled;
            } catch(IOException e) {
                if (_inNIOBuffer.length()==lengthBeforeFill) {
                    if (_outNIOBuffer!=null) {
                        _outNIOBuffer.clear();
                        freeOutBuffer();
                    }

                    throw e;
                }
                break;
            }
        }

        // If we have no progress and no data
        if (totalFilled==0 && _inNIOBuffer.length() ==  lengthBeforeFill) {
            if(!isOpen()) {
                if (_outNIOBuffer != null) {
                    freeOutBuffer();
                }
                throw new EofException();
            }
            return -1;
        }

        return totalFilled;
    }

    private ByteBuffer extractInputBuffer(Buffer buffer) throws IOException
    {
        if (!(buffer instanceof NIOBuffer)) {
            throw new IOException("Error extracting the input buffer!");
        }

        NIOBuffer nbuf=(NIOBuffer)buffer;
        ByteBuffer bbuf=nbuf.getByteBuffer();
        bbuf.position(buffer.putIndex());
        return bbuf;
    }

    private ByteBuffer extractOutputBuffer(Buffer buffer)
    {
        if (buffer.buffer() instanceof NIOBuffer) {
            return ((NIOBuffer) buffer.buffer()).getByteBuffer();
        }

        return ByteBuffer.wrap(buffer.array());
    }




    private synchronized void freeOutBuffer()
    {
        if (!isBufferingOutput()) {
            _buffers.returnBuffer(_outNIOBuffer);
            _outNIOBuffer=null;
        }
    }

    private synchronized void freeInBuffer()
    {
        _buffers.returnBuffer(_inNIOBuffer);
        _inNIOBuffer=null;
    }

    /**
     * Use SimpleGSIEngine to wrap the contents of the passed buffer.
     * @param buffer The buffer containing the content that should be wrapped
     * @return number of bytes consumed from the buffer
     * @throws IOException Wrapping the content failed
     * @throws GSSException Wrapping the content failed
     */
    private int wrap(final Buffer buffer)
        throws IOException, GSSException {
        ByteBuffer byteBuffer = extractOutputBuffer(buffer);

        synchronized(byteBuffer) {
            byteBuffer.position(buffer.getIndex());
            byteBuffer.limit(buffer.putIndex());

            int consumed=0;
            ByteBuffer outBuffer=_outNIOBuffer.getByteBuffer();
            synchronized(outBuffer) {
                try {
                    _outNIOBuffer.clear();
                    outBuffer.position(0);
                    outBuffer.limit(outBuffer.capacity());
                    GSIEngineResult result =
                        _engine.wrap(byteBuffer, outBuffer);

                    switch (result.getStatus()) {
                    case OK:
                        _outNIOBuffer.setGetIndex(0);
                        _outNIOBuffer.setPutIndex(result.getBytesProduced());
                        consumed=result.getBytesConsumed();
                        break;
                    case BUFFER_UNDERFLOW:
                        /* try to fill buffer with more bytes */
                        break;
                    case BUFFER_OVERFLOW:
                        _logger.warn("Could not flush SSL packet due to a buffer overflow." +
                                     "Bytes consumed: " + result.getBytesConsumed() +
                                     ", bytes written: " + result.getBytesProduced());
                        throw new IOException("Tried to write a too large packet. " +
                                              "Seems like a bug.");
                    case CLOSED:
                        _closing = true;
                        break;
                    }

                } finally {
                    /* reset the buffers */
                    outBuffer.position(0);
                    buffer.setGetIndex(byteBuffer.position());
                    byteBuffer.clear();
                }
            }

            return consumed;
        }
    }

    /**
     * Use SimpleGSIEngine to wrap the contents of the passed buffers. This
     * currently happens in sequence and not as a gathering wrap.
     *
     * @param header Buffer containing the application data header
     * @param body Buffer containing the application data body
     * @return number of bytes consumed from the buffers
     * @throws IOException Wrapping failed in the engine
     * @throws GSSException Wrapping failed in the engine
     */
    private int wrap(final Buffer header, final Buffer body)
        throws IOException, GSSException {
        int consumed=0;

        ByteBuffer headerBuffer = extractOutputBuffer(header);

        synchronized(headerBuffer) {
            headerBuffer.position(header.getIndex());
            headerBuffer.limit(header.putIndex());

            ByteBuffer byteBuffer = extractOutputBuffer(body);

            synchronized(byteBuffer) {
                byteBuffer.position(body.getIndex());
                byteBuffer.limit(body.putIndex());

                ByteBuffer outBuffer=_outNIOBuffer.getByteBuffer();
                synchronized(outBuffer) {
                    try {
                        _outNIOBuffer.clear();
                        outBuffer.position(0);
                        outBuffer.limit(outBuffer.capacity());

                        GSIEngineResult result1 =
                            _engine.wrap(headerBuffer, outBuffer);
                        GSIEngineResult result2 = null;
                        int bytesProduced = 0;

                        if (result1.getStatus() == ResultStatus.OK) {

                            bytesProduced = result1.getBytesProduced();
                            consumed += result1.getBytesConsumed();

                             result2 = _engine.wrap(byteBuffer, outBuffer);
                        }

                       GSIEngineResult lastResult =
                           (result2 == null)?result1:result2;

                       switch (lastResult.getStatus()) {
                       /* will only apply if result2 AND result1 are okay */
                       case OK:
                           bytesProduced += lastResult.getBytesProduced();
                           _outNIOBuffer.setGetIndex(0);
                           _outNIOBuffer.setPutIndex(bytesProduced);
                           consumed += lastResult.getBytesConsumed();
                           break;
                       case BUFFER_OVERFLOW:
                           _logger.warn("Could not flush SSL packet due to a buffer overflow." +
                                        "Bytes consumed: " + lastResult.getBytesConsumed() +
                                        ", bytes written: " + lastResult.getBytesProduced());
                           throw new IOException("Tried to write a too large packet. " +
                                                 "Seems like a bug.");
                       case BUFFER_UNDERFLOW:
                           /* try to fill the buffer with more bytes */
                           break;
                       case CLOSED:
                           _closing = true;
                           break;
                       }

                    } finally {
                        /* synchronize the buffers */
                        outBuffer.position(0);
                        header.setGetIndex(headerBuffer.position());
                        headerBuffer.clear();
                        body.setGetIndex(byteBuffer.position());
                        byteBuffer.clear();
                    }
                }

                return consumed;
            }
        }
    }

    /**
     * Send data in the out-buffer to the client.
     *
     * (copied from org.eclipse.jetty.io.EndPoint)
     * Flush any buffered output.
     * May fail to write all data if endpoint is non-blocking
     */
    @Override
    public void flush() throws IOException {
        if (_outNIOBuffer==null) {
            return;
        }

        if (isBufferingOutput()) {
            int flushed = super.flush(_outNIOBuffer);
            if (isBufferingOutput()) {
                // Try again after yield.... cheaper than a reschedule.
                Thread.yield();
                flushed += super.flush(_outNIOBuffer);
            }

            _logger.debug("Flushed {} bytes.", flushed);
        }
    }

    /**
     * Flush all output still left in the output buffer. When done, call close
     * on the SimpleGSIEngine. When this is done, tear down the channel by
     * calling close on SelectChannelEndPoint.
     *
     * (Copied from org.eclipse.jetty.io.EndPoint)
     * Close any backing stream associated with the endpoint
     */
    @Override
    public void close() throws IOException
    {
        _closing = true;

        try {

            /* will cause the connection to be closed after the channel's associated socket's timeout period */
            long end = System.currentTimeMillis() + ((SocketChannel)_channel).socket().getSoTimeout();

            while (isOpen() && (System.currentTimeMillis() < end)) {
                if (isBufferingOutput()) {
                    flush();
                } else if (_engine.getHandshakeStatus() != HandshakeStatus.CH_CLOSED) {
                    _engine.close();
                } else {
                    break;
                }

                /*
                 * can not use a separate scheduled thread which calls this in
                 * regular intervals due to scalability issues.
                 * Unfortunately, SelectChannelEndPoint does not have
                 * a callback mechanism for tearing down the connection either.
                 *
                 * That's why we are currently using Thread.sleep. In the
                 * majority of cases this loop will finish after a maximum
                 * of 3 loop runs.
                 */
                Thread.sleep(100);
            }

        } catch (GSSException gssex) {
            _logger.error("Could not close engine context due to: {}.", gssex);
        } catch (InterruptedException e) {
            _logger.warn("Loop waiting for channel close was interrupted.");
        } finally {
            _logger.debug("Closing the GSI channel finished, calling close() on super-class");
            super.close();
        }
    }

    public GSSContext getContext()
    {
        return _engine.getContext();
    }
}
