/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.gsi;

import org.eclipse.jetty.io.ArrayByteBufferPool;
import org.eclipse.jetty.io.ByteBufferPool;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import java.nio.ByteBuffer;

/**
 * SSLEngine decorator that provides limited capability to inject additional
 * communication with the client hidden from the caller of the SSLEngine.
 *
 * The class is tailored for implementing GSI credential delegation while
 * separating GSI logic from the low level SSLEngine wrapping and unwrapping
 * protocol.
 */
public class InterceptingSSLEngine extends ForwardingSSLEngine
{
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);
    private static final ByteBufferPool POOL = new ArrayByteBufferPool();
    private final SSLEngine delegate;

    private enum State { SEND, RECEIVE, PASSTHROUGH }

    private State state = State.PASSTHROUGH;

    private Callback callback;
    private ByteBuffer out;
    private ByteBuffer in;

    public InterceptingSSLEngine(SSLEngine delegate)
    {
        this.delegate = delegate;
    }

    @Override
    protected SSLEngine delegate()
    {
        return delegate;
    }

    /**
     * Receives one SSL frame worth of data and calls the callback when done.
     */
    public void receive(Callback callback)
    {
        this.state = State.RECEIVE;
        this.callback = callback;
        int size = getSession().getApplicationBufferSize();
        this.in = POOL.acquire(size, false);
        this.in.limit(size);
    }

    /**
     * Sends the data {@code out} and then receives one SSL frame worth of data and calls
     * the callback.
     */
    public void sendThenReceive(ByteBuffer out, Callback callback)
    {
        receive(callback);
        this.out = out;
        this.state = State.SEND;
    }

    @Override
    public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int length,
                                ByteBuffer dst) throws SSLException
    {
        SSLEngineResult result;
        switch (state) {
        case SEND:
            result = delegate().wrap(out, dst);
            if (result.getStatus() != SSLEngineResult.Status.OK) {
                return result;
            }
            if (delegate().getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                return result;
            }
            if (out.hasRemaining()) {
                return new SSLEngineResult(SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NEED_WRAP, 0, result.bytesProduced());
            }
            out = null;
            state = State.RECEIVE;
            return new SSLEngineResult(SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NEED_UNWRAP, 0, result.bytesProduced());

        case RECEIVE:
            result = delegate().wrap(EMPTY, dst);
            if (result.getStatus() != SSLEngineResult.Status.OK) {
                return result;
            }
            if (delegate().getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                return result;
            }
            return new SSLEngineResult(SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NEED_UNWRAP, 0, result.bytesProduced());

        default:
            return delegate().wrap(srcs, offset, length, dst);
        }
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts,
                                  int offset, int length) throws SSLException
    {
        SSLEngineResult result;
        switch (state) {
        case SEND:
            result = delegate().unwrap(src, dsts);
            if (result.getStatus() != SSLEngineResult.Status.OK && result.getStatus() != SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                return result;
            }
            if (result.bytesProduced() != 0) {
                delegate().closeOutbound();
                throw new SSLHandshakeException("Received unexpected data from client during handshake.");
            }
            if (delegate().getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                return result;
            }
            return new SSLEngineResult(SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NEED_WRAP,
                                       result.bytesConsumed(), 0);

        case RECEIVE:
            result = delegate().unwrap(src, in);
            if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                delegate().closeOutbound();
                throw new SSLHandshakeException("Received over sized data from client during handshake.");
            }
            if (result.getStatus() != SSLEngineResult.Status.OK) {
                return result;
            }
            if (delegate().getHandshakeStatus() != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                return result;
            }
            if (result.bytesProduced() == 0) {
                return new SSLEngineResult(SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NEED_UNWRAP,
                                           result.bytesConsumed(), 0);
            }
            Callback callback = this.callback;
            ByteBuffer in = this.in;
            this.callback = null;
            this.in = null;
            this.state = State.PASSTHROUGH;
            try {
                callback.call(in);
            } catch (SSLException e) {
                delegate().closeOutbound();
                throw e;
            } finally {
                POOL.release(in);
            }
            switch (state) {
            case SEND:
                return new SSLEngineResult(SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NEED_WRAP,
                                           result.bytesConsumed(), 0);
            case RECEIVE:
                return new SSLEngineResult(SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NEED_UNWRAP,
                                           result.bytesConsumed(), 0);
            default:
                return new SSLEngineResult(SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.FINISHED,
                                           result.bytesConsumed(), 0);
            }

        default:
            return delegate().unwrap(src, dsts, offset, length);
        }
    }

    @Override
    public void closeInbound() throws SSLException
    {
        state = State.PASSTHROUGH;
        callback = null;
        POOL.release(in);
        out = null;
        delegate().closeInbound();
    }

    @Override
    public void closeOutbound()
    {
        state = State.PASSTHROUGH;
        delegate().closeOutbound();
    }

    @Override
    public SSLEngineResult.HandshakeStatus getHandshakeStatus()
    {
        SSLEngineResult.HandshakeStatus handshakeStatus = delegate().getHandshakeStatus();
        if (handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            return handshakeStatus;
        }
        switch (state) {
        case SEND:
            return SSLEngineResult.HandshakeStatus.NEED_WRAP;
        case RECEIVE:
            return SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
        }
        return handshakeStatus;
    }

    public interface Callback
    {
        void call(ByteBuffer buffer) throws SSLException;
    }
}
