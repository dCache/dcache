/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014-2015 Deutsches Elektronen-Synchrotron
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

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * SSLEngine decorator that implements legacy GSI framing.
 *
 * The class auto-detects whether the client is using the framing format and
 * responds in kind.
 */
public class GsiFrameEngine extends ForwardingSSLEngine
{
    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);
    private static final int MAX_LEN = 32 * 1024 * 1024;

    private final ServerGsiEngine gsiEngine;
    private SSLEngine currentDelegate;

    public GsiFrameEngine(ServerGsiEngine delegate)
    {
        gsiEngine = delegate;
        currentDelegate = new FrameDetectingEngine();
    }

    /**
     * Determines if a given header is a SSLv3 packet
     * (has a SSL header) or a backward compatible version of TLS
     * using the same header format.
     *
     * @return true if the header is a SSLv3 header. False, otherwise.
     */
    private static boolean isSSLv3Packet(byte[] header)
    {
        return header[0] >= 20 && header[0] <= 26 &&
               (header[1] == 3 || (header[1] == 2 && header[2] == 0));
    }

    /**
     * Determines if a given header is a SSLv2 client or server hello packet
     *
     * @return true if the header is such a SSLv2 client or server hello
     *         packet. False, otherwise.
     */
    private static boolean isSSLv2HelloPacket(byte[] header)
    {
        return ((header[0] & 0x80) != 0 && (header[2] == 1 || header[2] == 4));
    }

    @Override
    protected SSLEngine delegate()
    {
        return currentDelegate;
    }

    private class FrameDetectingEngine extends ForwardingSSLEngine
    {
        private final byte[] header = new byte[4];

        @Override
        protected SSLEngine delegate()
        {
            return gsiEngine;
        }

        @Override
        public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int length, ByteBuffer dst) throws SSLException
        {
            throw new SSLException("Cannot wrap during frame detecting phase.");
        }

        public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) throws SSLException
        {
            if (src.remaining() < 4) {
                return new SSLEngineResult(SSLEngineResult.Status.BUFFER_UNDERFLOW, getHandshakeStatus(), 0, 0);
            }
            src.mark();
            try {
                src.get(header);
                if (isSSLv3Packet(header)) {
                    currentDelegate = gsiEngine;
                } else if (isSSLv2HelloPacket(header)) {
                    currentDelegate = gsiEngine;
                } else {
                    currentDelegate = new FrameEngine();
                }
            } finally {
                src.reset();
            }
            return currentDelegate.unwrap(src, dsts, offset, length);
        }
    }

    private class FrameEngine extends ForwardingSSLEngine
    {
        private ByteBuffer buffer = EMPTY;
        private SSLSession session = new Session();

        @Override
        protected SSLEngine delegate()
        {
            return gsiEngine;
        }

        @Override
        public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int length, ByteBuffer dst) throws SSLException
        {
            ByteBuffer tmp = ByteBuffer.allocate(dst.remaining() - 4);
            SSLEngineResult result = delegate().wrap(srcs, offset, length, tmp);
            if (result.bytesProduced() == 0) {
                return result;
            } else {
                dst.order(ByteOrder.BIG_ENDIAN);
                dst.putInt(result.bytesProduced());
                dst.put(tmp);
                return new SSLEngineResult(result.getStatus(), result.getHandshakeStatus(),
                                           result.bytesConsumed(), 4 + result.bytesProduced());
            }
        }

        @Override
        public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) throws SSLException
        {
            int bytesConsumed = read(src);
            int bytesProduced = 0;

            SSLEngineResult result;
            do {
                result = delegate().unwrap(buffer, dsts, offset, length);
                bytesProduced += result.bytesProduced();
            } while (result.getStatus() == SSLEngineResult.Status.OK);

            return new SSLEngineResult(result.getStatus(), result.getHandshakeStatus(), bytesConsumed, bytesProduced);
        }

        private int read(ByteBuffer src) throws SSLException
        {
            int bytesConsumed = 0;
            if (src.remaining() >= 4) {
                src.mark();
                src.order(ByteOrder.BIG_ENDIAN);
                int len = src.getInt();
                if (len > MAX_LEN) {
                    closeOutbound();
                    throw new SSLException("Token length " + len + " > " + MAX_LEN);
                } else if (len < 0) {
                    closeOutbound();
                    throw new SSLException("Token length " + len + " < 0");
                }
                if (src.remaining() >= len) {
                    int existingBytes = buffer.remaining();
                    int newBytes = src.remaining();
                    byte[] newBuffer = new byte[existingBytes + newBytes];
                    buffer.get(newBuffer, 0, existingBytes);
                    src.get(newBuffer, existingBytes, newBytes);
                    buffer = ByteBuffer.wrap(newBuffer);
                    bytesConsumed = existingBytes + 4;
                } else {
                    src.reset();
                }
            }
            return bytesConsumed;
        }

        @Override
        public SSLSession getSession()
        {
            return session;
        }

        private class Session extends ForwardingSSLSession
        {
            @Override
            protected SSLSession delegate()
            {
                return FrameEngine.super.getSession();
            }

            public int getPacketBufferSize()
            {
                return super.getPacketBufferSize() + 4;
            }
        }
    }
}
