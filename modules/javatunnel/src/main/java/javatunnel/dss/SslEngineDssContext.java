/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package javatunnel.dss;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.auth.Subject;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.cert.CertPath;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

public class SslEngineDssContext implements DssContext
{
    private static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[0]);

    private final SSLEngine engine;

    private final CertificateFactory cf;

    private boolean isClientModeSet;

    /** Token data received from the peer. */
    private ByteBuffer inToken;

    /** Token data to be delivered to the peer. */
    private ByteBuffer outToken;

    /** Application data received from the peer. */
    private ByteBuffer data;

    private Subject subject;

    public SslEngineDssContext(SSLEngine engine, CertificateFactory cf)
    {
        this.engine = engine;
        this.cf = cf;
        data = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
        outToken = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
    }

    private void addInToken(byte[] token)
    {
        if (inToken == null || inToken.remaining() == 0) {
            inToken = ByteBuffer.wrap(token);
        } else if (inToken.capacity() - inToken.remaining() >= token.length) {
            inToken.compact();
            inToken.put(token);
            inToken.flip();
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(inToken.remaining() + token.length);
            buffer.put(inToken);
            buffer.put(token);
            buffer.flip();
            inToken = buffer;
        }
    }

    private byte[] getOutToken()
    {
        return getBytes(outToken);
    }

    private void handshake() throws IOException
    {
        while (!isEstablished()) {
            switch (engine.getHandshakeStatus()) {
            case NOT_HANDSHAKING:
            case FINISHED:
                throw new IllegalStateException("Not handshaking");
            case NEED_TASK:
                Runnable task = engine.getDelegatedTask();
                if (task != null) {
                    task.run();
                }
                break;
            case NEED_WRAP:
                wrap(EMPTY);
                break;
            case NEED_UNWRAP:
                if (!unwrap()) {
                    return;
                }
                break;
            }
        }
    }

    private void wrap(ByteBuffer data) throws IOException
    {
        SSLEngineResult result;
        do {
            result = engine.wrap(data, outToken);
            switch (result.getStatus()) {
            case BUFFER_UNDERFLOW:
                throw new RuntimeException();
            case BUFFER_OVERFLOW:
                ByteBuffer buffer = ByteBuffer.allocate(outToken.capacity() + engine.getSession().getPacketBufferSize());
                outToken.flip();
                buffer.put(outToken);
                outToken = buffer;
                break;
            case OK:
                if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
                    subject = createSubject();
                }
                break;
            case CLOSED:
                throw new EOFException();
            }
        } while (result.getStatus() != SSLEngineResult.Status.OK);
        if (data.hasRemaining()) {
            throw new RuntimeException("SSLEngine did not wrap all data.");
        }
    }

    private boolean unwrap() throws IOException
    {
        while (true) {
            SSLEngineResult result = engine.unwrap(inToken, data);
            switch (result.getStatus()) {
            case BUFFER_UNDERFLOW:
                return false;
            case BUFFER_OVERFLOW:
                ByteBuffer buffer = ByteBuffer.allocate(
                        outToken.capacity() + engine.getSession().getApplicationBufferSize());
                data.flip();
                buffer.put(data);
                data = buffer;
                break;
            case OK:
                if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
                    subject = createSubject();
                }
                return true;
            case CLOSED:
                throw new EOFException();
            }
        }
    }

    private byte[] getData()
    {
        return getBytes(data);
    }

    private static byte[] getBytes(ByteBuffer buffer)
    {
        byte[] bytes;
        buffer.flip();
        if (!buffer.hasRemaining()) {
            bytes = null;
        } else {
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        }
        buffer.clear();
        return bytes;
    }

    @Override
    public byte[] init(byte[] token) throws IOException
    {
        checkState(!isEstablished());

        if (!isClientModeSet) {
            engine.setUseClientMode(true);
            isClientModeSet = true;
        }

        addInToken(token);
        handshake();
        return getOutToken();
    }

    @Override
    public byte[] accept(byte[] token) throws IOException
    {
        checkState(!isEstablished());

        if (!isClientModeSet) {
            engine.setUseClientMode(false);
            isClientModeSet = true;
        }

        addInToken(token);
        handshake();
        return getOutToken();
    }

    @Override
    public byte[] wrap(byte[] data, int offset, int len) throws IOException
    {
        checkState(isEstablished());
        wrap(ByteBuffer.wrap(data, offset, len));
        return getOutToken();
    }

    @Override
    public byte[] unwrap(byte[] token) throws IOException
    {
        checkState(isEstablished());
        addInToken(token);

        boolean underflow;
        do {
            underflow = !unwrap();
        } while (!underflow && inToken.hasRemaining());

        return getData();
    }

    @Override
    public boolean isEstablished()
    {
        return subject != null;
    }

    @Override
    public Subject getSubject()
    {
        return subject;
    }

    @Override
    public String getPrincipal()
    {
        try {
            return engine.getSession().getPeerPrincipal().getName();
        } catch (SSLPeerUnverifiedException e) {
            return null;
        }
    }

    private Subject createSubject() throws IOException
    {
        try {
            Certificate[] chain = engine.getSession().getPeerCertificates();
            CertPath certPath = cf.generateCertPath(asList(chain));
            return new Subject(false, emptySet(), singleton(certPath), emptySet());
        } catch (SSLPeerUnverifiedException e) {
            throw new IOException("Failed to establish identity of SSL peer: " + e.getMessage(), e);
        } catch (CertificateException e) {
            throw new IOException("Certificate failure: " + e.getMessage(),  e);
        }
    }
}
