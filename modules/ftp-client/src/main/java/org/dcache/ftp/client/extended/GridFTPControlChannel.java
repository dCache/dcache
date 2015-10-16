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
package org.dcache.ftp.client.extended;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Base64;

import org.dcache.dss.DssContext;
import org.dcache.dss.DssContextFactory;
import org.dcache.dss.SslEngineDssContext;
import org.dcache.ftp.client.exception.FTPReplyParseException;
import org.dcache.ftp.client.exception.ServerException;
import org.dcache.ftp.client.exception.UnexpectedReplyCodeException;
import org.dcache.ftp.client.vanilla.Command;
import org.dcache.ftp.client.vanilla.FTPControlChannel;
import org.dcache.ftp.client.vanilla.Flag;
import org.dcache.ftp.client.vanilla.Reply;

/**
 * GridFTP control channel wraps a vanilla control channel and
 * adds GSI encryption.
 */
public class GridFTPControlChannel extends FTPControlChannel
{
    protected final FTPControlChannel inner;

    protected final DssContext context;

    protected final HostnameVerifier hostnameVerifier = SSLConnectionSocketFactory.getDefaultHostnameVerifier();

    protected Reply lastReply;

    /**
     * Creates an encrypted control channel wrapping an unencrypted control channel.
     * The constructor will establish a common security context with the server.
     */
    public GridFTPControlChannel(FTPControlChannel inner, DssContextFactory factory, String expectedHostName)
            throws IOException, ServerException
    {
        super(inner.getHost(), inner.getPort());
        this.inner = inner;
        this.context = authenticate(factory, expectedHostName);
    }

    /**
     * Performs authentication with specified user credentials and
     * a specific username (assuming the user dn maps to the passed username).
     *
     * @throws IOException     on i/o error
     * @throws ServerException on server refusal or faulty server behavior
     */
    private DssContext authenticate(DssContextFactory factory, String expectedHostName)
            throws IOException, ServerException
    {
        DssContext context;
        try {
            try {
                Reply reply = inner.exchange(new Command("AUTH", "GSSAPI"));
                if (!Reply.isPositiveIntermediate(reply)) {
                    throw ServerException.embedUnexpectedReplyCodeException(
                            new UnexpectedReplyCodeException(reply),
                            "Server refused GSSAPI authentication.");
                }
            } catch (FTPReplyParseException rpe) {
                throw ServerException.embedFTPReplyParseException(
                        rpe, "Received faulty reply to AUTH GSSAPI.");
            }

            context = factory.create(inner.getRemoteAddress(), inner.getLocalAddress());

            Reply reply;
            byte[] inToken = new byte[0];
            do {
                byte[] outToken = context.init(inToken);
                reply = inner.exchange(new Command("ADAT", Base64.getEncoder().encodeToString(outToken != null ? outToken : new byte[0])));
                if (reply.getMessage().startsWith("ADAT=")) {
                    inToken = Base64.getDecoder().decode(reply.getMessage().substring(5));
                } else {
                    inToken = new byte[0];
                }
            } while (Reply.isPositiveIntermediate(reply) && !context.isEstablished());

            if (!Reply.isPositiveCompletion(reply)) {
                throw ServerException.embedUnexpectedReplyCodeException(
                        new UnexpectedReplyCodeException(reply), "Server failed GSI handshake.");
            }

            if (inToken.length > 0 || !context.isEstablished()) {
                byte[] outToken = context.init(inToken);
                if (outToken != null || !context.isEstablished()) {
                    throw new ServerException(ServerException.WRONG_PROTOCOL, "Unexpected GSI handshake completion.");
                }
            }

            SSLSession session = ((SslEngineDssContext) context).getSSLSession();
            if (!this.hostnameVerifier.verify(expectedHostName, session)) {
                final Certificate[] certs = session.getPeerCertificates();
                final X509Certificate x509 = (X509Certificate) certs[0];
                final X500Principal x500Principal = x509.getSubjectX500Principal();
                throw new SSLPeerUnverifiedException("Host name '" + expectedHostName + "' does not match " +
                                                     "the certificate subject provided by the peer (" + x500Principal.toString() + ")");
            }
        } catch (FTPReplyParseException e) {
            throw ServerException.embedFTPReplyParseException(e, "Received faulty reply to ADAT.");
        }
        return context;
    }

    @Override
    public String getHost()
    {
        return inner.getHost();
    }

    @Override
    public int getPort()
    {
        return inner.getPort();
    }

    @Override
    public InetSocketAddress getLocalAddress()
    {
        return inner.getLocalAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress()
    {
        return inner.getRemoteAddress();
    }

    @Override
    public boolean isIPv6()
    {
        return inner.isIPv6();
    }

    @Override
    public void open() throws IOException, ServerException
    {
        throw new UnsupportedOperationException("GridFTPControlChannel wraps existing control channel and cannot be opened.");
    }

    @Override
    public Reply getLastReply()
    {
        return lastReply;
    }

    @Override
    public void close() throws IOException
    {
        inner.close();
    }

    @Override
    public void waitFor(Flag aborted, int ioDelay,
                        int maxWait) throws ServerException, IOException, InterruptedException
    {
        inner.waitFor(aborted, ioDelay, maxWait);
    }

    @Override
    public Reply read() throws ServerException, IOException, FTPReplyParseException, EOFException
    {
        Reply reply = inner.read();
        if (reply.getCode() != 632 && reply.getCode() != 633) {
            throw ServerException.embedUnexpectedReplyCodeException(
                    new UnexpectedReplyCodeException(reply), "Expected 632 or 633 reply.");
        }
        byte[] token = Base64.getDecoder().decode(reply.getMessage());
        lastReply = new Reply(new BufferedReader(new StringReader(new String(context.unwrap(token)))));
        return lastReply;
    }

    @Override
    public void abortTransfer()
    {
        inner.abortTransfer();
    }

    @Override
    public void write(Command cmd) throws IOException, IllegalArgumentException
    {
        byte[] bytes = cmd.toString().getBytes(StandardCharsets.US_ASCII);
        byte[] token = context.wrap(bytes, 0, bytes.length);
        inner.write(new Command("ENC", Base64.getEncoder().encodeToString(token)));
    }
}
