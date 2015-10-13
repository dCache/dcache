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
package org.dcache.ftp.client.extended;

import eu.emi.security.authn.x509.X509Credential;
import org.globus.common.ChainedIOException;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.gsi.gssapi.auth.GSSAuthorization;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.dcache.ftp.client.GridFTPSession;
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
    protected static final int TIMEOUT = 120000;

    protected final FTPControlChannel inner;

    protected final GSSContext context;

    private Reply lastReply;

    /**
     * Creates an encrypted control channel wrapping an unencrypted control channel.
     * The constructor will establish a common security context with the server.
     */
    public GridFTPControlChannel(FTPControlChannel inner, X509Credential credential, int protection, Authorization authorization)
            throws IOException, ServerException
    {
        super(inner.getHost(), inner.getPort());
        this.inner = inner;
        this.context = authenticate(credential, protection, authorization);
    }

    /**
     * Performs authentication with specified user credentials and
     * a specific username (assuming the user dn maps to the passed username).
     *
     * @throws IOException     on i/o error
     * @throws ServerException on server refusal or faulty server behavior
     */
    private GSSContext authenticate(X509Credential credential, int protection, Authorization authorization)
            throws IOException, ServerException
    {
        GSSContext context;
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

            GlobusGSSCredentialImpl gssCredential = new GlobusGSSCredentialImpl(
                    new org.globus.gsi.X509Credential(credential.getKey(), credential.getCertificateChain()),
                    GSSCredential.INITIATE_ONLY);

            GSSName expectedName = null;
            if (authorization instanceof GSSAuthorization) {
                GSSAuthorization auth = (GSSAuthorization) authorization;
                expectedName = auth.getExpectedName(gssCredential, getHost());
            }

            GSSManager manager = ExtendedGSSManager.getInstance();
            context = manager.createContext(expectedName,
                                            GSSConstants.MECH_OID,
                                            gssCredential,
                                            GSSContext.DEFAULT_LIFETIME);
            context.requestCredDeleg(true);
            context.requestConf(protection == GridFTPSession.PROTECTION_PRIVATE);

            Reply reply;
            byte[] inToken = new byte[0];
            do {
                byte[] outToken = context.initSecContext(inToken, 0, inToken.length);
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
                byte[] outToken = context.initSecContext(inToken, 0, inToken.length);
                if (outToken != null || !context.isEstablished()) {
                    throw new ServerException(ServerException.WRONG_PROTOCOL, "Unexpected GSI handshake completion.");
                }
            }
        } catch (GSSException e) {
            throw new ChainedIOException("Authentication failed", e);
        } catch (FTPReplyParseException e) {
            throw ServerException.embedFTPReplyParseException(e, "Received faulty reply to ADAT.");
        }
        if (authorization != null) {
            try {
                authorization.authorize(context, getHost());
            } catch (AuthorizationException e) {
                throw new ChainedIOException("Authorization failed", e);
            }
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
        try {
            Reply reply = inner.read();
            if (reply.getCode() != 632 && reply.getCode() != 633) {
                throw ServerException.embedUnexpectedReplyCodeException(
                        new UnexpectedReplyCodeException(reply), "Expected 632 or 633 reply.");
            }
            byte[] token = Base64.getDecoder().decode(reply.getMessage());
            lastReply = new Reply(new BufferedReader(new StringReader(new String(context.unwrap(token, 0, token.length, null)))));
            return lastReply;
        } catch (GSSException e) {
            throw new IOException("Failed to decrypt reply: " + e.getMessage(), e);
        }
    }

    @Override
    public void abortTransfer()
    {
        inner.abortTransfer();
    }

    @Override
    public void write(Command cmd) throws IOException, IllegalArgumentException
    {
        try {
            byte[] bytes = cmd.toString().getBytes(StandardCharsets.US_ASCII);
            byte[] token = context.wrap(bytes, 0, bytes.length, null);
            inner.write(new Command(context.getConfState() ? "ENC" : "MIC", Base64.getEncoder().encodeToString(token)));
        } catch (GSSException e) {
            throw new IOException("Failed to encrypt command: " + e.getMessage(), e);
        }
    }
}
