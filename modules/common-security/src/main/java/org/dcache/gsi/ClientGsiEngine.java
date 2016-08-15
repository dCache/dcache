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

import com.google.common.io.ByteSource;
import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.proxy.ProxyGenerator;
import eu.emi.security.authn.x509.proxy.ProxyRequestOptions;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Wrapper for SSLEngine that implements GSI delegation. Only the client side of GSI is
 * supported.
 */
public class ClientGsiEngine extends InterceptingSSLEngine
{
    public static final char DELEGATION_CHAR = 'D';
    public static final char NO_DELEGATION_CHAR = '0';

    private boolean isDelegationLimited;
    private X509Credential credential;

    public ClientGsiEngine(SSLEngine delegate, X509Credential credential, boolean isDelegationEnabled, boolean isDelegationLimited)
    {
        super(delegate);
        this.isDelegationLimited = isDelegationLimited;
        this.credential = credential;
        if (isDelegationEnabled) {
            sendThenReceive(ByteBuffer.wrap(new byte[] { DELEGATION_CHAR }), new GotCsr());
        } else {
            send(ByteBuffer.wrap(new byte[]{NO_DELEGATION_CHAR}));
        }
    }

    @Override
    public void setUseClientMode(boolean isClientMode)
    {
        checkArgument(isClientMode, "Only the client side of GSI is supported by this engine.");
        super.setUseClientMode(isClientMode);
    }

    private class GotCsr implements Callback
    {
        private ByteSource data;

        @Override
        public void call(ByteBuffer buffer) throws SSLException
        {
            // read csr
            ByteSource chunk = ByteSource.wrap(buffer.array()).slice(buffer.arrayOffset(), buffer.position());
            ByteSource source = (data == null) ? chunk : ByteSource.concat(data, chunk);
            try {
                PKCS10CertificationRequest csr = new PKCS10CertificationRequest(source.read());

                // generate proxy
                ProxyRequestOptions options = new ProxyRequestOptions(credential.getCertificateChain(), csr);
                options.setLimited(isDelegationLimited);
                X509Certificate[] chain = ProxyGenerator.generate(options, credential.getKey());

                // send to server
                send(ByteBuffer.wrap(chain[0].getEncoded()));
            } catch (EOFException f) {
                try {
                    /* Incomplete - read another frame. */
                    ByteSource copy = ByteSource.wrap(chunk.read());
                    data = (data == null) ? copy : ByteSource.concat(data, copy);
                    receive(this);
                } catch (IOException e) {
                    f.addSuppressed(e);
                }
            } catch (CertificateParsingException | NoSuchAlgorithmException | NoSuchProviderException | SignatureException | CertificateEncodingException | InvalidKeyException | IOException e) {
                throw new SSLException("GSI delegation failed: " + e.toString(), e);
            }
        }
    }
}
