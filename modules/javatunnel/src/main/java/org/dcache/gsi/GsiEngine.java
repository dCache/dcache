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
import eu.emi.security.authn.x509.impl.CertificateUtils;
import eu.emi.security.authn.x509.impl.KeyAndCertCredential;
import eu.emi.security.authn.x509.proxy.ProxyCSRGenerator;
import eu.emi.security.authn.x509.proxy.ProxyCertificateOptions;
import org.bouncycastle.asn1.ASN1InputStream;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Wrapper for SSLEngine that implements GSI delegation. Only the server side of GSI is
 * supported. The code is partly taken from org.globus.gsi.gssapi.GlobusGSSContextImpl.
 */
public class GsiEngine extends InterceptingSSLEngine
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GsiEngine.class);

    public static final String X509_CREDENTIAL = "org.dcache.credential";

    /** The character sent on the wire to request delegation */
    public static final char DELEGATION_CHAR = 'D';

    private KeyPairCache keyPairCache = new KeyPairCache(30, TimeUnit.SECONDS);

    private final CertificateFactory cf;

    private boolean isUsingLegacyClose;
    private KeyPair keyPair;

    public GsiEngine(SSLEngine delegate, CertificateFactory cf)
    {
        super(delegate);
        this.cf = cf;
        receive(new GotDelegationCharacter());
    }

    @Override
    public void closeOutbound()
    {
        /* Our SRM client (or rather JGlobus) doesn't like a proper SSL shutdown. Throwing an exception
         * here forces Jetty to abruptly close the socket just like in old versions of Jetty.
         */
        if (isUsingLegacyClose) {
            throw new RuntimeException("Deliberately force the connection to close to workaround client bugs.");
        }
        super.closeOutbound();
    }

    public boolean isUsingLegacyClose()
    {
        return isUsingLegacyClose;
    }

    public void setUsingLegacyClose(boolean usingLegacyClose)
    {
        this.isUsingLegacyClose = usingLegacyClose;
    }

    public void setKeyPairCache(KeyPairCache cache)
    {
        keyPairCache = cache;
    }

    private ByteBuffer getCertRequest() throws SSLPeerUnverifiedException, GeneralSecurityException
    {
        X509Certificate[] chain = CertificateUtils.convertToX509Chain(getSession().getPeerCertificates());
        int bits = ((RSAPublicKey) chain[0].getPublicKey()).getModulus().bitLength();
        keyPair = keyPairCache.getKeyPair(bits);
        ProxyCertificateOptions options = new ProxyCertificateOptions(chain);
        options.setPublicKey(keyPair.getPublic());
        options.setLimited(true);
        byte[] req = ProxyCSRGenerator.generate(options, keyPair.getPrivate()).getCSR().getEncoded();
        return ByteBuffer.wrap(req, 0, req.length);
    }

    protected void verifyDelegatedCert(X509Certificate certificate)
            throws GeneralSecurityException
    {
        RSAPublicKey pubKey = (RSAPublicKey) certificate.getPublicKey();
        RSAPrivateKey privKey = (RSAPrivateKey) keyPair.getPrivate();
        if (!pubKey.getModulus().equals(privKey.getModulus())) {
            throw new GeneralSecurityException("Client delegated credentials do not match certificate request.");
        }
    }

    private void readDelegatedCredentials(ByteSource source) throws GeneralSecurityException, GSSException, IOException
    {
        SSLSession session = getSession();

        /* Parse the delegated certificate.
         */
        X509Certificate certificate;
        try (InputStream in = source.openStream()) {
            certificate = (X509Certificate) cf.generateCertificate(in);
        }
        LOGGER.trace("Received delegated cert: {}", certificate);

        /* Verify that it matches our certificate request.
         */
        verifyDelegatedCert(certificate);

        /* Build a certificate chain for the delegated certificate.
         */
        Certificate[] chain = session.getPeerCertificates();
        int chainLen = chain.length;
        X509Certificate[] newChain = new X509Certificate[chainLen + 1];
        newChain[0] = certificate;
        for (int i = 0; i < chainLen; i++) {
            newChain[i + 1] = (X509Certificate) chain[i];
        }

        /* Store GSI credentials in the SSL session. Use GsiRequestCustomizer to copy these
         * to the Request objects.
         */
        X509Credential proxy = new KeyAndCertCredential(keyPair.getPrivate(), newChain);
        session.putValue(X509_CREDENTIAL, proxy);
    }

    private class GotDelegationCharacter implements Callback
    {
        @Override
        public void call(ByteBuffer buffer) throws SSLException
        {
            if (buffer.get(0) == DELEGATION_CHAR) {
                try {
                    sendThenReceive(getCertRequest(), new GotDelegatedCredentials());
                } catch (GeneralSecurityException e) {
                    throw new SSLException("GSI delegation failed: " + e.toString(), e);
                }
            }
        }
    }

    private class GotDelegatedCredentials implements Callback
    {
        private int len = 0;
        private ByteSource data;

        @Override
        public void call(ByteBuffer buffer) throws SSLException
        {
            checkArgument(buffer.hasArray(), "Buffer must have backing array");

            len += buffer.position();
            ByteSource chunk = ByteSource.wrap(buffer.array()).slice(buffer.arrayOffset(), buffer.position());
            ByteSource source = (data == null) ? chunk : ByteSource.concat(data, chunk);
            try {
                readDelegatedCredentials(source);
            } catch (GeneralSecurityException | GSSException | IOException e) {
                /* Check if we got the entire BER encoded object. We rely on the fact that the delegated
                 * credential is transferred in its own SSL frames - i.e. buffer doesn't contain any
                 * application data.
                 *
                 * Relying on an EofException isn't the most elegant solution, but the alternative would
                 * be to implement a custom BER parser (REVISIT: check sun.security.provider.X509Factory
                 * for a possibly cheaper way to read the entire certificate - we would have to copy the code
                 * to get access to the relevant bits).
                 */
                try {
                    try (ASN1InputStream in = new ASN1InputStream(source.openStream(), len, true)) {
                        in.readObject();
                    } catch (EOFException f) {
                        /* Incomplete - read another frame. */
                        ByteSource copy = ByteSource.wrap(chunk.read());
                        data = (data == null) ? copy : ByteSource.concat(data, copy);
                        receive(this);
                        return;
                    }
                } catch (IOException f) {
                    e.addSuppressed(f);
                }

                throw new SSLException("GSI delegation failed: " + e.toString(), e);
            }
        }
    }
}
