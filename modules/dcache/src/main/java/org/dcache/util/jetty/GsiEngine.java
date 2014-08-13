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
package org.dcache.util.jetty;

import org.bouncycastle.jce.provider.X509CertificateObject;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.X509Credential;
import org.globus.gsi.bc.BouncyCastleCertProcessingFactory;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.GlobusGSSException;
import org.globus.gsi.gssapi.KeyPairCache;
import org.globus.gsi.util.CertificateLoadUtil;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;

import static com.google.common.base.Preconditions.checkArgument;
import static org.globus.axis.gsi.GSIConstants.GSI_CREDENTIALS;

/**
 * Wrapper for SSLEngine that implements GSI delegation. Only the server side of GSI is
 * supported. The code is partly taken from org.globus.gsi.gssapi.GlobusGSSContextImpl.
 */
public class GsiEngine extends InterceptingSSLEngine
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GsiEngine.class);

    private static final KeyPairCache KEY_PAIR_CACHE = KeyPairCache.getKeyPairCache();
    private static final BouncyCastleCertProcessingFactory CERT_FACTORY = BouncyCastleCertProcessingFactory.getDefault();

    private boolean isUsingLegacyClose;
    private KeyPair keyPair;

    public GsiEngine(SSLEngine delegate)
    {
        super(delegate);
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

    private ByteBuffer getCertRequest() throws SSLPeerUnverifiedException, GeneralSecurityException
    {
        Certificate[] chain = getSession().getPeerCertificates();
        X509Certificate cert = (X509Certificate) chain[0];
        int bits = ((RSAPublicKey) cert.getPublicKey()).getModulus().bitLength();
        keyPair = KEY_PAIR_CACHE.getKeyPair(bits);
        byte[] req = CERT_FACTORY.createCertificateRequest(cert, keyPair);
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

    private static X509Certificate bcConvert(X509Certificate cert)
            throws GSSException
    {
        if (!(cert instanceof X509CertificateObject)) {
            try {
                return CertificateLoadUtil.loadCertificate(new ByteArrayInputStream(cert.getEncoded()));
            } catch (Exception e) {
                throw new GlobusGSSException(GSSException.FAILURE, e);
            }
        } else {
            return cert;
        }
    }

    private void readDelegatedCredentials(ByteBuffer buffer) throws GeneralSecurityException, GSSException, IOException
    {
        checkArgument(buffer.hasArray(), "Buffer must have backing array");

        SSLSession session = getSession();

        /* Parse the delegated certificate.
         */
        buffer.flip();
        byte[] buf = buffer.array();
        int len = buffer.remaining();
        int off = buffer.arrayOffset();
        ByteArrayInputStream inStream = new ByteArrayInputStream(buf, off, len);
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate certificate = (X509Certificate) cf.generateCertificate(inStream);
        inStream.close();
        LOGGER.trace("Received delegated cert: {}", certificate);

        /* Verify that it matches our certificate request.
         */
        verifyDelegatedCert(certificate);

        /* Build a certificate chain for the delegated certificate.
         */
        Certificate[] chain = session.getPeerCertificates();
        int chainLen = chain.length;
        X509Certificate[] newChain = new X509Certificate[chainLen + 1];
        newChain[0] = bcConvert(certificate);
        for (int i = 0; i < chainLen; i++) {
            newChain[i + 1] = bcConvert((X509Certificate) chain[i]);
        }

        /* Store GSI attributes in the SSL session. Use GsiRequestCustomizer to copy these
         * to the Request objects.
         */
        X509Credential proxy =
                new X509Credential(this.keyPair.getPrivate(), newChain);
        GSSCredential delegCred =
                new GlobusGSSCredentialImpl(proxy, GSSCredential.INITIATE_AND_ACCEPT);

        session.putValue(GSI_CREDENTIALS, delegCred);
    }

    private class GotDelegationCharacter implements Callback
    {
        @Override
        public void call(ByteBuffer buffer) throws SSLException
        {
            if (buffer.get(0) == GSIConstants.DELEGATION_CHAR) {
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
        @Override
        public void call(ByteBuffer buffer) throws SSLException
        {
            try {
                readDelegatedCredentials(buffer);
            } catch (GeneralSecurityException | GSSException | IOException e) {
                throw new SSLException("GSI delegation failed: " + e.toString(), e);
            }
        }
    }
}
