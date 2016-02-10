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
package org.dcache.gridsite;

import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.impl.CertificateUtils;
import eu.emi.security.authn.x509.impl.KeyAndCertCredential;
import eu.emi.security.authn.x509.proxy.ProxyCSRGenerator;
import eu.emi.security.authn.x509.proxy.ProxyCertificateOptions;
import org.bouncycastle.openssl.PEMWriter;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import java.util.stream.Stream;

import org.dcache.delegation.gridsite2.DelegationException;

/**
 * An in-progress credential delegation that uses BouncyCastle.
 */
public class BouncyCastleCredentialDelegation implements CredentialDelegation
{
    private static final Logger LOG = LoggerFactory.getLogger(BouncyCastleCredentialDelegation.class);

    private final DelegationIdentity _id;
    private final X509Certificate[] _certificates;
    private final String _pemRequest;

    protected final KeyPair _keyPair;


    BouncyCastleCredentialDelegation(KeyPair keypair, DelegationIdentity id, X509Certificate[] certificates)
            throws DelegationException
    {
        _id = id;
        _certificates = certificates;
        _keyPair = keypair;

        try {
            _pemRequest = pemEncode(createRequest(certificates, keypair));
        } catch (GeneralSecurityException e) {
            LOG.error("Failed to create CSR: {}", e.toString());
            throw new DelegationException("cannot create certificate-signing" +
                    " request: " + e.getMessage());
        } catch (IOException e) {
            LOG.error("Failed to convert CSR to PEM: {}", e.toString());
            throw new DelegationException("cannot PEM-encode certificate-" +
                    "signing request: " + e.getMessage());
        }
    }

    private static PKCS10CertificationRequest createRequest(X509Certificate[] chain,
            KeyPair keyPair) throws GeneralSecurityException
    {
        ProxyCertificateOptions options = new ProxyCertificateOptions(chain);
        options.setPublicKey(keyPair.getPublic());
        options.setLimited(true);
        return ProxyCSRGenerator.generate(options, keyPair.getPrivate()).getCSR();
    }

    private static String pemEncode(Object item) throws IOException
    {
        StringWriter writer = new StringWriter();
        try (PEMWriter pem = new PEMWriter(writer)) {
            pem.writeObject(item);
        }
        return writer.toString();
    }

    @Override
    public String getCertificateSigningRequest()
    {
        return _pemRequest;
    }

    @Override
    public DelegationIdentity getId()
    {
        return _id;
    }

    @Override
    public X509Credential acceptCertificate(String encodedCertificate) throws DelegationException
    {
        X509Certificate certificate;
        try {
            certificate = CertificateUtils.loadCertificate(
                    new ByteArrayInputStream(encodedCertificate.getBytes(StandardCharsets.UTF_8)),
                    CertificateUtils.Encoding.PEM);
        } catch (IOException e) {
            LOG.debug("Bad certificate: {}", e.getMessage());
            throw new DelegationException("Supplied certificate is unacceptable: " + e.getMessage());
        }

        X509Certificate[] newCertificates =
                Stream.concat(Stream.of(certificate), Stream.of(_certificates))
                        .toArray(X509Certificate[]::new);
        try {
            return new KeyAndCertCredential(_keyPair.getPrivate(), newCertificates);
        } catch (KeyStoreException e) {
            LOG.error("Failed to create delegated credential: {}", e.getMessage());
            throw new DelegationException("Unable to create delegated credential: " + e.getMessage());
        }
    }
}
