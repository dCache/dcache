/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 - 2017 Deutsches Elektronen-Synchrotron
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

import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.impl.CertificateUtils;
import eu.emi.security.authn.x509.impl.KeyAndCertCredential;
import eu.emi.security.authn.x509.proxy.ProxyCSRGenerator;
import eu.emi.security.authn.x509.proxy.ProxyCertificateOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertPath;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;
import java.util.stream.Stream;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

/**
 * Common code for creating a delegation request and finalizing the delegated proxy credential.
 */
public final class X509DelegationHelper {

    public static X509Delegation newDelegation(CertPath path,
          KeyPairCache keyPairs)
          throws NoSuchAlgorithmException, NoSuchProviderException {
        X509Certificate[] certificates = path.getCertificates().toArray(X509Certificate[]::new);
        if (certificates.length == 0) {
            throw new IllegalArgumentException("Certificate path is empty.");
        }

        X509Certificate first = certificates[0];
        int bits = ((RSAPublicKey) first.getPublicKey()).getModulus().bitLength();
        return new X509Delegation(keyPairs.getKeyPair(bits), certificates);
    }

    public static String createRequest(X509Certificate[] chain, KeyPair keyPair)
          throws GeneralSecurityException, IOException {
        ProxyCertificateOptions options = new ProxyCertificateOptions(chain);
        options.setPublicKey(keyPair.getPublic());
        options.setLimited(true);
        return pemEncode(ProxyCSRGenerator.generate(options,
                    keyPair.getPrivate())
              .getCSR());
    }

    public static X509Credential acceptCertificate(String encodedCertificate,
          X509Delegation delegation)
          throws GeneralSecurityException {
        X509Certificate[] certificates = finalizeChain(encodedCertificate,
              delegation.getCertificates());
        return new KeyAndCertCredential(delegation.getKeyPair().getPrivate(),
              certificates);
    }

    public static X509Certificate[] finalizeChain(String encodedCertificate,
          X509Certificate[] certificates)
          throws GeneralSecurityException {
        X509Certificate certificate;
        try {
            certificate = CertificateUtils.loadCertificate(
                  new ByteArrayInputStream(
                        encodedCertificate.getBytes(
                              StandardCharsets.UTF_8)),
                  CertificateUtils.Encoding.PEM);
        } catch (IOException e) {
            throw new GeneralSecurityException("Supplied certificate is unacceptable: "
                  + e.getMessage());
        }

        return Stream.concat(Stream.of(certificate),
              Stream.of(certificates)).toArray(
              X509Certificate[]::new);
    }

    private static String pemEncode(Object item) throws IOException {
        StringWriter writer = new StringWriter();
        try (JcaPEMWriter pem = new JcaPEMWriter(writer)) {
            pem.writeObject(item);
        }
        return writer.toString();
    }

    private X509DelegationHelper() {
    } // static utility
}
