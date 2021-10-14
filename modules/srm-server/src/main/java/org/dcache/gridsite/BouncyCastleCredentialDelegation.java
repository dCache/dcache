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
import eu.emi.security.authn.x509.impl.KeyAndCertCredential;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import org.dcache.delegation.gridsite2.DelegationException;
import org.dcache.gsi.X509DelegationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An in-progress credential delegation that uses BouncyCastle.
 */
public class BouncyCastleCredentialDelegation implements CredentialDelegation {

    private static final Logger LOGGER = LoggerFactory.getLogger(
          BouncyCastleCredentialDelegation.class);

    private final DelegationIdentity _id;
    private final X509Certificate[] _certificates;
    private final String _pemRequest;

    protected final KeyPair _keyPair;

    BouncyCastleCredentialDelegation(KeyPair keypair, DelegationIdentity id,
          X509Certificate[] certificates)
          throws DelegationException {
        _id = id;
        _certificates = certificates;
        _keyPair = keypair;

        try {
            _pemRequest = X509DelegationHelper.createRequest(certificates, keypair);
        } catch (GeneralSecurityException e) {
            LOGGER.error("Failed to create CSR: {}", e.toString());
            throw new DelegationException("cannot create certificate-signing" +
                  " request: " + e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Failed to convert CSR to PEM: {}", e.toString());
            throw new DelegationException("cannot PEM-encode certificate-" +
                  "signing request: " + e.getMessage());
        }
    }

    @Override
    public String getCertificateSigningRequest() {
        return _pemRequest;
    }

    @Override
    public DelegationIdentity getId() {
        return _id;
    }

    @Override
    public X509Credential acceptCertificate(String encodedCertificate) throws DelegationException {
        try {
            X509Certificate[] newCertificates
                  = X509DelegationHelper.finalizeChain(encodedCertificate,
                  _certificates);
            return new KeyAndCertCredential(_keyPair.getPrivate(), newCertificates);
        } catch (KeyStoreException e) {
            LOGGER.error("Failed to create delegated credential: {}", e.getMessage());
            throw new DelegationException(
                  "Unable to create delegated credential: " + e.getMessage());
        } catch (GeneralSecurityException e) {
            LOGGER.debug("Bad certificate: {}", e.getMessage());
            throw new DelegationException(
                  "Supplied certificate is unacceptable: " + e.getMessage());
        }
    }
}
