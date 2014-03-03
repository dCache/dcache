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
package org.dcache.gridsite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPublicKey;

import org.dcache.delegation.gridsite2.DelegationException;

/**
 * The factory class for generating delegated credentials using Bouncy Castle.
 */
public class BouncyCastleCredentialDelegationFactory implements CredentialDelegationFactory
{
    private static final Logger LOG =
            LoggerFactory.getLogger(BouncyCastleCredentialDelegationFactory.class);

    private KeyPairCache _keypairs;

    public void setKeyPairCache(KeyPairCache keyPairs)
    {
        _keypairs = keyPairs;
    }

    @Override
    public CredentialDelegation newDelegation(DelegationIdentity id,
            Iterable<X509Certificate> certificates) throws DelegationException
    {
        X509Certificate first = certificates.iterator().next();

        int bits =
            ((RSAPublicKey)first.getPublicKey()).getModulus().bitLength();

        KeyPair keypair;
        try {
            keypair = _keypairs.getKeyPair(bits);
        } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
            LOG.error("Failed to create key-pair for request: {}",
                    e.getMessage());
            throw new DelegationException("Internal error: cannot create key-pair");
        }

        return new BouncyCastleCredentialDelegation(keypair, id, certificates);
    }
}
