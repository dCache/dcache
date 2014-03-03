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

import org.ietf.jgss.GSSCredential;

import org.dcache.delegation.gridsite2.DelegationException;

/**
 * A CredentialDelegation represents the process of creating a delegated
 * credential.  At any time, a CredentialDelegation object is either incomplete
 * or complete.  When created, it is initially incomplete.
 * <p>
 * When incomplete, calls to {@code getCertificateSigningRequest} will provide
 * a CSR that contains the public key of a key-pair.  The delegator should sign
 * this with the private key of their credential, creating a certificate.  The
 * resulting certificate should be provided by calling
 * {@code acceptCertificate}.  If successful, this call will complete the
 * CredentialDelegation and the resulting credential will be returned.
 */
public interface CredentialDelegation
{
    /**
     * Obtain the certificate signing request to send to the delegator.
     */
    public String getCertificateSigningRequest();

    /**
     * Provide the identity of the anticipated delegated credential.
     */
    public DelegationIdentity getId();

    /**
     * Complete a in-progress CredentialDelegation with the supplied
     * certificate and return the delegated credential.  Throws a
     * DelegationException if the certificate is invalid or a certificate has
     * already been accepted.
     */
    public GSSCredential acceptCertificate(String certificate)
            throws DelegationException;
}
