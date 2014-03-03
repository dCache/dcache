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

import org.dcache.delegation.gridsite2.DelegationException;

/**
 * A CredentialDelegationStore provide storage for on-going delegations.
 * The delegation process requires two iterations between client and server
 * before a delegated credential is created.  After the server has supplied the
 * Certificate Signing Request and before the client has replied with the
 * certificate, some CredentalDelegationStore will hold the CredentialDelegation
 * object that represents this incomplete delegated credential.
 */
public interface CredentialDelegationStore
{
    /**
     * Fetch the matching in-progress CredentialDelegation.  If there is
     * no matching CredentialDelegation then DelegationException is thrown.
     */
    public CredentialDelegation get(DelegationIdentity id)
            throws DelegationException;

    /**
     * Add a CredentialDelegation to this store.  Throws DelegationException if
     * there is already an incomplete delegation with the same
     * DelegationIdentity as that of delegation.
     */
    public void add(CredentialDelegation delegation) throws DelegationException;

    /**
     * Remove the on-going delegation request with this id and returns it.
     * Throws an exception if there is no CredentialDelegation for this id.
     */
    public CredentialDelegation remove(DelegationIdentity id) throws DelegationException;

    /**
     * Remove any on-going delegation request with this id.  Does nothing
     * if there is no CredentialDelegation for this id.
     */
    public void removeIfPresent(DelegationIdentity id);

    /**
     * Establish whether there is any on-going delegation for this id.
     */
    public boolean has(DelegationIdentity id);
}
