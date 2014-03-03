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

import java.util.Calendar;

import org.dcache.delegation.gridsite2.DelegationException;

/**
 * A CredentialStore provides access to some storage of delegated credentials.
 * The storage should have reasonably long persistence, typically surviving a
 * JVM restart.  For example, it could use the local file-system or a database.
 */
public interface CredentialStore
{
    /**
     * Get the delegated credential for this DelegationIdentity.  Throws a
     * DegationException if there is no valid credential stored.
     */
    public GSSCredential get(DelegationIdentity id) throws DelegationException;

    /**
     * Store a delegated credential against this DelegationIdentity.  Silently
     * replace any delegated credential already stored against this id.
     */
    public void put(DelegationIdentity id, GSSCredential credential)
            throws DelegationException;

    /**
     * Remove the delegated credential stored against this id.  Throws a
     * DelegationException if there is no valid credential currently stored.
     */
    public void remove(DelegationIdentity id) throws DelegationException;

    /**
     * Check whether there is a delegated credential stored against this id.
     * @throws DelegationException if there's some problem with the underlying
     * storage
     */
    public boolean has(DelegationIdentity id) throws DelegationException;

    /**
     * Provide the expiry date for the delegated credential stored against
     * this DelegatedIdentity.  Throws an exception if there is no credential
     * stored against this identity or if the credential never expires.
     */
    public Calendar getExpiry(DelegationIdentity id) throws DelegationException;
}
