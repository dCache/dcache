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

import com.google.common.collect.Iterables;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.dcache.auth.util.GSSUtils;
import org.dcache.delegation.gridsite2.DelegationException;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;

import static org.dcache.gridsite.Utilities.assertThat;

/**
 * The SrmCredentialStore acts as a bridge between the SRM's delegation store
 * and the API expected by GridSite.
 */
public class SrmCredentialStore implements CredentialStore
{
    private RequestCredentialStorage _store;

    @Required
    public void setRequestCredentialStorage(RequestCredentialStorage store)
    {
        _store = store;
    }

    @Override
    public GSSCredential get(DelegationIdentity id) throws DelegationException
    {
        RequestCredential credential =
                _store.getRequestCredential(nameFromId(id), null);
        assertThat(credential != null, "no stored credential", id);
        return credential.getDelegatedCredential();
    }

    @Override
    public void put(DelegationIdentity id, GSSCredential credential)
            throws DelegationException
    {
        try {
            Iterable<String> fqans = GSSUtils.getFQANsFromGSSCredential(credential);
            String primaryFqan = Iterables.getFirst(fqans, null);

            RequestCredential srmCredential = new RequestCredential(nameFromId(id),
                    primaryFqan, credential, _store);
            _store.saveRequestCredential(srmCredential);
        } catch (AuthorizationException | GSSException | RuntimeException e) {
            throw new DelegationException("failed to save credential: " +
                    e.getMessage());
        }
    }

    @Override
    public void remove(DelegationIdentity id) throws DelegationException
    {
        boolean isSuccessful;

        try {
            isSuccessful = _store.deleteRequestCredential(nameFromId(id), null);
        } catch (IOException e) {
            throw new DelegationException("internal problem: " + e.getMessage());
        }

        assertThat(isSuccessful, "no credential", id);
    }

    @Override
    public boolean has(DelegationIdentity id) throws DelegationException
    {
        try {
            return _store.hasRequestCredential(nameFromId(id), null);
        } catch (IOException e) {
            throw new DelegationException("internal problem: " + e.getMessage());
        }
    }

    @Override
    public Calendar getExpiry(DelegationIdentity id) throws DelegationException
    {
        RequestCredential credential =
                _store.getRequestCredential(nameFromId(id), null);

        assertThat(credential != null, "no credential", id);

        Date expiry = new Date(credential.getDelegatedCredentialExpiration());
        Calendar result = Calendar.getInstance();
        result.setTime(expiry);
        return result;
    }


    private static String nameFromId(DelegationIdentity id)
    {
        // Treat the delegation ID 'gsi' as a special case that maps to
        // the storage for this user via GSI.
        if (id.getDelegationId().equals("gsi")) {
            return id.getDn();
        } else {
            return id.getDelegationId() + " " + id.getDn();
        }
    }
}
