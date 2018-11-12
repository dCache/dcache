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

import eu.emi.security.authn.x509.X509Credential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

import org.dcache.auth.FQAN;
import org.dcache.delegation.gridsite2.DelegationException;
import org.dcache.security.util.X509Credentials;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;
import org.dcache.util.Exceptions;
import org.dcache.util.SqlGlob;

import static org.dcache.gridsite.Utilities.assertThat;

/**
 * The SrmCredentialStore acts as a bridge between the SRM's delegation store
 * and the API expected by GridSite.
 */
public class SrmCredentialStore implements CredentialStore
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SrmCredentialStore.class);

    private RequestCredentialStorage _store;

    @Required
    public void setRequestCredentialStorage(RequestCredentialStorage store)
    {
        _store = store;
    }

    @Override
    public X509Credential get(DelegationIdentity id) throws DelegationException
    {
        RequestCredential credential = _store.getRequestCredential(nameFromId(id));
        assertThat(credential != null, "no stored credential", id);
        return credential.getDelegatedCredential();
    }

    @Override
    public void put(DelegationIdentity id, X509Credential credential, FQAN primaryFqan)
            throws DelegationException
    {
        try {
            RequestCredential srmCredential =
                    new RequestCredential(nameFromId(id), Objects.toString(primaryFqan, null), credential, _store);
            _store.saveRequestCredential(srmCredential);
        } catch (RuntimeException e) {
            LOGGER.warn("Bug detected please report to <support@dcache.org>: ", e);
            throw new DelegationException("failed to save credential: " + Exceptions.messageOrClassName(e));
        }
    }

    @Override
    public void remove(DelegationIdentity id) throws DelegationException
    {
        boolean isSuccessful;

        try {
            isSuccessful = _store.deleteRequestCredential(nameFromId(id), null);
        } catch (IOException e) {
            throw new DelegationException("internal problem: " + Exceptions.messageOrClassName(e));
        }

        assertThat(isSuccessful, "no credential", id);
    }

    @Override
    public boolean has(DelegationIdentity id) throws DelegationException
    {
        try {
            return _store.hasRequestCredential(nameFromId(id), null);
        } catch (IOException e) {
            throw new DelegationException("internal problem: " + Exceptions.messageOrClassName(e));
        }
    }

    @Override
    public Calendar getExpiry(DelegationIdentity id) throws DelegationException
    {
        RequestCredential credential = _store.getRequestCredential(nameFromId(id));

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

    @Override
    public X509Credential search(String dn)
    {
        X509Credential bestWithFqan = search(dn, new SqlGlob("*"));
        X509Credential bestWithoutFqan = search(dn, (SqlGlob)null);

        if (bestWithFqan == null) {
            return bestWithoutFqan;
        } else if (bestWithoutFqan == null) {
            return bestWithFqan;
        }

        Date bestWithFqanLifetime = expiryDateFor(bestWithFqan);
        Date bestWithoutFqanLifetime = expiryDateFor(bestWithoutFqan);
        return bestWithoutFqanLifetime.after(bestWithFqanLifetime) ? bestWithoutFqan : bestWithFqan;

    }

    private static Date expiryDateFor(X509Credential credential)
    {
        return X509Credentials.calculateExpiry(credential)
                .map(Date::from)
                .orElseThrow(() -> new IllegalArgumentException("Certificate chain is empty."));
    }

    @Override
    public X509Credential search(String dn, String fqan)
    {
        return search(dn, fqan != null ? new SqlGlob(fqan) : null);
    }


    private X509Credential search(String dn, SqlGlob fqan)
    {
        long lifetime = 0;
        RequestCredential credential = null;

        RequestCredential gsiCredential = _store.searchRequestCredential(new SqlGlob(dn), fqan);
        if (gsiCredential != null) {
            lifetime = gsiCredential.getDelegatedCredentialRemainingLifetime();
            if (lifetime > 0) {
                credential = gsiCredential;
            }
        }

        RequestCredential gridsiteCredential = _store.searchRequestCredential(new SqlGlob("* " + dn), fqan);
        if (gridsiteCredential != null &&
                gridsiteCredential.getDelegatedCredentialRemainingLifetime() > lifetime) {
            credential = gridsiteCredential;
        }

        return credential != null ? credential.getDelegatedCredential() : null;
    }
}
