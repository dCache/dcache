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

import org.globus.gsi.gssapi.GSSConstants;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.italiangrid.voms.VOMSAttribute;
import org.italiangrid.voms.ac.VOMSACValidator;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.dcache.auth.FQAN;
import org.dcache.delegation.gridsite2.DelegationException;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;
import org.dcache.util.Glob;

import static org.dcache.gridsite.Utilities.assertThat;

/**
 * The SrmCredentialStore acts as a bridge between the SRM's delegation store
 * and the API expected by GridSite.
 */
public class SrmCredentialStore implements CredentialStore
{
    private RequestCredentialStorage _store;
    private VOMSACValidator validator;

    public void setVomsValidator(VOMSACValidator validator)
    {
        this.validator = validator;
    }

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
            FQAN primaryFqan;
            if (credential instanceof ExtendedGSSCredential) {
                X509Certificate[] chain = (X509Certificate[]) ((ExtendedGSSCredential) credential).inquireByOid(GSSConstants.X509_CERT_CHAIN);
                primaryFqan = getPrimary(validator.validate(chain));
            } else {
                primaryFqan = null;
            }

            RequestCredential srmCredential =
                    new RequestCredential(nameFromId(id), Objects.toString(primaryFqan, null), credential, _store);
            _store.saveRequestCredential(srmCredential);
        } catch (GSSException | RuntimeException e) {
            throw new DelegationException("failed to save credential: " +
                    e.getMessage());
        }
    }

    private static FQAN getPrimary(List<VOMSAttribute> attributes)
    {
        return attributes.stream().flatMap(a -> a.getFQANs().stream()).findFirst().map(FQAN::new).orElse(null);
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

    @Override
    public GSSCredential search(String dn)
    {
        GSSCredential bestWithFqan = search(dn, new Glob("*"));
        GSSCredential bestWithoutFqan = search(dn, (Glob)null);

        if (bestWithFqan == null) {
            return bestWithoutFqan;
        } else if (bestWithoutFqan == null) {
            return bestWithFqan;
        }

        long bestWithFqanLifetime;

        try {
            bestWithFqanLifetime = bestWithFqan.getRemainingLifetime();
        } catch (GSSException ignored) {
            // treat as expired
            bestWithFqanLifetime = 0;
        }

        long bestWithoutFqanLifetime;

        try {
            bestWithoutFqanLifetime = bestWithoutFqan.getRemainingLifetime();
        } catch (GSSException ignored) {
            // treat as expired
            bestWithoutFqanLifetime = 0;
        }

        if (bestWithoutFqanLifetime > bestWithFqanLifetime) {
            return bestWithoutFqan;
        }

        return (bestWithFqanLifetime > 0) ? bestWithFqan : null;
    }

    @Override
    public GSSCredential search(String dn, String fqan)
    {
        return search(dn, fqan != null ? new Glob(fqan) : null);
    }


    private GSSCredential search(String dn, Glob fqan)
    {
        long lifetime = 0;
        RequestCredential credential = null;

        RequestCredential gsiCredential = _store.searchRequestCredential(new Glob(dn), fqan);
        if (gsiCredential != null) {
            lifetime = gsiCredential.getDelegatedCredentialRemainingLifetime();
            if (lifetime > 0) {
                credential = gsiCredential;
            }
        }

        RequestCredential gridsiteCredential = _store.searchRequestCredential(new Glob("* " + dn), fqan);
        if (gridsiteCredential != null &&
                gridsiteCredential.getDelegatedCredentialRemainingLifetime() > lifetime) {
            credential = gridsiteCredential;
        }

        return credential != null ? credential.getDelegatedCredential() : null;
    }
}
