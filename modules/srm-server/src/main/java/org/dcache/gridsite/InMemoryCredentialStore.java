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
import org.italiangrid.voms.VOMSAttribute;
import org.italiangrid.voms.ac.VOMSACValidator;
import org.springframework.beans.factory.annotation.Required;

import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.dcache.auth.FQAN;
import org.dcache.delegation.gridsite2.DelegationException;

import static org.dcache.gridsite.Utilities.assertThat;

/**
 * An implementation of CredentialStore that holds credentials in-memory.
 * This is meant as a quick implementation to allow testing of the
 * infrastructure.  Real deployments will interface with SRM's existing
 * credential store.
 */
public class InMemoryCredentialStore implements CredentialStore
{
    private final Map<DelegationIdentity, X509Credential> _storage = new HashMap<>();

    private VOMSACValidator validator;

    @Required
    public void setVomsValidator(VOMSACValidator validator)
    {
        this.validator = validator;
    }

    @Override
    public X509Credential get(DelegationIdentity id) throws DelegationException
    {
        X509Credential credential = getAndCheckForExpired(id);
        assertThat(credential != null, "no credential", id);

        return credential;
    }

    // Simple wrapper that checks if the credential has expired
    private X509Credential getAndCheckForExpired(DelegationIdentity id) throws DelegationException
    {
        X509Credential credential = _storage.get(id);

        if(credential != null && hasExpired(credential)) {
            _storage.remove(id);
            credential = null;
        }

        return credential;
    }

    @Override
    public void put(DelegationIdentity id, X509Credential credential)
    {
        _storage.put(id, credential);
    }

    @Override
    public void remove(DelegationIdentity id) throws DelegationException
    {
        X509Credential credential = _storage.remove(id);

        if (credential != null && hasExpired(credential)) {
           _storage.remove(id);
           credential = null;
        }

        assertThat(credential != null, "no credential", id);
    }

    @Override
    public boolean has(DelegationIdentity id)
    {
        try {
            return getAndCheckForExpired(id) != null;
        } catch (DelegationException e) {
            return false;
        }
    }

    @Override
    public Calendar getExpiry(DelegationIdentity id) throws DelegationException
    {
        X509Credential credential = getAndCheckForExpired(id);
        assertThat(credential != null, "no credential", id);

        Date expires = getExpiryOf(credential);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(expires);
        return calendar;
    }

    private static Date getExpiryOf(X509Credential credential) throws DelegationException
    {
        return Stream.of(credential.getCertificateChain())
                .map(X509Certificate::getNotAfter)
                .min(Date::compareTo)
                .orElseThrow(() -> new DelegationException("Certificate chain is empty."));
    }

    private static boolean hasExpired(X509Credential credential) throws DelegationException
    {
        return getExpiryOf(credential).getTime() <= System.currentTimeMillis();
    }

    @Override
    public X509Credential search(String targetDn)
    {
        return bestCredentialMatching((dn, fqan) -> targetDn.equals(dn));
    }

    @Override
    public X509Credential search(String targetDn, String targetFqan)
    {
        return bestCredentialMatching((dn, fqan) -> targetDn.equals(dn) && Objects.equals(targetFqan, fqan));
    }

    private interface DnFqanMatcher
    {
        boolean matches(String dn, String fqan);
    }

    private X509Credential bestCredentialMatching(DnFqanMatcher predicate)
    {
        X509Credential bestCredential = null;
        Date bestExpirationTime = new Date(0);

        for (Map.Entry<DelegationIdentity, X509Credential> entry : _storage.entrySet()) {
            try {
                X509Credential credential = entry.getValue();

                X509Certificate[] chain = credential.getCertificateChain();
                FQAN primaryFqan = getPrimary(validator.validate(chain));

                if (!predicate.matches(entry.getKey().getDn(), Objects.toString(primaryFqan, null))) {
                    continue;
                }

                Date expires = getExpiryOf(credential);
                if (expires.after(bestExpirationTime)) {
                    bestExpirationTime = expires;
                    bestCredential = credential;
                }
            } catch (DelegationException ignored) {
                // Treat problematic credentials as having expired
            }
        }

        return bestCredential;
    }

    private static FQAN getPrimary(List<VOMSAttribute> attributes)
    {
        return attributes.stream().flatMap(a -> a.getFQANs().stream()).findFirst().map(FQAN::new).orElse(null);
    }
}
