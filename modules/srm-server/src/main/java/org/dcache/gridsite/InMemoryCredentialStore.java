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

import eu.emi.security.authn.x509.X509CertChainValidatorExt;
import org.globus.gsi.gssapi.GSSConstants;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.italiangrid.voms.VOMSAttribute;
import org.italiangrid.voms.VOMSValidators;
import org.italiangrid.voms.ac.VOMSACValidator;
import org.italiangrid.voms.store.VOMSTrustStore;
import org.italiangrid.voms.store.VOMSTrustStores;
import org.italiangrid.voms.util.CertificateValidatorBuilder;
import org.springframework.beans.factory.annotation.Required;

import java.security.cert.X509Certificate;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.dcache.auth.FQAN;
import org.dcache.delegation.gridsite2.DelegationException;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dcache.gridsite.Utilities.assertThat;

/**
 * An implementation of CredentialStore that holds credentials in-memory.
 * This is meant as a quick implementation to allow testing of the
 * infrastructure.  Real deployments will interface with SRM's existing
 * credential store.
 */
public class InMemoryCredentialStore implements CredentialStore
{
    private final Map<DelegationIdentity,GSSCredential> _storage = new HashMap<>();

    private VOMSTrustStore vomsTrustStore;
    private X509CertChainValidatorExt certChainValidator;

    @Required
    public void setCaCertificatePath(String caDir)
    {
        certChainValidator = new CertificateValidatorBuilder().trustAnchorsDir(caDir).build();
    }

    @Required
    public void setVomsdir(String vomsDir)
    {
        vomsTrustStore = VOMSTrustStores.newTrustStore(singletonList(vomsDir));
    }

    @Override
    public GSSCredential get(DelegationIdentity id) throws DelegationException
    {
        GSSCredential credential = getAndCheckForExpired(id);
        assertThat(credential != null, "no credential", id);

        return credential;
    }

    // Simple wrapper that checks if the credential has expired
    private GSSCredential getAndCheckForExpired(DelegationIdentity id)
    {
        GSSCredential credential = _storage.get(id);

        if(credential != null && hasExpired(credential)) {
            _storage.remove(id);
            credential = null;
        }

        return credential;
    }

    @Override
    public void put(DelegationIdentity id, GSSCredential credential)
    {
        _storage.put(id, credential);
    }

    @Override
    public void remove(DelegationIdentity id) throws DelegationException
    {
        GSSCredential credential = _storage.remove(id);

        if (credential != null && hasExpired(credential)) {
           _storage.remove(id);
           credential = null;
        }

        assertThat(credential != null, "no credential", id);
    }

    @Override
    public boolean has(DelegationIdentity id)
    {
        return getAndCheckForExpired(id) != null;
    }

    @Override
    public Calendar getExpiry(DelegationIdentity id) throws DelegationException
    {
        GSSCredential credential = getAndCheckForExpired(id);
        assertThat(credential != null, "no credential", id);

        int remaining = remainingLifetimeOf(credential);

        if (remaining == GSSCredential.INDEFINITE_LIFETIME) {
            throw new DelegationException("credential has no expiry date");
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis() + SECONDS.toMillis(remaining));
        return calendar;
    }

    private static int remainingLifetimeOf(GSSCredential credential)
    {
        int remaining;

        try {
            remaining = credential.getRemainingLifetime();
        } catch (GSSException ignored) {
            remaining = 0; // Treat problematic credentials as having expired
        }

        return remaining;
    }

    private static boolean hasExpired(GSSCredential credential)
    {
        return remainingLifetimeOf(credential) == 0;
    }

    @Override
    public GSSCredential search(String targetDn)
    {
        return bestCredentialMatching((dn, fqan) -> targetDn.equals(dn));
    }

    @Override
    public GSSCredential search(String targetDn, String targetFqan)
    {
        return bestCredentialMatching((dn, fqan) -> targetDn.equals(dn) && Objects.equals(targetFqan, fqan));
    }

    private interface DnFqanMatcher
    {
        boolean matches(String dn, String fqan);
    }

    private GSSCredential bestCredentialMatching(DnFqanMatcher predicate)
    {
        GSSCredential bestCredential = null;
        long bestRemainingLifetime = 0;

        for (Map.Entry<DelegationIdentity,GSSCredential> entry : _storage.entrySet()) {
            try {
                GSSCredential credential = entry.getValue();

                FQAN primaryFqan;
                if (credential instanceof ExtendedGSSCredential) {
                    X509Certificate[] chain = (X509Certificate[]) ((ExtendedGSSCredential) credential).inquireByOid(GSSConstants.X509_CERT_CHAIN);
                    VOMSACValidator validator = VOMSValidators.newValidator(vomsTrustStore, certChainValidator);
                    primaryFqan = getPrimary(validator.validate(chain));
                } else {
                    primaryFqan = null;
                }

                if (!predicate.matches(entry.getKey().getDn(), Objects.toString(primaryFqan, null))) {
                    continue;
                }

                long remainingLifetime = credential.getRemainingLifetime();

                if (remainingLifetime > bestRemainingLifetime) {
                    bestRemainingLifetime = remainingLifetime;
                    bestCredential = credential;
                }
            } catch (GSSException ignored) {
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
