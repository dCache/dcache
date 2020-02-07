/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 - 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.xrootd.security;

import eu.emi.security.authn.x509.X509CertChainValidatorExt;
import org.italiangrid.voms.VOMSValidators;
import org.italiangrid.voms.ac.VOMSACValidator;
import org.italiangrid.voms.store.VOMSTrustStore;
import org.italiangrid.voms.store.VOMSTrustStores;
import org.italiangrid.voms.util.CertificateValidatorBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.dcache.gsi.KeyPairCache;
import org.dcache.gsi.X509Delegation;

import static java.util.Arrays.asList;

public class ProxyDelegationStore
{
    /*
     * Visible to the client this is used to initialize.
     */
    final Map<String, X509Delegation> delegations = new ConcurrentHashMap<>();

    VOMSACValidator             vomsValidator;
    KeyPairCache                keyPairCache;

    private String   vomsDir;
    private String   caCertificatePath;
    private long     trustAnchorRefreshInterval;
    private TimeUnit trustAnchorRefreshIntervalUnit;

    public void initialize()
    {
        long refresh = trustAnchorRefreshIntervalUnit
                        .toMillis(trustAnchorRefreshInterval);
        VOMSTrustStore vomsTrustStore
                        = VOMSTrustStores.newTrustStore(asList(vomsDir));
        X509CertChainValidatorExt certChainValidator
                        = new CertificateValidatorBuilder()
                        .lazyAnchorsLoading(false)
                        .trustAnchorsUpdateInterval(refresh)
                        .trustAnchorsDir(caCertificatePath)
                        .build();
        vomsValidator = VOMSValidators.newValidator(vomsTrustStore,
                                                    certChainValidator);
    }

    public void setVomsDir(String vomsDir)
    {
        this.vomsDir = vomsDir;
    }

    public void setCaCertificatePath(String caCertificatePath)
    {
        this.caCertificatePath = caCertificatePath;
    }

    public void setKeyPairCache(KeyPairCache keyPairCache)
    {
        this.keyPairCache = keyPairCache;
    }

    public void setTrustAnchorRefreshInterval(long trustAnchorRefreshInterval)
    {
        this.trustAnchorRefreshInterval = trustAnchorRefreshInterval;
    }

    public void setTrustAnchorRefreshIntervalUnit(TimeUnit trustAnchorRefreshIntervalUnit)
    {
        this.trustAnchorRefreshIntervalUnit = trustAnchorRefreshIntervalUnit;
    }

    public void shutdown()
    {
        if (vomsValidator != null) {
            vomsValidator.shutdown();
        }
    }
}
