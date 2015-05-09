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
package org.dcache.webdav.transfer;

import io.milton.http.Response.Status;
import org.globus.gsi.X509Credential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import diskCacheV111.srm.dcache.SrmLoginBrokerPublisher.SrmLoginBrokerInfo;
import diskCacheV111.srm.dcache.SrmRequestCredentialMessage;
import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerInfo;
import dmg.cells.services.login.LoginBrokerSource;

import org.dcache.cells.CellStub;

/**
 * This class acts as a client to dCache SRM instance(s); in particular, to
 * their credential storage and discovery of their delegation end-point.
 */
public class SrmHandler
{
    private static final Logger _log = LoggerFactory.getLogger(SrmHandler.class);

    private CellStub _srmStub;
    private LoginBrokerSource _loginBrokerSource;

    @Required
    public void setLoginBrokerSource(LoginBrokerSource provider)
    {
        _loginBrokerSource = provider;
    }

    @Required
    public void setSrmStub(CellStub srmStub)
    {
        _srmStub = srmStub;
    }

    public String getDelegationEndpoints()
    {
        return _loginBrokerSource.writeDoorsByProtocol().get("srm").stream()
                .map(i -> ((SrmLoginBrokerInfo) i).getDelegationEndpoint())
                .collect(Collectors.joining(" "));
    }

    public X509Credential getDelegatedCredential(String dn, String primaryFqan,
            int minimumValidity, TimeUnit units) throws InterruptedException, ErrorResponseException
    {
        Collection<LoginBrokerInfo> srms = _loginBrokerSource.writeDoorsByProtocol().get("srm");

        if (srms.isEmpty()) {
            _log.error("Cannot advise client to delegate for third-party COPY: no srm service found.");
            throw new ErrorResponseException(Status.SC_INTERNAL_SERVER_ERROR,
                    "problem with internal communication");
        }

        long bestRemainingLifetime = 0;
        X509Credential bestCredential = null;

        for (LoginBrokerInfo srm : srms) {
            CellPath path = new CellPath(new CellAddressCore(srm.getCellName(), srm.getDomainName()));
            SrmRequestCredentialMessage msg = new SrmRequestCredentialMessage(dn, primaryFqan);
            try {
                msg = _srmStub.sendAndWait(path, msg);

                if (!msg.hasCredential()) {
                    continue;
                }

                X509Certificate[] certificates = msg.getCertificateChain();

                long lifetime = calculateRemainingLifetime(certificates);

                if (lifetime > bestRemainingLifetime) {
                    bestCredential = new X509Credential(msg.getPrivateKey(), certificates);
                    bestRemainingLifetime = lifetime;
                }
            } catch (CacheException e) {
                _log.debug("failed to contact SRM {} querying for {}, {}: {}",
                           path, dn, primaryFqan, e.getMessage());
            }
        }

        return bestRemainingLifetime < units.toMillis(minimumValidity) ? null : bestCredential;
    }

    private static long calculateRemainingLifetime(X509Certificate[] certificates)
    {
        long earliestExpiry = Long.MAX_VALUE;

        for (X509Certificate certificate : certificates) {
            earliestExpiry = Math.min(earliestExpiry, certificate.getNotAfter().getTime());
        }

        long now = System.currentTimeMillis();

        return (earliestExpiry <= now) ? 0 : earliestExpiry - now;
    }
}
