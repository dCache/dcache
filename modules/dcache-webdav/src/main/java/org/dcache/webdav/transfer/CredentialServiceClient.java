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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.impl.KeyAndCertCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.net.URI;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import diskCacheV111.srm.CredentialServiceAnnouncement;
import diskCacheV111.srm.CredentialServiceRequest;
import diskCacheV111.srm.dcache.SrmRequestCredentialMessage;
import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;

import org.dcache.cells.CellStub;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class acts as a client to credential services.
 */
public class CredentialServiceClient
    implements CellMessageReceiver, CellLifeCycleAware
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CredentialServiceClient.class);

    private CellStub topic;

    private Cache<CellAddressCore, URI> cache = CacheBuilder.newBuilder().expireAfterWrite(70, SECONDS).build();

    @Required
    public void setTopicStub(CellStub topic)
    {
        this.topic = topic;
    }

    @Override
    public void afterStart()
    {
        topic.notify(new CredentialServiceRequest());
    }

    @Override
    public void beforeStop()
    {

    }

    public void messageArrived(CredentialServiceAnnouncement message)
    {
        cache.put(message.getCellAddress(), message.getDelegationEndpoint());
    }

    public Collection<URI> getDelegationEndpoints()
    {
        return cache.asMap().values();
    }

    public X509Credential getDelegatedCredential(String dn, String primaryFqan,
            int minimumValidity, TimeUnit units) throws InterruptedException, ErrorResponseException
    {
        long bestRemainingLifetime = 0;
        X509Credential bestCredential = null;

        for (CellAddressCore address : cache.asMap().keySet()) {
            CellPath path = new CellPath(address);
            SrmRequestCredentialMessage msg = new SrmRequestCredentialMessage(dn, primaryFqan);
            try {
                msg = topic.sendAndWait(path, msg);

                if (!msg.hasCredential()) {
                    continue;
                }

                X509Certificate[] certificates = msg.getCertificateChain();
                long lifetime = calculateRemainingLifetime(certificates);
                if (lifetime > bestRemainingLifetime) {
                    bestCredential = new KeyAndCertCredential(msg.getPrivateKey(), certificates);
                    bestRemainingLifetime = lifetime;
                }
            } catch (CacheException e) {
                LOGGER.debug("failed to contact {} querying for {}, {}: {}",
                             path, dn, primaryFqan, e.getMessage());
            } catch (KeyStoreException e) {
                LOGGER.warn("Received invalid key pair from {} for {}, {}: {}",
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
