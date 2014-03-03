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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import org.dcache.delegation.gridsite2.DelegationException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.dcache.gridsite.Utilities.assertThat;

/**
 * An in-memory storage of in-progress delegations.  This implementation
 * has no persistent backing store so restarting the JVM will loose any
 * on-going delegation requests.
 *
 * To prevent partially delegated credentials where the client has vanished
 * from consuming memory indefinitely, delegation requests are expunged after
 * a configurable period.
 *
 * To prevent a denial-of-service attack from triggering an OOM, there is a
 * maximum number of concurrent on-going delegations.  While this doesn't
 * stop a client (or many clients) from denying a client from delegating, it
 * does allow the system to survive such attacks.
 *
 * The class is thread-safe.
 */
public class InMemoryCredentialDelegationStore implements
        CredentialDelegationStore
{
    private static final Logger LOG =
            LoggerFactory.getLogger(InMemoryCredentialDelegationStore.class);

    private final RemovalListener<DelegationIdentity,CredentialDelegation>
            LOG_REMOVALS = new RemovalListener<DelegationIdentity,CredentialDelegation>() {
                @Override
                public void onRemoval(RemovalNotification<DelegationIdentity,
                        CredentialDelegation> event)
                {
                    DelegationIdentity identity = event.getKey();
                    switch(event.getCause()) {
                    case EXPIRED:
                        LOG.debug("removing delegation from {}: client took" +
                                " too long to reply", identity.getDn());
                        break;
                    case SIZE:
                        LOG.debug("removing delegation from {}: too many" +
                                " on-going delegations", identity.getDn());
                        break;
                    }
                }
            };

    private Cache<DelegationIdentity,CredentialDelegation> _storage;
    private long _expireAfter;
    private long _maxOngoing;

    @Required
    public void setExpireAfter(long expire)
    {
        _expireAfter = expire;
    }

    public long getExpireAfter()
    {
        return _expireAfter;
    }

    @Required
    public void setMaxOngoing(long value)
    {
        _maxOngoing = value;
    }

    public long getMaxOngoing()
    {
        return _maxOngoing;
    }


    public void start()
    {
        _storage = CacheBuilder.newBuilder().
            maximumSize(_maxOngoing).
            expireAfterWrite(_expireAfter, MILLISECONDS).
            removalListener(LOG_REMOVALS).
            build();
    }


    @Override
    public synchronized CredentialDelegation get(DelegationIdentity id)
            throws DelegationException
    {
        CredentialDelegation delegation = _storage.getIfPresent(id);
        assertThat(delegation != null, "no on-going delegation", id);
        return delegation;
    }

    @Override
    public synchronized void add(CredentialDelegation delegation)
            throws DelegationException
    {
        DelegationIdentity id = delegation.getId();
        assertThat(_storage.getIfPresent(id) == null,
                "already on-going delegation", id);
        _storage.put(id, delegation);
    }

    @Override
    public synchronized CredentialDelegation remove(DelegationIdentity id)
            throws DelegationException
    {
        CredentialDelegation delegation = _storage.getIfPresent(id);
        assertThat(delegation != null, "no on-going delegation", id);
        _storage.invalidate(id);
        return delegation;
    }

    @Override
    public synchronized void removeIfPresent(DelegationIdentity id)
    {
        _storage.invalidate(id);
    }

    @Override
    public synchronized boolean has(DelegationIdentity id)
    {
        return _storage.getIfPresent(id) != null;
    }
}
