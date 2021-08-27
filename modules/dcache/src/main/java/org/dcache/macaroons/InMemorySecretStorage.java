/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.macaroons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * In-memory, non-persistent storage for multiple expiring IdentifiedSecret
 * objects.
 */
public class InMemorySecretStorage
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InMemorySecretStorage.class);

    private final SortedMap<Instant,IdentifiedSecret> _secrets = new TreeMap<>();
    private final Map<String,IdentifiedSecret> _secretsByIdentifier = new HashMap<>();

    /**
     * Find a secret with the earliest expiry that is strictly greater than the
     * supplied Instant.
     */
    public synchronized Optional<IdentifiedSecret> firstExpiringAfter(Instant earliestExpiry)
    {
        return _secrets.tailMap(earliestExpiry).values().stream().findFirst();
    }

    /**
     * Store a secret with associated expiry time.
     */
    public synchronized IdentifiedSecret put(Instant expiry, IdentifiedSecret secret)
    {
        LOGGER.debug("Adding secret {} expiring at {}", secret.getIdentifier(), expiry);
        _secrets.put(expiry, secret);
        _secretsByIdentifier.put(secret.getIdentifier(), secret);
        return secret;
    }

    /**
     * Remove a secret that is to expire at the stated time.
     */
    public synchronized void remove(Instant expiry)
    {
        IdentifiedSecret secret = _secrets.remove(expiry);
        if (secret != null) {
            String id = secret.getIdentifier();
            secret = _secretsByIdentifier.remove(id);
            if (secret == null) {
                LOGGER.warn("Removed secret {} expiring at {}, but failed to remove from identifier map", id, expiry);
            } else {
                LOGGER.debug("Removed secret {} expiring at {}", id, expiry);
            }
        } else {
            LOGGER.debug("Requested removal of secret expiring at {}, but none found", expiry);
        }
    }

    /**
     * Get a secret identified by the supplied identifier.
     */
    public synchronized byte[] get(String identifier)
    {
        IdentifiedSecret secret = _secretsByIdentifier.get(identifier);
        return secret != null ? secret.getSecret() : null;
    }

    /**
     * Return the secret with the specified expiry.
     */
    public synchronized Optional<IdentifiedSecret> get(Instant expiry)
    {
        return Optional.ofNullable(_secrets.get(expiry));
    }

    /**
     * Remove all secrets and identify those that have an expiry time before
     * the supplied cutoff Instant.
     * @return a non-null Set of Instant for secret expiries.
     */
    public synchronized Set<Instant> expiringBefore(Instant cutoff)
    {
        LOGGER.debug("Checking for expired secrets");

        Set<Instant> expired = _secrets.headMap(cutoff).keySet();

        return expired.isEmpty() ? Collections.emptySet() : new HashSet<>(expired);
    }
}
