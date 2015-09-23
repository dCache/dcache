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

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.function.Supplier;

/**
 * A class that implements SecretHandler is responsible for managing the
 * secret(s) needed when building and verifying macaroon credentials.
 * Secrets have a corresponding identifier that is used to discover which
 * secret was used.  This allows the SecretHandler to expire secrets to minimise
 * the damage should there be a leak of secrets.
 */
public interface SecretHandler
{
    /**
     * Find a secret that will be valid until the minimum expiry time.
     * Implementation are free to return a secret that is still valid after
     * the desired expiry time.
     */
    IdentifiedSecret secretExpiringAfter(Instant earliestExpiry, Supplier<IdentifiedSecret> newSecret) throws Exception;

    /**
     * Find the secret for the identifier.
     * @return the byte array of the secret corresponding to the supplied
     * identifier, or null if no such secret could be found.
     */
    @Nullable
    byte[] findSecret(String identifier);
}
