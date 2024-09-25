/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2024 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.alise;

import java.net.URI;
import java.security.Principal;
import java.util.Collection;
import org.dcache.util.Result;

/**
 * A service that allows looking up principals (local identity) from the user's
 * an OIDC subject.
 */
@FunctionalInterface
public interface LookupAgent {
    /**
     * Look up the principals associated with an OIDC subject from some
     * trusted issuer.  The result is either a corresponding set of principals
     * or an error message.
     * @param identity the federated identity.
     * @return The result of the lookup; if successful a collection of
     * principals that identify the user, if failure a suitable error message.
     */
    public Result<Collection<Principal>,String> lookup(Identity identity);

    /**
     * A method called when the plugin is shutting down.  Should release
     * established resources (e.g., shutting down threads).
     */
    default public void shutdown() {}
}
