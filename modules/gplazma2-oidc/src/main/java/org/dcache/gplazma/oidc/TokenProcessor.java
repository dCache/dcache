/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.oidc;

import org.dcache.gplazma.AuthenticationException;

/**
 * A class that implements TokenProcessor provides a strategies through which a set of claims is
 * obtained from an access token.  These claims come (ultimately) from a trusted identity provider.
 * If multiple identity providers are trusted then the strategy is also responsible for deciding
 * which of them issued the token.
 * <p>
 * The claims describe attributes of a person (or agent thereof) who authenticated when first
 * obtaining the token.  They may also describe aspects of the token itself; e.g., how the token
 * was obtained, which services are the intended recipients of the token.
 */
public interface TokenProcessor {

    /**
     * Obtain information about the token.
     * @param token The access token being queried.
     * @return The information describing this token.
     * @throws AuthenticationException if the strategy has found a terminal problem with the token.
     * @throws UnableToProcess if this strategy cannot process the token but a different strategy might.
     */
    ExtractResult extract(String token) throws AuthenticationException, UnableToProcess;

    /**
     * Signal that the TokenProcessor will not receive any further requests.   Any resources that
     * the processor has reserved should now be released.
     */
    void shutdown();
}
