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
package org.dcache.auth.attributes;

import java.io.Serializable;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * This LoginAttribute identifies a time after which any session associated
 * with this login should terminate.  It is left unspecified whether or not
 * on-going user-triggered activity should terminate when the session is
 * terminated.  It is also left unspecified how quickly a session should
 * terminate once the expiry time has elapsed.  A time unlimited login with no
 * automatic time-based expiry is represented with the absence of this
 * LoginAttribute.  It is legitimate for a LoginReply to have multiple Expire
 * LoginAttributes; the Expiry with the earliest getExpiry response wins.
 */
public class Expiry implements LoginAttribute, Serializable
{
    private static final long serialVersionUID = -4933206451561151996L;

    private final Instant _expiry;

    public Expiry(Instant expiry)
    {
        _expiry = requireNonNull(expiry);
    }

    public Instant getExpiry()
    {
        return _expiry;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + getExpiry() + "]";
    }
}
