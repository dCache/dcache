/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.providers;

import java.util.List;
import java.util.Objects;

/**
 * Class to hold information for a JSON response querying information
 * about a user.
 */
public class UserAttributes
{
    public enum AuthenticationStatus {ANONYMOUS, AUTHENTICATED};

    /**
     * Whether the current user has authenticated with the system.
     * ANONYMOUS indicates that the user supplied no credentials or that
     * the credentials failed to authenticate the user (e.g., wrong password).
     *
     */
    private AuthenticationStatus status;

    /**
     * The UID of the user, if the user has status AUTHENTICATED, null
     * otherwise.
     */
    private Long uid;

    private List<Long> gids;

    public AuthenticationStatus getStatus()
    {
        return status;
    }

    public void setStatus(AuthenticationStatus status)
    {
        this.status = Objects.requireNonNull(status);
    }

    public Long getUid()
    {
        return uid;
    }

    public void setUid(Long uid)
    {
        this.uid = uid;
    }

    public List<Long> getGids()
    {
        return gids;
    }

    public void setGids(List<Long> gids)
    {
        this.gids = gids;
    }
}
