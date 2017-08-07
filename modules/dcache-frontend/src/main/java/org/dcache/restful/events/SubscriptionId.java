/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.events;

import static java.util.Objects.requireNonNull;

/** The identity of some subscription. */
class SubscriptionId
{
    private final String eventType;
    private final String id;

    SubscriptionId(String eventType, String id)
    {
        this.eventType = requireNonNull(eventType);
        this.id = requireNonNull(id);
    }

    @Override
    public int hashCode()
    {
        return id.hashCode() ^ eventType.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }

        if (!(other instanceof SubscriptionId)) {
            return false;
        }

        SubscriptionId otherId = (SubscriptionId) other;
        return otherId.eventType.equals(eventType) && otherId.id.equals(id);
    }
}

