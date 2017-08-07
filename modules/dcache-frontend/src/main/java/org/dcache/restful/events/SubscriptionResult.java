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

import org.dcache.restful.events.spi.SelectionResult;
import org.dcache.restful.events.spi.SelectionStatus;

/** Represents the result of a client request that subscribes to events.  */
public class SubscriptionResult
{
    private final SelectionStatus status;
    private final String id;
    private final String message;

    public SubscriptionResult(SelectionResult result)
    {
        status = result.getStatus();
        id = result.getSelectedEventStream() == null
                ? null : result.getSelectedEventStream().getId();
        message = result.getMessage();
    }

    public SelectionStatus getStatus()
    {
        return status;
    }

    public String getId()
    {
        return id;
    }

    public String getMessage()
    {
        return message;
    }
}

