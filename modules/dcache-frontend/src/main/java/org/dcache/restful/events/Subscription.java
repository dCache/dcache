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

import com.fasterxml.jackson.databind.JsonNode;

import org.dcache.restful.events.spi.SelectedEventStream;
import org.dcache.restful.util.CloseableWithTasks;

/**
 * Represents a client's subscription to events of a specific type.
 * In effect, this class decorates a SelectedEventStream by adding the
 * eventType.
 */
public class Subscription extends CloseableWithTasks
{
    private final String eventType;
    private final SelectedEventStream ses;

    Subscription(String eventType, SelectedEventStream ses)
    {
        this.eventType = eventType;
        this.ses = ses;
        onClose(this::closeSelection);
    }

    public String getEventType()
    {
        return eventType;
    }

    public String getId()
    {
        return ses.getId();
    }

    public JsonNode getSelector()
    {
        return ses.selector();
    }

    private void closeSelection()
    {
        ses.close();
    }
}

