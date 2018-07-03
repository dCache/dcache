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
package org.dcache.events;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import diskCacheV111.vehicles.Message;

import org.dcache.namespace.events.InotifyEvent;

/**
 * A Message that notifies some interested parties of Events.  A
 * NotificationMessage is sent "unicast", targeting a specific destination.  If
 * multiple destinations are interested in the same event then multiple
 * NotificationMessage objects are sent.
 * <p>
 * The message may contain an arbitrary mixture of event categories; however, it
 * is likely that any message will contain single event categories.
 * <p>
 * This message may describe multiple events. To support this, events are
 * available through the {@link #forEachEvent(java.util.function.Consumer)
 * method.  This provides an absolute ordering of events from within a single
 * source, but there is no such guarantee of event ordering relative to other
 * event sources.
 */
public class NotificationMessage extends Message
{
    private final List<Event> events;

    public NotificationMessage(Event event)
    {
        this(Collections.singletonList(event));
    }

    public NotificationMessage(Collection<? extends Event> events)
    {
        this.events = new ArrayList<>(events);
    }

    public void forEachEvent(Consumer<Event> consumer)
    {
        events.forEach(consumer);
    }

    @Override
    public int hashCode()
    {
        return events.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }

        if (!(other instanceof NotificationMessage)) {
            return false;
        }

        return ((NotificationMessage)other).events.equals(events);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + events.stream()
                .map(Event::toString)
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
