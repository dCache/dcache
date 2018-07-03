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

import javax.annotation.Nonnull;

import java.io.Serializable;

/**
 * An Event is some arbitrary change in dCache that one or more clients
 * are interested in observing.  A class that implements Event allows
 * the transportation of this information from one dCache cell to another.
 * <p>
 * Event objects that may be processed in a similar fashion are grouped together
 * into a common category.  Often, objects of the same category have the same
 * class and two objects with different categories will use different classes,
 * but this is not a requirement.  A receiver that targets a specific category
 * of events should silently reject all objects it does not know how to process.
 */
public interface Event extends Serializable
{
    /**
     * The category of this Event.  This groups together events that are somehow
     * similar and may be reasonably expected to be processed by some common
     * consumer.  Often Event objects with the same category will use the same
     * class and Event objects with different categories will use different
     * Event objects, but this is not a requirement.
     * @return the name of this Event's category.
     */
    @Nonnull
    String getCategory();

    /**
     * Any Event class must implement toString that yields a String with no
     * line-break and a short description of the event.
     * @return
     */
    @Override
    public String toString();
}
