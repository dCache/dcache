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

/**
 * A SystemEvent is an event that describes events pertaining to the event
 * delivery system itself.
 */
public class SystemEvent implements Event
{
    /**
     * The kind of event affecting the event delivery system.
     */
    public enum Type {
        /** The system was forced to drop events. */
        OVERFLOW,
    }

    private final Type type;

    public SystemEvent(Type type)
    {
        this.type = type;
    }

    @Override
    public String getCategory()
    {
        return "SYSTEM";
    }

    public Type getType()
    {
        return type;
    }
}
