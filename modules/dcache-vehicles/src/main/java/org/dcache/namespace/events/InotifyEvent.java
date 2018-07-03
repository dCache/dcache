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
package org.dcache.namespace.events;

import javax.annotation.Nullable;

import java.util.Objects;

import diskCacheV111.util.PnfsId;

import org.dcache.events.Event;
import org.dcache.namespace.FileType;

/**
 * A specific inotify(7)-like event.
 */
public class InotifyEvent implements Event
{
    private static final long serialVersionUID = 1L;

    private final EventType eventType;
    private final String name;
    private final String cookie;
    private final PnfsId target;
    private final FileType type;

    public InotifyEvent(EventType eventType, PnfsId target, String name,
            String cookie, FileType type)
    {
        this.eventType = eventType;
        this.name = name;
        this.cookie = cookie;
        this.target = target;
        this.type = type;
    }

    public String getCategory()
    {
        return "inotify";
    }

    /**
     * The kind of inotify event represented by this object.
     * @return which activity triggered the event.
     */
    public EventType getEventType()
    {
        return eventType;
    }

    /**
     * If the target is a directory and the event arises from some child of that
     * directory then this field provides the name of that child.  Otherwise
     * null is returned.
     * @return the name of the child
     */
    public @Nullable String getName()
    {
        return name;
    }

    /**
     * The same non-null value is returned for the IN_MOVED_FROM and IN_MOVED_TO
     * events triggered by a single move/rename operation.  Each move/rename
     * operation has a unique cookie value.  This allows these two events to
     * be correlated, provided the client sees them both.  For all other events
     * null is returned.
     * @return the unique identifier for matching move events.
     */
    public @Nullable String getCookie()
    {
        return cookie;
    }

    /**
     * The watched file or directory through which the client learns of this
     * event.  For some eventType (OVERFLOW) there is no target and null is
     * returned.
     * @return the watch identity
     */
    public @Nullable PnfsId getTarget()
    {
        return target;
    }

    /**
     * The kind of object that triggered the event.  Note that, in inotify(7),
     * the only distinction is between directory and non-directory.  For some
     * eventTypes (OVERFLOW) there is no target and null is returned.
     * @return the kind of object that triggered the event.
     */
    public @Nullable FileType getFileType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(eventType, name, cookie, target, type);
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }

        if (!(other instanceof InotifyEvent)) {
            return false;
        }

        InotifyEvent otherEvent = (InotifyEvent)other;
        return otherEvent.eventType == eventType
                && Objects.equals(otherEvent.name, name)
                && Objects.equals(otherEvent.cookie, cookie)
                && Objects.equals(otherEvent.target, target)
                && Objects.equals(otherEvent.type, type);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append(eventType);

        if (name != null) {
            sb.append(" \"").append(name).append('\"');
        }

        if (type != null) {
            sb.append(' ').append(type);
        }

        if (cookie != null) {
            sb.append(" [").append(cookie).append(']');
        }

        if (target != null) {
            sb.append(' ').append(target.toString());
        }

        return sb.toString();
    }
}
