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
package diskCacheV111.namespace;

import diskCacheV111.util.PnfsId;

import org.dcache.namespace.events.EventType;
import org.dcache.namespace.FileType;

/**
 * A class that implements EventReceiver receives inotify-like events.  It is
 * expected that a class implementing this interface never blocks when
 * receiving an event.
 */
public interface EventReceiver
{
    /**
     * Receive notification that a namespace object has suffered an event.
     * Note that only a subset of EventType events are valid: IN_ACCESS,
     * IN_ATTRIB, IN_CLOSE_WRITE IN_CLOSE_NOWRITE, IN_DELETE_SELF, IN_MODIFY,
     * IN_MOVE_SELF, IN_OPEN and OVERFLOW.  The behaviour of a class
     * implementing this interface if other event types are used is unspecified.
     * @param eventType Describes what happened
     * @param target the ID of the object that experience the event
     * @param fileType what kind of object experienced the event.
     */
    void notifySelfEvent(EventType eventType, PnfsId target, FileType fileType);

    /**
     * Receive notification that a child object of some directory has suffered
     * a non-move event.  Note that only a subset of EventType events are valid
     * here: IN_ACCESS, IN_ATTRIB, IN_CLOSE_WRITE, IN_CLOSE_NOWRITE, IN_CREATE,
     * IN_DELETE, IN_MODIFY, IN_OPEN, OVERFLOW.  The behaviour of a class
     * implementing this interface if other event types are used is unspecified.
     * @param eventType Describes what happened
     * @param target the ID of the parent directory containing the object
     * experiencing the event.
     * @param name the name of the object that experienced the event, in the
     * target directory.
     * @param fileType what kind of object experienced the event.
     */
    void notifyChildEvent(EventType eventType, PnfsId target, String name, FileType fileType);

    /**
     * Receive notification that a child object of some directory has moved.
     * Note that only a subset of EventType events are valid here: IN_MOVED_FROM,
     * IN_MOVED_TO, OVERFLOW.  The behaviour of a class implementing this
     * interface if other event types are used is unspecified.
     * @param eventType Describes what happened
     * @param target the ID of the parent directory
     * @param name the name of the object that experienced the event
     * @param cookie an that is shared between coupled IN_MOVED_FROM and
     * IN_MOVED_TO events, but otherwise unique.
     * @param fileType what kind of object experienced the event.
     */
    void notifyMovedEvent(EventType eventType, PnfsId target, String name, String cookie, FileType fileType);
}
