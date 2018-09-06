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
package org.dcache.restful.events.streams.inotify;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

import org.dcache.namespace.events.EventType;

/**
 * The flags that may be supplied when requesting a new watch.
 */
public enum AddWatchFlag
{
    // Which events are of interest.
    IN_ACCESS,
    IN_ATTRIB,
    IN_CLOSE_WRITE,
    IN_CLOSE_NOWRITE,
    IN_CREATE,
    IN_DELETE,
    IN_DELETE_SELF,
    IN_MODIFY,
    IN_MOVE_SELF,
    IN_MOVED_FROM,
    IN_MOVED_TO,
    IN_OPEN,

    // The following three macros is equivalent to a specific subset of the above event flags.

    /** A macro, equivalent to all events. */
    IN_ALL_EVENTS,

    /** A macro, equivalent to IN_CLOSE_WRITE | IN_CLOSE_NOWRITE. */
    IN_CLOSE,

    /** A macro, equivalent to IN_MOVED_FROM | IN_MOVED_TO. */
    IN_MOVE,

    // Additional flags that modify behaviour without selecting additional events

    /** Report namespace events against a symlink itself, not what it points to. */
    IN_DONT_FOLLOW,

    /** Stop sending events after a file is unlinked, but still opened. */
    IN_EXCL_UNLINK,

    /** When a request is merged, use union of flags, rather than replacing. */
    IN_MASK_ADD,

    /** Close the watch after the first event is sent. */
    IN_ONESHOT,

    /** Accept the request, but do not send events if the target is not a dir. */
    IN_ONLYDIR;

    public static final EnumSet<AddWatchFlag> ALL_EVENTS = EnumSet.of(
            AddWatchFlag.IN_ACCESS, AddWatchFlag.IN_ATTRIB,
            AddWatchFlag.IN_CLOSE_WRITE, AddWatchFlag.IN_CLOSE_NOWRITE,
            AddWatchFlag.IN_CREATE, AddWatchFlag.IN_DELETE,
            AddWatchFlag.IN_DELETE_SELF, AddWatchFlag.IN_MODIFY,
            AddWatchFlag.IN_MOVE_SELF, AddWatchFlag.IN_MOVED_FROM,
            AddWatchFlag.IN_MOVED_TO, AddWatchFlag.IN_OPEN);

    public static Set<EventType> asEventType(Collection<AddWatchFlag> flags)
    {
        EnumSet<EventType> requestFlags = EnumSet.noneOf(EventType.class);

        for (AddWatchFlag flag : flags) {
            switch (flag) {
            case IN_ACCESS:
                requestFlags.add(EventType.IN_ACCESS);
                break;
            case IN_ATTRIB:
                requestFlags.add(EventType.IN_ATTRIB);
                break;
            case IN_CLOSE_WRITE:
                requestFlags.add(EventType.IN_CLOSE_WRITE);
                break;
            case IN_CLOSE_NOWRITE:
                requestFlags.add(EventType.IN_CLOSE_NOWRITE);
                break;
            case IN_CREATE:
                requestFlags.add(EventType.IN_CREATE);
                break;
            case IN_DELETE:
                requestFlags.add(EventType.IN_DELETE);
                break;
            case IN_DELETE_SELF:
                requestFlags.add(EventType.IN_DELETE_SELF);
                break;
            case IN_MODIFY:
                requestFlags.add(EventType.IN_MODIFY);
                break;
            case IN_MOVE_SELF:
                requestFlags.add(EventType.IN_MOVE_SELF);
                break;
            case IN_MOVED_FROM:
                requestFlags.add(EventType.IN_MOVED_FROM);
                break;
            case IN_MOVED_TO:
                requestFlags.add(EventType.IN_MOVED_TO);
                break;
            case IN_OPEN:
                requestFlags.add(EventType.IN_OPEN);
                break;
            }
        }

        return requestFlags;
    }
}
