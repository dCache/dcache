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

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;

import org.dcache.events.SystemEvent;
import org.dcache.namespace.FileType;
import org.dcache.namespace.events.EventType;

/**
 * An inotify event that is sent to one (or more) subscriptions.  This class
 * exists to build the JSON representation of an event.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class InotifyEventPayload
{
    private static final Map<EventType,EventFlag> TYPE_AS_FLAG = new EnumMap(EventType.class);

    static {
        TYPE_AS_FLAG.put(EventType.IN_ACCESS,        EventFlag.IN_ACCESS);
        TYPE_AS_FLAG.put(EventType.IN_ATTRIB,        EventFlag.IN_ATTRIB);
        TYPE_AS_FLAG.put(EventType.IN_CLOSE_NOWRITE, EventFlag.IN_CLOSE_NOWRITE);
        TYPE_AS_FLAG.put(EventType.IN_CLOSE_WRITE,   EventFlag.IN_CLOSE_WRITE);
        TYPE_AS_FLAG.put(EventType.IN_CREATE,        EventFlag.IN_CREATE);
        TYPE_AS_FLAG.put(EventType.IN_DELETE,        EventFlag.IN_DELETE);
        TYPE_AS_FLAG.put(EventType.IN_DELETE_SELF,   EventFlag.IN_DELETE_SELF);
        TYPE_AS_FLAG.put(EventType.IN_MODIFY,        EventFlag.IN_MODIFY);
        TYPE_AS_FLAG.put(EventType.IN_MOVED_FROM,    EventFlag.IN_MOVED_FROM);
        TYPE_AS_FLAG.put(EventType.IN_MOVED_TO,      EventFlag.IN_MOVED_TO);
        TYPE_AS_FLAG.put(EventType.IN_MOVE_SELF,     EventFlag.IN_MOVE_SELF);
        TYPE_AS_FLAG.put(EventType.IN_OPEN,          EventFlag.IN_OPEN);
    }

    public final String name;
    public final String cookie;
    public final EnumSet<EventFlag> mask;

    public InotifyEventPayload(EventFlag event)
    {
        name = null;
        cookie = null;
        mask = EnumSet.of(event);
    }

    public InotifyEventPayload(SystemEvent event)
    {
        mask = EnumSet.noneOf(EventFlag.class);
        switch (event.getType()) {
        case OVERFLOW:
            mask.add(EventFlag.IN_Q_OVERFLOW);
            break;
        }
        name = null;
        cookie = null;
    }

    public InotifyEventPayload(org.dcache.namespace.events.InotifyEvent event)
    {
        mask = EnumSet.of(TYPE_AS_FLAG.get(event.getEventType()));
        if (event.getFileType() == FileType.DIR) {
            mask.add(EventFlag.IN_ISDIR);
        }
        name = event.getName();
        cookie = event.getCookie();
    }
}
