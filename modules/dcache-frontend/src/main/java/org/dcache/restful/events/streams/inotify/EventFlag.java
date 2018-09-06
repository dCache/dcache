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

/**
 * Possible flags used when generating inotify responses.  A single inotify
 * event may return multiple of these; although, the only flag that is returned
 * in combination is IN_ISDIR, which is returned in combination with one of the
 * "event type" flags.
 */
public enum EventFlag
{
    // Event type: only (at most) one of these.
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

    // Additional inotify_event flags
    IN_IGNORED,
    IN_ISDIR,
    IN_Q_OVERFLOW,
    IN_UNMOUNT;
}
