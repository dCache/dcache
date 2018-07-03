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

/**
 * The different kinds of inotify events generated within dCache.
 */
public enum EventType
{
    /*
     *  The descriptions for the events are taken directly from inotify(7) man
     *  page dated 2016-12-12
     */

    /** File was accessed (e.g., read(2), execve(2)).*/
    IN_ACCESS,

    /**
     * Metadata changed—for example, permissions
     * (e.g., chmod(2)), timestamps (e.g., utimensat(2)),
     * extended attributes (setxattr(2)), link count (since Linux 2.6.25;
     * e.g., for the target of link(2) and for unlink(2)), and user/group ID
     * (e.g., chown(2)).
     */
    IN_ATTRIB,

    /** File opened for writing was closed. */
    IN_CLOSE_WRITE,

    /** File or directory not opened for writing was closed. */
    IN_CLOSE_NOWRITE,

    /**
     * File/directory created in watched directory (e.g., open(2) O_CREAT,
     * mkdir(2), link(2), symlink(2), bind(2) on a UNIX domain socket).
     */
    IN_CREATE,

    /**
     * File/directory deleted from watched directory.
     */
    IN_DELETE,

    /**
     * Watched  file/directory  was  itself  deleted.  (This event also occurs
     * if an object is moved to another filesystem, since mv(1) in effect copies
     * the file to the other filesystem and then deletes it from the original
     * filesystem.)  In addition, an IN_IGNORED event  will  subsequently be
     * generated for the watch descriptor.
     */
    IN_DELETE_SELF,

    /** File was modified (e.g., write(2), truncate(2)). */
    IN_MODIFY,

    /** Watched file/directory was itself moved. */
    IN_MOVE_SELF,

    /** Generated for the directory containing the old filename when a file is renamed. */
    IN_MOVED_FROM,

    /** Generated for the directory containing the new filename when a file is renamed. */
    IN_MOVED_TO,

    /** File or directory was opened. */
    IN_OPEN;
}
