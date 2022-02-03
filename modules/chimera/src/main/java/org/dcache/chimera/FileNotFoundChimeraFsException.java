/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera;

import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.posix.Stat.StatAttributes;

/**
 * The requested file could not be found.  A message is always present and
 * describes what is missing.  The missing resource is described by a path
 * when that information is available, otherwise either a PNFS-ID or the
 * Chimera-internal ID.
 */
public class FileNotFoundChimeraFsException extends ChimeraFsException {

    private static final long serialVersionUID = 2898082345212568953L;

    private static String idFromInode(FsInode inode) {
        Stat stat = inode.getStatCache();
        if (stat != null && stat.isDefined(StatAttributes.FILEID)) {
            return "PNFSID " + stat.getId();
        } else {
            return "internal ID " + inode.ino();
        }
    }

    public static FileNotFoundChimeraFsException ofPath(String path) {
        return new FileNotFoundChimeraFsException("No such file or directory: "
                + path);
    }

    public static FileNotFoundChimeraFsException ofPnfsId(String id) {
        return new FileNotFoundChimeraFsException("No such file or directory "
                + "with PNFSID: " + id);
    }

    public static FileNotFoundChimeraFsException of(FsInode id) {
        return FileNotFoundChimeraFsException.of(id, null);
    }

    public static FileNotFoundChimeraFsException of(FsInode id, Throwable cause) {
        return new FileNotFoundChimeraFsException("No file or directory with "
                + idFromInode(id), cause);
    }

    public static FileNotFoundChimeraFsException ofLevel(FsInode file, int level) {
        return FileNotFoundChimeraFsException.ofLevel(file, level, null);
    }

    public static FileNotFoundChimeraFsException ofLevel(FsInode file, int level,
            Throwable cause) {
        return new FileNotFoundChimeraFsException("File with " + idFromInode(file)
                + " has no level " + level, cause);
    }

    public static FileNotFoundChimeraFsException ofTag(FsInode directory, String tag) {
        return new FileNotFoundChimeraFsException("Directory with "
                + idFromInode(directory) + " has no tag " + tag);
    }

    public static FileNotFoundChimeraFsException ofTag(FsInode tag) {
        return new FileNotFoundChimeraFsException("No tag with "
                + idFromInode(tag));
    }

    public static FileNotFoundChimeraFsException ofFileInDirectory(FsInode directory,
            String name) {
        return new FileNotFoundChimeraFsException("Directory with "
                + idFromInode(directory) + " has no file " + name);
    }

    private FileNotFoundChimeraFsException(String message) {
        super(message);
    }

    private FileNotFoundChimeraFsException(String message, Throwable cause) {
        super(message, cause);
    }
}
