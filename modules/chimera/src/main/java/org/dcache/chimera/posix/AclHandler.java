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
package org.dcache.chimera.posix;

public interface AclHandler {

    /**
     *
     *  The idea stolen from AFS:
     *
     * Lookup, which allows a user to list the contents of the AFS directory,
     *      examine the ACL associated with the directory and access subdirectories.
     * Insert, which allows a user to add new files or subdirectories to the directory.
     * Delete, which allows a user to remove files and subdirectories from the directory.
     * Administer, which allows a user to change the ACL for the directory.
     *      Users always have this right on their home directory, even if
     *      they accidentally remove themselves from the ACL.
     * Read, which allows a user to look at the contents of files in a directory
     *      and list files in subdirectories. Files that are to be granted read
     *      access to any user, including the owner, need to have the standard
     *      UNIX "owner read" permission set. This can be done with the command
     *      chmod o+r filename.
     * Write, which allows a user to modify files in a directory. Files that are
     *      to be granted write access to any user, including the owner, need to
     *      have the standard UNIX "owner write" permission set. This can be done
     *      with the chmod o+w filename command.
     * Lock, which allows the processor to run programs that need to "flock" files
     *      in the directory. See the UNIX man page for "flock" for more details.
     *
     */
    int ACL_READ = 0;
    int ACL_WRITE = 2;
    int ACL_DELETE = 3;
    int ACL_LOOKUP = 4;
    int ACL_ADMINISTER = 5;
    int ACL_INSERT = 6;
    int ACL_LOCK = 7;
    int ACL_EXECUTE = 8;

    boolean isAllowed(Acl acl, User user, int requsetedAcl);
}
