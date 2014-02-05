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
package org.dcache.chimera.cli;

import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsFactory;
import org.dcache.chimera.FsInode;

public class Lstag {

    public static void main(String[] args) throws Exception {

        if (args.length != FsFactory.ARGC +1) {
            System.err.println(
                    "Usage : " + Lstag.class.getName() + " " + FsFactory.USAGE
                    + " <chimera path>");
            System.exit(4);
        }

        try (FileSystemProvider fs = FsFactory.createFileSystem(args)) {
            FsInode inode = fs.path2inode(args[FsFactory.ARGC]);

            String[] tags = fs.tags(inode);

            System.out.println("Total: " + tags.length);
            for (String tag : tags) {
                System.out.println(tag);
            }
        }

    }
}
