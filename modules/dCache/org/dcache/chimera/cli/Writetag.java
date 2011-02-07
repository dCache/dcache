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

import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;

public class Writetag {

    public static void main(String[] args) throws Exception {

        if (args.length != FsFactory.ARGC + 2) {
            System.err.println(
                    "Usage : " + Readtag.class.getName() + " " + FsFactory.USAGE
                    + " <chimera path> <tag>");
            System.exit(4);
        }

        FileSystemProvider fs = FsFactory.createFileSystem(args);

        FsInode inode = fs.path2inode(args[FsFactory.ARGC]);

        try {

            fs.statTag(inode, args[FsFactory.ARGC + 1]);

        } catch (FileNotFoundHimeraFsException fnf) {
            fs.createTag(inode, args[FsFactory.ARGC + 1]);
        }


        byte[] data = new byte[4096];

        int len = 0;

        while (len < data.length) {

            int n = System.in.read(data, len, data.length - len);
            if (n <= 0) {
                break;
            }
            len += n;

        }
        if (len > 0) {
            fs.setTag(inode, args[FsFactory.ARGC + 1], data, 0, len);
        }

    }
}
