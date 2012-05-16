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

import java.util.Date;
import java.util.Formatter;

import org.dcache.chimera.DirectoryStreamB;
import org.dcache.chimera.FileSystemProvider;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.HimeraDirectoryEntry;
import org.dcache.chimera.posix.Stat;

public class Ls {

    public static void main(String[] args) throws Exception {

        if (args.length != FsFactory.ARGC + 1) {
            System.err.println("Usage : " + Ls.class.getName() + " " + FsFactory.USAGE
                    + " <chimera path>");
            System.exit(4);
        }

        FileSystemProvider fs = FsFactory.createFileSystem(args);
        FsInode inode = fs.path2inode(args[FsFactory.ARGC]);
        try {
            DirectoryStreamB<HimeraDirectoryEntry> dirStream = inode.newDirectoryStream();
            int count = 0;
            try {
                for (HimeraDirectoryEntry entry : dirStream) {
                    Stat stat = entry.getStat();
                    String s =
                        String.format("%o %6d %6d %6d %6d %s %s",
                                      stat.getMode(), stat.getNlink(),
                                      stat.getUid(), stat.getGid(),
                                      stat.getSize(), new Date(stat.getMTime()),
                                      entry.getName());
                    System.out.println(s);
                }
            } finally {
                dirStream.close();
            }
            System.out.println("Total: " + count);
        } finally {
            fs.close();
        }
    }
}
