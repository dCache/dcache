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
import org.dcache.chimera.FsFactory;
import org.dcache.chimera.FsInode;

import static com.google.common.io.ByteStreams.toByteArray;

/**
 * Simple program for creating a tag or updating a tag's
 * contents.
 */
public class Writetag {

    public static void main(String[] args) throws Exception
    {
        int programArgc = args.length - FsFactory.ARGC;

        if (programArgc < 2 || programArgc > 3) {
            System.err.println(
                    "Usage : " + Writetag.class.getName() + " " + FsFactory.USAGE
                    + " <chimera path> <tag> [<data>]");
            System.exit(4);
        }

        try (FileSystemProvider fs = FsFactory.createFileSystem(args)) {
            FsInode inode = fs.path2inode(args[FsFactory.ARGC]);
            String tag = args[FsFactory.ARGC + 1];

            try {
                fs.statTag(inode, tag);
            } catch (FileNotFoundHimeraFsException fnf) {
                fs.createTag(inode, tag);
            }

            byte[] data = programArgc == 2 ? toByteArray(System.in) :
                    newLineTerminated(args[FsFactory.ARGC + 2]).getBytes();

            if (data.length > 0) {
                fs.setTag(inode, tag, data, 0, data.length);
            }
        }

    }

    private static String newLineTerminated(String unknown)
    {
        return unknown.endsWith("\n") ? unknown : unknown + "\n";
    }
}
