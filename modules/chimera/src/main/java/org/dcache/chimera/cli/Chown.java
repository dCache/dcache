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

import static com.google.common.base.Preconditions.checkArgument;

public class Chown
{
    private static final int DUMMY_GID_VALUE = -1;

    private static int _uid;
    private static int _gid;

    public static void main(String[] args) throws Exception
    {

        if (args.length != FsFactory.ARGC + 2) {
            System.err.println(
                    "Usage : " + Chown.class.getName() + " " + FsFactory.USAGE
                    + " <chimera path> <uid>[:<gid>]");
            System.exit(4);
        }

        try {
            parseOwnership(args[FsFactory.ARGC + 1]);
        } catch(IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        try (FileSystemProvider fs = FsFactory.createFileSystem(args)) {
            FsInode inode = fs.path2inode(args[FsFactory.ARGC]);
            inode.setUID(_uid);
            if(_gid != DUMMY_GID_VALUE) {
                inode.setGID(_gid);
            }
        }
    }

    private static void parseOwnership(String ownership)
    {
        int colon = ownership.indexOf(':');


        if(colon == -1) {
            _uid = parseInteger(ownership);
            _gid = DUMMY_GID_VALUE;
        } else {
            checkArgument(colon > 0 && colon < ownership.length()-1,
                    "colon must separate two integers");

            _uid = parseInteger(ownership.substring(0, colon));
            _gid = parseInteger(ownership.substring(colon+1));
            checkArgument(_gid >= 0, "gid must be 0 or greater");
        }

        checkArgument(_uid >= 0, "uid must be 0 or greater");
    }

    private static int parseInteger(String value)
    {
        try {
            return Integer.valueOf(value);
        } catch(NumberFormatException e) {
            throw new IllegalArgumentException("only integer values are " +
                    "allowed and \"" + value +"\" is not an integer");
        }
    }
}
