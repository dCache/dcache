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

public class FsInode_ID extends FsInode {

    public FsInode_ID(FileSystemProvider fs, long ino) {
        super(fs, ino, FsInodeType.ID);
    }

    @Override
    public Stat stat() throws ChimeraFsException {

        Stat ret = new Stat(super.stat());

        ret.setSize(ret.getId().length() + 1);
        ret.setMode((ret.getMode() & 0000777) | UnixPermission.S_IFREG);

        return ret;
    }

    @Override
    public int write(long pos, byte[] data, int offset, int len) {
        return -1;
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len) throws ChimeraFsException
    {
        byte b[] = (statCache().getId() + "\n").getBytes();

        /*
         * are we still inside ?
         */
        if (pos > b.length) {
            return 0;
        }

        int copyLen = Math.min(len, b.length - (int) pos);
        System.arraycopy(b, (int) pos, data, 0, copyLen);

        return copyLen;
    }
}
