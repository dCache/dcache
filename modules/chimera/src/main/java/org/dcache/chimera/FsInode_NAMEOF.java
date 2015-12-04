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

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.dcache.chimera.posix.Stat;

public class FsInode_NAMEOF extends FsInode {

    byte[] _name;

    public FsInode_NAMEOF(FileSystemProvider fs, long ino) {
        super(fs, ino, FsInodeType.NAMEOF);
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len) {

        if (_name == null) {
            File f;
            try {
                f = new File(_fs.inode2path(this));
            } catch (ChimeraFsException e) {
                return -1;
            }
            _name = (f.getName()+'\n').getBytes(StandardCharsets.UTF_8);
        }

        /*
         * are we still inside ?
         */
        if (pos > _name.length) {
            return 0;
        }

        int copyLen = Math.min(len, _name.length - (int) pos);
        System.arraycopy(_name, (int) pos, data, 0, copyLen);

        return copyLen;
    }

    @Override
    public Stat stat() throws ChimeraFsException {
        Stat ret = super.stat();
        ret.setMode((ret.getMode() & 0000777) | UnixPermission.S_IFREG);
        if (_name == null) {
            File f = new File(_fs.inode2path(this));
            _name = (f.getName()+'\n').getBytes(StandardCharsets.UTF_8);
        }
        ret.setSize(_name.length);
        return ret;
    }

    @Override
    public int write(long pos, byte[] data, int offset, int len) {
        return -1;
    }
}
