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

public class FsInode_PARENT extends FsInode {

    FsInode _parent;

    public FsInode_PARENT(FileSystemProvider fs, long ino) {
        super(fs, ino, FsInodeType.PARENT);
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len) {

        int rc = -1;

        if (_parent == null) {
            try {
                _parent = _fs.getParentOf(this);
            } catch (ChimeraFsException e) {
                return -1;
            }
        }

        // if parent and we have same id then that's end
        if (_parent.ino() != ino()) {
            byte[] b;
            try {
                b = (_parent.statCache().getId() + '\n').getBytes();
            } catch (ChimeraFsException e) {
                return -1;
            }

            /*
             * are we still inside ?
             */
            if (pos > b.length) {
                return 0;
            }

            int copyLen = Math.min(len, b.length - (int) pos);
            System.arraycopy(b, (int) pos, data, 0, copyLen);

            rc = copyLen;
        }

        return rc;
    }

    @Override
    public Stat stat() throws ChimeraFsException {

        Stat ret = new Stat(super.stat());
        ret.setMode((ret.getMode() & 0000777) | UnixPermission.S_IFREG);
        if (_parent == null) {
            FsInode parentInode = _fs.getParentOf(this);
            if (parentInode == null) {
                throw new FileNotFoundHimeraFsException();
            }
            _parent = parentInode;
        }

        if (_parent.ino() == ino()) {
            throw new ChimeraFsException("Parent and child equal");
        }

        ret.setSize(_parent.statCache().getId().length() + 1);
        return ret;
    }

    @Override
    public int write(long pos, byte[] data, int offset, int len) {
        return -1;
    }
}
