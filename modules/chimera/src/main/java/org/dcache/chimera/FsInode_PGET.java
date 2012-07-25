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

import java.util.Arrays;

import org.dcache.chimera.posix.Stat;

public class FsInode_PGET extends FsInode {

    private String[] _args;

    public FsInode_PGET(FileSystemProvider fs, String id, String[] args) {
        super(fs, id, FsInodeType.PGET);
        _args = args.clone();
    }

    @Override
    public Stat stat() throws ChimeraFsException {
        Stat ret = super.stat();
        ret.setMode((ret.getMode() & 0000777) | UnixPermission.S_IFREG);
        ret.setSize(0);
        return ret;
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len) {
        return 0;
    }

    @Override
    public int write(long pos, byte[] data, int offset, int len) {
        return -1;
    }

    @Override
    public String toFullString() {
        StringBuilder sb = new StringBuilder();

        sb.append(_fs.getFsId()).append(":").append(type()).append(":").append(_id);

        for (int i = 0; i < _args.length; i++) {
            sb.append(":").append(_args[i]);
        }

        return sb.toString();
    }

    /* (non-Javadoc)
     * @see org.dcache.chimera.FsInode#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {

        if (o == this) {
            return true;
        }

        if (!(o instanceof FsInode_PGET)) {
            return false;
        }

        return super.equals(o) && Arrays.equals(_args, ((FsInode_PGET) o)._args);
    }


    /* (non-Javadoc)
     * @see org.dcache.chimera.FsInode#hashCode()
     */
    @Override
    public int hashCode() {
        return 17;
    }
}
