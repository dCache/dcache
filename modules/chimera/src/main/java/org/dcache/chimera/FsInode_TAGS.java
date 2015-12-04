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

public class FsInode_TAGS extends FsInode {

    public FsInode_TAGS(FileSystemProvider fs, long ino) {
        super(fs, ino, FsInodeType.TAGS);
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len) {

        StringBuilder sb = new StringBuilder();
        int rc;
        String[] list;
        try {
            list = _fs.tags(this);
            for (String tag : list) {
                sb.append(".(tag)(").append(tag).append(")\n");
            }

            rc = sb.length();
            /*
             * are we still inside ?
             */
            if (pos > rc) {
                return 0;
            }

            byte[] tmp = sb.toString().getBytes();

            int copyLen = Math.min(len, tmp.length - (int) pos);
            System.arraycopy(tmp, 0, data, 0, copyLen);
            rc = copyLen;
        } catch (Exception e) {
            e.printStackTrace();
            rc = -1;
        }

        return rc;
    }

    @Override
    public Stat stat() throws ChimeraFsException {

        long size = 0;
        Stat stat = super.stat();
        stat.setNlink(1);
        stat.setMode(0444 | UnixPermission.S_IFREG);

        try {
            String[] list = _fs.tags(this);
            for (String tag : list) {
                size += (9 + tag.length());
            }
            stat.setSize(size);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return stat;
    }

    @Override
    public int write(long pos, byte[] data, int offset, int len) {
        return -1;
    }
}
