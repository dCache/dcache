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

import com.google.common.base.Charsets;
import org.dcache.chimera.posix.Stat;

public class FsInode_CONST extends FsInode {

    private static final String _title = "\n >> Chimera FS Engine Version 0.0.9 $Rev: 897 $ << \n";
    private final byte[] _version;

    public FsInode_CONST(FileSystemProvider fs, long ino) {
        super(fs, ino, FsInodeType.CONST);

        _version = (_title + '\n' + _fs.getInfo() + '\n').getBytes(Charsets.UTF_8);
    }

    @Override
    public boolean exists() {
        return true;
    }

    // as soon as exists is overridden, we have to override isLink and isDirectory
    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public boolean isLink() {
        return false;
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len) {

        /*
         * are we still inside ?
         */
        if (pos > _version.length) {
            return 0;
        }
        int copyLen = Math.min(len, _version.length - (int) pos);
        System.arraycopy(_version, (int) pos, data, 0, copyLen);

        return copyLen;
    }

    @Override
    public void setStat(Stat predefinedStat) {
	// nop
    }

    /**
     * fake reply: the inode is a file
     * size is equal to size of version string
     * access time, creation time, modification time is current time.
     */
    @Override
    public Stat stat() throws ChimeraFsException {

        Stat ret = new Stat(super.stat());
        ret.setNlink(1);
        ret.setMode(0444 | UnixPermission.S_IFREG);
        ret.setSize(_version.length);
        ret.setATime(System.currentTimeMillis());
        ret.setMTime(ret.getATime());
        ret.setCTime(ret.getATime());
        ret.setUid(0);
        ret.setGid(0);

        return ret;

    }

    /*
     *  so to say:  read only file
     */
    @Override
    public int write(long pos, byte[] data, int offset, int len) {
        return -1;
    }
}
