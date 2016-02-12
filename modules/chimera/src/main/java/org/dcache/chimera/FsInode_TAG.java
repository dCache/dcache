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

public class FsInode_TAG extends FsInode {

    private final String _tag;

    /**
     *
     * @param fs pointer to 'File System'
     * @param ino inode number of the
     * @param tag
     */
    public FsInode_TAG(FileSystemProvider fs, long ino, String tag) {
        super(fs, ino, FsInodeType.TAG);
        _tag = tag;
    }

    @Override
    public boolean exists() {
        boolean rc = false;
        try {
            String[] list = _fs.tags(this);
            for (String tag : list) {
                if (tag.equals(_tag)) {
                    rc = true;
                }
            }
        } catch (Exception e) {
        }
        return rc;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public boolean isLink() {
        return false;
    }

    @Override
    public Stat stat() throws ChimeraFsException {
        if (!exists()) {
            throw new FileNotFoundHimeraFsException("tag do not exist");
        }

        Stat ret = _fs.statTag(this, _tag);
        ret.setMode((ret.getMode() & 0000777) | UnixPermission.S_IFREG);
        return ret;
    }

    @Override
    public int write(long pos, byte[] data, int offset, int len) {
        int ret = -1;
        try {
            ret = _fs.setTag(this, _tag, data, offset, len);
        } catch (ChimeraFsException e) {
        }
        return ret;
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len) {
        int ret = -1;
        try {
            ret = _fs.getTag(this, _tag, data, offset, len);
        } catch (ChimeraFsException e) {
        }
        return ret;
    }

    @Override
    public byte[] getIdentifier() {
        return byteBase(_tag.getBytes(Charsets.UTF_8));
    }


    /* (non-Javadoc)
     * @see org.dcache.chimera.FsInode#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {

        if (o == this) {
            return true;
        }

        if (!(o instanceof FsInode_TAG)) {
            return false;
        }

        return super.equals(o) && _tag.equals(((FsInode_TAG) o)._tag);
    }

    public String tagName() {
        return _tag;
    }

    /* (non-Javadoc)
     * @see org.dcache.chimera.FsInode#hashCode()
     */
    @Override
    public int hashCode() {
        return 17;
    }
}
