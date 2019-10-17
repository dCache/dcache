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

import java.nio.charset.StandardCharsets;
import org.dcache.chimera.posix.Stat;
import org.dcache.util.ChecksumType;

/**
 *   cat ".(checksums)()" returns the available checksum types.
 */
public class FsInode_CKSTYP extends FsInode {

    private final String checksums;

    public FsInode_CKSTYP(FileSystemProvider fs, long ino) {
        super(fs, ino, FsInodeType.CKSTYP);
        checksums = getChecksums();
    }

    @Override
    public boolean exists() {
        return true;
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
    public int read(long pos, byte[] data, int offset, int len) {
        if (pos > checksums.length()) {
            return 0;
        }

        byte[] tmp = checksums.getBytes(StandardCharsets.US_ASCII);

        int copyLen = Math.min(len, tmp.length - (int) pos);
        System.arraycopy(tmp, 0, data, 0, copyLen);
        return copyLen;
    }

    @Override
    public void setStat(Stat predefinedStat) {
        // nop
    }

    /*
     *  Fake stat to make this behave like a file
     *  (cf. FsInode_CONST).
     */
    @Override
    public Stat stat() throws ChimeraFsException {
        Stat ret = new Stat(super.stat());
        ret.setNlink(1);
        ret.setMode(0444 | UnixPermission.S_IFREG);
        ret.setATime(System.currentTimeMillis());
        ret.setMTime(ret.getATime());
        ret.setCTime(ret.getATime());
        ret.setUid(0);
        ret.setGid(0);
        ret.setSize(checksums.length());
        return ret;
    }

    @Override
    public int write(long pos, byte[] data, int offset, int len) {
        return -1;
    }

    private String getChecksums() {
        StringBuilder sb = new StringBuilder();
        ChecksumType[] list = ChecksumType.values();

        for (ChecksumType type : list) {
            sb.append(type.getName()).append("\n");
        }

        return sb.toString();
    }
}
