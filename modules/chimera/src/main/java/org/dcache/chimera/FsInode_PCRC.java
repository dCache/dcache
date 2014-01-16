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

import java.util.Iterator;
import java.util.Set;

import org.dcache.chimera.posix.Stat;
import org.dcache.util.Checksum;

/**
 * This class retrieves all the stored checksums for this file, returned as a
 * type:value comma-delimited list.
 *
 * @author arossi
 */
public class FsInode_PCRC extends FsInode {
    private String _checksum;

    public FsInode_PCRC(FileSystemProvider fs, String id) {
        super(fs, id, FsInodeType.PCRC);
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len) {

        if (_checksum == null) {
            try {
                _checksum = getChecksums();
            } catch (ChimeraFsException e) {
                return -1;
            }
        }

        byte[] b = (_checksum).getBytes();

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

    @Override
    public Stat stat() throws ChimeraFsException {

        Stat ret = super.stat();
        ret.setMode((ret.getMode() & 0000777) | UnixPermission.S_IFREG);
        if (_checksum == null) {
            _checksum = getChecksums();
        }

        ret.setSize(_checksum.length());
        return ret;
    }

    @Override
    public int write(long pos, byte[] data, int offset, int len) {
        return -1;
    }

    private String getChecksums() throws ChimeraFsException {
        Set<Checksum> results = _fs.getInodeChecksums(this);
        StringBuilder sb = new StringBuilder();

        Iterator<Checksum> it = results.iterator();
        if (it.hasNext()) {
            Checksum result = it.next();
            sb.append(result.getType())
              .append(":")
              .append(result.getValue());
        }

        while (it.hasNext()) {
            Checksum result = it.next();
            sb.append(", ")
              .append(result.getType())
              .append(":")
              .append(result.getValue());
        }

        sb.append("\n");
        return sb.toString();
    }
}
