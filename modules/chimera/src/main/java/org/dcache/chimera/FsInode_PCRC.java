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

import org.dcache.util.Checksum;

/**
 * This class retrieves all the stored checksums for this file, returned as a
 * type:value comma-delimited list.
 *
 * @author arossi
 */
public class FsInode_PCRC extends FsInode_PGET {
    private String _checksum;

    public FsInode_PCRC(FileSystemProvider fs, long ino) {
        super(fs, ino, FsInodeType.PCRC);
    }

    protected String value() throws ChimeraFsException {
        if (_checksum == null) {
            Set<Checksum> results = _fs.getInodeChecksums(this);
            StringBuilder sb = new StringBuilder();

            Iterator<Checksum> it = results.iterator();
            if (it.hasNext()) {
                Checksum result = it.next();
                sb.append(result.getType()).append(":").append(
                                result.getValue());
            }

            while (it.hasNext()) {
                Checksum result = it.next();
                sb.append(", ").append(result.getType()).append(":").append(
                                result.getValue());
            }

            sb.append(NEWLINE);
            _checksum = sb.toString();
        }
        return _checksum;
    }
}
