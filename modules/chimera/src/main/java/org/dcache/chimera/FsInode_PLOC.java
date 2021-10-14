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

/**
 * This class retrieves file locality metadata via an invocation of the pool manager.
 *
 * @author arossi
 */
public class FsInode_PLOC extends FsInode_PGET {

    private String _locality;

    public FsInode_PLOC(FileSystemProvider fs, long ino) {
        super(fs, ino, FsInodeType.PLOC);
    }

    @Override
    protected String value() throws ChimeraFsException {
        if (_locality == null) {
            _locality = _fs.getFileLocality(this);
        }
        return _locality + NEWLINE;
    }
}
