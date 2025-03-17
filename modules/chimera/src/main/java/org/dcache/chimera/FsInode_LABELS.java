/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2025 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.chimera;

import org.dcache.chimera.posix.Stat;

public class FsInode_LABELS extends FsInode {

    /**
     * @param fs    pointer to 'File System'
     * @param ino   inode number of the label_id
     */
    public FsInode_LABELS(FileSystemProvider fs, long ino) {
        super(fs, ino, FsInodeType.LABELS);
    }

    public FsInode_LABELS(FileSystemProvider fs, long ino, Stat stat) {
        super(fs, ino, FsInodeType.LABELS, 0, stat);
    }

    @Override
    public boolean exists() {
        boolean rc = true;

        return rc;
    }

    @Override
    public Stat stat() throws ChimeraFsException {
        if (!exists()) {
            throw FileNotFoundChimeraFsException.ofTag(this);
        }

        Stat ret = _fs.statLabelsParent(this);
       // ret.setMode((ret.getMode() & 0000777) | UnixPermission.S_IFREG);
        return ret;
    }

    @Override
    public boolean isDirectory() {
        return true;
    }

    @Override
    public boolean isLink() {
        return false;
    }

}
