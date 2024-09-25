/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2023 Deutsches Elektronen-Synchrotron
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

public class FsInode_LABEL extends FsInode {

    /**
     * @param fs    pointer to 'File System'
     * @param ino   inode number of the label_id
     */
    public FsInode_LABEL(FileSystemProvider fs, long ino) {
        super(fs, ino, FsInodeType.LABEL);
    }

    @Override
    public boolean exists() {
        boolean rc = false;
        try {
            if (!_fs.getLabelById(ino()).isEmpty()) {
                rc = true;
            }
        } catch (Exception e) {
        }
        return rc;
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
