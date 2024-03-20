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

import java.util.Set;

public class FsInode_LABEL extends FsInode {

    public String getLabel() {
        return _label;
    }

    private final String _label;

    /**
     * @param fs    pointer to 'File System'
     * @param ino   inode number of the label_id
     * @param label
     */
    public FsInode_LABEL(FileSystemProvider fs, long ino, String label) {
        super(fs, ino, FsInodeType.LABEL);
        _label = label;
    }

    @Override
    public boolean exists() {
        boolean rc = false;
        try {
            Set<String> list = _fs.getLabels(this);
            if (list.contains(_label)) {
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
