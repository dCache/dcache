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
package org.dcache.chimera.store;

import org.dcache.chimera.FsInode;

/*
STORAGE INFO

Generic storage information. Represents X:Y@Z
ipnfsid          : pnfsid of the inode
ihsmName         : Z-component of storageGroup
istorageGroup    : X-component of storageGroup
istorageSubGroup : Y-component of storageGroup

 */
/*
 *	@Immutable
 */
public class InodeStorageInformation {

    private final FsInode _inode;
    private final String _hsmName;
    private final String _storageGroup;
    private final String _storageSubGroup;

    public InodeStorageInformation(FsInode inode, String hsm, String sGroup, String ssGroup) {

        if (inode == null) {
            throw new IllegalArgumentException("inode is null");
        }

        if (hsm == null) {
            throw new IllegalArgumentException("HSM is not defined");
        }

        if (sGroup == null) {
            throw new IllegalArgumentException("Storage Group is not defined");
        }

        if (ssGroup == null) {
            throw new IllegalArgumentException("Storage Sub Group is not defined");
        }

        _inode = inode;
        _hsmName = hsm;
        _storageGroup = sGroup;
        _storageSubGroup = ssGroup;
    }

    public FsInode inode() {
        return _inode;
    }

    public String hsmName() {
        return _hsmName;
    }

    public String storageGroup() {
        return _storageGroup;
    }

    public String storageSubGroup() {
        return _storageSubGroup;
    }
}
