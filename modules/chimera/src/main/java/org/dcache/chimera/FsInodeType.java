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

public enum FsInodeType {

    INODE(0),    // regular inode
    TAG(1),      // the content of the inode is a directory tag
    TAGS(2),     // the content of the inode is a list of a directory tags
    ID(3),       // the content of the inode is the id of the inode
    PATHOF(4),   // the content of the inode is the absolute path of the inode
    PARENT(5),   // the content of the inode is the of the parent inode
    NAMEOF(6),   // the content of the inode is the name of the inode
    PCUR(7),     // the content of the inode is the value of cursor
    PSET(8),     // by updating mtime of the inode the the defined attribute value is updated
    CONST(9),    // the content of the inode is a free form information
    PLOC(10),    // the content of the inode is the value of requested attributes
    PCRC(11);    // the content of the inode is a name-value list of checksum types and checksums

    private final int _id;

    FsInodeType(int id) {
        _id = id;
    }

    public int getType() {
        return _id;
    }

    public static FsInodeType valueOf(int id) {
        for (FsInodeType type : FsInodeType.values()) {
            if (type.getType() == id) {
                return type;
            }
        }
        throw new IllegalArgumentException("No such type: " + id);
    }
}
