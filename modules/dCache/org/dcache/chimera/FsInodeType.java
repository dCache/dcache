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

    INODE, // regular inode
    TAG, // the content of the inode is a directory tag
    TAGS, // the content of the inode is a list of a directory tags
    ID, // the content of the inode is the id of the inode
    PATHOF, // the content of the inode is the absolute path of the inode
    PARENT, // the content of the inode is the of the parent inode
    NAMEOF, // the content of the inode is the name of the inode
    PGET, // the content of the inode is the value of requested attributes
    PSET, // by updating mtime of the inode the the defined attribute value is updated
    CONST    // the content of the inode is a free form information
}
