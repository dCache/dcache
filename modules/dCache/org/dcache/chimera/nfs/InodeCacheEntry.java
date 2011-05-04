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
package org.dcache.chimera.nfs;

import org.dcache.chimera.FsInode;

/**
 * An inode entry with verifier.
 *
 * @param <T>
 */
public class InodeCacheEntry<T> {

    private final FsInode _inode;
    private final T _verifier;

    public InodeCacheEntry(FsInode inode, T verifier) {
        _inode = inode;
        _verifier = verifier;
    }

    @Override
    public boolean equals(Object obj) {

        if(obj == this) return true;

        if( !obj.getClass().equals(this.getClass()) ) return false;
        InodeCacheEntry<T> other = (InodeCacheEntry<T>)obj;
        return _inode.equals(other._inode) && _verifier.equals(other._verifier);
    }

    @Override
    public int hashCode() {
        return _inode.hashCode() ^ _verifier.hashCode();
    }
}
