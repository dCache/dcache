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
package org.dcache.chimera.posix;

/*
 * Immutable
 */
public class UnixAcl extends Acl {

    private final int _owner;
    private final int _group;
    private final int _permission;

    public UnixAcl(int owner, int group, int permission) {
        _owner = owner;
        _group = group;
        _permission = permission;
    }

    public int getGroup() {
        return _group;
    }

    public int getPermission() {
        return _permission;
    }

    public int getOwner() {
        return _owner;
    }
}
