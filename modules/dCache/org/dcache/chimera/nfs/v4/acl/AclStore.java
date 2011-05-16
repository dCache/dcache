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

package org.dcache.chimera.nfs.v4.acl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.dcache.chimera.FsInode;
import org.dcache.chimera.nfs.v4.xdr.nfsace4;

public class AclStore {


	private final static AclStore ACL_STORE = new AclStore();

	private AclStore() {}

	static public AclStore getInstance() {
		return ACL_STORE;
	}

	private final Map<FsInode, nfsace4[] > _store = new ConcurrentHashMap<FsInode, nfsace4[] >();

	public void setAcl(FsInode inode, nfsace4[] ace) {
		_store.put(inode, ace);
	}

	public nfsace4[] getAcl(FsInode inode) {
		return _store.get(inode);
	}

}
