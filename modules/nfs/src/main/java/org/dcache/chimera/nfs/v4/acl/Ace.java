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

import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.nfs.v4.HimeraNFS4Utils;
import org.dcache.chimera.nfs.v4.xdr.aceflag4;
import org.dcache.chimera.nfs.v4.xdr.acemask4;
import org.dcache.chimera.nfs.v4.xdr.acetype4;
import org.dcache.chimera.nfs.v4.xdr.nfs4_prot;
import org.dcache.chimera.nfs.v4.xdr.nfsace4;
import org.dcache.chimera.nfs.v4.xdr.uint32_t;
import org.dcache.chimera.nfs.v4.xdr.utf8str_mixed;
import org.dcache.chimera.nfs.v4.xdr.utf8string;

public class Ace {


	private final nfsace4[] _ace;


	public Ace(nfsace4[] ace) {
		_ace = ace;
	}

	/**
	 *
	 * converts unix permission mode to NFSv4 Ace
	 *
	 * @param unixMode
	 * @param isDir true, if mode belongs to a directory
	 * @return Ace representation on unix mode
	 */
	public static Ace valueOf(int unixMode, boolean isDir) {

		nfsace4[] ace = new nfsace4[6];

		nfsace4 ownerAceAllow = new nfsace4();
		nfsace4 groupAceAllow = new nfsace4();
		nfsace4 otherAceAllow = new nfsace4();

		nfsace4 ownerAceDeny = new nfsace4();
		nfsace4 groupAceDeny = new nfsace4();
		nfsace4 otherAceDeny = new nfsace4();

		ace[0] = ownerAceAllow;
		ace[1] = ownerAceDeny;

		ace[2] = groupAceAllow;
		ace[3] = groupAceDeny;

		ace[4] = otherAceAllow;
		ace[5] = otherAceDeny;


		int mode = unixMode & UnixPermission.S_PERMS;

		int maskAllow = 0;
		int maskDeny = 0;

		/*
		 * user
		 */


		/*
		 * owner of the file is allowed to modify permissions and ACLs
		 */


		maskAllow = nfs4_prot.ACE4_READ_ACL |nfs4_prot.ACE4_WRITE_ACL | nfs4_prot.ACE4_WRITE_ATTRIBUTES;
		maskDeny = 0;

		if( (mode & UnixPermission.S_IRUSR) == UnixPermission.S_IRUSR ) {
			maskAllow |= nfs4_prot.ACE4_READ_DATA;
		} else {
			maskDeny |= nfs4_prot.ACE4_READ_DATA;
		}

		if( (mode & UnixPermission.S_IWUSR) == UnixPermission.S_IWUSR ) {
			maskAllow |= nfs4_prot.ACE4_WRITE_DATA |nfs4_prot.ACE4_APPEND_DATA ;
			if(isDir) {
				maskAllow |= nfs4_prot.ACE4_DELETE_CHILD;
			}
		}else{
			maskDeny |= nfs4_prot.ACE4_WRITE_DATA |nfs4_prot.ACE4_APPEND_DATA ;
			if(isDir) {
				maskDeny |= nfs4_prot.ACE4_DELETE_CHILD;
			}
		}


		if( (mode & UnixPermission.S_IXUSR) == UnixPermission.S_IXUSR ) {
			maskAllow |= nfs4_prot.ACE4_EXECUTE;
		}else{
			maskDeny |= nfs4_prot.ACE4_EXECUTE;
		}


		ownerAceAllow.access_mask = new acemask4( new uint32_t( maskAllow ) );
		ownerAceAllow.type = new acetype4( new uint32_t( nfs4_prot.ACE4_ACCESS_ALLOWED_ACE_TYPE ) );
		ownerAceAllow.flag = new aceflag4( new uint32_t(0) );
		ownerAceAllow.who = new utf8str_mixed( new utf8string( "OWNER@".getBytes() ));



		ownerAceDeny.access_mask = new acemask4( new uint32_t( maskDeny ) );
		ownerAceDeny.type = new acetype4( new uint32_t( nfs4_prot.ACE4_ACCESS_DENIED_ACE_TYPE ) );
		ownerAceDeny.flag = new aceflag4( new uint32_t(0) );
		ownerAceDeny.who = new utf8str_mixed( new utf8string( "OWNER@".getBytes() ));



		/*
		 * group
		 */

		maskAllow = 0;
		maskDeny = 0;

		if( (mode & UnixPermission.S_IRGRP) == UnixPermission.S_IRGRP ) {
			maskAllow |= nfs4_prot.ACE4_READ_DATA;
		}else{
			maskDeny |= nfs4_prot.ACE4_READ_DATA;
		}

		if( (mode & UnixPermission.S_IWGRP) == UnixPermission.S_IWGRP ) {
			maskAllow |= nfs4_prot.ACE4_WRITE_DATA |nfs4_prot.ACE4_APPEND_DATA ;
			if(isDir) {
				maskAllow |= nfs4_prot.ACE4_DELETE_CHILD;
			}
		}else{
			maskDeny |= nfs4_prot.ACE4_WRITE_DATA |nfs4_prot.ACE4_APPEND_DATA ;
			if(isDir) {
				maskDeny |= nfs4_prot.ACE4_DELETE_CHILD;
			}
		}


		if( (mode & UnixPermission.S_IXGRP) == UnixPermission.S_IXGRP ) {
			maskAllow |= nfs4_prot.ACE4_EXECUTE;
		}else{
			maskDeny |= nfs4_prot.ACE4_EXECUTE;
		}

		groupAceAllow.access_mask = new acemask4( new uint32_t( maskAllow ) );
		groupAceAllow.type = new acetype4( new uint32_t( nfs4_prot.ACE4_ACCESS_ALLOWED_ACE_TYPE ) );
		groupAceAllow.flag = new aceflag4( new uint32_t(0) );
		groupAceAllow.who = new utf8str_mixed( new utf8string( "GROUP@".getBytes() ));

		groupAceDeny.access_mask = new acemask4( new uint32_t( maskDeny ) );
		groupAceDeny.type = new acetype4( new uint32_t( nfs4_prot.ACE4_ACCESS_DENIED_ACE_TYPE ) );
		groupAceDeny.flag = new aceflag4( new uint32_t(0) );
		groupAceDeny.who = new utf8str_mixed( new utf8string( "GROUP@".getBytes() ));


		/*
		 * other
		 */

		maskAllow = 0;
		maskDeny = 0;

		if( (mode & UnixPermission.S_IROTH) == UnixPermission.S_IROTH ) {
			maskAllow |= nfs4_prot.ACE4_READ_DATA;
		}else{
			maskDeny |= nfs4_prot.ACE4_READ_DATA;
		}

		if( (mode & UnixPermission.S_IWOTH) == UnixPermission.S_IWOTH ) {
			maskAllow |= nfs4_prot.ACE4_WRITE_DATA |nfs4_prot.ACE4_APPEND_DATA ;
			if(isDir) {
				maskAllow |= nfs4_prot.ACE4_DELETE_CHILD;
			}
		}else{
			maskDeny |= nfs4_prot.ACE4_WRITE_DATA |nfs4_prot.ACE4_APPEND_DATA ;
			if(isDir) {
				maskDeny |= nfs4_prot.ACE4_DELETE_CHILD;
			}
		}


		if( (mode & UnixPermission.S_IXOTH) == UnixPermission.S_IXOTH ) {
			maskAllow |= nfs4_prot.ACE4_EXECUTE;
		}else{
			maskDeny |= nfs4_prot.ACE4_EXECUTE;
		}

		otherAceAllow.access_mask = new acemask4( new uint32_t( maskAllow ) );
		otherAceAllow.type = new acetype4( new uint32_t( nfs4_prot.ACE4_ACCESS_ALLOWED_ACE_TYPE ) );
		otherAceAllow.flag = new aceflag4( new uint32_t(0) );
		otherAceAllow.who = new utf8str_mixed( new utf8string( "EVERYONE@".getBytes() ));

		otherAceDeny.access_mask = new acemask4( new uint32_t( maskDeny ) );
		otherAceDeny.type = new acetype4( new uint32_t( nfs4_prot.ACE4_ACCESS_DENIED_ACE_TYPE ) );
		otherAceDeny.flag = new aceflag4( new uint32_t(0) );
		otherAceDeny.who = new utf8str_mixed( new utf8string( "EVERYONE@".getBytes() ));


		return new Ace(ace);

	}


	public String toString() {

		StringBuilder sb = new StringBuilder();

		for(nfsace4 ace: _ace) {
			sb.append( HimeraNFS4Utils.aceToString(ace) ).append("\n");
		}

		return sb.toString();
	}


	public nfsace4[] getNfs4Ace() {
		return _ace;
	}

	public static void main(String[] args) {

		System.out.println( Ace.valueOf(0644, false) );

	}


}
