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

package org.dcache.chimera.nfs.v4;

import org.dcache.chimera.nfs.v4.xdr.nfsstat4;

public class NFSv41Error {

    private NFSv41Error() {
        // no instances allowed
    }

    public static String errcode2string(int errorCode) {

        switch (errorCode) {
            case nfsstat4.NFS4_OK:
                return "NFS4_OK";
            case nfsstat4.NFS4ERR_PERM:
                return "NFS4ERR_PERM";
            case nfsstat4.NFS4ERR_NOENT:
                return "NFS4ERR_NOENT";
            case nfsstat4.NFS4ERR_IO:
                return "NFS4ERR_IO";
            case nfsstat4.NFS4ERR_NXIO:
                return "NFS4ERR_NXIO";
            case nfsstat4.NFS4ERR_ACCESS:
                return "NFS4ERR_ACCESS";
            case nfsstat4.NFS4ERR_EXIST:
                return "NFS4ERR_EXIST";
            case nfsstat4.NFS4ERR_XDEV:
                return "NFS4ERR_XDEV";
            case nfsstat4.NFS4ERR_NOTDIR:
                return "NFS4ERR_NOTDIR";
            case nfsstat4.NFS4ERR_ISDIR:
                return "NFS4ERR_ISDIR";
            case nfsstat4.NFS4ERR_INVAL:
                return "NFS4ERR_INVAL";
            case nfsstat4.NFS4ERR_FBIG:
                return "NFS4ERR_FBIG";
            case nfsstat4.NFS4ERR_NOSPC:
                return "NFS4ERR_NOSPC";
            case nfsstat4.NFS4ERR_ROFS:
                return "NFS4ERR_ROFS";
            case nfsstat4.NFS4ERR_MLINK:
                return "NFS4ERR_MLINK";
            case nfsstat4.NFS4ERR_NAMETOOLONG:
                return "NFS4ERR_NAMETOOLONG";
            case nfsstat4.NFS4ERR_NOTEMPTY:
                return "NFS4ERR_NOTEMPTY";
            case nfsstat4.NFS4ERR_DQUOT:
                return "NFS4ERR_DQUOT";
            case nfsstat4.NFS4ERR_STALE:
                return "NFS4ERR_STALE";
            case nfsstat4.NFS4ERR_BADHANDLE:
                return "NFS4ERR_BADHANDLE";
            case nfsstat4.NFS4ERR_BAD_COOKIE:
                return "NFS4ERR_BAD_COOKIE";
            case nfsstat4.NFS4ERR_NOTSUPP:
                return "NFS4ERR_NOTSUPP";
            case nfsstat4.NFS4ERR_TOOSMALL:
                return "NFS4ERR_TOOSMALL";
            case nfsstat4.NFS4ERR_SERVERFAULT:
                return "NFS4ERR_SERVERFAULT";
            case nfsstat4.NFS4ERR_BADTYPE:
                return "NFS4ERR_BADTYPE";
            case nfsstat4.NFS4ERR_DELAY:
                return "NFS4ERR_DELAY";
            case nfsstat4.NFS4ERR_SAME:
                return "NFS4ERR_SAME";
            case nfsstat4.NFS4ERR_DENIED:
                return "NFS4ERR_DENIED";
            case nfsstat4.NFS4ERR_EXPIRED:
                return "NFS4ERR_EXPIRED";
            case nfsstat4.NFS4ERR_LOCKED:
                return "NFS4ERR_LOCKED";
            case nfsstat4.NFS4ERR_GRACE:
                return "NFS4ERR_GRACE";
            case nfsstat4.NFS4ERR_FHEXPIRED:
                return "NFS4ERR_FHEXPIRED";
            case nfsstat4.NFS4ERR_SHARE_DENIED:
                return "NFS4ERR_SHARE_DENIED";
            case nfsstat4.NFS4ERR_WRONGSEC:
                return "NFS4ERR_WRONGSEC";
            case nfsstat4.NFS4ERR_CLID_INUSE:
                return "NFS4ERR_CLID_INUSE";
            case nfsstat4.NFS4ERR_RESOURCE:
                return "NFS4ERR_RESOURCE";
            case nfsstat4.NFS4ERR_MOVED:
                return "NFS4ERR_MOVED";
            case nfsstat4.NFS4ERR_NOFILEHANDLE:
                return "NFS4ERR_NOFILEHANDLE";
            case nfsstat4.NFS4ERR_MINOR_VERS_MISMATCH:
                return "NFS4ERR_MINOR_VERS_MISMATCH";
            case nfsstat4.NFS4ERR_STALE_CLIENTID:
                return "NFS4ERR_STALE_CLIENTID";
            case nfsstat4.NFS4ERR_STALE_STATEID:
                return "NFS4ERR_STALE_STATEID";
            case nfsstat4.NFS4ERR_OLD_STATEID:
                return "NFS4ERR_OLD_STATEID";
            case nfsstat4.NFS4ERR_BAD_STATEID:
                return "NFS4ERR_BAD_STATEID";
            case nfsstat4.NFS4ERR_BAD_SEQID:
                return "NFS4ERR_BAD_SEQID";
            case nfsstat4.NFS4ERR_NOT_SAME:
                return "NFS4ERR_NOT_SAME";
            case nfsstat4.NFS4ERR_LOCK_RANGE:
                return "NFS4ERR_LOCK_RANGE";
            case nfsstat4.NFS4ERR_SYMLINK:
                return "NFS4ERR_SYMLINK";
            case nfsstat4.NFS4ERR_RESTOREFH:
                return "NFS4ERR_RESTOREFH";
            case nfsstat4.NFS4ERR_LEASE_MOVED:
                return "NFS4ERR_LEASE_MOVED";
            case nfsstat4.NFS4ERR_ATTRNOTSUPP:
                return "NFS4ERR_ATTRNOTSUPP";
            case nfsstat4.NFS4ERR_NO_GRACE:
                return "NFS4ERR_NO_GRACE";
            case nfsstat4.NFS4ERR_RECLAIM_BAD:
                return "NFS4ERR_RECLAIM_BAD";
            case nfsstat4.NFS4ERR_RECLAIM_CONFLICT:
                return "NFS4ERR_RECLAIM_CONFLICT";
            case nfsstat4.NFS4ERR_BADXDR:
                return "NFS4ERR_BADXDR";
            case nfsstat4.NFS4ERR_LOCKS_HELD:
                return "NFS4ERR_LOCKS_HELD";
            case nfsstat4.NFS4ERR_OPENMODE:
                return "NFS4ERR_OPENMODE";
            case nfsstat4.NFS4ERR_BADOWNER:
                return "NFS4ERR_BADOWNER";
            case nfsstat4.NFS4ERR_BADCHAR:
                return "NFS4ERR_BADCHAR";
            case nfsstat4.NFS4ERR_BADNAME:
                return "NFS4ERR_BADNAME";
            case nfsstat4.NFS4ERR_BAD_RANGE:
                return "NFS4ERR_BAD_RANGE";
            case nfsstat4.NFS4ERR_LOCK_NOTSUPP:
                return "NFS4ERR_LOCK_NOTSUPP";
            case nfsstat4.NFS4ERR_OP_ILLEGAL:
                return "NFS4ERR_OP_ILLEGAL";
            case nfsstat4.NFS4ERR_DEADLOCK:
                return "NFS4ERR_DEADLOCK";
            case nfsstat4.NFS4ERR_FILE_OPEN:
                return "NFS4ERR_FILE_OPEN";
            case nfsstat4.NFS4ERR_ADMIN_REVOKED:
                return "NFS4ERR_ADMIN_REVOKED";
            case nfsstat4.NFS4ERR_CB_PATH_DOWN:
                return "NFS4ERR_CB_PATH_DOWN";
            case nfsstat4.NFS4ERR_BADIOMODE:
                return "NFS4ERR_BADIOMODE";
            case nfsstat4.NFS4ERR_BADLAYOUT:
                return "NFS4ERR_BADLAYOUT";
            case nfsstat4.NFS4ERR_BAD_SESSION_DIGEST:
                return "NFS4ERR_BAD_SESSION_DIGEST";
            case nfsstat4.NFS4ERR_BADSESSION:
                return "NFS4ERR_BADSESSION";
            case nfsstat4.NFS4ERR_BADSLOT:
                return "NFS4ERR_BADSLOT";
            case nfsstat4.NFS4ERR_COMPLETE_ALREADY:
                return "NFS4ERR_COMPLETE_ALREADY";
            case nfsstat4.NFS4ERR_CONN_NOT_BOUND_TO_SESSION:
                return "NFS4ERR_CONN_NOT_BOUND_TO_SESSION";
            case nfsstat4.NFS4ERR_DELEG_ALREADY_WANTED:
                return "NFS4ERR_DELEG_ALREADY_WANTED";
            case nfsstat4.NFS4ERR_BACK_CHAN_BUSY:
                return "NFS4ERR_BACK_CHAN_BUSY";
            case nfsstat4.NFS4ERR_LAYOUTTRYLATER:
                return "NFS4ERR_LAYOUTTRYLATER";
            case nfsstat4.NFS4ERR_LAYOUTUNAVAILABLE:
                return "NFS4ERR_LAYOUTUNAVAILABLE";
            case nfsstat4.NFS4ERR_NOMATCHING_LAYOUT:
                return "NFS4ERR_NOMATCHING_LAYOUT";
            case nfsstat4.NFS4ERR_RECALLCONFLICT:
                return "NFS4ERR_RECALLCONFLICT";
            case nfsstat4.NFS4ERR_UNKNOWN_LAYOUTTYPE:
                return "NFS4ERR_UNKNOWN_LAYOUTTYPE";
            case nfsstat4.NFS4ERR_SEQ_MISORDERED:
                return "NFS4ERR_SEQ_MISORDERED";
            case nfsstat4.NFS4ERR_SEQUENCE_POS:
                return "NFS4ERR_SEQUENCE_POS";
            case nfsstat4.NFS4ERR_REQ_TOO_BIG:
                return "NFS4ERR_REQ_TOO_BIG";
            case nfsstat4.NFS4ERR_REP_TOO_BIG:
                return "NFS4ERR_REP_TOO_BIG";
            case nfsstat4.NFS4ERR_REP_TOO_BIG_TO_CACHE:
                return "NFS4ERR_REP_TOO_BIG_TO_CACHE";
            case nfsstat4.NFS4ERR_RETRY_UNCACHED_REP:
                return "NFS4ERR_RETRY_UNCACHED_REP";
            case nfsstat4.NFS4ERR_UNSAFE_COMPOUND:
                return "NFS4ERR_UNSAFE_COMPOUND";
            case nfsstat4.NFS4ERR_TOO_MANY_OPS:
                return "NFS4ERR_TOO_MANY_OPS";
            case nfsstat4.NFS4ERR_OP_NOT_IN_SESSION:
                return "NFS4ERR_OP_NOT_IN_SESSION";
            case nfsstat4.NFS4ERR_HASH_ALG_UNSUPP:
                return "NFS4ERR_HASH_ALG_UNSUPP";
            case nfsstat4.NFS4ERR_CONN_BINDING_NOT_ENFORCED:
                return "NFS4ERR_CONN_BINDING_NOT_ENFORCED";
            case nfsstat4.NFS4ERR_CLIENTID_BUSY:
                return "NFS4ERR_CLIENTID_BUSY";
            case nfsstat4.NFS4ERR_PNFS_IO_HOLE:
                return "NFS4ERR_PNFS_IO_HOLE";
            case nfsstat4.NFS4ERR_SEQ_FALSE_RETRY:
                return "NFS4ERR_SEQ_FALSE_RETRY";
            case nfsstat4.NFS4ERR_BAD_HIGH_SLOT:
                return "NFS4ERR_BAD_HIGH_SLOT";
            case nfsstat4.NFS4ERR_DEADSESSION:
                return "NFS4ERR_DEADSESSION";
            case nfsstat4.NFS4ERR_ENCR_ALG_UNSUPP:
                return "NFS4ERR_ENCR_ALG_UNSUPP";
            case nfsstat4.NFS4ERR_PNFS_NO_LAYOUT:
                return "NFS4ERR_PNFS_NO_LAYOUT";
            case nfsstat4.NFS4ERR_NOT_ONLY_OP:
                return "NFS4ERR_NOT_ONLY_OP";
            case nfsstat4.NFS4ERR_WRONG_CRED:
                return "NFS4ERR_WRONG_CRED";
            case nfsstat4.NFS4ERR_WRONG_TYPE:
                return "NFS4ERR_WRONG_TYPE";
            case nfsstat4.NFS4ERR_DIRDELEG_UNAVAIL:
                return "NFS4ERR_DIRDELEG_UNAVAIL";
            case nfsstat4.NFS4ERR_REJECT_DELEG:
                return "NFS4ERR_REJECT_DELEG";
            case nfsstat4.NFS4ERR_RETURNCONFLICT:
                return "NFS4ERR_RETURNCONFLICT";
            default:
                return "unknown error code : " + errorCode;
        }

    }

}
