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


/**
 * NFSv4 file attribute mask
 *
 */

public interface NFSv4FileAttributes {
//  word0
    static final int FATTR4_SUPPORTED_ATTRS  =  (1 << 0) ;
    static final int FATTR4_TYPE             =  (1 << 1) ;
    static final int FATTR4_FH_EXPIRE_TYPE   =  (1 << 2) ;
    static final int FATTR4_CHANGE           =  (1 << 3) ;
    static final int FATTR4_SIZE             =  (1 << 4) ;
    static final int FATTR4_LINK_SUPPORT     =  (1 << 5) ;
    static final int FATTR4_SYMLINK_SUPPORT  =  (1 << 6) ;
    static final int FATTR4_NAMED_ATTR       =  (1 << 7) ;
    static final int FATTR4_FSID             =  (1 << 8) ;
    static final int FATTR4_UNIQUE_HANDLES   =  (1 << 9) ;
    static final int FATTR4_LEASE_TIME       =  (1 << 10) ;
    static final int FATTR4_RDATTR_ERROR     =  (1 << 11) ;
    static final int FATTR4_ACL              =  (1 << 12) ;
    static final int FATTR4_ACLSUPPORT       =  (1 << 13) ;
    static final int FATTR4_ARCHIVE          =  (1 << 14) ;
    static final int FATTR4_CANSETTIME       =  (1 << 15) ;
    static final int FATTR4_CASE_INSENSITIVE =  (1 << 16) ;
    static final int FATTR4_CASE_PRESERVING  =  (1 << 17) ;
    static final int FATTR4_CHOWN_RESTRICTED =  (1 << 18) ;
    static final int FATTR4_FILEHANDLE       =  (1 << 19) ;
    static final int FATTR4_FILEID           =  (1 << 20) ;
    static final int FATTR4_FILES_AVAIL      =  (1 << 21) ;
    static final int FATTR4_FILES_FREE       =  (1 << 22) ;
    static final int FATTR4_FILES_TOTAL      =  (1 << 23) ;
    static final int FATTR4_FS_LOCATIONS     =  (1 << 24) ;
    static final int FATTR4_HIDDEN           =  (1 << 25) ;
    static final int FATTR4_HOMOGENEOUS      =  (1 << 26) ;
    static final int FATTR4_MAXFILESIZE      =  (1 << 27) ;
    static final int FATTR4_MAXLINK          =  (1 << 28) ;
    static final int FATTR4_MAXNAME          =  (1 << 29) ;
    static final int FATTR4_MAXREAD          =  (1 << 30) ;
    static final int FATTR4_MAXWRITE         =  (1 << 31) ;


//     word1
    static final int FATTR4_MIMETYPE         =  (1 << 0) ;
    static final int FATTR4_MODE             =  (1 << 1) ;
    static final int FATTR4_NO_TRUNC         =  (1 << 2) ;
    static final int FATTR4_NUMLINKS         =  (1 << 3) ;
    static final int FATTR4_OWNER            =  (1 << 4) ;
    static final int FATTR4_OWNER_GROUP      =  (1 << 5) ;
    static final int FATTR4_QUOTA_HARD       =  (1 << 6) ;
    static final int FATTR4_QUOTA_SOFT       =  (1 << 7) ;
    static final int FATTR4_QUOTA_USED       =  (1 << 8) ;
    static final int FATTR4_RAWDEV           =  (1 << 9) ;
    static final int FATTR4_SPACE_AVAIL      =  (1 << 10) ;
    static final int FATTR4_SPACE_FREE       =  (1 << 11) ;
    static final int FATTR4_SPACE_TOTAL      =  (1 << 12) ;
    static final int FATTR4_SPACE_USED       =  (1 << 13) ;
    static final int FATTR4_SYSTEM           =  (1 << 14) ;
    static final int FATTR4_TIME_ACCESS      =  (1 << 15) ;
    static final int FATTR4_TIME_ACCESS_SET  =  (1 << 16) ;
    static final int FATTR4_TIME_BACKUP      =  (1 << 17) ;
    static final int FATTR4_TIME_CREATE      =  (1 << 18) ;
    static final int FATTR4_TIME_DELTA       =  (1 << 19) ;
    static final int FATTR4_TIME_METADATA    =  (1 << 20) ;
    static final int FATTR4_TIME_MODIFY      =  (1 << 21) ;
    static final int FATTR4_TIME_MODIFY_SET  =  (1 << 22) ;
    static final int FATTR4_MOUNTED_ON_FILEID=  (1 << 23) ;
    static final int FATTR4_FS_LAYOUT_TYPE   =  (1 << 30) ; // NFSv4.1 (pNFS)


    /**
     * NFSv4 mandatory attributes according rfc3530
     */
    public final static int NFS4_MANDATORY =
            FATTR4_SUPPORTED_ATTRS  |
            FATTR4_TYPE             |
            FATTR4_FH_EXPIRE_TYPE   |
            FATTR4_CHANGE           |
            FATTR4_SIZE             |
            FATTR4_LINK_SUPPORT     |
            FATTR4_SYMLINK_SUPPORT  |
            FATTR4_NAMED_ATTR       |
            FATTR4_FSID             |
            FATTR4_UNIQUE_HANDLES   |
            FATTR4_LEASE_TIME       |
            FATTR4_RDATTR_ERROR     ;

    /*
     * byte0 mandatory + recommended
     */
    public final static int NFS4_SUPPORTED_ATTRS_MASK0 = NFS4_MANDATORY |
    		FATTR4_ACL              |
    		FATTR4_ACLSUPPORT		|
            FATTR4_CANSETTIME       |
            FATTR4_CASE_INSENSITIVE |
            FATTR4_CASE_PRESERVING  |
            FATTR4_CHOWN_RESTRICTED |
            FATTR4_FILEHANDLE       |
            FATTR4_FILEID           |
            FATTR4_FILES_AVAIL      |
            FATTR4_FILES_FREE       |
            FATTR4_FILES_TOTAL      |
            FATTR4_HOMOGENEOUS      |
            FATTR4_MAXFILESIZE      |
            FATTR4_MAXLINK          |
            FATTR4_MAXNAME          |
            FATTR4_MAXREAD          |
            FATTR4_MAXWRITE         ;

    /*
     * byte1 recommended
     */
    public final static int NFS4_SUPPORTED_ATTRS_MASK1 =
        FATTR4_MODE                 |
        FATTR4_NO_TRUNC             |
        FATTR4_NUMLINKS             |
        FATTR4_OWNER                |
        FATTR4_OWNER_GROUP          |
        FATTR4_RAWDEV               |
        FATTR4_SPACE_AVAIL          |
        FATTR4_SPACE_FREE           |
        FATTR4_SPACE_TOTAL          |
        FATTR4_SPACE_USED           |
        FATTR4_TIME_ACCESS          |
        FATTR4_TIME_ACCESS_SET      |
        FATTR4_TIME_CREATE          |
        FATTR4_TIME_DELTA           |
        FATTR4_TIME_METADATA        |
        FATTR4_TIME_MODIFY          |
        FATTR4_TIME_MODIFY_SET      |
        FATTR4_MOUNTED_ON_FILEID    |
        FATTR4_FS_LAYOUT_TYPE      ; // want to be a 4.1 server

}
