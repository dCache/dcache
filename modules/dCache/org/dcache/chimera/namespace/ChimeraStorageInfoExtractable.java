/*
 * $Id: ChimeraStorageInfoExtractable.java,v 1.1 2007-06-19 10:06:33 tigran Exp $
 */
package org.dcache.chimera.namespace;

import diskCacheV111.util.CacheException;
import org.dcache.chimera.FsInode;
import diskCacheV111.vehicles.StorageInfo;


public interface ChimeraStorageInfoExtractable {

    public StorageInfo getStorageInfo( FsInode inode )
    throws CacheException ;

    public void setStorageInfo( FsInode inode,
                         StorageInfo storageInfo , int accessMode )
    throws CacheException ;
}


