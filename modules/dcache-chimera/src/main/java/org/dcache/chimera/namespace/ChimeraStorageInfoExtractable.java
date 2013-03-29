/*
 * $Id: ChimeraStorageInfoExtractable.java,v 1.1 2007-06-19 10:06:33 tigran Exp $
 */
package org.dcache.chimera.namespace;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.chimera.FsInode;


public interface ChimeraStorageInfoExtractable {

    public StorageInfo getStorageInfo( FsInode inode )
    throws CacheException ;

    void setStorageInfo(FsInode inode, StorageInfo storageInfo) throws CacheException;
}


