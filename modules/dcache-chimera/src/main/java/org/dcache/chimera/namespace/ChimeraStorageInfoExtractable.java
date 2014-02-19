package org.dcache.chimera.namespace;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.chimera.FsInode;

/**
 * Implementations of this interface extract information specific to particular
 * HSM types from the name space.
 *
 * A typical implementation will have means to obtain default values from
 * directory tags.
 */
public interface ChimeraStorageInfoExtractable {
    /**
     * Returns the AccessLatency of an inode.
     *
     * @param inode An inode
     * @return The AccessLatency of inode
     * @throws CacheException if the AccessLatency could not be read
     */
    AccessLatency getAccessLatency(ExtendedInode inode) throws CacheException;

    /**
     * Returns the RetentionPolicy of an inode.
     *
     * @param inode An inode
     * @return The RetentionPolicy of inode
     * @throws CacheException if the RetentionPolicy could not be read
     */
    RetentionPolicy getRetentionPolicy(ExtendedInode inode) throws CacheException;

    /**
     * Returns the StorageInfo of an inode.
     *
     * @param inode An inode
     * @return The StorageInfo of inode
     * @throws CacheException if the StorageInfo could not be read
     */
    StorageInfo getStorageInfo(ExtendedInode inode) throws CacheException;

    /**
     * Updates the StorageInfo of an inode.
     *
     * @param inode An inode
     * @param storageInfo StorageInfo to write to inode
     * @throws CacheException if the StorageInfo could not be written
     */
    void setStorageInfo(FsInode inode, StorageInfo storageInfo) throws CacheException;
}


