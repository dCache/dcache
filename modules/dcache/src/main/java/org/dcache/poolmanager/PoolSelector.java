package org.dcache.poolmanager;

import java.util.List;

import diskCacheV111.util.CacheException;

/**
 * A PoolSelector encapslutes pool selection logic for a particular file and
 * a particular transfer. Thus pools selected by a PoolSelector are only
 * suitable for the file and transfer for which the selector was created.
 */
public interface PoolSelector
{
    /**
     * Returns all available read pools for this PoolSelector. Read pools
     * are grouped and ordered according to link preferences.
     */
    List<List<PoolInfo>> getReadPools();

    /**
     * Returns the partition used for the last result of selectReadPool.
     */
    Partition getCurrentPartition();

    /**
     * Returns a pool for reading the file.
     *
     * The partition used for the pool selection is available after
     * this method returns by calling getCurrentPartition().
     *
     * @throw FileNotInCacheException if the file is not on any
     *        pool that is online.
     * @throw PermissionDeniedCacheException if the file is is not
     *        on a pool from which we are allowed to read it
     * @throw CostExceededException if a read pool is available, but
     *        it exceed cost limits; the exception contains information
     *        about how the caller may recover
     */
    SelectedPool selectReadPool() throws CacheException;

    /**
     * Returns a pool for writing a file described by this PoolSelector.
     *
     * @param preallocated Space in bytes reserved for the file, or zero.
     */
    SelectedPool selectWritePool(long preallocated) throws CacheException;

    Partition.P2pPair selectPool2Pool(boolean force) throws CacheException;

    SelectedPool selectStagePool(String previousPool, String previousHost)
            throws CacheException;

    SelectedPool selectPinPool() throws CacheException;
}
