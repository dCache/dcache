package org.dcache.pool.repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import diskCacheV111.repository.SpaceRequestable;

public class FairDiskSpaceAllocator<T> implements PoolSpaceAllocatable<T> {


    private final static Logger _logSpaceAllocation =
        Logger.getLogger("logger.dev.org.dcache.poolspacemonitor." + FairDiskSpaceAllocator.class.getName());

    /**
     * Maps entries to the amount of bytes allocated for that entry.
     */
    private final Map<T, Long> _allocations = new HashMap<T, Long>();


    /**
     * Exclusive access lock.  We need this lock to be able to have
     * per-thread conditions.  This will make possible to notify only
     * one specific thread.
     */
    private final Lock _exclusiveLock = new ReentrantLock();

    /**
     * Mapping between <i>Condition</i> and corresponding sizes.  Each
     * Condition is associated with one allocations and by signaling
     * only this condition we will wake up only one threat particular
     * thread.  LinkedHashMap used to guarantee map order.
     */
    private final Map<Condition,Long> _waitingThreads =
        new LinkedHashMap<Condition, Long>();

    /**
     * List of registered space listeners on this space monitor.
     */
    private final List<SpaceRequestable> _listener =
        new ArrayList<SpaceRequestable>();

    /**
     * Total size of managed space in bytes.
     */
    private long _totalSpace = 0;

    /**
     * Amount of non allocated space in bytes.
     */
    private long _freeSpace = 0;

    /**
     * Creates a new FairDiskSpaceAllocator with a total size of 0.
     */
    public FairDiskSpaceAllocator() {
        this(0);
    }

    /**
     * Creates a new FairDiskSpaceAllocator with the given size.
     *
     * @param totalSpace
     *            size of space managed by allocator
     * @throws IllegalArgumentException
     *             if <i>totalSpace<i> is negative
     */
    public FairDiskSpaceAllocator(long totalSpace)
            throws IllegalArgumentException {

        if (totalSpace < 0) {
            throw new IllegalArgumentException("Negative total size");
        }

        if( _logSpaceAllocation.isDebugEnabled() ) {
            _logSpaceAllocation.debug("Initializing Space Allocator Total = " + totalSpace );
        }
        _totalSpace = totalSpace;
        _freeSpace = _totalSpace;
    }

    public void allocate(T entry, long size, long timeout)
            throws IllegalArgumentException, NullPointerException,
            InterruptedException {

        if (entry == null) {
            throw new NullPointerException("passed null as reference entry");
        }

        if (size < 0) {
            throw new IllegalArgumentException("negative size value");
        }

        _exclusiveLock.lock();
        try{

            Condition needSpace = _exclusiveLock.newCondition();
            _waitingThreads.put(needSpace, size);

            try {
                long timeToWait = timeout;
                while (_freeSpace < size) {

                    if (timeToWait == 0) {
                        throw new MissingResourceException("No space available", "", "");
                    }

                    /*
                     * trigger a callback to free some space
                     */
                    requestSpace(size - _freeSpace);

                    if (timeToWait < 0) {
                        needSpace.await();
                    } else {

                        final long now = System.currentTimeMillis();
                        needSpace.await(timeToWait, TimeUnit.MILLISECONDS);
                        timeToWait -= System.currentTimeMillis() - now;
                        // expired if we wait more than expected
                        if (timeToWait < 0) timeToWait = 0;
                    }
                }

            }finally{
                _waitingThreads.remove(needSpace);
            }

            long current = 0;
            final Long currentAllocation = _allocations.get(entry);
            if (currentAllocation != null) {
                current = currentAllocation;
            }
            if( _logSpaceAllocation.isDebugEnabled() ) {
                _logSpaceAllocation.debug("Allocating for " + entry + " " + size + " to existing " + current);
            }
            _allocations.put(entry, Long.valueOf(current + size));
            _freeSpace -= size;

            /*
             * some space is still left
             */
            signalFreeSpaceAvailable(_freeSpace);

        }finally{
            _exclusiveLock.unlock();
        }

    }

    public void free(T entry) throws NullPointerException,
            IllegalArgumentException {

        if (entry == null) {
            throw new NullPointerException("passed null as reference entry");
        }

        _exclusiveLock.lock();
        try {
            final Long allocation = _allocations.get(entry);
            if (allocation != null) {
                if( _logSpaceAllocation.isDebugEnabled() ) {
                    _logSpaceAllocation.debug("Freeing for " + entry + " " + allocation);
                }
                _freeSpace += allocation;
                _allocations.remove(entry);
                signalFreeSpaceAvailable(_freeSpace);
            } else {
                throw new IllegalArgumentException("cannot free non existing entry");
            }
        }finally{
            _exclusiveLock.unlock();
        }

    }

    public long getUsedSpace(T entry) throws NullPointerException,
            IllegalArgumentException {

        long currentAllocation;
        if (entry == null) {
            throw new NullPointerException("passed null as reference entry");
        }

        _exclusiveLock.lock();
        try {
            final Long allocation = _allocations.get(entry);
            if (allocation != null) {
                currentAllocation = allocation;
            } else {
                throw new IllegalArgumentException(
                        "non existing entry");
            }
        } finally {
            _exclusiveLock.unlock();
        }

        return currentAllocation;

    }

    public long getFreeSpace() {
        _exclusiveLock.lock();
        try {
            return _freeSpace;
        }finally{
            _exclusiveLock.unlock();
        }
    }

    public long getTotalSpace() {
        _exclusiveLock.lock();
        try{
            return _totalSpace;
        }finally{
            _exclusiveLock.unlock();
        }
    }

    public long getUsedSpace() {
        _exclusiveLock.lock();
        try{
            return _totalSpace - _freeSpace;
        }finally{
            _exclusiveLock.unlock();
        }
    }

    public void reallocate(T entry, long size)
            throws IllegalArgumentException, NullPointerException,
            MissingResourceException {

        if (entry == null) {
            throw new NullPointerException("passed null as reference entry");
        }

        if (size < 0) {
            throw new IllegalArgumentException("negative size value");
        }


        _exclusiveLock.lock();
        try {
            Long currentallocation = _allocations.get(entry);

            /*
             * we have to handle following cases:
             *
             * 1. entry does not exist == new allocation
             * 2. entry exist and current allocation == new
             *    do nothing.
             * 3. entry exist and current allocation > new
             *    free some space
             * 4. entry exit and current allocation < new
             *    add some more
             */

            // case 1
            if (currentallocation == null) {

                if( _logSpaceAllocation.isDebugEnabled() ) {
                    _logSpaceAllocation.debug("Re-Allocating (initial) for " + entry + " " + size);
                }
                try {
                    allocate(entry, size, 0);
                } catch (InterruptedException ie) {
                    // never happens
                }

            } else {
                // case 2,3,4

                // case 2
                if (currentallocation == size) {
                    // we are done
                    if( _logSpaceAllocation.isDebugEnabled() ) {
                        _logSpaceAllocation.debug("Re-Allocating (current == new)for " + entry + " " + size);
                    }
                    return;
                }

                // case 3
                if (currentallocation > size) {

                    long delta = currentallocation - size;
                    _freeSpace += delta;
                    _allocations.put(entry, size);
                    if( _logSpaceAllocation.isDebugEnabled() ) {
                        _logSpaceAllocation.debug("Re-Allocating (current > new) for " + entry + " " + size + " freeing " + delta);
                    }
                    signalFreeSpaceAvailable(_freeSpace);
                } else {
                    // case 4
                    long delta = size - currentallocation;
                    try {
                        if( _logSpaceAllocation.isDebugEnabled() ) {
                            _logSpaceAllocation.debug("Re-Allocating (current < new) for " + entry + " " + size + " requesting " + delta);
                        }
                        allocate(entry, delta, 0);
                    } catch (InterruptedException ie) {
                        // never happens
                    }

                }
            }
        }finally{
            _exclusiveLock.unlock();
        }

    }

    public void setTotalSpace(long space)
            throws MissingResourceException, IllegalArgumentException {

        /*
         * there is three cases to take care:
         *
         * 1. new space equal to old space
         *    do noting
         * 2. new space > old space
         *    increase total and free
         * 3. new space < old space
         *   3a. new space > used space
         *      decrease total and free space
         *   3b. new space < used space
         *     throw MissingResourceException
         */

        if (space < 0) {
            throw new IllegalArgumentException("negate space");
        }


        _exclusiveLock.lock();
        try {
            // case 1
            if (space == _totalSpace) {
                if( _logSpaceAllocation.isDebugEnabled() ) {
                    _logSpaceAllocation.debug("Space Allocator Change size (current == new) " + space );
                }
                return;
            }

            // case 2
            if (space > _totalSpace) {

                long delta = space - _totalSpace;

                if( _logSpaceAllocation.isDebugEnabled() ) {
                    _logSpaceAllocation.debug("Space Allocator Change size (current < new) old/oldfree/new/delta "
                            + _totalSpace + " / " + _freeSpace + " / "+ space +  " / " + delta );
                }

                _totalSpace = space;
                _freeSpace += delta;
                signalFreeSpaceAvailable(_freeSpace);
            } else {

                // case 3a
                if (space > (_totalSpace - _freeSpace)) {
                    long delta = _totalSpace - space;

                    if( _logSpaceAllocation.isDebugEnabled() ) {
                        _logSpaceAllocation.debug("Space Allocator Change size (used < new < current) old/oldfree/new/delta "
                                + _totalSpace + " / " + _freeSpace + " / "+ space +  " / " + delta );
                    }

                    _totalSpace = space;
                    _freeSpace -= delta;
                } else {
                    // case 3b
                    if( _logSpaceAllocation.isDebugEnabled() ) {
                        _logSpaceAllocation.debug("Space Allocator Change size (new < used) old/oldfree/new "
                                + _totalSpace + " / " + _freeSpace + " / "+ space );
                    }
                    throw new MissingResourceException(
                            "can't set total space smaller than used space", "", "");
                }

            }
        }finally{
            _exclusiveLock.unlock();
        }

    }

    public void addSpaceRequestListener(SpaceRequestable spaceListener) {

        _exclusiveLock.lock();
        try {
            _listener.add(spaceListener);
        }finally{
            _exclusiveLock.unlock();
        }

    }

    public void removeSpaceRequestListener(SpaceRequestable spaceListener) {

        _exclusiveLock.lock();
        try {
            _listener.remove(spaceListener);
        }finally{
            _exclusiveLock.unlock();
        }

    }


    /**
     * notify a thread waiting for a space.
     * @param space
     */
    private void signalFreeSpaceAvailable(long space) {

        for( Map.Entry<Condition, Long> needSpace: _waitingThreads.entrySet() ) {
            if( space > needSpace.getValue() ) {
                needSpace.getKey().signal();
                break;
            }
        }

    }

    private void requestSpace( long space ) {

        for(SpaceRequestable listener: _listener) {
            listener.spaceNeeded(space);
        }

    }

    public List<T> allocations() {
        _exclusiveLock.lock();
        try {
            List<T> allocations = new ArrayList<T>(_allocations.keySet() );
            return allocations;
        }finally{
            _exclusiveLock.unlock();
        }
    }

}
