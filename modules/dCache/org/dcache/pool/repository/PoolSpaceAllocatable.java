/*
 * $Id:$
 */
package org.dcache.pool.repository;

import java.util.MissingResourceException;

import diskCacheV111.repository.SpaceRequestable;

public interface PoolSpaceAllocatable<T> {

    /**
     * Allocate <i>size</i> bytes and add to current allocation
     * associated with <i>entry</i>. The call will block the requested
     * amount of space is available or until timeout has expired.
     *
     * @param entry
     * @param size
     *            in bytes
     * @param timeout
     *            timeout in milliseconds or -1 for no timeout
     * @throws IllegalArgumentException
     *             if <i>size</i> < 0
     * @throws NullPointerException
     *             if <i>entry</i> in null
     * @throws MissingResourceException
     *             if no space available in defined time period
     * @throws InterruptedException if current thread was interrupted
     */
    public void allocate(T entry, long size, long timeout)
            throws IllegalArgumentException, NullPointerException,
            MissingResourceException, InterruptedException;

    /**
     * Free any space associated with <i>entry</i>.
     *
     * @param entry
     * @throws NullPointerException
     *             if <i>entry</i> in null
     * @throws IllegalArgumentException
     *             if no such entry exist
     */
    public void free(T entry) throws NullPointerException,
            IllegalArgumentException;

    /**
     * Adjusts the allocation for <i>entry/<i> to <i>expectedSize</i>.
     * If entry does not exist, this method has the same behavior as
     * <i>allocate(entry, expectedSize, 0)</i>.
     *
     * @param entry
     * @param expectedSize
     * @throws NullPointerException
     *             if <i>entry</i> in null
     * @throws MissingResourceException
     *             if <i>expectedSize</i> is bigger than current
     *             allocation and there is no free space available
     * @throws IllegalArgumentException
     *             if <i>expectedSize</i> is negative
     */
    public void reallocate(T entry, long newSize)
            throws NullPointerException, MissingResourceException,
            IllegalArgumentException;

    /**
     * Set total size of managed space.
     *
     * @param space
     * @throws MissingResourceException
     *             if new <i>space</i> smaller than current used space
     * @throws IllegalArgumentException
     *             if <i>expectedSize</i> is negative
     */
    public void setTotalSpace(long space)
        throws MissingResourceException, IllegalArgumentException;


    /**
     * Adds a space listener. A space listener is resposible for
     * freeing space when space is needed.
     *
     * @param spaceListener
     */
    public void addSpaceRequestListener(SpaceRequestable spaceListener);

    /**
     * Removes space listener previous added with
     * <i>addSpaceRequestListener</i>.
     *
     * @param spaceListener
     */
    public void removeSpaceRequestListener(SpaceRequestable spaceListener);

    /**
     *
     *
     * @param entry
     * @return space currently allocated for the <i>entry</i>
     * @throws NullPointerException
     *             if <i>entry</i> in null
     * @throws IllegalArgumentException
     *             if no such entry exist
     */
    public long getUsedSpace(T entry)
        throws NullPointerException, IllegalArgumentException;

    /**
     * Returns the equivalent of <i>getTotalSpace() - getFreeSpace()</i>.
     *
     * @return amount of free space in bytes
     */
    public long getFreeSpace();

    /**
     *
     * @return <i>total</i> size of managed space in bytes
     */
    public long getTotalSpace();

    /**
     *
     * @return currently allocated space in bytes
     */
    public long getUsedSpace();

}
