/*
 * $Id:$
 */
package org.dcache.pool.repository;

import java.util.MissingResourceException;

import diskCacheV111.repository.SpaceRequestable;

public interface PoolSpaceAllocatable<T> {

    /**
     * allocate <i>size</i> bytes and add to current allocation associated with
     * <i>entry</i>. The call will block until timeout is not expired or
     * forever, if timeout value equal -1.
     *
     * @param entry
     * @param size
     *            in bytes
     * @param timeout
     *            in milliseconds
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
     * free space associated with <i>entry</i>
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
     * synchronize <i>entry</i> allocated size with <i>expectedSize</i>. If
     * entry does't exist has the same behavior as <i>allocate(entry,
     * expectedSize, 0)</i>
     *
     * @param entry
     * @param expectedSize
     * @throws NullPointerException
     *             if <i>entry</i> in null
     * @throws MissingResourceException
     *             if <i>expectedSize</i> is bigger than current allocation and
     *             there is no free space available
     * @throws IllegalArgumentException
     *             if <i>expectedSize</i> < 0
     */
    public void reallocate(T entry, long expectedSize)
            throws NullPointerException, MissingResourceException,
            IllegalArgumentException;

    /**
     * set total managed space size
     *
     * @param space
     * @throws MissingResourceException
     *             if new <i>space</i> smaller than current used space
     * @throws IllegalArgumentException
     *             if <i>expectedSize</i> < 0
     */
    public void setTotalSpace(long space) throws MissingResourceException, IllegalArgumentException;


    /**
     * Add space listener, which frees some entries, when space needed
     * @param spaceListener
     */
    public void addSpaceRequestListener(SpaceRequestable spaceListener);

    /**
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
    public long getUsedSpace(T entry) throws NullPointerException, IllegalArgumentException;

    /**
     *
     * @return <i>total</i> space - <i>allocated</i> space
     */
    public long getFreeSpace();

    /**
     *
     * @return <i>total</i> space
     */
    public long getTotalSpace();

    /**
     *
     * @return currently total allocated space
     */
    public long getUsedSpace();

}
