// $Id: FairQueueAllocation.java,v 1.7 2007-07-03 13:51:31 tigran Exp $
package diskCacheV111.repository;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the SpaceMonitor interface, which serves requests
 * in FIFO order.
 */
public class FairQueueAllocation implements SpaceMonitor
{
    private long _usedSpace     = 0;
    private long _totalSpace    = 0;

    /**
     * A list of threads waiting for space. Requests are served in the
     * order they appear in this list.
     */
    private final List<Thread> _list =
        new ArrayList<Thread>();

    /**
     * Callbacks registered on this space monitor.
     */
    private final List<SpaceRequestable> _listener =
        new ArrayList<SpaceRequestable>();

    /**
     * @throws IllegalArgumentException if <code>space</code> is
     *                                  negative
     */
    public FairQueueAllocation(long space)
    {
        if (space < 0)
            throw new IllegalArgumentException("Space must not be negative");
        _totalSpace = space;
    }

    /**
     * Triggers a callback to ask for more free space. The sweeper
     * will receive this event and start deleting some files.
     *
     * Notice that <code>space</code> must be positive. Once issued,
     * we cannot reliably undo the request, as the sweeper may already
     * have begun deleting files.
     */
    private void triggerCallbacks(long space)
    {
        assert space >= 0;
        for (SpaceRequestable sr: _listener) {
            sr.spaceNeeded(space);
        }
    }

    /**
     * Allocate space. If not enough free space is available, the
     * thread blocks for up to <code>millis</code> milliseconds until
     * free space is made available.
     *
     * In case not enough space is available, a callback is triggered
     * such that the sweeper knows that additional space is required.
     */
    public synchronized void allocateSpace(long space, long millis)
        throws InterruptedException
    {
        if (space < 0)
            throw new IllegalArgumentException("Cannot allocate negative space");

        /* If we do not have enough free space, or if other threads
         * are waiting in the queue ahead of us, then we have to
         * wait. A queue of threads is used to guarantee that requests
         * are served in FIFO order.
         */
        if (!_list.isEmpty() || getFreeSpace() < space) {
            long end = System.currentTimeMillis() + millis;
            Thread self = Thread.currentThread();
            _list.add(self);
            try {
                /* Notice that we request the full amount of space.
                 * We do so because we may not be the only thread
                 * waiting for space and thus the currently available
                 * amount of space is not necessarily available to us.
                 * REVISIT: We could check if we are the only thread
                 * waiting and then only request the missing amount of
                 * space.
                 */
                triggerCallbacks(space);

                while (_list.get(0) != self || getFreeSpace() < space) {
                    long rest = end - System.currentTimeMillis();
                    if (rest <= 0)
                        throw new InterruptedException("Wait timed out");
                    wait(rest);
                }
            } finally {
                _list.remove(self);
            }
        }

        _usedSpace += space;

        /* As space is typically released in batches, it may be the
         * case that we have space left for the next
         * request. Therefore we notify them.
         */
        notifyAll();
    }

    /**
     * Allocate space. If not enough free space is available, the
     * thread blocks until free space is made available.
     *
     * In case not enough space is available, a callback is triggered
     * such that the sweeper knows that additional space is required.
     */
    public synchronized void allocateSpace(long space)
        throws InterruptedException
    {
        if (space < 0)
            throw new IllegalArgumentException("Cannot allocate negative space");

        /* If we do not have enough free space, or if other threads
         * are waiting in the queue ahead of us, then we have to
         * wait. A queue of threads is used to guarantee that requests
         * are served in FIFO order.
         */
        if (!_list.isEmpty() || getFreeSpace() < space) {
            Thread self = Thread.currentThread();
            _list.add(self);
            try {
                /* Notice that we request the full amount of space.
                 * We do so because we may not be the only thread
                 * waiting for space and thus the currently available
                 * amount of space is not necessarily available to us.
                 * REVISIT: We could check if we are the only thread
                 * waiting and then only request the missing amount of
                 * space.
                 */
                triggerCallbacks(space);

                while (_list.get(0) != self || getFreeSpace() < space) {
                    wait();
                }
            } finally {
                _list.remove(self);
            }
        }
        _usedSpace += space;

        /* As space is typically released in batches, it may be the
         * case that we have space left for the next
         * request. Therefore we notify them.
         */
        notifyAll();
    }

    /**
     * Frees some space.
     *
     * @throws IllegalArgumentException if <code>space</code> is
     *                                  negative or larger than the
     *                                  amount of used space.
     */
    public synchronized void freeSpace(long space)
    {
        if (space < 0)
            throw new IllegalArgumentException("Cannot free negative space");
        if (space > _usedSpace)
            throw new IllegalArgumentException("Cannot free space that was not allocated");
        _usedSpace -= space;
        notifyAll();
    }

    /**
     * Sets the size of the space mananged by this space monitor.
     *
     * @throws IllegalArgumentException if <code>space</code> is less
     *                                  than the current amount of
     *                                  used space
     */
    public synchronized void setTotalSpace(long space)
    {
        assert _usedSpace >= 0;
        if (space < _usedSpace)
            throw new IllegalArgumentException("Cannot set total space to less than used space");
        _totalSpace = space;
        notifyAll();
    }

    public synchronized long getFreeSpace()
    {
        assert _totalSpace >= _usedSpace;
        return _totalSpace - _usedSpace;
    }

    public synchronized long getTotalSpace()
    {
        assert _totalSpace >= 0;
        return _totalSpace;
    }

    public synchronized void addSpaceRequestListener(SpaceRequestable listener)
    {
        _listener.add(listener);
    }

    public synchronized void removeSpaceRequestListener(SpaceRequestable listener)
    {
        _listener.remove(listener);
    }

    /**
     * Main method for test purposes.
     */
    public static void main(String [] args)
        throws Exception
    {
        final SpaceMonitor m = new FairQueueAllocation(1000);

        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {
                    public void run() {
                        Thread t = Thread.currentThread();
                        while (true) {
                            System.out.println("Waiting " + t.getName());
                            try {
                                m.allocateSpace(500);
                            } catch (Exception e) {
                                System.err.println(e);
                            }
                            System.out.println("Got it " + t.getName());
                        }
                    }
                }).start();
        }
        new Thread(new Runnable(){
                public void run(){
                    while(true){
                        m.freeSpace(1500);
                        System.out.println("freed");
                        try{
                            Thread.sleep(500);
                        } catch(Exception e) {
                            System.err.println(e);
                        }
                    }
                }
            }).start();
    }

}
