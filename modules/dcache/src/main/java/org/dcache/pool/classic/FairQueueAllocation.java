// $Id: FairQueueAllocation.java,v 1.7 2007-07-03 13:51:31 tigran Exp $
package org.dcache.pool.classic;

import java.util.ArrayList;
import java.util.List;
import org.dcache.pool.repository.Account;
import org.dcache.pool.repository.Allocator;

/**
 * Implementation of the SpaceMonitor interface, which serves requests
 * in FIFO order.
 */
public class FairQueueAllocation
    implements Allocator
{
    private Account _account;

    /**
     * A list of threads waiting for space. Requests are served in the
     * order they appear in this list.
     */
    private final List<Thread> _list =
        new ArrayList<Thread>();

    public FairQueueAllocation()
    {
    }

    public synchronized void setAccount(Account account)
    {
        _account = account;
    }

    private synchronized void enqueue(Thread thread)
    {
        _list.add(thread);
    }

    private synchronized void dequeue(Thread thread)
    {
        _list.remove(thread);
        notifyAll();
    }

    private synchronized void waitForTurn(Thread thread)
        throws InterruptedException
    {
        while (_list.get(0) != thread) {
            wait();
        }
    }

    /**
     * Allocate space. If not enough free space is available, the
     * thread blocks until free space is made available.
     *
     * In case not enough space is available, a callback is triggered
     * such that the sweeper knows that additional space is required.
     */
    public void allocate(long space)
        throws InterruptedException
    {
        if (space < 0)
            throw new IllegalArgumentException("Cannot allocate negative space");

        final Thread self = Thread.currentThread();
        enqueue(self);
        try {
            waitForTurn(self);
            _account.allocate(space);
        } finally {
            dequeue(self);
        }
    }

    public void free(long space)
    {
        _account.free(space);
    }
}
