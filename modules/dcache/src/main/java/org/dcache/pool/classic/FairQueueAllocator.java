// $Id: FairQueueAllocation.java,v 1.7 2007-07-03 13:51:31 tigran Exp $
package org.dcache.pool.classic;

import java.util.ArrayList;
import java.util.List;

import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.ForwardingAllocator;
import org.dcache.pool.repository.OutOfDiskException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Implementation of Allocator that ensures that an active thread does not
 * prevent other threads from allocating capacity.
 */
public class FairQueueAllocator extends ForwardingAllocator
{
    private final Allocator _inner;

    /**
     * A list of threads waiting for space. Requests are served in the
     * order they appear in this list.
     */
    private final List<Thread> _list = new ArrayList<>();

    public FairQueueAllocator(Allocator inner)
    {
        _inner = inner;
    }

    @Override
    protected Allocator getAllocator()
    {
        return _inner;
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
    @Override
    public void allocate(PnfsId id, long space) throws InterruptedException, OutOfDiskException
    {
        checkArgument(space >= 0, "Cannot allocate negative space");

        final Thread self = Thread.currentThread();
        enqueue(self);
        try {
            waitForTurn(self);
            super.allocate(id, space);
        } finally {
            dequeue(self);
        }
    }
}
