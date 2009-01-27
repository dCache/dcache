package org.dcache.pool.repository;


/**
 * Encapsulation of space accounting information for a
 * repository. 
 *
 * Used as a synchronisation point between several repository
 * components. The object is thread safe and external synchronisations
 * are allowed. Any modification of the object triggers a call to
 * notifyAll on the object.
 */ 
public class Account
{
    private long _total;
    private long _used;
    private long _precious;
    private long _removable;
    private long _requested;
    private long _lru;

    public synchronized long getTotal()
    {
        return _total;
    }

    public synchronized long getUsed()
    {
        return _used;
    }

    public synchronized long getFree()
    {
        return _total - _used;
    }

    public synchronized long getRemovable()
    {
        return _removable;
    }

    public synchronized long getPrecious()
    {
        return _precious;
    }

    public synchronized long getRequested()
    {
        return _requested;
    }

    public synchronized void setTotal(long total)
    {
        if (total < _used) {
            throw new IllegalArgumentException("Cannot set repository size below amount of used space");
        }
        _total = total;
        notifyAll();
    }

    /**
     * Moves <code>space</code> bytes from used to free space.
     */
    public synchronized void free(long space)
    {
        notifyAll();
        _used -= space;
    }

    /**
     * Allocates up to <code>request</code> bytes. If less space is
     * free, then nothing is allocated.
     *
     * @return true if and only if the request was served
     */
    public synchronized boolean allocateNow(long request)
    {
        if (request > getFree()) {
            return false;
        }

        notifyAll();
        _used += request;
        return true;
    }

    /**
     * Allocates <code>request</code> bytes. If less space is
     * available, the request is added to the request pool and the
     * call blocks. Space is not allocated until the complete request
     * can be served. For this reason, large requests can starve.
     */
    public synchronized void allocate(long request) 
        throws InterruptedException
    {
        _requested += request;
        try {
            while (request > getFree()) {
                notifyAll();
                wait();
            }
            _used += request;
            notifyAll();
        } finally {
            _requested -= request;
        }
    }

    public synchronized void adjustRemovable(long delta)
    {
        _removable += delta;
        notifyAll();
    }

    public synchronized void adjustPrecious(long delta)
    {
        _precious += delta;
        notifyAll();
    }

    public synchronized void setLRU(long lru)
    {
        if (_lru != lru) {
            _lru = lru;
            notifyAll();
        }
    }

    public synchronized SpaceRecord getSpaceRecord()
    {
        long lru = (System.currentTimeMillis() - _lru) / 1000L;
        return new SpaceRecord(_total, getFree(), _precious, _removable, lru);
    }
    

}