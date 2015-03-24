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
    private long _timeOfLastFree;

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

    public synchronized long getTimeOfLastFree()
    {
        return _timeOfLastFree;
    }

    public synchronized void setTotal(long total)
    {
        if (total < _used) {
            throw new IllegalArgumentException("Cannot set repository size below amount of used space.");
        }
        _total = total;
        notifyAll();
    }

    /**
     * Moves <code>space</code> bytes from used to free space.
     */
    public synchronized void free(long space)
    {
        if (space < 0) {
            throw new IllegalArgumentException("Cannot free negative space.");
        }
        if (_used < space) {
            throw new IllegalArgumentException("Cannot set used space to a negative value.");
        }

        notifyAll();
        _used -= space;
        _timeOfLastFree = System.currentTimeMillis();
    }

    /**
     * Allocates up to <code>request</code> bytes. If less space is
     * free, then nothing is allocated.
     *
     * @return true if and only if the request was served
     */
    public synchronized boolean allocateNow(long request)
             throws InterruptedException
    {
        if (request < 0) {
            throw new IllegalArgumentException("Cannot allocate negative space.");
        }
        _requested += request;
        try {
            while (request > getFree() && request <= getFree() + getRemovable()) {
                notifyAll();
                wait();
            }
            if (request > getFree()) {
                return false;
            }
            _used += request;
            notifyAll();
        } finally {
            _requested -= request;
        }
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
        if (request < 0) {
            throw new IllegalArgumentException("Cannot allocate negative space.");
        }
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
        long removable = _removable + delta;
        if (removable < 0) {
            throw new IllegalArgumentException("Negative removable space is not allowed.");
        }
        if (removable > _total) {
            throw new IllegalArgumentException("Removable space would exceed repository size.");
        }
        _removable = removable;
        notifyAll();
    }

    public synchronized void adjustPrecious(long delta)
    {
        long precious = _precious + delta;
        if (precious < 0) {
            throw new IllegalArgumentException("Negative precious space is not allowed.");
        }
        if (precious > _total) {
            throw new IllegalArgumentException("Precious space would exceed repository size.");
        }
        _precious = precious;
        notifyAll();
    }

    public synchronized SpaceRecord getSpaceRecord()
    {
        return new SpaceRecord(_total, getFree(), _precious, _removable, 0);
    }


}
