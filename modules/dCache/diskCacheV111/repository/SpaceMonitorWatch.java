// $Id: SpaceMonitorWatch.java,v 1.1.2.2 2007/09/25 00:29:12 timur Exp $
package diskCacheV111.repository;

import java.util.MissingResourceException;

/**
 * SpaceMonitorWatch is a decorator around the a space monitor,
 * watching the allocations and frees. Finally a correctSpace can be
 * called once with the correct, final size of the file.
 */
public class SpaceMonitorWatch extends SpaceMonitorDecorator
{
    private long _totalAllocated  = 0L;
    private long _totalFreed      = 0L;
    private int  _countAllocated  = 0;
    private int  _countFreed      = 0;
    private boolean _finished     = false;

    public SpaceMonitorWatch(SpaceMonitor monitor)
    {
        super(monitor);
    }

    public synchronized void allocateSpace(long space)
        throws InterruptedException
    {
        if (_finished) {
            throw new IllegalStateException("SpaceMonitorWatch: allocation is not allowed anymore");
        }
        super.allocateSpace(space);
        _totalAllocated += space;
        _countAllocated++;
    }

    public synchronized void allocateSpace(long space, long millis)
        throws InterruptedException,
               MissingResourceException
    {
        if (_finished) {
            throw new IllegalStateException("SpaceMonitorWatch: allocation is not allowed anymore");
        }
        super.allocateSpace(space, millis);
        _totalAllocated += space;
        _countAllocated++;
    }

    public synchronized void freeSpace(long space)
    {
        if (_finished) {
            throw new IllegalStateException("SpaceMonitorWatch: allocation is not allowed anymore");
        }
        super.freeSpace(space);
        _totalFreed += space;
        _countFreed++;
    }

    public String toString()
    {
        return "SpaceMonitorWatch(AL="+_totalAllocated+"/"+_countAllocated+
            ";FR="+_totalFreed+"/"+_countFreed+
            ";DF="+( _totalAllocated - _totalFreed)+")";
    }

    /**
     * correctSpace adjusts the space calculation of the repository if
     * the correct size of the file is specified here.
     */
    public synchronized long correctSpace(long totalSpaceUsed)
        throws InterruptedException,
               MissingResourceException
    {
        if (_finished)
            throw new IllegalStateException("SpaceMonitorWatch: correctSpace called multiple times");

        _finished = true;
        long overbooked =
            (_totalAllocated - _totalFreed) - totalSpaceUsed;
        if (overbooked > 0) {
            super.freeSpace(overbooked);
        } else if (overbooked < 0) {
            super.allocateSpace(-overbooked);
        }
        return overbooked;
    }
}
