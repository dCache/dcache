package diskCacheV111.repository;

import java.util.MissingResourceException;

/**
 * Base class for decorators of SpaceMonitor implementations.
 */
public class SpaceMonitorDecorator implements SpaceMonitor
{
    private final SpaceMonitor _monitor;

    public SpaceMonitorDecorator(SpaceMonitor monitor)
    {
        _monitor = monitor;
    }

    public void allocateSpace(long space)
        throws InterruptedException
    {
        _monitor.allocateSpace(space);
    }

    public void allocateSpace(long space, long millis)
        throws InterruptedException, MissingResourceException
    {
        _monitor.allocateSpace(space, millis);
    }

    public void freeSpace(long space)
    {
        _monitor.freeSpace(space);
    }

    public void setTotalSpace(long space)
    {
        _monitor.setTotalSpace(space);
    }

    public long getFreeSpace()
    {
        return _monitor.getFreeSpace();
    }

    public long getTotalSpace()
    {
        return _monitor.getTotalSpace();
    }

    public void addSpaceRequestListener(SpaceRequestable listener)
    {
        _monitor.addSpaceRequestListener(listener);
    }
}