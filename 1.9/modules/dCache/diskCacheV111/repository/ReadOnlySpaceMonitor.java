package diskCacheV111.repository;

import java.util.MissingResourceException;

/**
 * Space monitor decorator which disallows space allocation and
 * deallocation.
 */
public class ReadOnlySpaceMonitor extends SpaceMonitorDecorator
{
    public ReadOnlySpaceMonitor(SpaceMonitor monitor)
    {
        super(monitor);
    }

    public void allocateSpace(long space)
        throws InterruptedException
    {
        throw new IllegalStateException("Cannot allocate space in read only space monitor");
    }

    public void allocateSpace(long space, long millis)
        throws InterruptedException, MissingResourceException
    {
        throw new IllegalStateException("Cannot allocate space in read only space monitor");
    }

    public void freeSpace(long space)
    {
        throw new IllegalStateException("Cannot free space in read only space monitor");
    }
}