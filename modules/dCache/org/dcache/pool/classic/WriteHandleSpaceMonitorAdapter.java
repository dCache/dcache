package org.dcache.pool.classic;

import org.dcache.pool.repository.v5.CacheRepositoryV5;
import org.dcache.pool.repository.WriteHandle;
import java.util.MissingResourceException;
import java.util.concurrent.TimeoutException;

/**
 * The <code>WriteHandleSpaceMonitorAdapter</code> exposes a
 * repository write handle through the <code>SpaceMonitor</code>
 * interface. It supports allocation and deallocation of space,
 * although the deallocation calls are silently ignored, since the
 * write handle automatically cleans up over allocation.
 */
class WriteHandleSpaceMonitorAdapter
    extends RepositorySpaceMonitorAdapter
{
    private final WriteHandle _handle;

    public WriteHandleSpaceMonitorAdapter(CacheRepositoryV5 repository,
                                          WriteHandle handle)
    {
        super(repository);
        _handle = handle;
    }

    public void allocateSpace(long space)
        throws InterruptedException
    {
        _handle.allocate(space);
    }

    public void allocateSpace(long space, long millis)
        throws InterruptedException, MissingResourceException
    {
        try {
            _handle.allocate(space, millis);
        } catch (TimeoutException e) {
            throw new MissingResourceException("Timeout in space allocation",
                                               null, null);
        }
    }

    public void freeSpace(long space)
    {
        // Ignored, since the write handle will adjust the reservation
        // when it is closed.
    }
}