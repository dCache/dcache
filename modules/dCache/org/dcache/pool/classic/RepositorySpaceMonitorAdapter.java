package org.dcache.pool.classic;

import diskCacheV111.repository.SpaceMonitor;
import diskCacheV111.repository.SpaceRequestable;
import org.dcache.pool.repository.v5.CacheRepositoryV5;
import java.util.MissingResourceException;

/**
 * The <code>RepositorySpaceMonitorAdapter</code> exposes information
 * about repository space through the <code>SpaceMonitor</code>
 * interface. The adapter is read only in the sense that all
 * operations that would modify the state of the repository throw
 * <code>IllegalStateException</code>.
 */
class RepositorySpaceMonitorAdapter implements SpaceMonitor
{
    private final CacheRepositoryV5 _repository;

    public RepositorySpaceMonitorAdapter(CacheRepositoryV5 repository)
    {
        _repository = repository;
    }

    public void setTotalSpace(long space)
    {
        throw new IllegalStateException("Resizing the repository is not allowed here");
    }

    public long getFreeSpace()
    {
        return _repository.getSpaceRecord().getFreeSpace();
    }

    public long getTotalSpace()
    {
        return _repository.getSpaceRecord().getTotalSpace();
    }

    public void addSpaceRequestListener(SpaceRequestable listener)
    {
        throw new IllegalStateException("Registering space request listeners is not allowed here");
    }

    public void allocateSpace(long space)
        throws InterruptedException
    {
        throw new IllegalStateException("Allocating space is not allowed here");
    }

    public void allocateSpace(long space, long millis)
        throws InterruptedException, MissingResourceException
    {
        throw new IllegalStateException("Allocating space is not allowed here");
    }

    public void freeSpace(long space)
    {
        throw new IllegalStateException("Freeing space is not allowed here");
    }
}