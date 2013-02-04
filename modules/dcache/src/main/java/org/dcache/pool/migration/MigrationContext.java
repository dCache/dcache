package org.dcache.pool.migration;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import org.dcache.cells.CellStub;
import org.dcache.pool.repository.Repository;

import diskCacheV111.util.PnfsId;

/**
 * Describes the context of migration jobs.
 */
public class MigrationContext
{
    private String _poolName;
    private ScheduledExecutorService _executor;
    private CellStub _pool;
    private CellStub _pnfs;
    private CellStub _poolManager;
    private CellStub _pinManager;
    private Repository _repository;
    private final ConcurrentMap<PnfsId,PnfsId> _locks =
        new ConcurrentHashMap<>();

    public String getPoolName()
    {
        return _poolName;
    }

    public void setPoolName(String poolName)
    {
        _poolName = poolName;
    }

    public ScheduledExecutorService getExecutor()
    {
        return _executor;
    }

    public void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
    }

    public CellStub getPoolStub()
    {
        return _pool;
    }

    public void setPoolStub(CellStub pool)
    {
        _pool = pool;
    }

    public CellStub getPnfsStub()
    {
        return _pnfs;
    }

    public void setPnfsStub(CellStub pnfs)
    {
        _pnfs = pnfs;
    }

    public CellStub getPoolManagerStub()
    {
        return _poolManager;
    }

    public void setPoolManagerStub(CellStub poolManager)
    {
        _poolManager = poolManager;
    }

    public CellStub getPinManagerStub()
    {
        return _pinManager;
    }

    public void setPinManagerStub(CellStub pinManager)
    {
        _pinManager = pinManager;
    }

    public Repository getRepository()
    {
        return _repository;
    }

    public void setRepository(Repository repository)
    {
        _repository = repository;
    }

    public boolean lock(PnfsId pnfsId)
    {
        return (_locks.put(pnfsId, pnfsId) == null);
    }

    public void unlock(PnfsId pnfsId)
    {
        _locks.remove(pnfsId);
    }

    public boolean isActive(PnfsId pnfsId)
    {
        return _locks.containsKey(pnfsId);
    }
}
