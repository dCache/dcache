package org.dcache.pool.migration;

import static java.util.Objects.requireNonNull;

import com.google.common.util.concurrent.MoreExecutors;
import diskCacheV111.vehicles.PoolManagerGetPoolsByPoolGroupOfPoolMessage;
import org.dcache.cells.CellStub;

class PoolListByPoolGroupOfPool
      extends PoolListFromPoolManager {

    private final CellStub _poolManager;
    private final String _poolName;

    public PoolListByPoolGroupOfPool(CellStub poolManager, String poolName) {
        _poolManager = requireNonNull(poolManager);
        _poolName = requireNonNull(poolName);
    }

    @Override
    public void refresh() {
        CellStub.addCallback(
              _poolManager.send(new PoolManagerGetPoolsByPoolGroupOfPoolMessage(_poolName)),
              this, MoreExecutors.directExecutor());
    }

    @Override
    public String toString() {
        return String.format("source pool %s, %d pools",
                _poolName, _pools.size());
    }
}
