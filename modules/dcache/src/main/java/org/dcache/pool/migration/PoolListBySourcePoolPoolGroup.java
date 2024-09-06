package org.dcache.pool.migration;

import static java.util.Objects.requireNonNull;

import com.google.common.util.concurrent.MoreExecutors;
import diskCacheV111.vehicles.PoolManagerGetPoolsBySourcePoolPoolGroupMessage;
import org.dcache.cells.CellStub;

class PoolListBySourcePoolPoolGroup
      extends PoolListFromPoolManager {

    private final CellStub _poolManager;
    private final String _sourcePool;

    public PoolListBySourcePoolPoolGroup(CellStub poolManager, String sourcePool) {
        _poolManager = requireNonNull(poolManager);
        _sourcePool = requireNonNull(sourcePool);
    }

    @Override
    public void refresh() {
        CellStub.addCallback(
              _poolManager.send(new PoolManagerGetPoolsBySourcePoolPoolGroupMessage(_sourcePool)),
              this, MoreExecutors.directExecutor());
    }

    @Override
    public String toString() {
        return String.format("source pool %s, %d pools",
              _sourcePool, _pools.size());
    }
}
