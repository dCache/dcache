package org.dcache.pool.migration;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import diskCacheV111.vehicles.PoolManagerGetPoolsByPoolGroupOfPoolMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.List;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PoolListByPoolGroupOfPool
      extends AbstractMessageCallback<PoolManagerGetPoolsByPoolGroupOfPoolMessage>
      implements RefreshablePoolList {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(PoolListByPoolGroupOfPool.class);

    private final CellStub _poolManager;
    private final String _poolName;
    private ImmutableMap<String, List<PoolManagerPoolInformation>> _poolsMap;
    private ImmutableList<String> _offlinePools = ImmutableList.of();

    private boolean _isValid;

    public PoolListByPoolGroupOfPool(CellStub poolManager, String poolName) {
        _poolManager = requireNonNull(poolManager);
        _poolName = requireNonNull(poolName);
    }

    @Override
    public synchronized boolean isValid() {
        return _isValid;
    }

    @Override
    public ImmutableList<String> getOfflinePools() {
        return _offlinePools;
    }

    @Override
    public ImmutableList<PoolManagerPoolInformation> getPools() {
        return _poolsMap.isEmpty() ?
              ImmutableList.of() :
              ImmutableList.copyOf(_poolsMap.values().iterator().next());
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
              _poolName, getPools().size());
    }

    @Override
    public void success(PoolManagerGetPoolsByPoolGroupOfPoolMessage message) {
        _poolsMap = ImmutableMap.copyOf(message.getPoolsMap());
        _offlinePools = ImmutableList.copyOf(message.getOfflinePools());
        _isValid = (_poolsMap.size() == 1);
    }

    @Override
    public void failure(int rc, Object error) {
        LOGGER.error("Failed to query pool manager ({})", error);
    }
}
