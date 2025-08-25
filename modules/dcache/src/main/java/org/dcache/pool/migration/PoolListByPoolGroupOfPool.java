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
        LOGGER.debug("PoolListByPoolGroupOfPool() called with poolName={}", poolName);
        _poolManager = requireNonNull(poolManager);
        _poolName = requireNonNull(poolName);
    }

    @Override
    public synchronized boolean isValid() {
        LOGGER.debug("isValid() called for poolName={}, isValid={}", _poolName, _isValid);
        return _isValid;
    }

    @Override
    public ImmutableList<String> getOfflinePools() {
        LOGGER.debug("getOfflinePools() called for poolName={}, offlinePools.size={}", _poolName, _offlinePools.size());
        return _offlinePools;
    }

    @Override
    public ImmutableList<PoolManagerPoolInformation> getPools() {
        int size = _poolsMap.isEmpty() ? 0 : _poolsMap.values().iterator().next().size();
        LOGGER.debug("getPools() called for poolName={}, pools.size={}", _poolName, size);
        return _poolsMap.isEmpty() ?
              ImmutableList.of() :
              ImmutableList.copyOf(_poolsMap.values().iterator().next());
    }

    @Override
    public void refresh() {
        LOGGER.debug("refresh() called for poolName={}, poolManager={}", _poolName, _poolManager);
        CellStub.addCallback(
              _poolManager.send(new PoolManagerGetPoolsByPoolGroupOfPoolMessage(_poolName)),
              this, MoreExecutors.directExecutor());
    }

    @Override
    public void success(PoolManagerGetPoolsByPoolGroupOfPoolMessage message) {
        LOGGER.debug("success() called for poolName={}, poolsMap.size={}, offlinePools.size={}",
            _poolName,
            message.getPoolsMap().size(),
            message.getOfflinePools().size());
        _poolsMap = ImmutableMap.copyOf(message.getPoolsMap());
        _offlinePools = ImmutableList.copyOf(message.getOfflinePools());
        _isValid = (_poolsMap.size() == 1);
    }

    @Override
    public void failure(int rc, Object error) {
        LOGGER.error("Failed to query pool manager ({})", error);
    }
}
