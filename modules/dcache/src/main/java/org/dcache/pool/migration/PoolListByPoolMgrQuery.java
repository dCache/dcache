package org.dcache.pool.migration;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolMgrQueryPoolsMsg;
import diskCacheV111.vehicles.PoolManagerGetPoolsByNameMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.ArrayList;
import java.util.List;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RefreshablePoolList implementation that queries PoolManager for eligible pools using
 * PoolMgrQueryPoolsMsg, which performs full selection matching including read preferences and file
 * metadata.
 */
class PoolListByPoolMgrQuery
      extends AbstractMessageCallback<PoolMgrQueryPoolsMsg>
      implements RefreshablePoolList {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(PoolListByPoolMgrQuery.class);

    private final CellStub _poolManager;
    private final PnfsId _pnfsId;
    private final String _protocolUnit;
    private final String _netUnitName;
    private final FileAttributes _fileAttributes;

    private ImmutableList<PoolManagerPoolInformation> _pools = ImmutableList.of();
    private ImmutableList<String> _offlinePools = ImmutableList.of();
    private boolean _isValid;

    /**
     * Creates a new pool list that queries PoolManager for eligible pools.
     *
     * @param poolManager the PoolManager cell stub
     * @param pnfsId the PNFS ID of the file
     * @param fileAttributes the file attributes for selection
     * @param protocolUnit the protocol unit (e.g., "DCap/3")
     * @param netUnitName the network unit name (IP address or null)
     */
    public PoolListByPoolMgrQuery(CellStub poolManager,
          PnfsId pnfsId,
          FileAttributes fileAttributes,
          String protocolUnit,
          String netUnitName) {
        _poolManager = requireNonNull(poolManager);
        _pnfsId = requireNonNull(pnfsId);
        _fileAttributes = requireNonNull(fileAttributes);
        _protocolUnit = requireNonNull(protocolUnit);
        _netUnitName = netUnitName;
    }

    @Override
    public synchronized boolean isValid() {
        return _isValid;
    }

    @Override
    public synchronized ImmutableList<String> getOfflinePools() {
        return _offlinePools;
    }

    @Override
    public synchronized ImmutableList<PoolManagerPoolInformation> getPools() {
        return _pools;
    }

    @Override
    public void refresh() {
        // Ensure we have STORAGEINFO attribute
        if (!_fileAttributes.isDefined(FileAttribute.STORAGEINFO)) {
            LOGGER.warn("FileAttributes for {} missing STORAGEINFO, cannot query PoolManager",
                  _pnfsId);
            synchronized (this) {
                _isValid = false;
            }
            return;
        }

        PoolMgrQueryPoolsMsg msg = new PoolMgrQueryPoolsMsg(
              DirectionType.READ,
              _protocolUnit,
              _netUnitName,
              _fileAttributes);

        CellStub.addCallback(
              _poolManager.send(msg),
              this,
              MoreExecutors.directExecutor());
    }

    @Override
    public void success(PoolMgrQueryPoolsMsg message) {
        List<String>[] poolLists = message.getPools();
        if (poolLists == null || poolLists.length == 0) {
            synchronized (this) {
                _pools = ImmutableList.of();
                _offlinePools = ImmutableList.of();
                _isValid = true;
            }
            return;
        }

        // Flatten the preference-ordered pool lists into a single list
        // The pools are already ordered by desirability from PoolManager
        List<String> allPools = new ArrayList<>();
        for (List<String> poolList : poolLists) {
            if (poolList != null) {
                allPools.addAll(poolList);
            }
        }

        // Query PoolManager for full pool information (including cost)
        if (allPools.isEmpty()) {
            synchronized (this) {
                _pools = ImmutableList.of();
                _offlinePools = ImmutableList.of();
                _isValid = true;
            }
            return;
        }

        PoolManagerGetPoolsByNameMessage poolInfoMsg =
              new PoolManagerGetPoolsByNameMessage(allPools);

        CellStub.addCallback(
              _poolManager.send(poolInfoMsg),
              new AbstractMessageCallback<PoolManagerGetPoolsByNameMessage>() {
                  @Override
                  public void success(PoolManagerGetPoolsByNameMessage msg) {
                      synchronized (PoolListByPoolMgrQuery.this) {
                          _pools = ImmutableList.copyOf(msg.getPools());
                          _offlinePools = ImmutableList.copyOf(msg.getOfflinePools());
                          _isValid = true;
                      }
                  }

                  @Override
                  public void failure(int rc, Object error) {
                      LOGGER.error("Failed to get pool information from PoolManager ({})", error);
                      synchronized (PoolListByPoolMgrQuery.this) {
                          _isValid = false;
                      }
                  }
              },
              MoreExecutors.directExecutor());
    }

    @Override
    public void failure(int rc, Object error) {
        LOGGER.error("Failed to query pool manager for eligible pools ({})", error);
        synchronized (this) {
            _isValid = false;
        }
    }

    @Override
    public String toString() {
        return String.format("PoolMgrQuery(%s, %d pools)", _protocolUnit, _pools.size());
    }
}

