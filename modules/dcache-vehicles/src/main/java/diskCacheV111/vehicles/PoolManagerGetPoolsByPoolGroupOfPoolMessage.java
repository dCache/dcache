package diskCacheV111.vehicles;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PoolManagerGetPoolsByPoolGroupOfPoolMessage
      extends Message {

    private static final long serialVersionUID = -4022990392097610436L;

    private final String _poolName;
    private Map<String, List<PoolManagerPoolInformation>> _poolsMap;
    private Collection<String> _offlinePools;

    public PoolManagerGetPoolsByPoolGroupOfPoolMessage(String poolName) {
        super(true);
        _poolName = requireNonNull(poolName);
    }

    public String getPoolName() {
        return _poolName;
    }

    public void setPoolsMap(Map<String, List<PoolManagerPoolInformation>> poolsMap) {
        _poolsMap = new HashMap<>(poolsMap);
    }

    public void setOfflinePools(Collection<String> _offlinePools) {
        this._offlinePools = _offlinePools;
    }

    public Map<String, List<PoolManagerPoolInformation>> getPoolsMap() {
        return _poolsMap;
    }

    public Collection<String> getOfflinePools() {
        return (_offlinePools == null) ? Collections.emptyList() : _offlinePools;
    }
}
