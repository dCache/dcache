package diskCacheV111.vehicles;

import static java.util.Objects.requireNonNull;

public class PoolManagerGetPoolsByPoolGroupOfPoolMessage
      extends PoolManagerGetPoolsMessage {

    private static final long serialVersionUID = 4423670920097918847L;

    private final String _poolName;

    public PoolManagerGetPoolsByPoolGroupOfPoolMessage(String poolName) {
        _poolName = requireNonNull(poolName);
    }

    public String getPoolName() {
        return _poolName;
    }
}
