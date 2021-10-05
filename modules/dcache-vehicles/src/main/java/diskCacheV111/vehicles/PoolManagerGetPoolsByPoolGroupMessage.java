package diskCacheV111.vehicles;

import static java.util.Objects.requireNonNull;

public class PoolManagerGetPoolsByPoolGroupMessage
      extends PoolManagerGetPoolsMessage {

    private static final long serialVersionUID = 2808625734157545379L;

    private final Iterable<String> _poolGroups;

    public PoolManagerGetPoolsByPoolGroupMessage(Iterable<String> poolGroups) {
        _poolGroups = requireNonNull(poolGroups);
    }

    public Iterable<String> getPoolGroups() {
        return _poolGroups;
    }
}
