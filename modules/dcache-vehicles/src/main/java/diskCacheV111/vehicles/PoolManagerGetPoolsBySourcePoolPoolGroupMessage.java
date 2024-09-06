package diskCacheV111.vehicles;

import static java.util.Objects.requireNonNull;

public class PoolManagerGetPoolsBySourcePoolPoolGroupMessage
      extends PoolManagerGetPoolsMessage {

    private static final long serialVersionUID = 4423670920097918847L;

    private final String _sourcePool;

    public PoolManagerGetPoolsBySourcePoolPoolGroupMessage(String sourcePool) {
        _sourcePool = requireNonNull(sourcePool);
    }

    public String getSourcePool() {
        return _sourcePool;
    }
}
