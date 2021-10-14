package diskCacheV111.poolManager;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class PGroup extends PoolCore implements SelectionPoolGroup {

    private static final long serialVersionUID = 3883973457610397314L;
    final Map<String, Pool> _poolList = new ConcurrentHashMap<>();

    private final boolean resilient;

    PGroup(String name, boolean resilient) {
        super(name);
        this.resilient = resilient;
    }

    @Override
    public boolean isResilient() {
        return resilient;
    }

    @Override
    public boolean isPrimary() {
        return resilient;
    }

    @Override
    public String toString() {
        return getName() + "(links=" + _linkList.size()
              + "; pools=" + _poolList.size() + "; resilient=" + resilient + ")";
    }

    private String[] getPools() {
        return _poolList.keySet().toArray(String[]::new);
    }
}
