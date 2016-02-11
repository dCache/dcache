package diskCacheV111.poolManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;

class PGroup extends PoolCore implements SelectionPoolGroup {
    private static final long serialVersionUID = 3883973457610397314L;
    final Map<String, Pool> _poolList = new ConcurrentHashMap<>();

    PGroup(String name) {
        super(name);
    }

    private String[] getPools()
    {
        return _poolList.keySet().toArray(new String[_poolList.size()]);
    }

    @Override
    public String toString() {
        return getName() + "  (links=" + _linkList.size() + ";pools=" + _poolList.size() + ")";
    }

}
