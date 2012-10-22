package diskCacheV111.poolManager;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import java.util.HashMap;
import java.util.Map;

class PGroup extends PoolCore implements SelectionPoolGroup {
    private static final long serialVersionUID = 3883973457610397314L;
    final Map<String, Pool> _poolList = new HashMap<>();

    PGroup(String name) {
        super(name);
    }

    @Override
    public String toString() {
        return _name + "  (links=" + _linkList.size() + ";pools=" + _poolList.size() + ")";
    }

}
