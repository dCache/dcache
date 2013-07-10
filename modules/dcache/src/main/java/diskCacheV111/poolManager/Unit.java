package diskCacheV111.poolManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;

class Unit implements Serializable, SelectionUnit {
    private static final long serialVersionUID = -2534629882175347637L;
    private final String _name;
    private final int _type;
    final Map<String, UGroup> _uGroupList = new ConcurrentHashMap<>();

    Unit(String name, int type) {
        _name = name;
        _type = type;
    }

    @Override
    public String getName() {
        return _name;
    }

    public String getCanonicalName() {
        return getName();
    }

    public int getType() {
        return _type;
    }

    @Override
    public Collection<SelectionUnitGroup> getMemberOfUnitGroups() {
        return new ArrayList(_uGroupList.values());
    }

    @Override
    public String getUnitType() {
        switch (_type) {
        case PoolSelectionUnitV2.STORE: return "Store";
        case PoolSelectionUnitV2.DCACHE: return "DCache";
        case PoolSelectionUnitV2.PROTOCOL: return "Protocol";
        case PoolSelectionUnitV2.NET: return "Net";
        }
        return "Unknown";
    }

    @Override
    public String toString() {
        return _name + "  (type=" + getUnitType() + ";canonical=" + getCanonicalName() + ";uGroups=" + _uGroupList.size() + ")";
    }

}
