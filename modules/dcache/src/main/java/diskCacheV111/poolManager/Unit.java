package diskCacheV111.poolManager;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class Unit implements Serializable, SelectionUnit {
    static final long serialVersionUID = -2534629882175347637L;
    String _name = null;
    int _type = 0;
    Map<String, UGroup> _uGroupList = new HashMap<String, UGroup>();

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

    private String getType() {
        switch (_type) {
            case PoolSelectionUnitV2.STORE: return "Store";
            case PoolSelectionUnitV2.DCACHE: return "DCache";
            case PoolSelectionUnitV2.PROTOCOL: return "Protocol";
            case PoolSelectionUnitV2.NET: return "Net";
        }
        return "Unknown";
    }

    @Override
    public Collection<SelectionUnitGroup> getMemberOfUnitGroups() {
        return new ArrayList(_uGroupList.values());
    }

    @Override
    public String getUnitType() {
        return getType();
    }

    @Override
    public String toString() {
        return _name + "  (type=" + getType() + ";canonical=" + getCanonicalName() + ";uGroups=" + _uGroupList.size() + ")";
    }

}
