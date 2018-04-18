package diskCacheV111.poolManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.UnitType;

class Unit implements Serializable, SelectionUnit {
    private static final long serialVersionUID = -2534629882175347637L;
    private final String _name;
    private final UnitType _type;
    final Map<String, UGroup> _uGroupList = new ConcurrentHashMap<>();

    Unit(String name, UnitType type) {
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

    public UnitType getType() {
        return _type;
    }

    @Override
    public Collection<SelectionUnitGroup> getMemberOfUnitGroups() {
        return new ArrayList(_uGroupList.values());
    }

    @Override
    public String toString() {
        return _name + "  (type=" + _type + ";canonical=" + getCanonicalName() + ";uGroups=" + _uGroupList.size() + ")";
    }

}
