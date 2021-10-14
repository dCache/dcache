package diskCacheV111.poolManager;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class UGroup implements Serializable, SelectionUnitGroup {

    private static final long serialVersionUID = 8169708306745935858L;
    private final String _name;
    final Map<String, Link> _linkList = new ConcurrentHashMap<>();
    final Map<String, Unit> _unitList = new ConcurrentHashMap<>(); // !!!
    // DCache,
    // STore,
    // Net
    // names
    // must
    // be
    // different

    UGroup(String name) {
        _name = name;
    }

    @Override
    public Collection<SelectionUnit> getMemberUnits() {
        return new ArrayList<>(_unitList.values());
    }

    @Override
    public Collection<SelectionLink> getLinksPointingTo() {
        return new ArrayList<>(_linkList.values());
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public String toString() {
        return _name + "  (links=" + _linkList.size() + ";units=" + _unitList.size() + ")";
    }

}
