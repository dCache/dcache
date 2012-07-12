package diskCacheV111.poolManager;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Link implements SelectionLink, Serializable {
    static final long serialVersionUID = 4480385941491281821L;
    final String _name;
    final Map<String, PoolCore> _poolList = new HashMap<String, PoolCore>();
    final Map<String, UGroup> _uGroupList = new HashMap<String, UGroup>();
    int _readPref = 0;
    int _writePref = 0;
    int _cachePref = 0;
    int _p2pPref = -1;
    String _tag = null;
    private LinkGroup _linkGroup = null;

    Link(String name) {
        _name = name;
    }

    @Override
    public String getTag() {
        return _tag;
    }

    @Override
    public Collection<SelectionPoolGroup> getPoolGroupsPointingTo() {
        Collection<SelectionPoolGroup> pGroups = new ArrayList<SelectionPoolGroup>();
        for (PoolCore pGroup : _poolList.values()) {
            PGroup newPGroup = new PGroup(pGroup.getName());
            pGroups.add(newPGroup);
        }
        return pGroups;
    }

    @Override
    public Collection<SelectionUnitGroup> getUnitGroupsTargetedBy() {
        return new ArrayList<SelectionUnitGroup>(_uGroupList.values());
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public LinkReadWritePreferences getPreferences() {
        return new LinkReadWritePreferences(_readPref, _writePref, _cachePref, _p2pPref);
    }

    @Override
    public String toString() {
        return _name + "  (pref=" + _readPref + "/" + _cachePref + "/" + _p2pPref + "/" + _writePref + ";" + (_tag == null ? "" : _tag) + ";" + "ugroups=" + _uGroupList.size() + ";pools=" + _poolList.size() + ")";
    }

    public String getAttraction() {
        return "-readpref=" + _readPref + " -writepref=" + _writePref + " -cachepref=" + _cachePref + " -p2ppref=" + _p2pPref + (_tag == null ? "" : " -section=" + _tag);
    }

    @Override
    public Collection<SelectionPool> pools() {
        List<SelectionPool> list = new ArrayList<SelectionPool>();
        for (Object o : _poolList.values()) {
            if (o instanceof Pool) {
                list.add((Pool) o);
            } else if (o instanceof PGroup) {
                for (Pool pool : ((PGroup) o)._poolList.values()) {
                    list.add(pool);
                }
            }
        }
        return list;
    }

    public boolean exec(Map variableMap) {
        return true;
    }

    public void setLinkGroup(LinkGroup lg) {
        _linkGroup = lg;
    }

    public LinkGroup getLinkGroup() {
        return _linkGroup;
    }

}
