package diskCacheV111.poolManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;

class Link implements SelectionLink, Serializable {
    private static final long serialVersionUID = 4480385941491281821L;
    private final String _name;
    final Map<String, PoolCore> _poolList = new ConcurrentHashMap<>();
    final Map<String, UGroup> _uGroupList = new ConcurrentHashMap<>();
    private int _readPref;
    private int _writePref;
    private int _cachePref;
    private int _p2pPref = -1;
    private String _tag;
    private LinkGroup _linkGroup;

    Link(String name) {
        _name = name;
    }

    @Override
    public String getTag() {
        return _tag;
    }

    @Override
    public Collection<SelectionPoolGroup> getPoolGroupsPointingTo() {
        Collection<SelectionPoolGroup> pGroups = new ArrayList<>();
        for (PoolCore pGroup : _poolList.values()) {
            PGroup newPGroup = new PGroup(pGroup.getName());
            pGroups.add(newPGroup);
        }
        return pGroups;
    }

    @Override
    public Collection<SelectionUnitGroup> getUnitGroupsTargetedBy() {
        return new ArrayList<>(_uGroupList.values());
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public LinkReadWritePreferences getPreferences() {
        return new LinkReadWritePreferences(getReadPref(), getWritePref(), getCachePref(), getP2pPref());
    }

    @Override
    public String toString() {
        return getName() + "  (pref=" + getReadPref() + "/" + getCachePref() + "/" + getP2pPref() + "/" + getWritePref() + ";" + (getTag() == null ? "" : getTag()) + ";" + "ugroups=" + _uGroupList.size() + ";pools=" + _poolList.size() + ")";
    }

    public String getAttraction() {
        return "-readpref=" + getReadPref() + " -writepref=" + getWritePref() + " -cachepref=" + getCachePref() + " -p2ppref=" + getP2pPref() + (getTag() == null ? "" : " -section=" + getTag());
    }

    @Override
    public Collection<SelectionPool> getPools() {
        List<SelectionPool> list = new ArrayList<>();
        for (Object o : _poolList.values()) {
            if (o instanceof Pool) {
                list.add((Pool) o);
            } else if (o instanceof PGroup) {
                list.addAll(((PGroup) o)._poolList.values());
            }
        }
        return list;
    }

    public boolean exec(Map<String, String> variableMap) {
        return true;
    }

    public void setLinkGroup(LinkGroup lg) {
        _linkGroup = lg;
    }

    public LinkGroup getLinkGroup() {
        return _linkGroup;
    }

    public int getReadPref()
    {
        return _readPref;
    }

    public void setReadPref(int readPref)
    {
        _readPref = readPref;
    }

    public int getWritePref()
    {
        return _writePref;
    }

    public void setWritePref(int writePref)
    {
        _writePref = writePref;
    }

    public int getCachePref()
    {
        return _cachePref;
    }

    public void setCachePref(int cachePref)
    {
        _cachePref = cachePref;
    }

    public int getP2pPref()
    {
        return _p2pPref;
    }

    public void setP2pPref(int p2pPref)
    {
        _p2pPref = p2pPref;
    }

    public void setTag(String tag)
    {
        _tag = tag;
    }
}
