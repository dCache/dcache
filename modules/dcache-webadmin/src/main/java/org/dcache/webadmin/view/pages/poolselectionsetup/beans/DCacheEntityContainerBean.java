package org.dcache.webadmin.view.pages.poolselectionsetup.beans;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author jans
 */
public class DCacheEntityContainerBean implements Serializable {

    private static final long serialVersionUID = -2664828166664741877L;
    private Map<String, PoolEntity> _pools = new HashMap<>();
    private Map<String, PoolGroupEntity> _poolGroups = new HashMap<>();
    private Map<String, LinkEntity> _links = new HashMap<>();
    private Map<String, UGroupEntity> _unitGroups = new HashMap<>();
    private Map<String, UnitEntity> _units = new HashMap<>();

    public List<PoolEntity> getPools() {
        return Lists.newArrayList(_pools.values());
    }

    public List<PoolGroupEntity> getPoolGroups() {
        return Lists.newArrayList(_poolGroups.values());
    }

    public List<LinkEntity> getLinks() {
        return Lists.newArrayList(_links.values());
    }

    public List<UGroupEntity> getUnitGroups() {
        return Lists.newArrayList(_unitGroups.values());
    }

    public List<UnitEntity> getUnits() {
        return Lists.newArrayList(_units.values());
    }

    public PoolEntity getPool(String name) {
        return _pools.get(name);
    }

    public void setPools(Collection<PoolEntity> pools) {
        _pools.clear();
        for (PoolEntity pool : pools) {
            _pools.put(pool.getName(), pool);
        }
    }

    public void setLinks(Collection<LinkEntity> links) {
        _links.clear();
        for (LinkEntity link : links) {
            _links.put(link.getName(), link);
        }
    }

    public void setPoolGroups(Collection<PoolGroupEntity> pGroups) {
        _poolGroups.clear();
        for (PoolGroupEntity poolGroup : pGroups) {
            _poolGroups.put(poolGroup.getName(), poolGroup);
        }
    }

    public void setUnitGroups(Collection<UGroupEntity> uGroups) {
        _unitGroups.clear();
        for (UGroupEntity unitGroup : uGroups) {
            _unitGroups.put(unitGroup.getName(), unitGroup);
        }
    }

    public void setUnits(Collection<UnitEntity> units) {
        _units.clear();
        for (UnitEntity unit : units) {
            _units.put(unit.getName(), unit);
        }

    }

    public DCacheEntity getEntity(String name, EntityType type) {
        switch (type) {
            case POOL:
                return _pools.get(name);
            case POOLGROUP:
                return _poolGroups.get(name);
            case LINK:
                return _links.get(name);
            case UNITGROUP:
                return _unitGroups.get(name);
            case UNIT:
                return _units.get(name);
            default:
                throw new IllegalArgumentException("Entity not yet supported");
        }
    }
}
