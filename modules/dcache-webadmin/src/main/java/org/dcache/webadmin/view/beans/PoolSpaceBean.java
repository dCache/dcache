package org.dcache.webadmin.view.beans;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import diskCacheV111.pools.PoolV2Mode;

import org.dcache.webadmin.view.util.DiskSpaceUnit;

/**
 * Bean for the PoolUsage Page. Contains information concerning pools like
 * total space, name, domain etc.
 * @author jans
 */
public class PoolSpaceBean implements Comparable<PoolSpaceBean>, Serializable {

    private static final Float ROUNDING_FACTOR = 10F;
    private static final Logger _log = LoggerFactory.getLogger(PoolSpaceBean.class);
    private static final long serialVersionUID = -2932451489426746640L;
    private String _name = "";
    private String _domainName = "";
    private boolean _enabled;
    private PoolV2Mode _poolMode = new PoolV2Mode();
    private boolean _selected;
    private long _freeSpace;
    private long _preciousSpace;
    private long _totalSpace;
    private long _removableSpace;
    private long _usedSpace;
    private float _percentagePrecious;
    private float _percentageFree;
    private float _percentagePinned;
    private float _percentageRemovable;
    private DiskSpaceUnit _displayUnit = DiskSpaceUnit.MIBIBYTES;
    private boolean pending = false;

    public PoolSpaceBean() {
        calculatePercentages();
        _log.debug("poolBean created");
    }

    private void calculatePercentages() {
//        didn't take care for the case of
//        sum(usedSpace + freeSpace) > totalSpace
//        if pool has zero total space all are set to zero but free to 100%
        if (_totalSpace == 0) {
            setPercentagesForEmptyPool();
        } else {
            _percentagePrecious = calculatePercentage(_preciousSpace, _totalSpace);
            _percentageFree = calculatePercentage(_freeSpace, _totalSpace);
            _percentageRemovable = calculatePercentage(_removableSpace, _totalSpace);
            _percentagePinned = 100F - _percentagePrecious - _percentageFree -
                    _percentageRemovable;
        }
    }

    private void setPercentagesForEmptyPool() {
        _percentagePinned = 0;
        _percentageRemovable = 0;
        _percentagePrecious = 0;
        _percentageFree = 100;
    }

    private float calculatePercentage(float absoluteValue, float total) {
        float unrounded = absoluteValue / total * 100;
        return roundPercentage(unrounded);
    }

    private float roundPercentage(float percentage) {
        return Math.round((percentage * ROUNDING_FACTOR)) / ROUNDING_FACTOR;
    }

    public void addPoolSpace(PoolSpaceBean poolToAdd) {
        if (poolToAdd == null) {
            throw new IllegalArgumentException();
        }
        _totalSpace += poolToAdd._totalSpace;
        _freeSpace += poolToAdd._freeSpace;
        _preciousSpace += poolToAdd._preciousSpace;
        _usedSpace += poolToAdd._usedSpace;
        _removableSpace += poolToAdd._removableSpace;
        calculatePercentages();
    }

    public void setName(String name) {
        _name = name;
    }

    public String getName() {
        return _name;
    }

    public void setEnabled(boolean enabled) {
        _enabled = enabled;
    }

    public boolean isEnabled() {
        return _enabled;
    }

    public boolean isStatePending() {
        return pending;
    }

    public void setStatePending(boolean pending) {
        this.pending = pending;
    }

    public float getPercentageFree() {
        return _percentageFree;
    }

    public float getPercentagePrecious() {
        return _percentagePrecious;
    }

    public float getPercentagePinned() {
        return _percentagePinned;
    }

    public float getPercentageRemovable() {
        return _percentageRemovable;
    }

    public long getUsedSpace() {
        return DiskSpaceUnit.BYTES.convert(_usedSpace, _displayUnit);
    }

    public void setUsedSpace(long usedSpace) {
        _usedSpace = usedSpace;
        calculatePercentages();
    }

    public long getTotalSpace() {
        return DiskSpaceUnit.BYTES.convert(_totalSpace, _displayUnit);
    }

    public void setTotalSpace(long totalSpace) {
        _totalSpace = totalSpace;
        calculatePercentages();
    }

    public long getPreciousSpace() {
        return DiskSpaceUnit.BYTES.convert(_preciousSpace, _displayUnit);
    }

    public void setPreciousSpace(long preciousSpace) {
        _preciousSpace = preciousSpace;
        calculatePercentages();
    }

    public long getFreeSpace() {
        return DiskSpaceUnit.BYTES.convert(_freeSpace, _displayUnit);
    }

    public void setFreeSpace(long freeSpace) {
        _freeSpace = freeSpace;
        calculatePercentages();
    }

    public long getRemovableSpace() {
        return DiskSpaceUnit.BYTES.convert(_removableSpace, _displayUnit);
    }

    public void setRemovableSpace(long removableSpace) {
        _removableSpace = removableSpace;
        calculatePercentages();
    }

    public String getDomainName() {
        return _domainName;
    }

    public void setDomainName(String domainName) {
        _domainName = domainName;
    }

    public boolean isSelected() {
        return _selected;
    }

    public void setSelected(boolean selected) {
        _selected = selected;
    }

    public PoolV2Mode getPoolMode() {
        return _poolMode;
    }

    public void setPoolMode(PoolV2Mode poolMode) {
        _poolMode = poolMode;
    }

    @Override
    public int compareTo(PoolSpaceBean other) {
        return this.getName().compareTo(other.getName());
    }

    @Override
    public int hashCode() {
        return _name.hashCode();
    }

    /**
     * only considering the names, since pools are well-known cells and have to
     * have a unique name
     */
    @Override
    public boolean equals(Object testObject) {
        if (this == testObject) {
            return true;
        }

        if (!(testObject instanceof PoolSpaceBean)) {
            return false;
        }

        PoolSpaceBean otherPoolBean = (PoolSpaceBean) testObject;

        if (!(otherPoolBean._name.equals(_name))) {
            return false;
        }

        return true;
    }
}
