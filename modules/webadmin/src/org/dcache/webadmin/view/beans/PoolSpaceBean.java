package org.dcache.webadmin.view.beans;

/**
 * Bean for the PoolUsage Page. Contains information concerning pools like
 * total space, name, domain etc.
 * @author jans
 */
import java.io.Serializable;
import org.dcache.webadmin.controller.util.DiskSpaceUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PoolSpaceBean implements Comparable, Serializable {

    private static final Float ROUNDING_FACTOR = 10F;
    private static final Logger _log = LoggerFactory.getLogger(PoolSpaceBean.class);
    private String _name = "";
    private String _domainName = "";
    private boolean _enabled;
    private boolean _selected = false;
    private long _freeSpace = 0;
    private long _preciousSpace = 0;
    private long _totalSpace = 0;
    private long _usedSpace = 0;
    private float _percentageUsed = 0;
    private float _percentagePrecious = 0;
    private float _percentageFree = 0;
    private DiskSpaceUnit _displayUnit = DiskSpaceUnit.MIBIBYTES;

    public PoolSpaceBean() {
        calculatePercentages();
        _log.debug("poolBean created");
    }

    private void calculatePercentages() {
//        didn't take care for the case of
//        sum(usedSpace + preciousSpace + freeSpace) > totalSpace
//        if pool has zero total space all are set to zero but free to 100%
        if (_totalSpace == 0) {
            setPercentagesForEmptyPool();
        } else {
            _percentagePrecious = calculatePercentage(_preciousSpace, _totalSpace);
            _percentageUsed = calculatePercentage(_usedSpace, _totalSpace);
            _percentageFree = calculatePercentage(_freeSpace, _totalSpace);
        }
    }

    private void setPercentagesForEmptyPool() {
        _percentageUsed = 0;
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
        calculatePercentages();
    }

    public void setName(String name) {

        _name = name;
    }

    public String getName() {

        return _name;
    }

    public void setEnabled(boolean enabled) {

        this._enabled = enabled;
    }

    public boolean isEnabled() {

        return _enabled;
    }

    /**
     *
     * @return the value of percentageFree
     */
    public float getPercentageFree() {
        return _percentageFree;
    }

    /**
     *
     * @return the value of percentagePrecious
     */
    public float getPercentagePrecious() {
        return _percentagePrecious;
    }

    /**
     *
     * @return the value of percentageUsed
     */
    public float getPercentageUsed() {
        return _percentageUsed;
    }

    /**
     *
     * @return the value of usedSpace
     */
    public long getUsedSpace() {
        return DiskSpaceUnit.BYTES.convert(_usedSpace, _displayUnit);
    }

    /**
     *
     * @param usedSpace new value of usedSpace
     */
    public void setUsedSpace(long usedSpace) {
        _usedSpace = usedSpace;
        calculatePercentages();
    }

    /**
     *
     * @return the value of totalSpace
     */
    public long getTotalSpace() {
        return DiskSpaceUnit.BYTES.convert(_totalSpace, _displayUnit);
    }

    /**
     *
     * @param totalSpace new value of totalSpace
     */
    public void setTotalSpace(long totalSpace) {
        _totalSpace = totalSpace;
        calculatePercentages();
    }

    /**
     * @return the value of preciousSpace
     */
    public long getPreciousSpace() {
        return DiskSpaceUnit.BYTES.convert(_preciousSpace, _displayUnit);
    }

    /**
     * @param preciousSpace new value of preciousSpace
     */
    public void setPreciousSpace(long preciousSpace) {
        _preciousSpace = preciousSpace;
        calculatePercentages();
    }

    /**
     *
     * @return the value of freeSpace
     */
    public long getFreeSpace() {
        return DiskSpaceUnit.BYTES.convert(_freeSpace, _displayUnit);
    }

    /**
     *
     * @param freeSpace new value of freeSpace
     */
    public void setFreeSpace(long freeSpace) {
        _freeSpace = freeSpace;
        calculatePercentages();
    }

    /**
     *
     * @return the value of domainName
     */
    public String getDomainName() {
        return _domainName;
    }

    /**
     *
     * @param domainName new value of domainName
     */
    public void setDomainName(String domainName) {
        _domainName = domainName;
    }

    public boolean isSelected() {
        return _selected;
    }

    public void setSelected(boolean selected) {
        _selected = selected;
    }

    public int compareTo(Object other) {

        if (other == null) {
            throw new NullPointerException();
        }
//      throws ClassCastException if wrong object is delivered, according to
//      specification
        PoolSpaceBean otherBean = (PoolSpaceBean) other;
        return this.getName().compareTo(otherBean.getName());
    }

    @Override
    public int hashCode() {
        return (int) (_name.hashCode() ^ _domainName.hashCode() ^ _totalSpace ^
                _freeSpace ^ _preciousSpace ^ _usedSpace);
    }

    /**
     * enabled is not considered during equal comparison
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

        if (!(otherPoolBean._domainName.equals(_domainName))) {
            return false;
        }

        if (!(otherPoolBean._freeSpace == _freeSpace)) {
            return false;
        }
        if (!(otherPoolBean._preciousSpace == _preciousSpace)) {
            return false;
        }
        if (!(otherPoolBean._totalSpace == _totalSpace)) {
            return false;
        }
        if (!(otherPoolBean._usedSpace == _usedSpace)) {
            return false;
        }

        return true;
    }
}
