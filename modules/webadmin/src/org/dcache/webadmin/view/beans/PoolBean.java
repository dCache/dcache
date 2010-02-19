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

public class PoolBean implements Comparable, Serializable {

    private static final Float ROUNDING_FACTOR = 10F;
    private static final Logger _log = LoggerFactory.getLogger(PoolBean.class);
    private String _name = "";
    private String _domainName = "";
    private boolean _enabled;
    private long _freeSpace = 0;
    private long _preciousSpace = 0;
    private long _totalSpace = 0;
    private long _usedSpace = 0;
    private float _percentageUsed = 0;
    private float _percentagePrecious = 0;
    private float _percentageFree = 0;
    private DiskSpaceUnit _displayUnit = DiskSpaceUnit.MIBIBYTES;

    public PoolBean() {
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

    public void setName(String name) {

        this._name = name;
    }

    public String getName() {

        return this._name;
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
        return _usedSpace;
    }

    /**
     *
     * @param usedSpace new value of usedSpace
     */
    public void setUsedSpace(long usedSpace) {
        this._usedSpace = usedSpace;
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
        this._totalSpace = totalSpace;
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
        this._preciousSpace = preciousSpace;
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
        this._freeSpace = freeSpace;
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
        this._domainName = domainName;
    }

    public int compareTo(Object other) {

        if (other == null) {
            throw new NullPointerException();
        }
//      throws ClassCastException if wrong object is delivered, according to
//      specification
        PoolBean otherBean = (PoolBean) other;
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

        if (!(testObject instanceof PoolBean)) {
            return false;
        }

        PoolBean otherPoolBean = (PoolBean) testObject;

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
