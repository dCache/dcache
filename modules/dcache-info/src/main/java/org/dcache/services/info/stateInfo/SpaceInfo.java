package org.dcache.services.info.stateInfo;

import diskCacheV111.pools.PoolCostInfo;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;

/**
 * Information about some (generic) space.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class SpaceInfo {

    public static final String PATH_ELEMENT_TOTAL = "total";
    public static final String PATH_ELEMENT_FREE = "free";
    public static final String PATH_ELEMENT_PRECIOUS = "precious";
    public static final String PATH_ELEMENT_REMOVABLE = "removable";
    public static final String PATH_ELEMENT_USED = "used";

    private long _total;
    private long _free;
    private long _precious;
    private long _removable;
    private long _used;

    public SpaceInfo(long totalSpace, long freeSpace, long preciousSpace, long removableSpace) {
        _total = totalSpace;
        _free = freeSpace;
        _precious = preciousSpace;
        _removable = removableSpace;

        // Derived data.
        _used = totalSpace - freeSpace;
    }

    /**
     * Create a new SpaceInfo that duplicates information in otherInfo
     * @param otherInfo the SpaceInfo to duplicate
     */
    public SpaceInfo(SpaceInfo otherInfo) {
        this(otherInfo.getTotal(), otherInfo.getFree(),
                otherInfo.getPrecious(), otherInfo.getRemovable());
    }

    /**
     * Create a zero-sized Space.
     */
    public SpaceInfo() {
            _total = _free = _precious = _removable = _used = 0;
    }


    /**
     * Create an initially-empty space information.
     * @param capacity the size of the space.
     */
    public SpaceInfo(long capacity) {
        _free = _total = capacity;
        _used = _precious = _removable = 0;
    }

    public SpaceInfo(PoolCostInfo.PoolSpaceInfo spaceInfo) {
        _total = spaceInfo.getTotalSpace();
        _free = spaceInfo.getFreeSpace();
        _precious = spaceInfo.getPreciousSpace();
        _removable = spaceInfo.getRemovableSpace();
        //_used = spaceInfo.getUsedSpace();
        _used = _total-_free;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SpaceInfo)) {
            return false;
        }
        SpaceInfo info = (SpaceInfo) o;
        return info._total == _total && info._free == _free && info._precious == _precious && info._removable == _removable && info._used == _used;
    }

    @Override
    public int hashCode() {
        return (int)_total+(int)_free+(int)_precious+(int)_removable;
    }

    /**
     * Increase space values by the values specified in otherSpace
     * @param otherSpace the SpaceInfo to add
     */
    public void add(SpaceInfo otherSpace) {
        if (otherSpace == null) {
            return;
        }

        _total += otherSpace._total;
        _free += otherSpace._free;
        _precious += otherSpace._precious;
        _removable += otherSpace._removable;
        _used += otherSpace._used;
    }

    public void setTotal(long totalSpace) {
        _total = totalSpace;
    }

    public void setFree(long freeSpace) {
        _free = freeSpace;
    }

    public void setPrecious(long preciousSpace) {
        _precious = preciousSpace;
    }

    public void setRemovable(long removableSpace) {
        _removable = removableSpace;
    }

    public void setUsed(long usedSpace) {
        _used = usedSpace;
    }

    /**
     * Add additional space unconditionally to the recorded total space.
     * @param extraTotalSpace amount to add
     */
    public void addToTotal(long extraTotalSpace) {
        _total += extraTotalSpace;
    }

    /**
     * Add additional space unconditionally to the recorded free space.
     * @param extraFreeSpace amount to add
     */
    public void addToFree(long extraFreeSpace) {
        _free += extraFreeSpace;
    }

    /**
     * Add additional space unconditionally to the recorded removable space.
     * @param extraRemovableSpace amount to add
     */
    public void addToRemovable(long extraRemovableSpace) {
        _removable += extraRemovableSpace;
    }

    /**
     * Add additional space unconditionally to the recorded precious space
     * @param extraPreciousSpace amount to add
     */
    public void addToPrecious(long extraPreciousSpace) {
        _precious += extraPreciousSpace;
    }

    /**
     * Add additional space unconditionally to the recorded used space
     * @param extraUsedSpace amount to add
     */
    public void addToUsed(long extraUsedSpace) {
        _used += extraUsedSpace;
    }

    /**
     * Update the precious space, applying a delta.
     * If the delta would be impossible, it is capped.
     * The free space is also adjusted.
     * @param change the change to precious space: positive number increases space usage.
     */
    public void updatePrecious(long change) {
        if (change > _free) {
            change = _free;
        }

        if (change < -_precious) {
            change = -_precious;
        }

        _precious += change;
        recalcFree();
    }

    /**
     * Update the space used to store removable data.  The
     * value is updated by applying a delta.  If the delta would
     * be impossible, it is capped.  The free space is also adjusted
     * @param change the change to removable space: positive number increases space usage.
     */
    public void updateRemovable(long change) {
        if (change > _free) {
            change = _free;
        }

        if (change < -_removable) {
            change = -_removable;
        }

        _removable += change;
        recalcFree();
    }

    /**
     * Recalculate the free space based on total capacity, precious
     * removable and pinned.  If the free-space would
     * be negative (due to inconsistent information), it is
     * capped at 0.
     */
    public void recalcFree() {
        _used = _precious + _removable;
        _free = _used < _total ? _total - _used : 0;
    }

    public long getTotal() {
        return _total;
    }

    public long getFree() {
        return _free;
    }

    public long getPrecious() {
        return _precious;
    }

    public long getRemovable() {
        return _removable;
    }

    public long getUsed() {
        return _used;
    }

    /**
     * Add StateUpdate entries to update dCache state that add or update the standard metrics
     * values.  All metrics are added under a common StatePath.  StateValues will be Mortal and
     * will expire after the given duration has elapsed.
     * @param update the StateUpdate to append with the new metrics
     * @param path the point in dCache state that the metrics will be children of.
     * @param duration how long this metric should survive, in seconds.
     */
    public void addMetrics(StateUpdate update, StatePath path, long duration) {
        update.appendUpdate(path.newChild(PATH_ELEMENT_TOTAL), new IntegerStateValue(_total, duration));
        update.appendUpdate(path.newChild(PATH_ELEMENT_FREE), new IntegerStateValue(_free, duration));
        update.appendUpdate(path.newChild(PATH_ELEMENT_PRECIOUS), new IntegerStateValue(_precious, duration));
        update.appendUpdate(path.newChild(PATH_ELEMENT_REMOVABLE), new IntegerStateValue(_removable, duration));
        update.appendUpdate(path.newChild(PATH_ELEMENT_USED), new IntegerStateValue(_used, duration));
    }

    /**
     * Add StateUpdate entries to update dCache state that add or update the standard metrics
     * values.  All metrics are added under a common StatePath.  StateValues will be Ephemeral
     * or Immortal
     * @param update the StateUpdate to append these values.
     * @param path the StatePath under which the StateValues will be added.
     * @param isImmortal if true, the metric will be immortal, otherwise ephemeral.
     */
    public void addMetrics(StateUpdate update, StatePath path, boolean isImmortal) {
        update.appendUpdate(path.newChild(PATH_ELEMENT_TOTAL), new IntegerStateValue(_total, isImmortal));
        update.appendUpdate(path.newChild(PATH_ELEMENT_FREE), new IntegerStateValue(_free, isImmortal));
        update.appendUpdate(path.newChild(PATH_ELEMENT_PRECIOUS), new IntegerStateValue(_precious, isImmortal));
        update.appendUpdate(path.newChild(PATH_ELEMENT_REMOVABLE), new IntegerStateValue(_removable, isImmortal));
        update.appendUpdate(path.newChild(PATH_ELEMENT_USED), new IntegerStateValue(_used, isImmortal));
    }


    /**
     * A string describing this SpaceInfo object.
     */
    @Override
    public String toString() {
        return "[SpaceInfo: total="+_total + ", precious="+_precious + ", removable="+_removable + ", used="+_used + ", free="+_free +"]";
    }
}
