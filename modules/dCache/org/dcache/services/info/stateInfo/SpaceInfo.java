package org.dcache.services.info.stateInfo;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import diskCacheV111.pools.PoolCostInfo;

/**
 * Information about some (generic) space.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class SpaceInfo {
	private long _total;
	private long _free;
	private long _precious;
	private long _removable;
	
	public SpaceInfo( long totalSpace, long freeSpace, long preciousSpace, long removableSpace) {
		_total = totalSpace;
		_free = freeSpace;
		_precious = preciousSpace;
		_removable = removableSpace;
	}
	
	public SpaceInfo() {
		_total = _free = _precious = _removable = 0;
	}
	
	
	/**
	 * Create an initially-empty space information.
	 * @param capacity the size of the space.
	 */
	public SpaceInfo( long capacity) {
		_free = _total = capacity; 
		_precious = _removable = 0;
	}
	
	public SpaceInfo( PoolCostInfo.PoolSpaceInfo spaceInfo) {
		_total = spaceInfo.getTotalSpace();
		_free = spaceInfo.getFreeSpace();
		_precious = spaceInfo.getPreciousSpace();
		_removable = spaceInfo.getRemovableSpace();
	}
	
	public boolean equals( Object o) {
		if( !( o instanceof SpaceInfo))
			return false;
		SpaceInfo info = (SpaceInfo) o;
		return info._total == _total && info._free == _free && info._precious == _precious && info._removable == _removable;
	}
	
	public int hashCode() {
		return (int)_total+(int)_free+(int)_precious+(int)_removable;
	}
	
	/**
	 * Increase space values by the values specified in otherSpace
	 * @param otherSpace the SpaceInfo to add
	 */
	public void add( SpaceInfo otherSpace) {
		if( otherSpace == null)
			return;
		
		_total += otherSpace._total;
		_free += otherSpace._free;
		_precious += otherSpace._precious;
		_removable += otherSpace._removable;
	}
	
	public void setTotal( long totalSpace) {
		_total = totalSpace;
	}

	public void setFree( long freeSpace) {
		_free = freeSpace;
	}

	public void setPrecious( long preciousSpace) {
		_precious = preciousSpace;
	}
	
	public void setRemovable( long removableSpace) {
		_removable = removableSpace;
	}
	

	/**
	 * Add additional space unconditionally to the recorded total space.
	 * @param extraTotalSpace amount to add
	 */
	public void addToTotal( long extraTotalSpace) {
		_total += extraTotalSpace;
	}

	/**
	 * Add additional space unconditionally to the recorded free space.
	 * @param extraFreeSpace amount to add
	 */
	public void addToFree( long extraFreeSpace) {
		_free += extraFreeSpace;
	}

	/**
	 * Add additional space unconditionally to the recorded removable space.
	 * @param extraRemovableSpace amount to add
	 */
	public void addToRemovable( long extraRemovableSpace) {
		_removable += extraRemovableSpace;
	}
	
	/**
	 * Add additional space unconditionally to the recorded precious space
	 * @param extraPreciousSpace amount to add
	 */
	public void addToPrecious( long extraPreciousSpace) {
		_precious += extraPreciousSpace;
	}
	
	
	
	/**
	 * Update the precious space, applying a delta.
	 * If the delta would be impossible, it is capped.
	 * The free space is also adjusted. 
	 * @param change the change to precious space: +ve number increases space usage.
	 */
	public void updatePrecious( long change) {
		long maxInc = _total - (_precious + _removable);
		
		if( change > maxInc)
			change = maxInc;
		
		if( change < -_precious)
			change = -_precious;
		
		_precious += change;
		recalcFree();			
	}
	
	/**
	 * Update the space used to store removable data.  The
	 * value is updated by applying a delta.  If the delta would
	 * be impossible, it is capped.  The free space is also adjusted
	 * @param change the change to removable space: +ve number increases space usage.
	 */
	public void updateRemovable( long change) {
		long maxInc = _total - (_precious + _removable);
		
		if( change > maxInc)
			change = maxInc;
		
		if( change < -_removable)
			change = -_removable;
		
		_removable += change;
		recalcFree();		
	}
	
	/**
	 * Recalculate the free space based on total capacity
	 * and precious and removable.  If the free-space would
	 * be negative (due to inconsistent information), it is
	 * capped at 0.
	 */
	public void recalcFree() {
		long used = _precious + _removable;
		_free = used < _total ? _total - used : 0; 
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
		
	/**
	 * Add StateUpdate entries to update dCache state that add or update the standard metrics
	 * values.  All metrics are added under a common StatePath.  StateValues will be Mortal and
	 * will expire after the given duration has elapsed.
	 * @param update the StateUpdate to append with the new metrics
	 * @param path the point in dCache state that the metrics will be children of.
	 * @param duration how long this metric should survive, in seconds.
	 */
	public void addMetrics( StateUpdate update, StatePath path, long duration) {
		update.appendUpdate(path.newChild("total"), new IntegerStateValue( _total, duration));
		update.appendUpdate(path.newChild("free"), new IntegerStateValue( _free, duration));
		update.appendUpdate(path.newChild("precious"), new IntegerStateValue( _precious, duration));
		update.appendUpdate(path.newChild("removable"), new IntegerStateValue( _removable, duration));
	}
	
	/**
	 * Add StateUpdate entries to update dCache state that add or update the standard metrics
	 * values.  All metrics are added under a common StatePath.  StateValues will be Ephemeral
	 * or Immortal
	 * @param update the StateUpdate to append these values.  
	 * @param path the StatePath under which the StateValues will be added.
	 * @param isImmortal if true, the metric will be immortal, otherwise ephemeral.
	 */
	public void addMetrics( StateUpdate update, StatePath path, boolean isImmortal) {
		update.appendUpdate(path.newChild("total"), new IntegerStateValue( _total, isImmortal));
		update.appendUpdate(path.newChild("free"), new IntegerStateValue( _free, isImmortal));
		update.appendUpdate(path.newChild("precious"), new IntegerStateValue( _precious, isImmortal));
		update.appendUpdate(path.newChild("removable"), new IntegerStateValue( _removable, isImmortal));		
	}
	
	
	/**
	 * A string describing this SpaceInfo object.
	 */
	public String toString() {
		return "[SpaceInfo: total="+_total+", precious=" + _precious + ", removable="+ _removable + ", free="+_free+"]";
	}
}
