/**
 * Copyright 2007 the dCache team
 */
package org.dcache.services.info.base;

import java.util.Date;

/**
 * A base-type for a metric within the dCache state.  All metrics of different type
 * must extend this base class.
 * <p>
 * Object from this Class (and subclasses) are immutable.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
abstract public class StateValue implements StateComponent {
	
	/** The granularity of expiryTime, in milliseconds */ 
	private static final int _granularity = 500;
	private static final int _millisecondsInSecond = 1000;
	
	Date _creationTime;
	Date _expiryTime;
	
	/**
	 * Create a StateValue that will never expire.
	 */
	protected StateValue() {
		_creationTime = new Date();
		_expiryTime = null;
	}
	
	/**
	 * Create a StateValue that will expire some point in
	 * the future.  This allows "soft state" registration of
	 * information.
	 * @param duration the length of time, in seconds, this information will be held.
	 */
	protected StateValue( long duration) {
		_creationTime = new Date();
		
		long tim = System.currentTimeMillis() + duration * _millisecondsInSecond;

		/**
		 *  round up to nearest _granularity milliseconds.  This is to attempt to allow metrics
		 *  to be purged at the same time.
		 */
		tim = (1 + tim / _granularity) * _granularity;
		
		_expiryTime = new Date(tim);
	}
	

	/**
	 * Discover when this data was added into dCache's State.
	 * @return the Date this object was created.
	 */
	public Date getCreationTime() {
		return _creationTime;
	}
	
	
	/**
	 * Make the actual data/time this value will expire available.
	 * @return when this StateValue will expire
	 */
	public Date getExpiryDate() {
		return _expiryTime;
	}
	
	/**
	 * Discover whether the expiry time has elapsed.  For static StateValues
	 * (those without an expiry date), this will always return false.
	 * @return True if this value is scheduled to expiry and that time has elapsed,
	 * false otherwise.
	 */
	public boolean hasExpired() {
		Date now = new Date();
		return _expiryTime != null ? now.after(_expiryTime) : false;
	}
	
	/** Provide mechanism for generating read-only copies */
	public abstract StateValue clone();
	
	/** Provide a generic name for subclasses of StateValue */
	public abstract String getTypeName();
	
	/** Sub-classes must provide a leaf-node's visitor support */
	public abstract void acceptVisitor( StatePath path, StateVisitor visitor);
	
	/**
	 * A simple wrapper to check for non-null start values.
	 */
	public void acceptVisitor( StatePath path, StatePath start, StateVisitor visitor) {
		if( start != null)
			return;
		
		/** Call leaf-node specific visitor method. */
		acceptVisitor( path, visitor);
	}


	/** Trying to add a new metric under a metric isn't going to work! */
	public void add( StatePath path, StateComponent value)  throws BadStatePathException {
		throw new MetricStatePathException( path.toString()); //TODO: is this correct exception?
	}
	
		
	protected void removeExpiredChildren() {
		// Simply do nothing.
	}
}
