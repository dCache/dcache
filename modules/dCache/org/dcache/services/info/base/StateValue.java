/**
 * Copyright 2007 the dCache team
 */
package org.dcache.services.info.base;

import java.util.Date;

/**
 * A base-type for all metric values within the dCache state.  The different metrics types
 * all extend this base Class.
 * <p> 
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
abstract public class StateValue implements StateComponent {
	
	private static final boolean DUMMY_ISEPHEMERAL_VALUE = false;
	
	/** The granularity of expiryTime, in milliseconds */ 
	private static final int _granularity = 500;
	private static final int _millisecondsInSecond = 1000;
	
	private final Date _expiryTime;
	private final boolean _isEphemeral;
	
	/**
	 *  Create a StateValue that is either Immortal or Ephemeral
	 *  @param isImmortal true if the StateValue is immortal
	 */
	protected StateValue( boolean isImmortal) {
		_expiryTime = null;
		_isEphemeral = !isImmortal;
	}
	
	/**
	 * Create a StateValue that will expire some point in
	 * the future.  This allows "soft state" registration of
	 * information.
	 * @param duration the length of time, in seconds, this information will be held.
	 */
	protected StateValue( long duration) {
		
		long tim = System.currentTimeMillis() + duration * _millisecondsInSecond;

		/**
		 *  round up to nearest _granularity milliseconds.  This is to attempt to allow metrics
		 *  to be purged at the same time.
		 */
		tim = (1 + tim / _granularity) * _granularity;
		
		_expiryTime = new Date(tim);
		_isEphemeral = DUMMY_ISEPHEMERAL_VALUE;
	}
	

	/**
	 * Make the actual data/time this value will expire available.
	 * @return when this StateValue will expire
	 */
	public Date getExpiryDate() {
		return _expiryTime != null ? new Date( _expiryTime.getTime()) : null;
	}
	
	/**
	 * Discover whether the expiry time has elapsed.  For static StateValues
	 * (those without an expiry date), this will always return false.
	 * @return True if this value is scheduled to expiry and that time has elapsed,
	 * false otherwise.
	 */
	public boolean hasExpired() {
		if( _expiryTime == null)
			return false;
		
		Date now = new Date();
		return now.after(_expiryTime);
	}
	
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
	
	
	/**
	 * Sub-classes of StateValue all ignore the transition when being visited: the StateComposite takes
	 * care of all effects from processing the transition.
	 */
	public void acceptVisitor( StateTransition transition, StatePath path, StatePath start, StateVisitor visitor) {
		acceptVisitor( path, start, visitor);
	}

	
	public void applyTransition( StatePath ourPath, StateTransition transition) {
		// Simply do nothing. All activity takes place in StateComposite.
	}
	
	
	public void buildTransition( StatePath ourPath, StatePath childPath, StateComponent newChild, StateTransition transition) throws MetricStatePathException {
		// If we're here, the user has specified a path with a metric in it.
		throw new MetricStatePathException( ourPath.toString());
	}
		
	public void buildRemovalTransition( StatePath ourPath, StateTransition transition, boolean forced) {
		// Simply do nothing, all activity takes place in StateComposites
	}
	
	public boolean predicateHasBeenTriggered( StatePath ourPath, StatePathPredicate predicate, StateTransition transition) throws MetricStatePathException {
		throw new MetricStatePathException( ourPath.toString());
	}
	
	public boolean isMortal() {
		return _expiryTime != null;
	}

	public boolean isEphemeral() {
		return _expiryTime == null && _isEphemeral;
	}
	
	public boolean isImmortal() {
		return _expiryTime == null && !_isEphemeral;
	}

	public Date getEarliestChildExpiryDate() {
		return null; // we never have children.
	}

}
