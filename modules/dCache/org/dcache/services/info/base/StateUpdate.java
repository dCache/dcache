/**
 * Copyright 2007 the dCache team
 */
package org.dcache.services.info.base;

import java.util.*;

/**
 * A concrete representation of one or more concurrent changes to
 * dCache's State.
 * 
 * Objects from this class are immutable and read-only.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StateUpdate {

	private Map<StatePath,StateComponent> _updates;
	private Date _expiryDate = null;
	
	public StateUpdate() {
		_updates = new Hashtable<StatePath,StateComponent>();
	}

	
	/**
	 * Provide a "cheap" read-only copy of an StateUpdate.
	 * @param update
	 */
	protected StateUpdate( StateUpdate su) {		
		Map<StatePath,StateComponent> _updates = new Hashtable<StatePath,StateComponent>( su._updates.size());
		_updates.putAll(su._updates);
	}
	
	
	/**
	 * Provide a mechanism whereby we can append additional updates to
	 * this state.
	 * @param path: the StatePath of the new StateValue.
	 * @param value: the new 
	 */
	protected void appendUpdate( StatePath path, StateComponent value) {
		_updates.put( new StatePath(path), value);

		Date expiryDate = value.getExpiryDate();
		
		if( expiryDate != null) {
			if( _expiryDate != null && _expiryDate.after(expiryDate))
				_expiryDate = expiryDate;
		}
	}
	
	/**
	 * Provide the earliest date when any of the new metric values will expire.
	 * 
	 * @return first time data will expire, or null if all metric values are static.
	 */
	public Date getEarliestExpDate() {
		return _expiryDate;
	}
	

	/**
	 * Check whether a watcher is interested in any of the updates present.
	 * @param watcher  the StateWatcher in question
	 * @return true if this StateWatcher StatePredicate matches any updates. 
	 */
	public boolean watcherIsInterested( StateWatcher watcher) {		
		return watcher.getPredicate().matches( _updates.keySet());
	}
	
		
	/**
	 * Add the changes under a specific StateComposite.  This "applies" each of the changes
	 * to the "live" StateComponent state tree.
	 * <p>
	 * The process will attempt to apply all of the StateUpdate's entries in turn.  For each
	 * entry [a (StatePath, StateValue) ordered pair], the StateComposite's add() method is
	 * called.
	 * <p>
	 * Any number of these (StatePath,StateValue) ordered pairs may prove impossible to
	 * satisfy.  If so, then a BadStatePathException will be thrown.
	 * <p>
	 * This method will attempt to satisfy as much of the update as possible: only the first
	 * exception will be reported after the update has completed, any subsequent Exceptions
	 * are ignored. 
	 * @param sc the StateComposite the changes should be added underneath.
	 * @throws BadStatePathException one (or more) StatePath s were impossible to satisfy.
	 */
	protected void updateStateUnderComposite( StateComposite sc) throws BadStatePathException {
		BadStatePathException caughtThis = null;
		
		for( Map.Entry<StatePath, StateComponent> entry : _updates.entrySet()) {
			try {
				sc.add( entry.getKey(), entry.getValue());
			} catch (BadStatePathException e) {
				if( caughtThis == null)
					caughtThis = e;
			}
		}
		
		if( caughtThis != null)
			throw caughtThis;
	}
}
