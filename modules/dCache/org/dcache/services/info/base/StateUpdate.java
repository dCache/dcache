/**
 * Copyright 2007 the dCache team
 */
package org.dcache.services.info.base;

import java.util.*;

import org.apache.log4j.Logger;

/**
 * The StateUpdate is a simple collection containing zero or more proposed concurrent
 * changes to dCache's State.  Each change consists of a StatePath and a StateComponent.
 * The StatePath indicates where the update is to take place and the StateComponent is the
 * new value to be stored.
 * <p>
 * StateUpdate objects are immutable, providing read-only access to proposed set of changes. 
 * 
 * @author Paul Millar <paul.millar@desy.de>
 * @see StateTransition
 */
public class StateUpdate {

	private static Logger _log = Logger.getLogger(StateUpdate.class);
	
	/**
	 * A single update to dCache
	 */
	private class StateUpdateInstance {
		StatePath _path;
		StateComponent _newValue;
		
		StateUpdateInstance( StatePath path, StateComponent newValue) {
			_path = path;
			_newValue = newValue;
		}
	}

	private List<StateUpdateInstance> _updates;
	
	public StateUpdate() {
		_updates = new Vector<StateUpdateInstance>();
	}

	
	/**
	 * Count the number of metrics that are to be updated.
	 * @return the number of metric updates contained within this StateUpdate.
	 */
	public int count() {
		return _updates.size();
	}
	
	/**
	 * Provide a mechanism whereby we can append additional updates to
	 * this state.
	 * @param path: the StatePath of the new StateValue.
	 * @param value: the new 
	 */
	public void appendUpdate( StatePath path, StateComponent value) {
		_updates.add( new StateUpdateInstance( path, value));
	}

	
	/**
	 * Go through each of the proposed updates recorded and update the StateTransition object.
	 * @param top the top-most StateComposite within dCache state
	 * @param transition the StateTransition to update.
	 * @throws BadStatePathException
	 */
	protected void updateTransition( StateComposite top, StateTransition transition) throws BadStatePathException {
		BadStatePathException caughtThis = null;
		
		for( StateUpdateInstance update : _updates) {
			
			if( _log.isDebugEnabled())
				_log.debug( "preparing transition to alter "+update._path.toString());
			
			try {
				top.buildTransition( null, update._path, update._newValue, transition);
			} catch (BadStatePathException e) {
				if( caughtThis == null)
					caughtThis = e;
			}
		}
		
		if( caughtThis != null)
			throw caughtThis;		
	}
}
