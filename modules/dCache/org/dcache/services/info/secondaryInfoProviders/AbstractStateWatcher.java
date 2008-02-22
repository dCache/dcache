/**
 * 
 */
package org.dcache.services.info.secondaryInfoProviders;

import org.dcache.services.info.base.StatePathPredicate;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateWatcher;

/**
 * Provide secondary information about pools
 * @author Paul Millar <paul.millar@desy.de>
 */
abstract public class AbstractStateWatcher implements StateWatcher {

	protected String _paths[] = {};
	private StatePathPredicate _predicate;
	
	public AbstractStateWatcher() {		
		_predicate = new StatePathPredicate( _paths);
	}

	public StatePathPredicate getPredicate() {
		return _predicate;
	}
		
	abstract public StateUpdate evaluate(StateUpdate updatedState);
}
