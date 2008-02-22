package org.dcache.services.info.secondaryInfoProviders;

import org.dcache.services.info.base.AppendableStateUpdate;
import org.dcache.services.info.base.StateUpdate;

public class PoolStateWatcher extends AbstractStateWatcher {

	protected String _paths[] = { "dCache.pools"};
	
	public StateUpdate evaluate(StateUpdate updatedState) {
		AppendableStateUpdate myUpdate = new AppendableStateUpdate();
		
		return myUpdate;
	}
	
}
