package org.dcache.services.info.secondaryInfoProviders;

import org.apache.log4j.Logger;
import org.dcache.services.info.base.State;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateTransition;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.stateInfo.PoolSummaryVisitor;
import org.dcache.services.info.stateInfo.SpaceInfo;

public class PoolsSummaryMaintainer extends AbstractStateWatcher {
	private static Logger _log = Logger.getLogger( PoolsSummaryMaintainer.class);
	private static final String PREDICATE_PATHS[] = { "pools.*.space.*"};
	private static final StatePath SUMMARY_POOLS_SPACE_PATH = StatePath.parsePath("summary.pools.space");


	/**
	 * Provide a list of the paths we're interested in.
	 */
	@Override
	protected String[] getPredicates() {
		return PREDICATE_PATHS;
	}

	
	/**
	 * Something's changed, recalculate the summary information.
	 */
	public void trigger(StateTransition str) {
		
		if( _log.isInfoEnabled())
			_log.info( "Watcher " + this.getClass().getSimpleName() + " triggered");

		//  Visit the new state, extracting summary information
		SpaceInfo info = PoolSummaryVisitor.getDetails( str);

		if( _log.isDebugEnabled())
			_log.debug( "got summary: " + info.toString());
		
		// Add our new information as immortal data
		StateUpdate update = new StateUpdate();
		info.addMetrics( update, SUMMARY_POOLS_SPACE_PATH, true);		
		State.getInstance().updateState( update);
	}

}
