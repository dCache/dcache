package org.dcache.services.info.secondaryInfoProviders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateTransition;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.stateInfo.PoolSummaryVisitor;
import org.dcache.services.info.stateInfo.SpaceInfo;

public class PoolsSummaryMaintainer extends AbstractStateWatcher {
	private static Logger _log = LoggerFactory.getLogger( PoolsSummaryMaintainer.class);
	private static final String PREDICATE_PATHS[] = { "pools.*.space.*"};
	private static final StatePath SUMMARY_POOLS_SPACE_PATH = StatePath.parsePath("summary.pools.space");

	private final StateExhibitor _exhibitor;

	/**
	 * Create a new secondary information provider that uses the provided StateExhibitor
	 * to query the current and future dCache state.
	 * @param exhibitor
	 */
	public PoolsSummaryMaintainer( StateExhibitor exhibitor) {
		_exhibitor = exhibitor;
	}

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
	@Override
    public void trigger(StateTransition str, StateUpdate update) {
		super.trigger( str, update);
		
		if( _log.isInfoEnabled())
			_log.info( "Watcher " + this.getClass().getSimpleName() + " triggered");

		//  Visit the new state, extracting summary information
		SpaceInfo info = PoolSummaryVisitor.getDetails( _exhibitor, str);

		if( _log.isDebugEnabled())
			_log.debug( "got summary: " + info.toString());
		
		// Add our new information as immortal data
		info.addMetrics( update, SUMMARY_POOLS_SPACE_PATH, true);		
	}

}
