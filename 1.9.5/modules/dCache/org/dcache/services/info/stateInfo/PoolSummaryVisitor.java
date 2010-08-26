package org.dcache.services.info.stateInfo;

import org.apache.log4j.Logger;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StateTransition;

/**
 * Scan through the current list of pools and calculate aggregated statistics.
 */
public class PoolSummaryVisitor extends AbstractPoolSpaceVisitor {

	private static Logger _log = Logger.getLogger( PoolSummaryVisitor.class);
		
	/**
	 * Obtain some summary statistics about all available pools.
	 * @return the aggregated information about the pools.
	 */
	static public SpaceInfo getDetails( StateExhibitor exhibitor) {
		if( _log.isDebugEnabled())
			_log.debug( "Gathering summary information.");
		
		PoolSummaryVisitor visitor = new PoolSummaryVisitor();
		exhibitor.visitState(visitor, POOLS_PATH);		

		return visitor._summaryInfo;
	}
	
	
	/**
	 * Obtain the summary statistics about all available pools after a transition
	 * has taken place.
	 * @param transition the transition that is to be processed.
	 * @return the new pool summary information after the transition.
	 */
	static public SpaceInfo getDetails( StateExhibitor exhibitor, StateTransition transition) {
		if( _log.isDebugEnabled())
			_log.debug( "Gathering summary information.");
		
		PoolSummaryVisitor visitor = new PoolSummaryVisitor();
		exhibitor.visitState( transition, visitor, POOLS_PATH);
		
		return visitor._summaryInfo;		
	}
		
	private SpaceInfo _summaryInfo = new SpaceInfo();
		
	@Override
	protected void newPool( String poolName, SpaceInfo space) {		
		_summaryInfo.add( space);
	}
		
}
