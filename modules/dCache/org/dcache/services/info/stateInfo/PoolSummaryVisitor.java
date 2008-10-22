package org.dcache.services.info.stateInfo;

import java.util.Map;

import org.apache.log4j.Logger;
import org.dcache.services.info.base.BooleanStateValue;
import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.State;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateTransition;
import org.dcache.services.info.base.StateVisitor;
import org.dcache.services.info.base.StringStateValue;

/**
 * Scan through the current list of pools and calculate aggregated statistics.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class PoolSummaryVisitor extends AbstractPoolSpaceVisitor {

	private static Logger _log = Logger.getLogger( PoolSummaryVisitor.class);
		
	/**
	 * Obtain some summary statistics about all available pools.
	 * @return the aggregated information about the pools.
	 */
	static public SpaceInfo getDetails() {
		if( _log.isDebugEnabled())
			_log.debug( "Gathering summary information.");
		
		PoolSummaryVisitor visitor = new PoolSummaryVisitor();
		State.getInstance().visitState(visitor, POOLS_PATH);		

		return visitor._summaryInfo;
	}
	
	
	/**
	 * Obtain the summary statistics about all available pools after a transition
	 * has taken place.
	 * @param transition the transition that is to be processed.
	 * @return the new pool summary information after the transition.
	 */
	static public SpaceInfo getDetails( StateTransition transition) {
		if( _log.isDebugEnabled())
			_log.debug( "Gathering summary information.");
		
		PoolSummaryVisitor visitor = new PoolSummaryVisitor();
		State.getInstance().visitState( transition, visitor, POOLS_PATH);
		
		return visitor._summaryInfo;		
	}
		
	private SpaceInfo _summaryInfo = new SpaceInfo();
		
	@Override
	protected void newPool( String poolName, SpaceInfo space) {		
		_summaryInfo.add( space);
	}
		
}
