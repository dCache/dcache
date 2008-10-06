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
public class PoolSummaryVisitor implements StateVisitor {

	private static Logger _log = Logger.getLogger( PoolSummaryVisitor.class);
	
	private static final StatePath POOLS_PATH = new StatePath( "pools");
	private static final String SPACE_BRANCH_NAME = "space";
	
	private static final String METRIC_NAME_FREE      = "free";
	private static final String METRIC_NAME_PRECIOUS  = "precious";
	private static final String METRIC_NAME_TOTAL     = "total";
	private static final String METRIC_NAME_REMOVABLE = "removable";
	private static final String METRIC_NAME_USED      = "used";
	
	/**
	 * Obtain some summary statistics about all available pools.
	 * @return the aggregated information about the pools.
	 */
	static public SpaceInfo getDetails() {
		if( _log.isDebugEnabled())
			_log.debug( "Gathering summary information.");
		
		PoolSummaryVisitor visitor = new PoolSummaryVisitor();
		State.getInstance().visitState(visitor, POOLS_PATH);		

		return visitor.getSpaceInfo();
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
		
		return visitor.getSpaceInfo();		
	}
	
	
	private SpaceInfo _info = new SpaceInfo();
	private boolean _inSpaceBranch;
		
	public void visitCompositePostSkipDescend(StatePath path, Map<String, String> metadata) {}
	public void visitCompositePreSkipDescend(StatePath path, Map<String, String> metadata) {}
	public void visitCompositePreLastDescend(StatePath path, Map<String, String> metadata) {}

	public void visitCompositePreDescend(StatePath path,
			Map<String, String> metadata) {		
		_inSpaceBranch = path.getLastElement().equals( SPACE_BRANCH_NAME);
	}

	public void visitCompositePostDescend(StatePath path,
			Map<String, String> metadata) {
		_inSpaceBranch = false;
	}


	public void visitBoolean(StatePath path, BooleanStateValue value) {}
	public void visitString(StatePath path, StringStateValue value) {}
	public void visitFloatingPoint(StatePath path, FloatingPointStateValue value) {}

	public void visitInteger(StatePath path, IntegerStateValue value) {
		if( ! _inSpaceBranch)
			return;

		if( path.getLastElement().equals(METRIC_NAME_REMOVABLE)) {
			_info.addToRemovable( value.getValue());
		} else if( path.getLastElement().equals(METRIC_NAME_FREE)) {
			_info.addToFree( value.getValue());
		} else if( path.getLastElement().equals(METRIC_NAME_TOTAL)) {
			_info.addToTotal( value.getValue());
		} else if( path.getLastElement().equals(METRIC_NAME_PRECIOUS)) {
			_info.addToPrecious( value.getValue());
		} else if( path.getLastElement().equals(METRIC_NAME_USED)) {
			_info.addToUsed( value.getValue());
		}
	}
	
	
	/**
	 * Obtain the summary information object.
	 */
	protected SpaceInfo getSpaceInfo() {
		return _info;
	}
	
}
