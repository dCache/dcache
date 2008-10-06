package org.dcache.services.info.stateInfo;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.State;
import org.dcache.services.info.base.StateTransition;

/**
 * Scan through dCache state and build a map containing all pools and
 * their corresponding SpaceInfo information.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class PoolSpaceVisitor extends SkeletonListVisitor {
	
	private static Logger _log = Logger.getLogger( PoolSpaceVisitor.class);
	
	private static StatePath POOLS_PATH = new StatePath( "pools");
	
	/**
	 * Obtain a Map between pools and their space information for current dCache state.
	 * @return
	 */
	public static Map <String,SpaceInfo> getDetails() {
		if( _log.isInfoEnabled())
			_log.info( "Gathering current status");

		PoolSpaceVisitor visitor = new PoolSpaceVisitor();
		
		State.getInstance().visitState( visitor, POOLS_PATH);
		
		return visitor._poolgroups;
	}
	
	/**
	 * Obtain a Map between pools and their space information for the dCache state after
	 * the supplied StateTransition has been applied.
	 * @param transition  the StateTransition to consider.
	 * @return Map between a pool's name and that pool's SpaceInfo 
	 */
	public static Map <String,SpaceInfo> getDetails( StateTransition transition) {
		if( _log.isInfoEnabled())
			_log.info( "Gathering status after transition");

		PoolSpaceVisitor visitor = new PoolSpaceVisitor();
		State.getInstance().visitState(transition, visitor, POOLS_PATH);		
		return visitor._poolgroups;	
	}

	private Map <String,SpaceInfo> _poolgroups = new HashMap<String,SpaceInfo>();
	private SpaceInfo _thisPoolSpaceInfo;
	private StatePath _poolSpacePath = null;
	
	public PoolSpaceVisitor() {
		super( POOLS_PATH);
	}
	
	@Override
	protected void newListItem( String itemName) {		
		if( _log.isDebugEnabled())
			_log.debug( "Found pool " + itemName);

		_thisPoolSpaceInfo = new SpaceInfo();
		_poolgroups.put( itemName, _thisPoolSpaceInfo);
		
		_poolSpacePath = getPathToList().newChild(itemName).newChild("space");
	}

	@Override
	public void visitInteger( StatePath path, IntegerStateValue value) {
		if( _poolSpacePath == null || ! _poolSpacePath.isParentOf( path))
			return;
		
		String metricName = path.getLastElement();
		
		if( _log.isDebugEnabled())
			_log.debug( "Found metric " + path.getLastElement() + " = " + value.getValue());

		if( metricName.equals("removable"))
			_thisPoolSpaceInfo.setRemovable( value.getValue());
		else if( metricName.equals("free"))
			_thisPoolSpaceInfo.setFree( value.getValue());
		else if( metricName.equals("total"))
			_thisPoolSpaceInfo.setTotal( value.getValue());
		else if( metricName.equals("precious"))
			_thisPoolSpaceInfo.setPrecious( value.getValue());			
		else if( metricName.equals("used"))
			_thisPoolSpaceInfo.setUsed( value.getValue());			
	}
	
}
