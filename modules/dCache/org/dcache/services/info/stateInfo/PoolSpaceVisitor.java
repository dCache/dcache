package org.dcache.services.info.stateInfo;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dcache.services.info.base.BooleanStateValue;
import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateVisitor;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.base.State;
import org.dcache.services.info.base.StateTransition;

/**
 * Scan through dCache state and build a map containing all pools and their corresponding SpaceInfo
 * information.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class PoolSpaceVisitor implements StateVisitor {
	
	private static Logger _log = Logger.getLogger( PoolSpaceVisitor.class);

	
	private static StatePath _poolsPath = new StatePath( "pools");
	
	/**
	 * Obtain a Map between pools and their space information for current dCache state.
	 * @return
	 */
	public static Map <String,SpaceInfo> getDetails() {
		if( _log.isInfoEnabled())
			_log.info( "Gathering current status");

		PoolSpaceVisitor visitor = new PoolSpaceVisitor();
		
		State.getInstance().visitState(visitor, _poolsPath);		
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
		State.getInstance().visitState(transition, visitor, _poolsPath);		
		return visitor._poolgroups;		
	}

	private Map <String,SpaceInfo> _poolgroups = new HashMap<String,SpaceInfo>();
	private SpaceInfo _thisPoolSpaceInfo;
	private StatePath _poolSpacePath = null;

	public void visitCompositePreDescend( StatePath path, Map<String,String> metadata) {			
		// If something like pools.<some pool>
		if( _poolsPath.isParentOf( path)) {
			
			if( _log.isDebugEnabled())
				_log.debug( "Found pool " + path.getLastElement());

			_thisPoolSpaceInfo = new SpaceInfo();
			_poolgroups.put( path.getLastElement(), _thisPoolSpaceInfo);
			_poolSpacePath = path.newChild("space");
		}			
	}
	public void visitInteger( StatePath path, IntegerStateValue value) {
		if( _poolSpacePath == null || ! _poolSpacePath.isParentOf( path))
			return;
		
		if( _log.isDebugEnabled())
			_log.debug( "Found metric " + path.getLastElement() + " = " + value.getValue());

		if( path.getLastElement().equals("removable"))
			_thisPoolSpaceInfo.setRemovable( value.getValue());
		else if( path.getLastElement().equals("free"))
			_thisPoolSpaceInfo.setFree( value.getValue());
		else if( path.getLastElement().equals("total"))
			_thisPoolSpaceInfo.setTotal( value.getValue());
		else if( path.getLastElement().equals("precious"))
			_thisPoolSpaceInfo.setPrecious( value.getValue());			
	}
	
	public void visitCompositePreLastDescend( StatePath path, Map<String,String> metadata) {}		
	public void visitCompositePostDescend( StatePath path, Map<String,String> metadata) {}
	public void visitCompositePreSkipDescend( StatePath path, Map<String,String> metadata) {}
	public void visitCompositePostSkipDescend( StatePath path, Map<String,String> metadata) {}
	public void visitString( StatePath path, StringStateValue value) {}
	public void visitBoolean( StatePath path, BooleanStateValue value) {}
	public void visitFloatingPoint( StatePath path, FloatingPointStateValue value) {}		
}
