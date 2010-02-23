package org.dcache.services.info.stateInfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.services.info.base.BooleanStateValue;
import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateTransition;
import org.dcache.services.info.base.StateVisitor;
import org.dcache.services.info.base.StringStateValue;

/**
 * Scan through a dCache state tree, building a list of poolgroup-to-pools associations.
 * @author Paul Millar <paul.millar@desy.de>
 */
public class PoolgroupToPoolsVisitor implements StateVisitor {

	private static Logger _log = LoggerFactory.getLogger( PoolgroupToPoolsVisitor.class);

	private static final StatePath POOLGROUPS_PATH = new StatePath( "poolgroups");

	/**
	 * Obtain a Map between a poolgroup and the pools that are currently members of this poolgroup.
	 * @return
	 */
	public static Map <String,Set<String>> getDetails( StateExhibitor exhibitor) {
		if( _log.isInfoEnabled())
			_log.info( "Gathering current status");

		PoolgroupToPoolsVisitor visitor = new PoolgroupToPoolsVisitor();
		exhibitor.visitState(visitor, POOLGROUPS_PATH);
		return visitor._poolgroups;
	}

	/**
	 * Obtain a Map between a poolgroup and the pools that will be members of this poolgroup after
	 * the given StateTransition
	 * @param transition the StateTransition to consider
	 * @return
	 */
	public static Map <String,Set<String>> getDetails( StateExhibitor exhibitor, StateTransition transition) {
		if( _log.isInfoEnabled())
			_log.info( "Gathering status after transition");

		PoolgroupToPoolsVisitor visitor = new PoolgroupToPoolsVisitor();
		exhibitor.visitState(transition, visitor, POOLGROUPS_PATH);
		return visitor._poolgroups;
	}


	Map <String,Set<String>> _poolgroups = new HashMap<String,Set<String>>();
	Set<String> _currentPoolgroupPools;
	StatePath _poolMembershipPath;

	public void visitCompositePreDescend( StatePath path, Map<String,String> metadata) {
		if( _log.isDebugEnabled())
			_log.debug( "Examining "+path);

		// If something like poolgroups.<some poolgroup>
		if( POOLGROUPS_PATH.isParentOf( path)) {
			if( _log.isDebugEnabled())
				_log.debug( "Found poolgroup "+path.getLastElement());

			_currentPoolgroupPools = new HashSet<String>();
			_poolgroups.put( path.getLastElement(), _currentPoolgroupPools);

			_poolMembershipPath = path.newChild("pools");
		}

		// If something like poolgroups.<some poolgroup>.pools.<some pool>
		if( _poolMembershipPath != null && _poolMembershipPath.isParentOf(path)) {
			if( _log.isDebugEnabled())
				_log.debug( "Found pool "+path.getLastElement());

			_currentPoolgroupPools.add( path.getLastElement());
		}
	}

	public void visitCompositePostDescend( StatePath path, Map<String,String> metadata) {}
	public void visitCompositePreSkipDescend( StatePath path, Map<String,String> metadata) {}
	public void visitCompositePostSkipDescend( StatePath path, Map<String,String> metadata) {}
	public void visitString( StatePath path, StringStateValue value) {}
	public void visitBoolean( StatePath path, BooleanStateValue value) {}
	public void visitInteger( StatePath path, IntegerStateValue value) {}
	public void visitFloatingPoint( StatePath path, FloatingPointStateValue value) {}
}


