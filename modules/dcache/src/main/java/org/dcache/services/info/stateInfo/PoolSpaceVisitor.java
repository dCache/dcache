package org.dcache.services.info.stateInfo;

import java.util.HashMap;
import java.util.Map;

import org.dcache.services.info.base.StateExhibitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scan through dCache state and build a map containing all pools and
 * their corresponding SpaceInfo information.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class PoolSpaceVisitor extends AbstractPoolSpaceVisitor {

	private static final Logger _log = LoggerFactory.getLogger( PoolSpaceVisitor.class);

	/**
	 * Obtain a Map between pools and their space information for current dCache state.
	 * @return
	 */
	public static Map <String,SpaceInfo> getDetails( StateExhibitor exhibitor) {
		if( _log.isInfoEnabled()) {
                    _log.info("Gathering current status");
                }

		PoolSpaceVisitor visitor = new PoolSpaceVisitor();
		exhibitor.visitState( visitor);
		return visitor._poolgroups;
	}

	private final Map <String,SpaceInfo> _poolgroups = new HashMap<String,SpaceInfo>();

	@Override
	protected void newPool( String poolName, SpaceInfo space) {
		_poolgroups.put( poolName, space);
	}
}
