package org.dcache.services.info.gathers;

import org.apache.log4j.Logger;
import org.dcache.services.info.base.*;

/**
 * Process an incoming message from PoolManager about a specific UnitGroup.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class UGroupInfoMsgHandler extends CellMessageHandlerSkel {

	private static Logger _log = Logger.getLogger( UGroupInfoMsgHandler.class);
	
	private StatePath _uGPath = new StatePath( "unitgroups");
	
	public void process(Object msgPayload, long metricLifetime) {
		
		Object array[];
		
		StateUpdate update = new StateUpdate();
		
		array = (Object []) msgPayload;
		
		if( array.length != 3) {
			_log.error( "Unexpected array size: "+array.length);
			return;
		}
		
		/**
		 * array[0] = group name
		 * array[1] = unit list
		 * array[2] = link list
		 */
		
		StatePath thisUGroupPath = _uGPath.newChild((String) array[0]);

		addItems( update, thisUGroupPath.newChild("units"), (Object []) array [1], metricLifetime);
		addItems( update, thisUGroupPath.newChild("links"), (Object []) array [2], metricLifetime);
		
		applyUpdates( update);
	}

}
