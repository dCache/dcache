package org.dcache.services.info.gathers;

import org.dcache.services.info.InfoProvider;
import org.dcache.services.info.base.*;

/**
 * Process an incoming message from PoolManager about a specific UnitGroup.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class UGroupInfoMsgHandler extends CellMessageHandlerSkel {

	private StatePath _uGPath = new StatePath( "unitgroups");
	
	public void process(Object msgPayload, long metricLifetime) {
		
		Object array[];
		
		AppendableStateUpdate update = new AppendableStateUpdate();
		
		array = (Object []) msgPayload;
		
		if( array.length != 3) {
			InfoProvider.getInstance().say("Unexpected array size: "+array.length);
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
