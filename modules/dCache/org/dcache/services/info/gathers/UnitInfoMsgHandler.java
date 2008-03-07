package org.dcache.services.info.gathers;

import org.apache.log4j.Logger;
import org.dcache.services.info.InfoProvider;
import org.dcache.services.info.base.*;

/**
 * Process incoming Object array (*shudder*) and update state.
 *  
 * @author Paul Millar <paul.millar@desy.de>
 */
public class UnitInfoMsgHandler extends CellMessageHandlerSkel {

	private static Logger _log = Logger.getLogger( UnitInfoMsgHandler.class);

	private StatePath _uPath = new StatePath( "units");
	
	public void process(Object msgPayload, long metricLifetime) {
		Object array[];
		
		StateUpdate update = new StateUpdate();
		
		array = (Object []) msgPayload;
		
		if( array.length != 3) {
			_log.error( "Unexpected array size: "+array.length);
			return;
		}

		/**
		 * array[0] = name
		 * array[1] = type
		 * array[2] = list of unitgroups.
		 */
		
		StatePath thisUnitPath = _uPath.newChild((String) array[0]);

		update.appendUpdate( thisUnitPath.newChild("type"),
					new StringStateValue( (String) array[1], metricLifetime));
		
		addItems( update, thisUnitPath.newChild("unitgroups"), (Object []) array [2], metricLifetime);

		applyUpdates( update);
	}

}
