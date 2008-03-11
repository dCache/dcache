package org.dcache.services.info.gathers;

import org.apache.log4j.Logger;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StatePath;

public class PoolGroupInfoMsgHandler extends CellMessageHandlerSkel {

	private static Logger _log = Logger.getLogger( PoolGroupInfoMsgHandler.class);

	private static final StatePath POOLGROUPS_PATH = new StatePath( "poolgroups");

	public void process(Object msgPayload, long metricLifetime) {

		_log.info( "processing new poolgroup information");
		
		StateUpdate update = new StateUpdate();
		
		if( !msgPayload.getClass().isArray()) {
			_log.error( "received a message that isn't an array");
			return;
		}
		
		Object array[] = (Object []) msgPayload;
		
		if( array.length != 3) {
			_log.error( "Unexpected array size: "+array.length);
			return;
		}
		
		String poolgroupName = (String) array[0];

		StatePath thisPoolGroupPath = POOLGROUPS_PATH.newChild( poolgroupName);
				
		addItems( update, thisPoolGroupPath.newChild("pools"), (Object []) array[1], metricLifetime);
		addItems( update, thisPoolGroupPath.newChild("links"), (Object []) array[2], metricLifetime);

		applyUpdates( update);
	}
}
