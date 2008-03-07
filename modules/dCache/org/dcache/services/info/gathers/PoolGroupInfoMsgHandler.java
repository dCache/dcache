package org.dcache.services.info.gathers;

import org.apache.log4j.Logger;
import org.dcache.services.info.InfoProvider;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StatePath;

public class PoolGroupInfoMsgHandler extends CellMessageHandlerSkel {

	private static Logger _log = Logger.getLogger( PoolGroupInfoMsgHandler.class);

	private StatePath _pgPath = new StatePath( "poolgroups");

	public void process(Object msgPayload, long metricLifetime) {

		Object array[];
		
		StateUpdate update = new StateUpdate();
		
		array = (Object []) msgPayload;
		
		if( array.length != 3) {
			_log.error( "Unexpected array size: "+array.length);
			return;
		}

		StatePath thisPoolGroupPath = _pgPath.newChild( (String) array[0]); // poolgroup's name
				
		addItems( update, thisPoolGroupPath.newChild("pools"), (Object []) array[1], metricLifetime);
		addItems( update, thisPoolGroupPath.newChild("links"), (Object []) array[2], metricLifetime);

		applyUpdates( update);
	}
}
