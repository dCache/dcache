package org.dcache.services.info.gathers;

import org.dcache.services.info.InfoProvider;
import org.dcache.services.info.base.*;

public class PoolInfoMsgHandler extends CellMessageHandlerSkel {

	private StatePath _pPath = new StatePath( "pools");

	public void process(Object msgPayload, long metricLifetime) {

		AppendableStateUpdate update = new AppendableStateUpdate();
		
		Object[] array = (Object []) msgPayload;
		
		if( array.length != 6) {
			InfoProvider.getInstance().say("Unexpected array size: "+array.length);
			return;
		}

		Boolean isEnabled = (Boolean) array[3];
		Long heartBeat = (Long) array[4];
		Boolean isReadOnly = (Boolean) array [5];

		StatePath thisPoolPath = _pPath.newChild((String) array[0]); // pool's name
			
		addItems( update, thisPoolPath.newChild("poolgroups"), (Object []) array [1], metricLifetime); 
		addItems( update, thisPoolPath.newChild("links"), (Object []) array [2], metricLifetime); 

		update.appendUpdate( thisPoolPath.newChild("read-only"),
				new BooleanStateValue( isReadOnly.booleanValue(), metricLifetime));

		update.appendUpdate( thisPoolPath.newChild("enabled"),
				new BooleanStateValue( isEnabled.booleanValue(), metricLifetime));

		update.appendUpdate( thisPoolPath.newChild("last-heartbeat"),
				new IntegerStateValue( heartBeat.intValue(), metricLifetime));

		applyUpdates( update);
	}
		
}
