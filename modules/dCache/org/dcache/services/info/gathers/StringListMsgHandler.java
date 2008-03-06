package org.dcache.services.info.gathers;

import org.dcache.services.info.base.*;

public class StringListMsgHandler extends CellMessageHandlerSkel {
	
	private StatePath _path;
	
	public StringListMsgHandler( String path) {
		_path = new StatePath(path);
	}
	
	public void process( Object msgPayload, long metricLifetime) {		
		Object array[];
		
		StateUpdate update = new StateUpdate();
		
		array = (Object []) msgPayload;
		
		if( array.length == 0)
			return;
		
		for( int i = 0; i < array.length; i++) {
			String listItem = (String) array[i];
			update.appendUpdate( _path.newChild(listItem), new StateComposite(metricLifetime));
		}
	
		applyUpdates( update);
	}

}
