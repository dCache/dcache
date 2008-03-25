package org.dcache.services.info.gathers;

import org.apache.log4j.Logger;
import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StringStateValue;

import dmg.cells.services.login.LoginBrokerInfo;

/**
 * Parse the reply messages from sending the LoginBroker CellMessages with "ls -binary".
 * These replies are an array of LoginBrokerInfo objects.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class LoginBrokerLsMsgHandler extends CellMessageHandlerSkel {

	private static Logger _log = Logger.getLogger( LoginBrokerLsMsgHandler.class);

	public void process(Object msgPayload, long metricLifetime) {

		StateUpdate update = null;

		Object[] array = (Object []) msgPayload;
		
		StatePath pathToDoors = new StatePath( "doors");
		
		for( int i = 0; i < array.length; i++) {
			
			if( !(array [i] instanceof LoginBrokerInfo)) {
				_log.warn( "Skipping array element that is not LoginBrokerInfo");
				continue;
			}
			
			LoginBrokerInfo info = (LoginBrokerInfo) array[i];
		
			if( update == null)
				update = new StateUpdate();
			
			addDoorInfo( update, pathToDoors, info, metricLifetime);
		}
		
		if( update != null)
			applyUpdates( update);
	}
	
	
	/**
	 * Add additional state-update to record information about a door.
	 * @param update the StateUpdate we are to add metrics to.
	 * @param pathToDoors a StatePath under which we are to add data.
	 * @param info the information about the door.
	 * @param lifetime the duration, in seconds, for this information
	 */
	private void addDoorInfo( StateUpdate update, StatePath pathToDoors, LoginBrokerInfo info, long lifetime) {
		
		StatePath pathToDoor = pathToDoors.newChild( info.getIdentifier());

		conditionalAddString( update, pathToDoor, "host", info.getHost(), lifetime);
		
		StatePath pathToProtocol = pathToDoor.newChild( "protocol");
		
		conditionalAddString( update, pathToProtocol, "engine", info.getProtocolEngine(), lifetime);
		conditionalAddString( update, pathToProtocol, "family", info.getProtocolFamily(), lifetime);

		update.appendUpdate( pathToDoor.newChild("load"),
					new FloatingPointStateValue( info.getLoad(), lifetime));
		update.appendUpdate( pathToDoor.newChild( "port"),
					new IntegerStateValue( info.getPort(), lifetime));
		update.appendUpdate( pathToDoor.newChild( "update-time"),
					new IntegerStateValue( info.getUpdateTime(), lifetime));

		StatePath pathToHosts = pathToDoor.newChild("hosts");
		
		String[] hosts = info.getHosts();
		
		if( hosts != null) {
			for( int i = 0; i < hosts.length; i++) {
				if( hosts[i] != null)
					update.appendUpdate( pathToHosts.newChild(hosts[i]),
								new StateComposite(lifetime));
			}
		}
	}
	
	
	/**
	 * Add a string metric at a specific point in the State tree if the value is not NULL.
	 * @param update the StateUpdate to append with the metric definition 
	 * @param parentPath the path to the parent branch for this metric
	 * @param name the name of the metric
	 * @param value the metric's value, or null if the metric should not be added.
	 * @param storeTime how long, in seconds the metric should be preserved.
	 */
	private void conditionalAddString( StateUpdate update, StatePath parentPath, String name, String value, long storeTime) {
		if( value != null) {
			update.appendUpdate( parentPath.newChild(name),
						new StringStateValue( value, storeTime));
		}
	}

}
