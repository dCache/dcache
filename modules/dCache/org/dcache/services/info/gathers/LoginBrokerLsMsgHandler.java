package org.dcache.services.info.gathers;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
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

		addHostInfo( update, pathToDoor, "host", info.getHost(), lifetime);
		
		StatePath pathToProtocol = pathToDoor.newChild( "protocol");
		
		conditionalAddString( update, pathToProtocol, "engine", info.getProtocolEngine(), lifetime);
		conditionalAddString( update, pathToProtocol, "family", info.getProtocolFamily(), lifetime);

		update.appendUpdate( pathToDoor.newChild("load"),
					new FloatingPointStateValue( info.getLoad(), lifetime));
		update.appendUpdate( pathToDoor.newChild( "port"),
					new IntegerStateValue( info.getPort(), lifetime));
		
		update.appendUpdate( pathToDoor.newChild( "cell"),
					new StringStateValue( info.getCellName(), lifetime));

		update.appendUpdate( pathToDoor.newChild( "domain"),
				new StringStateValue( info.getDomainName(), lifetime));

		update.appendUpdate( pathToDoor.newChild( "update-time"),
					new IntegerStateValue( info.getUpdateTime(), lifetime));

		StatePath pathToHosts = pathToDoor.newChild("hosts");
		
		String[] hosts = info.getHosts();
		
		if( hosts != null) {
			for( int i = 0; i < hosts.length; i++) {
				if( hosts[i] != null)
					addHostInfo( update, pathToHosts, hosts[i], hosts[i], lifetime);
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
	

	/**
	 * Add a standardised amount of information about a host.  This is in the form:
	 * <pre>
	 *     [parentPath]
	 *       |
	 *       |
	 *       +--[ id ] (branch)
	 *       |   |
	 *       |   +-- "name" (string metric: the host's name, as presented by the door)
	 *       |   +-- "FQDN" (string metric: the host's FQDN)
	 *       |   +--[ "IP" ] (branch)
	 *       |       |
	 *       |       +-- "address" (string metric: the host's IP address)
	 *       |       +-- "type"    (string metric: "IPv4", "IPv6" or "unknown")
	 *       |
	 * </pre> 
	 * @param update The StateUpdate to append the new metrics.
	 * @param parentPath the path that the id branch will be added.  
	 * @param id the name of the branch containing this host's information
	 * @param name something that identifies the host (e.g., IP address or simple name).
	 * @param lifetime how long the created metrics should last.
	 */
	private void addHostInfo( StateUpdate update, StatePath parentPath, String id, String name, long lifetime) {
		
		StatePath pathToHostBranch = parentPath.newChild(id);
		InetAddress address;
		
		try {
			address = InetAddress.getByName(name);
		} catch( UnknownHostException e) {
			// Simply add the name and quit.
			update.appendUpdate( pathToHostBranch.newChild( "name"), new StringStateValue( name, lifetime));
			return;
		}
		
		/**
		 *  Add information under the main branch.
		 */

		update.appendUpdate( pathToHostBranch.newChild( "name"), new StringStateValue( name, lifetime));
		update.appendUpdate( pathToHostBranch.newChild( "FQDN"), new StringStateValue( address.getCanonicalHostName(), lifetime));
				
		/**
		 *   Add the IP address.
		 */
		
		StatePath pathToIP = pathToHostBranch.newChild( "IP");
		String type = "unknown";
		
		if( address instanceof Inet4Address) {
			type = "IPv4";
		} else if( address instanceof Inet6Address) {
			type = "IPv6";
		}

		update.appendUpdate( pathToIP.newChild( "address"), new StringStateValue( address.getHostAddress(), lifetime));
		update.appendUpdate( pathToIP.newChild( "type"), new StringStateValue( type, lifetime));		
	}

}
