package org.dcache.services.infoCollector;

import java.net.*;
import java.text.*;

/**
 * <code>SchemaFiller</code> for <code>GlueSchemaV1_2</code> implementation
 * of the <code>Schema</code>.<br>
 */
public class GlueSchemaV1_2Filler extends SchemaFiller{
	
	
	public GlueSchemaV1_2Filler(){
		schema = new GlueSchemaV1_2();
	}
	
	public void fillSchema(){
		
			GlueSchemaV1_2 schema = (GlueSchemaV1_2)this.schema;		
			long space[];
			DecimalFormat f = new DecimalFormat("#");

			space = inf.getGlobalSpace();
			//			 Size in Gbyte without decimal part
			schema.se.sizeTotal = f.format(space[0]/1E9); 
			schema.se.sizeFree = f.format(space[1]/1E9);
				
		
			Object[] pgroups = inf.pgroup_pools.keySet().toArray();
			schema.se.storage_area = new GlueSchemaV1_2.StorageArea[pgroups.length];
			for(int i=0; i<pgroups.length; i++){
				
				GlueSchemaV1_2.StorageArea sa = new GlueSchemaV1_2.StorageArea();
				schema.se.storage_area[i] = sa;
				
				sa.localID = pgroups[i].toString();  
				space = inf.getGroupSpace(pgroups[i].toString());
				//				 Size in Kbyte without decimal part
				sa.state_availableSpace = f.format(space[1]/1E3);
				sa.state_usedSpace = f.format((space[0]-space[1])/1E3);
			}
			
			
			if(inf.protocols != null){
				schema.se.access_protocol  = new GlueSchemaV1_2.AccessProtocol[inf.protocols.length];
				for(int i=0; i<inf.protocols.length; i++){
					
					GlueSchemaV1_2.AccessProtocol ap = new GlueSchemaV1_2.AccessProtocol();
					schema.se.access_protocol[i] = ap;
					
					ap.localID = inf.protocols[i].getIdentifier();
					ap.type = inf.protocols[i].getProtocolFamily();
					ap.version = inf.protocols[i].getProtocolVersion();
					ap.port =  inf.protocols[i].getPort();
					String host;
					try{ host = InetAddress.getByName(inf.protocols[i].getHost()).getHostName(); 
			    	} catch (UnknownHostException e){ host = inf.protocols[i].getHost(); }
			    	ap.endPoint = ap.type + "://" + host + ':' + inf.protocols[i].getPort();
				}
			}
			
			if(inf.srm != null){
				schema.se.control_protocol  = new GlueSchemaV1_2.ControlProtocol[inf.srm.length];
				for(int i=0; i<inf.srm.length; i++){
					
					GlueSchemaV1_2.ControlProtocol cp = new GlueSchemaV1_2.ControlProtocol();
					schema.se.control_protocol[i] = cp;
					
					cp.localID = inf.srm[i].getIdentifier();
					cp.type = inf.srm[i].getProtocolFamily();
					cp.version = inf.srm[i].getProtocolVersion();
					String host;
					try{ host = InetAddress.getByName(inf.srm[i].getHost()).getHostName(); 
			    	} catch (UnknownHostException e){ host = inf.srm[i].getHost(); }
			    	cp.endPoint = "httpg://" + host + ':' + inf.srm[i].getPort() + "/srm/managerv" + (cp.version.toCharArray())[0];
				}
			}
	}
		

}
