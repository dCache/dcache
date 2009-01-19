package org.dcache.services.infoCollector;
 
import java.text.SimpleDateFormat ;
import java.net.*;
import java.util.*;
import java.util.Date;
import dmg.cells.services.login.*;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.vehicles.CostModulePoolInfoTable;

/**
 * Information Base class.<br>
 * This class encapsulates the information that the <code>InfoCollector</code> 
 * class collects.<br>
 * Here the properties are package friendly so InfoCollector can access
 * directly to them, while the classes that are extern to this package can
 * only read them throught the get methods.
 * 
 * @see InfoCollector
 **/
public class InfoBase {
	
	private static SimpleDateFormat __dateFormat = new SimpleDateFormat("MM.dd HH:mm:ss"); 

	/** Array containg information about protocols **/
	LoginBrokerInfo[] protocols = null;
	
	/** Array containg information about srm protocols **/
	LoginBrokerInfo[] srm = null;
	
	/** Hashtable containing space informations for each pool**/
	CostModulePoolInfoTable pools = null;
	
	/** Hashtable containing the array of the pools in a poolgroup.
	 *  The pool group name is the key.
	 **/
	Hashtable pgroup_pools = new Hashtable();
	
	/** Hashtable containing the array of the links in a poolgroup.
	 *  The pool group name is the key.
	 **/
	Hashtable pgroup_links = new Hashtable();
	
	
	/**
	 * Global Space Calculation.<br>
	 * This method iterates on the <code>CostModulePoolInfoTable</code> values, and
	 * for each value, a PoolCostInfo object, then for every pool, accumulates 
	 * the free space and the total space. 
	 * @return [0]:total space, [1]:free space
	 */
	public long[] getGlobalSpace(){
		
		long total = 0;
		long free = 0;
		
		if(pools == null) return new long[]{0,0};
		Collection coll = pools.poolInfos(); 
		Iterator it = coll.iterator();
		PoolCostInfo infopool;
		while(it.hasNext()){
			infopool = (PoolCostInfo)it.next();
			total = total + infopool.getSpaceInfo().getTotalSpace();
			free = free + infopool.getSpaceInfo().getFreeSpace();
		}
		return new long[]{total,free};
	}

	/**
	 * Pool Group Space Calculation.<br>
	 * This method iterates on the array of the pool members in a specified
	 * (in the argument passed) pool group. 
	 * For each pool in the group, the free space and the total space (given 
	 * from the InfoPoolTable) are accumulated and finally returned. 
	 * @return [0]:total space, [1]:free space
	 * @param pgroup Pool Group name
	 */
	public long[] getGroupSpace( String pgroup ){
		long total = 0;
		long free =0;
		
		Object[] members = (Object[])pgroup_pools.get(pgroup);
		if(members == null) return new long[]{0,0};
		PoolCostInfo infopool;
		for(int i=0; i<members.length; i++){
			infopool = (PoolCostInfo)pools.getPoolCostInfoByName(members[i].toString());
			if(infopool == null) continue; //
			total = total + infopool.getSpaceInfo().getTotalSpace();
			free = free + infopool.getSpaceInfo().getFreeSpace();
		}
		return new long[]{total,free};
	}
	
	
	/**
	 * This method produces a String that summarize all information
	 * encapsulated in this class. This string provides a way to show
	 * the information from the console throught the <code>InfoCollector</code> 
	 * Cell
	 * @see InfoCollector
	 * @return Report String
	 **/
	public String show(){
	    long[] space;

	    StringBuffer sb = new StringBuffer(1024) ;
	    String s;
	    
	    sb.append("--- INFORMATION SYSTEM REPORT - ") ;
	    if( pools == null ){
	       sb.append("\n   Not enough information yet\n");
	       return sb.toString();
	    }
	    sb.append(__dateFormat.format(new Date(pools.getTimestamp()))).
	       append(" ---\n");

	    space = getGlobalSpace();
	    sb.append("   Total Space: ").append(space[0]).append(" bytes\n");
	    sb.append("   Free Space : ").append(space[1]).append(" bytes\n");

	    sb.append("   Pool Groups: \n");
	    Object[] pgroups = pgroup_pools.keySet().toArray();
	    for(int i=0; i<pgroups.length; i++){
		    sb.append("      [ ").append(pgroups[i].toString()).append(" ]\n");
		    space = getGroupSpace(pgroups[i].toString());
		    sb.append("         Total Space: ").append(space[0]).append(" bytes\n");
		    sb.append("         Free Space : ").append(space[1]).append(" bytes\n");
	    }
	    sb.append("   Protocols: \n");
	    if(protocols==null) sb.append("      none\n"); 
	    for(int i=0; protocols!=null && i<protocols.length; i++){
	    	sb.append("      [ ").append(protocols[i].getIdentifier()).append(" ]\n");
	    	sb.append("         Type: ").append(protocols[i].getProtocolFamily());
	    	sb.append("\t\t ver.").append(protocols[i].getProtocolVersion()).append('\n');
	    	try{
	    		s = InetAddress.getByName(protocols[i].getHost()).getHostName() + ':' + protocols[i].getPort();
	    	}catch (UnknownHostException e){ s = protocols[i].getHost() + ':' + protocols[i].getPort(); }
	    	sb.append("         Endpoint: ").append(s).append('\n');
	    }
	    sb.append("   SRM Protocols: \n");
	    if(srm==null) sb.append("      none\n");
	    for(int i=0; srm!=null && i<srm.length; i++){
	    	sb.append("      [ ").append(srm[i].getIdentifier()).append(" ]\n");
	    	sb.append("         Type: ").append(srm[i].getProtocolFamily());
	    	sb.append("\t\t ver.").append(srm[i].getProtocolVersion()).append('\n');
	    	try{
	    		s = InetAddress.getByName(srm[i].getHost()).getHostName() + ':' + srm[i].getPort();
	    	}catch (UnknownHostException e){ s = srm[i].getHost() + ':' + srm[i].getPort(); }
	    	sb.append("         Endpoint: ").append(s).append('\n');
	    }
	    
	    return sb.toString();
	}
}
