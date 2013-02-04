// $Id: PoolUpdateCacheStatisticsMessage.java,v 1.2 2004-11-05 12:07:20 tigran Exp $

package diskCacheV111.vehicles;


public class PoolUpdateCacheStatisticsMessage extends PoolMessage {

    private static final long serialVersionUID = 2023627678070843155L;

    // this is sent from the pnfs manager to the pools
    public PoolUpdateCacheStatisticsMessage(String poolName){
	super(poolName);
	setReplyRequired(false);
    }
}
