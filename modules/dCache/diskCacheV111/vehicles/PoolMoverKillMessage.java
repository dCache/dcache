// $Id: PoolMoverKillMessage.java,v 1.1 2004-11-08 23:01:47 timur Exp $

package diskCacheV111.vehicles;

import  diskCacheV111.util.PnfsId ;

public class PoolMoverKillMessage extends PoolMessage {

    static final long serialVersionUID = -8654307136745044047L;

    public int  moverId;
    public PoolMoverKillMessage(String poolName, int moverId){
	super(poolName);
        this.moverId = moverId ;
        setReplyRequired(true);
    }

    public int getMoverId(){ return moverId ; }
}


