  // $Id: PoolFlushGainControlMessage.java,v 1.2 2006-01-31 10:59:46 patrick Exp $

package diskCacheV111.vehicles;
//
//  Flush Control
//

public class PoolFlushGainControlMessage extends PoolFlushControlInfoMessage {

    private static final long serialVersionUID = 2092239799456859611L;

    private final long         _holdTimer;

    public PoolFlushGainControlMessage(String poolName,long holdTimer){
         super(poolName);
         _holdTimer = holdTimer ;
         setReplyRequired(true);
    }
    public long getHoldTimer(){
       return _holdTimer ;
    }
}




