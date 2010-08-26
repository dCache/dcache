 // $Id: PoolFlushControlMessage.java,v 1.2 2006-01-31 10:59:46 patrick Exp $

package diskCacheV111.vehicles;
//
//  Flush Control
//

public class PoolFlushControlMessage extends PoolMessage {
    
    private static final long serialVersionUID = 2092239799703859611L;
    
    public PoolFlushControlMessage(String poolName){
         super(poolName);
    }
}



