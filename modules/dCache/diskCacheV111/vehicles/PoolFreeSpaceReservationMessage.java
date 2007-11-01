// $Id: PoolFreeSpaceReservationMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

//
//  Handles pool space reservation
//


public class PoolFreeSpaceReservationMessage extends PoolSpaceReservationMessage {
    private long _cancelSpace = 0L ;
    
    private static final long serialVersionUID = -3762715776818134798L;
    
    public PoolFreeSpaceReservationMessage( String poolName , long cancelSpace ){
       super( poolName ) ;
       _cancelSpace = cancelSpace ;
    }
    public long getFreeSpaceReservationSize(){
       return _cancelSpace ;
    }
}

