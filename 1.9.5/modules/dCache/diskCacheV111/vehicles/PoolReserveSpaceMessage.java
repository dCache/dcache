// $Id: PoolReserveSpaceMessage.java,v 1.2 2004-11-05 12:07:20 tigran Exp $

package diskCacheV111.vehicles;

//
//  Handles pool space reservation
//


public class PoolReserveSpaceMessage extends PoolSpaceReservationMessage {
    private long _reservationRequest = 0L ;
    
    private static final long serialVersionUID = 8340015328166381504L;
    
    public PoolReserveSpaceMessage( String poolName , long requestedSpace ){
       super( poolName ) ;
       _reservationRequest = requestedSpace ;
    }
    public long getSpaceReservationSize(){
       return _reservationRequest ;
    }
}

