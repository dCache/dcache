// $Id: PoolQuerySpaceReservationMessage.java,v 1.2 2004-11-05 12:07:20 tigran Exp $

package diskCacheV111.vehicles;

//
//  Handles pool space reservation
//


public class PoolQuerySpaceReservationMessage extends PoolSpaceReservationMessage {
    
    private static final long serialVersionUID = 6518916194328197919L;
    
    public PoolQuerySpaceReservationMessage( String poolName ){
       super( poolName ) ;
    }
}

