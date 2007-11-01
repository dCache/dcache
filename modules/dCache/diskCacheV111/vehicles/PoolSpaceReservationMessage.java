// $Id: PoolSpaceReservationMessage.java,v 1.2 2004-11-05 12:07:20 tigran Exp $

package diskCacheV111.vehicles;

//
//  Handles pool space reservation
//


public class PoolSpaceReservationMessage extends PoolMessage {
   private long _reservedSpace = 0L ;
   
   private static final long serialVersionUID = -4773583841782997555L;
   
   public PoolSpaceReservationMessage( String poolName ){ 
      super( poolName ) ; 
      setReplyRequired(true);
   }
   public long getReservedSpace(){
      return _reservedSpace ;
   }
   public void setReservedSpace( long reservedSpace ){
      _reservedSpace = reservedSpace ;
   }   
}

