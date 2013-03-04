// $Id: PoolStatusChangedMessage.java,v 1.4 2004-11-05 12:07:20 tigran Exp $

package diskCacheV111.vehicles;

import diskCacheV111.pools.PoolV2Mode;

public class PoolStatusChangedMessage extends PoolMessage {

   public static final int UP      = 1 ;
   public static final int DOWN    = 2 ;
   public static final int RESTART = 3 ;

   public static final String [] statusString = { "UNKNOWN" , "UP" , "DOWN" , "RESTART" } ;

   private int _state;

   private PoolV2Mode _poolMode;
   private String     _detailMessage;
   private int        _detailCode;

   private static final long serialVersionUID = 7246217707829001604L;

   public PoolStatusChangedMessage( String poolName , int poolStatus ){
      super(poolName);
      if( ( poolStatus < 1 )  || ( poolStatus > 3 ) ) {
          throw new
                  IllegalArgumentException("Not a valid pool status");
      }
      _state = poolStatus ;
   }
   public int getPoolState(){ return _state ; }
   public String getPoolStatus(){ return statusString[_state] ; }
   public String toString(){
       return "PoolName="+getPoolName()+
              ";status="+statusString[_state]+
              ( _poolMode == null ? "" : ( ";mode="+ _poolMode.toString() )  ) +
              ";code=("+_detailCode+
              ( _detailMessage==null ? ")" : (","+_detailMessage+")") ) ;
   }
   public void setDetail( int code , String message ){
      _detailCode    = code ;
      _detailMessage = message ;
   }
   public int getDetailCode(){ return _detailCode ; }
   public String getDetailMessage(){ return _detailMessage ; }
   public void setPoolMode( PoolV2Mode mode ){ _poolMode = mode ; }
   public PoolV2Mode getPoolMode(){ return _poolMode ; }

}
