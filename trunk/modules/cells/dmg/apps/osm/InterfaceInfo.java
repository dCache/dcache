package dmg.apps.osm ;

class InterfaceInfo implements java.io.Serializable, Cloneable {
   private String _name = null ;
   long   _time = 0 ;
   int    _mtu  = 0 ;
   long   _maxSpeed = 0 , _transPackets = 0 , _recvPackets = 0 ;
   public InterfaceInfo( String name ){ 
      _name = name ; 
   }
   public long   getTime(){ return _time ; }
   public String getName(){ return _name ; } 
   public int    getMtu(){ return _mtu ; }
   public long   getMaxSpeed(){ return _maxSpeed ; }
   public long   getBytesTransmitted(){ return _transPackets * (long)_mtu ; }
   public long   getBytesReceived(){ return _recvPackets * (long)_mtu ; }
   public long   getPacketsTransmitted(){ return _transPackets ; }
   public long   getPacketsReceived(){ return _recvPackets  ; }
   public void   substract( InterfaceInfo info ){
      if( ! info.getName().equals(_name) )
        throw new
        IllegalArgumentException( "Not the same interface name" ) ;
      _time  -= info.getTime() ;
      _transPackets -= info.getPacketsTransmitted()  ;
      _recvPackets  -= info.getPacketsReceived() ;
   }
   public Object clone(){
      try{
         InterfaceInfo info = (InterfaceInfo)super.clone();
         info._name = _name ;
         info._time = _time ;
         info._mtu  = _mtu ;
         info._maxSpeed = _maxSpeed ;
         info._transPackets    = _transPackets ;
         info._recvPackets     = _recvPackets ;
         return info ;
       }catch( CloneNotSupportedException e){
         return null ;
       } 
   }
   public String toString(){

      return _name+";mtu="+_mtu+";m="+
             _maxSpeed+";t="+
             _transPackets+"("+(_transPackets*(long)_mtu)+");r="+
             _recvPackets+"("+(_recvPackets*(long)_mtu)+");x="+_time;
   }
}
