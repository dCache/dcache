package dmg.protocols.snmp ;
import  java.net.InetAddress ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SnmpEvent {
   InetAddress _address ;
   SnmpRequest _request ;
   public SnmpEvent( InetAddress remote , SnmpRequest request ){
      _address = remote ;
      _request = request ;
   }
   public InetAddress getSourceAddress(){ return _address ; }
   public SnmpRequest getSnmpRequest(){ return _request ; }
}
