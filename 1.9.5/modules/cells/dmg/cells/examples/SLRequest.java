package dmg.cells.examples ;

import java.io.* ;

public class SLRequest implements Serializable {


  private static int __serial = 0 ;
  
  private int    _serial ;
  private String _destination ;
  private Object _payload ;
  
  public SLRequest( String destination , Object payload ){
     _serial      = _getNextSerial() ;
     _destination = destination ;
     _payload     = payload ;
  
  }
  public Object getPayload(){ return _payload ; }
  public String getDestination(){ return _destination ; }
  public void   setPayload( Object obj ){ _payload = obj ; }
  
  private int _getNextSerial(){ 
     synchronized( this.getClass() ){
        return __serial++ ; 
     }
  }
  public String toString(){ return "(s="+_serial+") "+_payload ; }



}
