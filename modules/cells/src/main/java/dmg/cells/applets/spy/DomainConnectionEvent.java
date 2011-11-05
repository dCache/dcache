package dmg.cells.applets.spy ;

import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;


public class DomainConnectionEvent {

   private Object _source = null ;
   private String _message = "" ;
   
   public DomainConnectionEvent( Object source ){
      _source = source ;
   }
   public DomainConnectionEvent( Object source , String message ){
      _source = source ;
      _message = message ;
   }
   public Object getSource(){ return _source ; }
   public String getMessage(){ return _message ; }

}
