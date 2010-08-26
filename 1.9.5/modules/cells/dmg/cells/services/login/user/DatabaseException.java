// $Id: DatabaseException.java,v 1.2 2005-03-08 15:37:16 patrick Exp $
package dmg.cells.services.login.user  ;

import java.io.* ;
import java.util.* ;

public class DatabaseException  extends Exception {
   static final long serialVersionUID = -4022047931201607786L;
   private int _retCode = 0 ;
   public DatabaseException( int retCode  , String retMessage ){
      super( retMessage ) ;
      _retCode = retCode  ;
   }
   public DatabaseException( String retMessage ){
      super( retMessage ) ;
   }
   public int getCode(){ return _retCode ; }
   public String toString(){
      return super.toString()+ " Code="+_retCode ;
   }

}
