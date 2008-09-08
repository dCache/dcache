// $Id: HttpResponseEngine.java,v 1.2 2001-09-17 15:08:32 cvs Exp $

package dmg.util ;
import java.io.* ;
import java.util.* ;
/**
  * It is assumed, that the engine has one of the
  * following constructors :
  *
  *   - <init>( CellNucleus nucleus , String [] args ) ;
  *   - <init>( String [] args ) ;
  *   - <init>() 
  */
public interface HttpResponseEngine {

    public void queryUrl( HttpRequest request  ) 
           throws HttpException ;
}
