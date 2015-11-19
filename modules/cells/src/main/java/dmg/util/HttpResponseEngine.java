// $Id: HttpResponseEngine.java,v 1.2 2001-09-17 15:08:32 cvs Exp $

package dmg.util ;

/**
  * It is assumed, that the engine has one of the
  * following constructors :
  *
  *   - <init>( CellNucleus nucleus , String [] args ) ;
  *   - <init>( String [] args ) ;
  *   - <init>()
  */
public interface HttpResponseEngine {

    void queryUrl(HttpRequest request)
           throws HttpException ;

    /**
     * This method is called from the http thread when the engine
     * should start activity.
     */
    void startup();

    /**
     * Method called to indicate that the http engine should shutdown
     */
    void shutdown();
}
