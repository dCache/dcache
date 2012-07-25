// $Id: ContextPictureEngine.java,v 1.1 2004-08-05 20:51:07 patrick Exp $Cg

package  diskCacheV111.services.web ;

import   dmg.cells.nucleus.* ;
import   dmg.cells.network.* ;
import   dmg.util.* ;

import java.util.* ;
import java.text.* ;
import java.io.* ;
import java.net.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
public interface HttpRequest {

    public HashMap getRequestAttributes() ;
    public OutputStream getOutputStream() ;
    public PrintWriter  getPrintWriter() ;
    public String []    getRequestTokens() ;
    public int          getRequestTokenOffset() ;
    public boolean      isDirectory() ;
    public void         printHttpHeader( int size ) ;
    public boolean isAuthenticated();
    public String getUserName();
    public String getPassword();
    public void    setContentType( String type ) ;
}
*/

public class ContextPictureEngine implements HttpResponseEngine {

   private final static Logger _log =
       LoggerFactory.getLogger(ContextPictureEngine.class);

   private CellNucleus _nucleus;
   private Hashtable   _domainHash = new Hashtable() ;
   private String   [] _args;
   private DateFormat  _dataFormat = new SimpleDateFormat("MMM d, hh.mm.ss" ) ;

   public ContextPictureEngine( CellNucleus nucleus , String [] args ){
       _nucleus = nucleus ;
       _args    = args ;
   }

   @Override
   public void startup()
   {
       // No background activity to start
   }

   @Override
   public void shutdown()
   {
       // No background activity to shutdown
   }

   @Override
   public void queryUrl( HttpRequest request )
          throws HttpException {

      String [] tokens = request.getRequestTokens() ;
      int       offset = request.getRequestTokenOffset() ;

      _log.info("Offset : "+offset);
      for( int i =0 ; i < tokens.length ;i++ ){
         _log.info(""+i+" -> "+tokens[i]);
      }
      if( tokens.length < 2 ) {
          throw new
                  HttpException(404, "Illegal Request");
      }

      String contextName = tokens[1] ;

      Object obj = _nucleus.getDomainContext().get( contextName ) ;

      if( ! ( obj instanceof byte [] ) ) {
          throw new
                  HttpException(404, "Not a picture");
      }

      byte [] picture = (byte []) obj ;

      request.setContentType("image/png") ;
      request.printHttpHeader(picture.length) ;
      OutputStream stream = request.getOutputStream() ;
      try{
         stream.write( picture , 0 , picture.length ) ;
         stream.flush() ;
      }catch(IOException ee ){
         throw new HttpException(203,ee.getMessage());
      }
      return ;
   }
}
