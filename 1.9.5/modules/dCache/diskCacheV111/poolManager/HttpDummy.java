// $Id: HttpDummy.java,v 1.1 2001-09-17 15:37:03 cvs Exp $
package diskCacheV111.poolManager ;

import dmg.util.* ;
import dmg.cells.nucleus.* ;
import java.io.* ;
import java.util.* ;

public class HttpDummy implements HttpResponseEngine {
   private CellNucleus _nucleus = null ;

   public HttpDummy( CellNucleus nucleus , String [] args ){
       _nucleus = nucleus ;
   }
   private void printWml( HttpRequest request ){
      request.setContentType( "text/vnd.wap.wml" ) ;
      request.printHttpHeader(0) ;
      PrintWriter pw = request.getPrintWriter() ;
      pw.println( "<?xml version=\"1.0\"?>" ) ;
      pw.println( "<!DOCTYPE wml PUBLIC \"-//WAPFORUM//DTD WML 1.1/EN\"" ) ;
      pw.println( "    \"http://www.wapforum.org/DTD/wml_1.1.xml\">" ) ;
      pw.println( "<wml>" ) ;
      pw.println( "<head>" ) ;
      pw.println( "<meta http-equiv=\"Cache-Control\" content=\"max-age=10\"/>" ) ;
      pw.println( "</head>" ) ;
      pw.println( "<card id=\"card1\" title=\"card1\" newcontext=\"true\">" ) ;
      pw.println( "<p align=\"center\"><b>Please<br/>Wait<br/></b></p>" ) ;
      pw.println( "</card>" ) ;
      pw.println( "</wml>" ) ;                                                                                            
   }
   public void queryUrl( HttpRequest request )
          throws HttpException {

       PrintWriter pw     = request.getPrintWriter() ;
       String [] urlItems = request.getRequestTokens() ;
       int       offset   = request.getRequestTokenOffset() ;
       
       if( urlItems[offset].equals("otto") )
          throw new
          HttpException( 404 , "Not Found" ) ; 
          
       if( urlItems[offset].equals("karl")  &&
           ! request.isAuthenticated()             )
          throw new
          HttpBasicAuthenticationException( "myCells" ) ; 
          
       
       if( urlItems[offset].equals("wml" ) ){
          printWml( request ) ;
          return ;
       }
       request.printHttpHeader(0) ;
       pw.println( "<html>");
       pw.println( "<head><title>Cell Server xxx</title></head>");
       pw.println( "<body bgcolor=yellow>") ;
       
       try{
            pw.println("<h1>Dicke Trude</h1>");
            pw.println("<h2><a href=\"next\">Next</a></h2>");
            pw.println("<pre>");
            for( int i = offset ; i < urlItems.length ; i++ ){
              pw.println( " ["+i+"] "+urlItems[i] ) ;
            }
            pw.println("</pre>");
       }finally{
          pw.println( "</body>" ) ;
          pw.println( "</html>" ) ;
       }
   }
   private void printNotFoundHttpHeader( PrintWriter pw ){
      pw.println( "HTTP/1.1 404 Not Found" );
      pw.println( "Server: Java Cell Server "+this.getClass().getName() ) ;
      pw.println( "Date: Thursday, 02-Jul-97 09:29:49 GMT" ) ;
      pw.println( "Connection: close" ) ;
      pw.println( "Content-Type: text/html\n" ) ;
      pw.println( "<html><head><title>Not Found</title><head>");
      pw.println( "<body bgcolor=red><h1>Not found</body></html>");
   }
   private void printDummyHttpHeader( PrintWriter pw ){
      pw.println( "HTTP/1.0 200 Document follows" );
//      pw.println( "MIME-Version: 1.0" ) ;
      pw.println( "Server: Java Cell Server "+this.getClass().getName() ) ;
      pw.println( "Date: Thursday, 02-Jul-97 09:29:49 GMT" ) ;
//      pw.println( "Content-Length: 10000" ) ;
//      pw.println( "Last-Modified: Thursday, 03-Jul-97 10:01:00 GMT\n" ) ;
      pw.println( "Connection: close" ) ;
      pw.println( "Location: http://localhost:2488/otto/" ) ;
      pw.println( "Content-Type: text/html\n" ) ;
   }
}
