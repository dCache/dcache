package dmg.cells.examples ;

import dmg.util.* ;
import dmg.cells.nucleus.* ;
import java.io.* ;

public class HttpEngineExample implements HttpResponseEngine {
   private CellNucleus _nucleus = null ;

   public HttpEngineExample( CellNucleus nucleus ){
       _nucleus = nucleus ;
   }
   public void queryUrl( dmg.util.HttpRequest request  ){}

   public void queryUrl( PrintWriter pw , String [] urlItems )
          throws Exception {

       if( urlItems.length == 4 )throw new Exception( "Wrong number of ar.");
       printDummyHttpHeader( pw ) ;
       pw.println( "<h1>Hallo</h1>" ) ;
       pw.println( "<pre>" ) ;
       for( int i = 0 ; i < urlItems.length ; i++ )
          pw.println( urlItems[i] ) ;
       pw.println( "</body></html>" ) ;
   }
   private void printDummyHttpHeader( PrintWriter pw ){
      pw.println( "HTTP/1.0 200 Document follows" );
      pw.println( "MIME-Version: 1.0" ) ;
      pw.println( "Server: Java Cell Server" ) ;
      pw.println( "Date: Thursday, 02-Jul-97 09:29:49 GMT" ) ;
      pw.println( "Content-Type: text/html" ) ;
      pw.println( "Content-Length: 0" ) ;
      pw.println( "Last-Modified: Thursday, 03-Jul-97 10:01:00 GMT\n" ) ;
   }
    
}
