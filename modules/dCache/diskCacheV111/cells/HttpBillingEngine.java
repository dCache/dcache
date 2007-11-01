// $Id: HttpBillingEngine.java,v 1.10 2005-03-31 17:54:11 patrick Exp $
package diskCacheV111.cells ;

import dmg.util.* ;
import dmg.cells.nucleus.* ;
import java.io.* ;
import java.util.* ;

public class HttpBillingEngine implements HttpResponseEngine {
   private CellNucleus _nucleus = null ;
   private String _headerBackground = "#115259" ;
   private String _headerForeground = "white" ;
   private String [] _fieldBackground = { "#efefef" , "#bebebe"  } ;
   private String _fieldForeground = "black" ;
 
   public HttpBillingEngine( CellNucleus nucleus , String [] args ){
       _nucleus = nucleus ;
   }
   private void printTotalStatistics( PrintWriter pw , Object [] x )
           throws HttpException {

       pw.println( "<center><h2><font color=blue>Total Request Overview</font></h2><center>");
       pw.println( "<center><table border=1 cellspacing=0 cellpadding=5>") ;
       pw.println( "<tr>") ;
       pw.print( "<th bgcolor=\""+_headerBackground+"\" width=\"40%\">") ;
       pw.print( "<font color=\""+_headerForeground+"\">Action</font></th>");
       pw.print( "<th bgcolor=\""+_headerBackground+"\" width=\"30%\">") ;
       pw.print( "<font color=\""+_headerForeground+"\">Total Request Count</font></th>");
       pw.print( "<th bgcolor=\""+_headerBackground+"\" width=\"30%\">") ;
       pw.print( "<font color=\""+_headerForeground+"\">Request Failed</font></th>");
       pw.println( "</tr>") ;
       for( int i = 0 ; i < x.length ; i++ ){
           Object [] y   = (Object [])x[i] ;
           int    [] z   = (int [])y[1] ;
           String    key = (String)y[0] ;
           pw.print( "<tr><th bgcolor=\""+_headerBackground+"\">" ) ;
	   pw.print( "<font color=\""+_headerForeground+"\">"+key+"</font></th>");
           pw.print( "<td bgcolor=\""+_fieldBackground[0]+"\" align=center>" ) ;
	   pw.print( "<font color=\""+_fieldForeground+"\">"+z[0]+"</font></td>") ;
           pw.print( "<td bgcolor=\""+_fieldBackground[0]+"\" align=center>" ) ;
	   pw.print( "<font color=\""+_fieldForeground+"\">"+z[1]+"</font></td>") ;
	   pw.println("</tr>") ;
       }
       pw.println("</tr></table></center>") ;
           
   }
   private void printPoolStatistics( PrintWriter pw , HashMap map , String pool )
           throws HttpException {
          
      boolean perPool = pool != null ; 
      pw.println( "<center><h2><font color=blue>Pool Statistics") ;
      if( perPool )pw.println(" of <font color=red>"+pool+"</font>" ) ;
      pw.println( "</font></h2><center>");
      pw.println( "<p><center><table border=1 cellspacing=0 cellpadding=5>") ;
      pw.print( "<tr>" ) ;
      pw.print( "<th bgcolor=\""+_headerBackground+"\" width=\"20%\">") ;
      if( perPool )
         pw.println( "<font color=\""+_headerForeground+"\">StorageClass</font></th>");
      else
         pw.println( "<font color=\""+_headerForeground+"\">Pool</font></th>");
      pw.print( "<th bgcolor=\""+_headerBackground+"\" width=\"20%\">") ;
      pw.println( "<font color=\""+_headerForeground+"\">Mover Transfers</font></th>");
      pw.print( "<th bgcolor=\""+_headerBackground+"\" width=\"20%\">") ;
      pw.println( "<font color=\""+_headerForeground+"\">Restores from HSM</font></th>");
      pw.print( "<th bgcolor=\""+_headerBackground+"\" width=\"20%\">") ;
      pw.println( "<font color=\""+_headerForeground+"\">Stores to HSM</font></th>");
      pw.print( "<th bgcolor=\""+_headerBackground+"\" width=\"20%\">") ;
      pw.println( "<font color=\""+_headerForeground+"\">Total Errors</font></th>");
      
      long []  total = new long[4] ;
      Iterator i = new TreeMap( map ).entrySet().iterator() ;    
      for( int row = 0 ;  i.hasNext() ; row++ ){
      
         Map.Entry entry    = (Map.Entry)i.next() ;
	 String    poolName = (String)entry.getKey() ;
	 long []   counters = (long [])entry.getValue() ;
         pw.print( "<tr>" ) ;
	 pw.print( "<th bgcolor=\""+_headerBackground+"\">") ;
         if( ! perPool )pw.print( "<a href=pool/"+poolName+">");
	 pw.print( "<font color=\""+_headerForeground+"\">") ;
         pw.print(poolName) ;
         pw.print("</font>") ;
         if( ! perPool )pw.print("</a>");
         pw.println("</th>");
	 for( int n = 0 ; n < 4 ; n++ ){
            pw.print( "<td bgcolor=\""+_fieldBackground[row%_fieldBackground.length]+"\" align=center>" ) ;
	    pw.print( "<font color=\""+_fieldForeground+"\">"+counters[n] ) ;
	    pw.println("</font></td>") ;
	    total[n] += counters[n] ;
         }
	 pw.println( "</tr>");
      }
      //
      // total count
      //
      pw.print( "<tr>" ) ;
      pw.print( "<th bgcolor=\"#0000ff\"><font color=white>") ;
      pw.print("Total") ;
      pw.print("</font>") ;
      pw.println("</th>");
      for( int n = 0 ; n < 4 ; n++ ){
         pw.print( "<td bgcolor=\"#0000ff\" align=center>" ) ;
	 pw.print( "<font color=white>"+total[n]+"</font></td>") ;
      }
      pw.println( "</tr>");
      
      pw.println( "</table></center>" ) ;
   }
   private void printPerPoolStatisticsPage( HttpRequest request , PrintWriter pw , String pool)
           throws HttpException {
           
       CellMessage result = null ;
       try{
	  HashMap map = null ;
	  try{
             result = _nucleus.sendAndWait(
                        	  new CellMessage( new CellPath("billing") ,
                                                   "get pool statistics "+pool ) ,
                                            5000 ) ;
	     if( result == null )
               throw new
               HttpException( 500 , "Request Timed Out" ) ;

	     map = (HashMap)result.getMessageObject() ;
	  }catch(Exception ee ){
	     pw.print("<p><h3>This 'billingCell' doesn't support : " ) ;
	     pw.println(" 'get pool statistics &lt;poolName&gt;'</h3>");
	     pw.print("<pre>"+ee+"</pre>") ;
	  }
          
          sayHallo( request , pw ) ;
          printPoolStatistics( pw , map , pool) ;
          
       }catch(Exception ii){
          throw new
          HttpException( 500 , "Problem : "+ii.getMessage() ) ;
       }finally{
//          pw.println( "<br><br><br><address>Designed by Lusine</address>" ) ;
          pw.println( "</body>" ) ;
          pw.println( "</html>" ) ;
       }
   }
   private void sayHallo( HttpRequest request , PrintWriter pw ){
      request.printHttpHeader(0) ;
      pw.println( "<html>");
      pw.println( "<head><title>Billing Information</title></head>");
      pw.println( "<body background=\"/images/bg.jpg\">") ;
      pw.println( "<table border=0 cellpadding=10 cellspacing=0 width=\"90%\">");
      pw.println( "<tr><td align=center valign=center width=\"1%\">" ) ;
      pw.println( "<a href=\"/\"><img border=0 src=\"/images/eagleredtrans.gif\"></a>");
      pw.println( "<br><font color=red>Birds Home</font>" ) ;
      pw.println( "</td><td align=center>" ) ;
      pw.println( "<h1><font color=blue>dCache Billing</font></h1>");
      pw.println( "</td></tr></table>");
   }
   private void printMainStatisticsPage( HttpRequest request , PrintWriter pw )
           throws HttpException {
           
       CellMessage result = null ;
       try{
          result = _nucleus.sendAndWait(
                               new CellMessage( new CellPath("billing") ,
                                                "get billing info" ) ,
                                         5000 ) ;
       }catch(Exception ee){
          throw new
          HttpException( 500 , "Problem : "+ee.getMessage() ) ;
       }                                 
       if( result == null )
         throw new
         HttpException( 500 , "Request Timed Out" ) ;

        
       try{
          Object [] x = (Object [])result.getMessageObject() ;
       
          sayHallo( request , pw ) ;
          printTotalStatistics( pw , x ) ;
          
	  HashMap map = null ;
	  try{
             result = _nucleus.sendAndWait(
                        	  new CellMessage( new CellPath("billing") ,
                                                   "get pool statistics" ) ,
                                            5000 ) ;
	     if( result == null )
               throw new
               HttpException( 500 , "Request Timed Out" ) ;

	     map = (HashMap)result.getMessageObject() ;
	  }catch(Exception ee ){
	     pw.print("<p><h3>This 'billingCell' doesn't support : " ) ;
	     pw.println(" 'get pool statistics'</h3>");
	     pw.print("<pre>"+ee+"</pre>") ;
	  }
          printPoolStatistics( pw , map , null ) ;
          
       }catch(Exception ii){
          throw new
          HttpException( 500 , "Problem : "+ii.getMessage() ) ;
       }finally{
//          pw.println( "<br><br><br><address>Design by Lusine</address>" ) ;
          pw.println( "</body>" ) ;
          pw.println( "</html>" ) ;
       }
   }
   public void queryUrl( HttpRequest request )
          throws HttpException {

       PrintWriter pw     = request.getPrintWriter() ;
       String [] urlItems = request.getRequestTokens() ;
       int       offset   = request.getRequestTokenOffset() ;
       
       // System.out.println( "UrlItem (offset) "+offset ) ;
       // for( int i = 0 ; i < urlItems.length ; i++ )
       //   System.out.println("UrlItem : "+i+" "+urlItems[i] ) ;

       int argc = urlItems.length - offset ;
       if( argc > 0 ){
          if( urlItems[offset].equals("pool") && ( argc > 1 ) ){
             printPerPoolStatisticsPage( request , pw , urlItems[offset+1] ) ;
          }
       }else{
       
           printMainStatisticsPage( request , pw ) ;
       }
          
   }
}
