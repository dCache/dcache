package dmg.cells.services ;


import   dmg.cells.nucleus.* ;
import   dmg.cells.network.* ;
import   dmg.util.* ;

import java.util.* ;
import java.text.* ;
import java.io.* ;
import java.net.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSystemService implements HttpResponseEngine {

   private static final Logger _log =
       LoggerFactory.getLogger(HttpSystemService.class);

   private static final String __pageBg   = "#dddddd" ;
   private static final String __headerBg = "blue" ;
   private static final String __headerFg = "white" ;
   private static final String __tableBg  = "#bbbbbb" ;
   private static final String __tableFg  = "blue" ;

   private CellNucleus _nucleus    = null ;
   private Hashtable   _domainHash = new Hashtable() ;
   private String   [] _args       = null ;
   private DateFormat  _dataFormat = new SimpleDateFormat("MMM d, hh.mm.ss" ) ;

   public HttpSystemService( CellNucleus nucleus , String [] args ){
       _nucleus = nucleus ;
       _args    = args ;
   }
   public void queryUrl( HttpRequest request )
          throws HttpException {

       printSystem( request , request.getRequestTokens() , request.getPrintWriter() ) ;
   }
   private void printSystem( HttpRequest request , String [] cms , PrintWriter pw ){

      String      command = cms.length > 1 ? cms[1] : "gettopomap?topo" ;
      CellMessage msg    = null ;
      String      answer = null ;
      String      path   = null ;

      request.printHttpHeader( 0 ) ;
      pw.println( "<html><head><title>System "+command+" </title></head>");

      try{
         int pos = command.indexOf("?") ;
         if( pos < 0 ){
            path    = "System" ;
         }else{
            path    = command.substring(pos+1);
            command = command.substring(0,pos) ;
         }

         pw.println( "<body bgcolor=\""+__pageBg+"\">") ;

         if( command.equals( "detail" ) ){
            int p = path.indexOf( "@" ) ;
            if( p < 0 )throw new Exception( "Incomplete Path" ) ;
            String domainName = path.substring(p+1) ;
            String cellName   = path.substring(0,p) ;
            path = (String)_domainHash.get( domainName ) ;
            if( path == null )throw new Exception("domain not found : "+domainName);
            command = "getcellinfo "+cellName ;
         }
         _log.info( "Command : "+command+" !! "+path ) ;
         msg = _nucleus.sendAndWait(
                      new CellMessage(
                              new CellPath(path)  ,
                              command ) ,
                      10000                          ) ;

         if( msg == null )throw new Exception( "Response Timed Out" ) ;

         Object obj = msg.getMessageObject() ;

         if( obj instanceof CellDomainNode [] ){
            CellDomainNode [] nodes = (CellDomainNode[]) obj ;
            _domainHash.clear() ;
            for( int i = 0 ; i < nodes.length ; i++ ){
                _log.info( "assign : "+nodes[i].getName()+" -> "+nodes[i].getAddress());
                _domainHash.put( nodes[i].getName() , nodes[i].getAddress() ) ;
            }
            printCellDomainNodeTable( nodes , pw ) ;
         }else if( obj instanceof CellInfo ){
            printCellInfo( (CellInfo)obj , pw ) ;
         }else if( obj instanceof CellRoute [] ){
            printCellRoutes( (CellRoute [])obj , pw , path ) ;
         }else if( obj instanceof CellInfo []){
            CellInfo [] infos = (CellInfo [])obj ;
            if( infos.length == 0 )
              throw new Exception( "Cell info not yet available" ) ;

            printCellInfos( infos , pw ) ;
//                for( int i = 0 ; i < infos.length ; i++ )
//                    printCellInfo( infos[i] , pw ) ;
         }else if( obj instanceof Exception ){
            throw (Exception)obj ;
         }else{
            pw.println( "<pre>" ) ;
            pw.println( msg.getMessageObject().toString() ) ;
            pw.println( "</pre>") ;
         }

      }catch(Exception ee){
         printException( pw , ee ) ;
      }
      pw.println( "</body></html>");

   }
   private void printCellInfos( CellInfo [] info , PrintWriter pw ){
      if( info.length < 1 )return ;
      pw.println( "<center><h1>Cell Table of Domain " ) ;
      pw.println( "<font color=red>"+info[0].getDomainName()+"</font></h1></center>" ) ;
      pw.println( "<center><table border=0 cellpadding=4 ") ;
      pw.println( "cellspacing=4 width=\"90%\"><tr>" ) ;

      String prefix = "<th bgcolor=\""+__headerBg+"\">"+
                      "<font color=\""+__headerFg+"\">" ;
      String postfix = "</font></th>" ;

      pw.println( prefix+"CellName"+postfix) ;
      pw.println( prefix+"CellClass"+postfix ) ;
      pw.println( prefix+"CreationTime"+postfix ) ;
      pw.println( prefix+"Queue"+postfix ) ;
      pw.println( prefix+"Info"+postfix ) ;
      pw.println("</tr>" ) ;
      for( int i = 0 ; i < info.length ; i ++ ){
          pw.print( "<tr><td align=center bgcolor=\""+__tableBg+"\"><a href=detail?" ) ;
          pw.print( info[i].getCellName()+"@"+info[i].getDomainName()+">" ) ;
          pw.print( "<font color=\""+__tableFg+"\">");
          pw.println( info[i].getCellName() ) ;
          pw.println( "</font></a></td>" ) ;
          pw.println( "<td align=center bgcolor=\""+__tableBg+"\">" ) ;
          pw.println( "<font color=\""+__tableFg+"\">") ;
          pw.println(info[i].getCellClass()) ;
          pw.println("</font></td>" ) ;
          pw.println( "<td align=center bgcolor=\""+__tableBg+"\">" ) ;
          pw.println( "<font color=\""+__tableFg+"\">") ;
          pw.println(_dataFormat.format(info[i].getCreationTime())) ;
          pw.println("</font></td>" ) ;
          pw.println( "<td align=right align=center bgcolor=\""+__tableBg+"\">" ) ;
          pw.println( "<font color=\""+__tableFg+"\">") ;
          pw.println(info[i].getEventQueueSize() ) ;
          pw.println( "</font></td>" ) ;
          pw.println( "<td align=center bgcolor=\""+__tableBg+"\">" ) ;
          pw.println( "<font color=\""+__tableFg+"\">") ;
          pw.println( info[i].getShortInfo() ) ;
          pw.println("</font></td></tr>") ;
      }
      pw.println( "</table></center>" ) ;
      pw.println( "<p><hr><p>" ) ;
      pw.println( "<pre><a href=\"\">Back to topology</a></pre>" ) ;
   }
   private void printCellInfo( CellInfo info , PrintWriter pw ){
       String prefix = "<th bgcolor=\""+__headerBg+"\">"+
                       "<font color=\""+__headerFg+"\">" ;
       String postfix = "</font></th>" ;

       pw.print( "<center><h1>Cell Info of <font color=red>" ) ;
       pw.print( info.getCellName()+"@"+info.getDomainName() ) ;
       pw.println( "</font></h1></center>" ) ;
       pw.println( "<center><table border=0 cellpadding=4 ") ;
       pw.println( "cellspacing=4 width=\"90%\"><tr>" ) ;
       pw.println( "<tr>" ) ;
       pw.println( prefix+"Type"+postfix ) ;
       pw.println( prefix+"Value"+postfix ) ;
       pw.println( prefix+"Private"+postfix ) ;
       pw.println( "</tr>");

       prefix = "<td bgcolor=\""+__tableBg+"\">"+
                "<font color=\""+__tableFg+"\">" ;
       postfix = "</font></td>" ;

       String prefix2  = "<th bgcolor=\""+__headerBg+"\">"+
                         "<font color=\""+__headerFg+"\">" ;
       String postfix2 = "</font></th>" ;

       pw.println( "<tr>" ) ;
       pw.println( prefix2 + "Domain Name" + postfix2 );
       pw.println( prefix + info.getDomainName() + postfix ) ;

       pw.println( "<td rowspan=6 bgcolor=\""+__tableBg+"\">" ) ;
       pw.println( "<font color=\""+__tableFg+"\">" ) ;
       pw.println( "<pre>"+info.getPrivatInfo()+"</pre>"+postfix+"</tr>" ) ;

       pw.println( "<tr>") ;
       pw.println( prefix2 + "Cell Name" + postfix2);
       pw.println( prefix + info.getCellName() + postfix);
       pw.println( "</tr>" ) ;

       pw.println( "<tr>" ) ;
       pw.println( prefix2 + "Cell Class" + postfix2 ) ;
       pw.println( prefix + info.getCellClass() + postfix);
       pw.println( "</tr>" ) ;

       pw.println( "<tr>" ) ;
       pw.println( prefix2 + "Creation Time" + postfix2 ) ;
       pw.println( prefix + _dataFormat.format(info.getCreationTime())+ postfix ) ;
       pw.println( "</tr>" ) ;

       pw.println( "<tr>" ) ;
       pw.println( prefix2 + "Message Queue Size" + postfix2);
       pw.println( prefix + info.getEventQueueSize() + postfix ) ;
       pw.println( "</tr>" ) ;

       pw.println( "<tr>" ) ;
       pw.println( prefix2 + "Short Info" + postfix2 ) ;
       pw.println( prefix + info.getShortInfo() + postfix ) ;
       pw.println( "</tr>" ) ;


      pw.println( "</table></center>" ) ;
      pw.println( "<p><hr><p>" ) ;
      pw.println( "<pre>") ;
      pw.print( "<a href=\"\">Topology</a>       " ) ;
      String domain = (String)_domainHash.get( info.getDomainName() ) ;
      if( domain != null ){
        pw.print( "<a href=getcellinfos?"+domain+">" ) ;
        pw.print( "DomainInfo of "+info.getDomainName()+"</a>" ) ;
      }
      pw.println( "</pre>" ) ;

   }
   private void printCellRoutes( CellRoute [] route ,
                                 PrintWriter  pw ,
                                 String       path ){
       pw.println( "<center><h1>Routing table of" ) ;
       pw.println( "<font color=red>"+path+"</font></center><p><p>" ) ;

       pw.println( "<center><table border=0 cellpadding=4 ") ;
       pw.println( "cellspacing=4 width=\"90%\"><tr>" ) ;

       String prefix = "<th bgcolor=\""+__headerBg+"\">"+
                       "<font color=\""+__headerFg+"\">" ;
       String postfix = "</font></th>" ;

       pw.println( "<tr>" ) ;
       pw.println( prefix + "Destination Cell" + postfix ) ;
       pw.println( prefix + "Destination Domain" + postfix ) ;
       pw.println( prefix + "Gateway Cell" + postfix ) ;
       pw.println( prefix + "Route Type" + postfix ) ;
       pw.println( "</tr>" ) ;

       prefix = "<td align=center bgcolor=\""+__tableBg+"\">"+
                "<font color=\""+__tableFg+"\">" ;
       postfix = "</font></td>" ;
       for( int i = 0 ; i < route.length ; i++ ){
          pw.println( "<tr>" ) ;
          pw.println( prefix+route[i].getCellName()+postfix ) ;
          pw.println( prefix+route[i].getDomainName()+postfix ) ;
          pw.println( prefix+route[i].getTargetName()+postfix ) ;
          pw.println( prefix+route[i].getRouteTypeName()+postfix);
          pw.println( "</tr>" ) ;
       }
      pw.println( "</table></center>" ) ;
      pw.println( "<p><hr><p>" ) ;
      pw.println( "<pre>") ;
      pw.print( "<a href=\"\">Topology</a>" ) ;
      pw.println( "</pre>" ) ;

   }
   private void printCellDomainNodeTable( CellDomainNode [] info , PrintWriter pw ){
      pw.println( "<center><h1><font size=+5 color=red>");
      pw.println( "Topology Layout" ) ;
      pw.println( "</font></h1>" ) ;

       pw.println( "<center><table border=0 cellpadding=4 ") ;
       pw.println( "cellspacing=4 width=\"90%\"><tr>" ) ;

      pw.println( "<tr><th bgcolor=\""+__headerBg+"\">") ;
      pw.println( "<font color=\""+__headerFg+"\">Routes/Details</font></th>" ) ;
      for( int i = 0 ; i < info.length ; i++ ){
         pw.println( "<th bgcolor=\""+__headerBg+"\">");
         pw.println( "<a href=getcellinfos?"+info[i].getAddress()+">" ) ;
         pw.println( "<font color=\""+__headerFg+"\">") ;
         pw.println( info[i].getName()+"</font></a></th>" ) ;
      }
      pw.println( "</tr>" ) ;

      for( int j = 0 ; j < info.length ; j++ ){
         pw.println( "<tr>" ) ;
         pw.println( "<th bgcolor=\""+__headerBg+"\">" ) ;
         pw.println( "<a href=getroutes?"+info[j].getAddress()+">" ) ;
         pw.println( "<font color=\""+__headerFg+"\">") ;
         pw.println( info[j].getName()+"</font></a></th>" ) ;
         CellTunnelInfo [] tunnel = info[j].getLinks() ;
         for( int i = 0 ; i < info.length ; i++ ){
            int l = 0 ;
            for( l = 0 ;
                 ( l < tunnel.length ) &&
                 ( ! tunnel[l].getRemoteCellDomainInfo().
                              getCellDomainName().
                              equals( info[i].getName() ) ) ; l ++ ) ;
             if( l == tunnel.length )
                pw.println( "<td  bgcolor=\""+__tableBg+"\" align=center>-</td>" ) ;
             else{
                pw.println( "<td  bgcolor=\""+__tableBg+"\" align=center>" ) ;
                pw.println( "<font color=red>Connected</font></td>" ) ;
             }
         }
         pw.println( "</tr>" ) ;
      }
      pw.println( "</table>" ) ;

   }
   private void printCellDomainNode( CellDomainNode [] info , PrintWriter pw ){
      pw.println( "<center><h1><font size=+5 color=red>");
      pw.println( "Topology Layout" ) ;
      pw.println( "</font></h1>" ) ;
      for( int i = 0 ; i < info.length ; i++ ){
         pw.println( "<center><h1><a href=ps%20-a?"+info[i].getAddress()+
                     ">"+info[i].getName()+"</a></h1></center>" ) ;
         CellTunnelInfo [] tunnel = info[i].getLinks() ;
//         pw.println( "<blockquote>" ) ;
         for( int j = 0 ; j < tunnel.length ; j++ ){
            pw.println( "<center><h4>" ) ;
            pw.println( tunnel[j].
                        getRemoteCellDomainInfo().
                        getCellDomainName() ) ;
            pw.println( "</h4></center>" ) ;
         }
//         pw.println( "</blockquote>" ) ;
      }

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
   private static void printException( PrintWriter pw , Exception ee ){
      pw.println( "<h1><font color=red>"+
                   "An internal error occured"+
                   "</font></h1>" ) ;
      pw.println( "<h4>The Exception was : <font color=red>"+
                   ee.getClass().getName()+"</font></h4>" ) ;
      pw.println( "<h4>The message was : </h4>" ) ;
      pw.println( "<pre>" ) ;
      pw.println( ee.getMessage() ) ;
      pw.println( "</pre>" ) ;
   }

}
