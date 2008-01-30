// $Id: PoolInfoObserverEngineV2.java,v 1.1 2006-06-05 08:51:27 patrick Exp $Cg

package  diskCacheV111.services.web ;

import   dmg.cells.nucleus.* ;
import   dmg.cells.network.* ;
import   dmg.util.* ;

import   diskCacheV111.pools.* ;

import java.util.* ;
import java.text.* ;
import java.io.* ;
import java.net.* ;


public class PoolInfoObserverEngineV2 implements HttpResponseEngine {

   private CellNucleus _nucleus    = null ;
   private String   [] _args       = null ;
   private DateFormat  _dataFormat = new SimpleDateFormat("MMM d, hh.mm.ss" ) ;

   private int         _errorCounter   = 0 ;
   private int         _requestCounter = 0 ;
   private String      _cssFile        = "/pools/css/default.css" ;
   private Map         _tableSelection = new HashMap() ;

   private PoolCellQueryContainer _container = null ;

   public PoolInfoObserverEngineV2( CellNucleus nucleus , String [] args ){
       _nucleus = nucleus ;
       _args    = args ;
       for( int i = 0 ; i < args.length ; i++ ){
          _nucleus.say("PoolInfoObserverEngineV2 : argument : "+i+" : "+args[i]);
          if( args[i].startsWith("css=") ){
              decodeCss( args[i].substring(4) ) ;
          }
       }
       _tableSelection.put("Cell View"      , "cells" ) ;
       _tableSelection.put("Space Usage"    , "spaces" ) ;
       _tableSelection.put("Request Queues" , "queues" ) ;

   }

   public void queryUrl( HttpRequest request )
          throws HttpException {

      String   [] urlItems = request.getRequestTokens() ;
      int         offset   = request.getRequestTokenOffset() ;
      PrintWriter pw       = request.getPrintWriter() ;

      _requestCounter ++ ;

      try{
          if( urlItems.length < 1 )return ;

          if( ( urlItems.length > 1 ) && ( urlItems[1].equals("css") ) ){
              //
              // the internal css stuff (if nothing else is specifed)
              //
              if( urlItems.length > 2 ){
                   request.setContentType("text/css");
                   request.printHttpHeader(0);
                   printInternalCssFile( pw  ) ;
              }
              //
          }else if( ( urlItems.length > 1 ) && ( urlItems[1].equals("list") ) ){
             //
             request.printHttpHeader(0);
             printConfigurationHeader(pw);
             //
             //
             Object o = _nucleus.getDomainContext("poolgroup-map.ser");
             if( o ==  null ){
                 pw.println("<br><br><center><h3>Information not yet available</h3></center>") ;
                 return ;
             }else if( ! ( o instanceof PoolCellQueryContainer ) ){
                 pw.println("<h3>Internal error : poolgroup-map.ser contains unknown class</h3>") ;
                 return ;
             }
             _container = (PoolCellQueryContainer)o ;
             String className = urlItems.length > 2 ? urlItems[2] : null ;
             String groupName = urlItems.length > 3 ? urlItems[3] : null ;
             String selection = urlItems.length > 4 ? urlItems[4] : null ;

             printMenu( pw , className , groupName , selection ) ;

             if( ( className == null ) || ( groupName == null ) || ( selection == null ) )return ;

             Map poolMap = _container.getPoolMap( className , groupName ) ;
             if( poolMap == null )return ;

             if( selection.equals("cells") ){
                pw.println("<h3>Cell Info of group <font color=red>"+groupName);
                pw.println("</font> in view <font color=red>"+className+"</font></h3>");
                printCells( pw , poolMap ) ;
             }else if( selection.equals("spaces") ){
                pw.println("<h3>Space Info of group <font color=red>"+groupName);
                pw.println("</font> in view <font color=red>"+className+"</font></h3>");
                printPools( pw , poolMap ) ;
             }else if( selection.equals("queues") ){
                pw.println("<h3>Queue Info of group <font color=red>"+groupName);
                pw.println("</font> in view <font color=red>"+className+"</font></h3>");
                printPoolActions( pw , poolMap ) ;
             }
          }


       }catch(Exception ee ){
          _errorCounter ++ ;
          showProblem( pw , ee.getMessage() ) ;
          pw.println("<ul>");
          for( int i = 0 ; i < urlItems.length ; i++ ){
            pw.println("<li> ["+i+"] ") ;
            pw.println(urlItems[i]) ;
          }
          pw.println("</ul>");
       }finally{
          pw.println( "</body>" ) ;
          pw.println( "</html>" ) ;
       }

      return ;
   }
   private PrintPoolCellHelper _helper = new PrintPoolCellHelper() ;

   private void printPoolActions( PrintWriter pw , Map poolMap ){
       StringBuffer sb = new StringBuffer() ;
      _helper.printPoolActionTable( sb , new TreeMap( poolMap).values() ) ;
      pw.println(sb.toString());
   }
   private void printPools( PrintWriter pw , Map poolMap ){
       StringBuffer sb = new StringBuffer() ;
      _helper.printPoolInfoTable( sb , new TreeMap( poolMap).values() ) ;
      pw.println(sb.toString());
   }
   private void printCells( PrintWriter pw , Map poolMap ){
       StringBuffer sb = new StringBuffer() ;
      _helper.printCellInfoTable( sb , new TreeMap( poolMap).values() ) ;
      pw.println(sb.toString());
   }
   private void printMenu( PrintWriter pw , String className , String groupName , String selection ){

      printClassMenu( pw , className ) ;
      printGroupMenu( pw , className , groupName ) ;

      if( ( className == null ) || ( groupName == null ) )return ;

      pw.println("<h3>Table Selection</h3>");
      pw.println("<center>");
      printMenuTable( pw , _tableSelection.entrySet() ,
                      "/pools/list/"+className+"/"+groupName+"/" , selection ) ;

      pw.println("</center>");
   }
   private void printClassMenu( PrintWriter pw , String className ){
       Set classSet = _container.getPoolClassSet() ;
       pw.println("<h3>Pool Views</h3>");
       pw.println("<center>");
       printMenuTable( pw , classSet , "/pools/list/" , className ) ;
       pw.println("</center>");
   }
   private void printGroupMenu( PrintWriter pw , String className , String groupName ){
       if( className == null )return ;

       Set groupSet = _container.getPoolGroupSetByClassName(className);
       //
       // this shouldn't happen
       //
       if( groupSet == null )return ;

       pw.println("<h3>Pool Groups of View <font color=red>"+className+"</font></h3>");
       pw.println("<center>");
       printMenuTable( pw , groupSet , "/pools/list/"+className+"/" , groupName ) ;
       pw.println("</center>");
   }
   private int _menuColumns = 4 ;
   private void printMenuTable( PrintWriter pw , Set itemSet , String linkBase , String currentItem ){
       pw.println("<table class=\"m-table\" >") ;

       int n = 0 ;
       for(  Iterator i = itemSet.iterator() ; i.hasNext() ; n++ ){
          Object o = i.next() ;
          String name = null ;
          String linkName = null ;
          if( o instanceof String ){
             name = linkName = (String)o ;
          }else{
             Map.Entry e = (Map.Entry)o ;
             name     = (String)e.getKey() ;
             linkName = (String)e.getValue() ;
          }

          if( n == 0 ){
              pw.println("<tr class=\"m-table\">" );
          }else if( ( n % _menuColumns ) == 0 ){
              pw.println("</tr>" );
              pw.println("<tr class=\"m-table\">" );
          }
          boolean us = currentItem != null && currentItem.equals(linkName) ;
          String alternateClass=us?"class=\"m-table-active\"":"class=\"m-table\"" ;

          pw.print("<td class=\"m-table\"><a "+alternateClass+" href=\"") ;
          pw.print(linkBase);
          pw.print(linkName);
          pw.print("/\">");
          pw.print("<span "+alternateClass+" >");
          pw.print(name);
          pw.print("</span>");
          pw.println( "</td>" );

       }
       if( (n = n % _menuColumns) > 0 )
          for( ; n < _menuColumns ; n++ )
             pw.println("<td class=\"m-table\">&nbsp;</td>");


       pw.println("</tr>" );
       pw.println("</table>") ;
   }
   private void printConfigurationHeader( PrintWriter pw ){

      pw.println( "<html>");
      pw.println( "<head>");
      pw.println("<title>Pool Property Tables</title>");
      pw.println("<link rel=\"stylesheet\" type=\"text/css\" href=\""+_cssFile+"\">");
      pw.println("</head>");

      pw.println( "<body class=\"m-body\">") ;

      pw.println("<center><table width=\"95%\"><tr>") ;
      pw.println("<td valign=center align=center><a href=\"/\"><img border=0 src=\"/images/eagleredtrans.gif\"></a></td>");
      pw.println("<td valign=center align=center><a style='text-decoration:none'");
      pw.println(" href=\"/pools/list/*\"><h3><font color=red>Pool Info Home</font></h3></a></td>");
      pw.println("<td valign=center align=center width=\"80%\"><h1>Pool Property Tables</h1></td>");
      pw.println("</tr></table></center>");


   }
   private void decodeCss( String cssDetails ){

      cssDetails = cssDetails.trim() ;

      if(  ( cssDetails.length() > 0 ) && ! cssDetails.equals("default") )
         _cssFile = cssDetails ;

   }
   private void showProblem( PrintWriter pw , String message ){
      pw.print("<font color=red><h1>") ;
      pw.print(message) ;
      pw.println("</h1></font>");
   }
   private void printInternalCssFile( PrintWriter pw ){
      // pw.println("body { background-color:orange ; }");
      pw.println("");
      pw.println("body { background-image:url(/images/bg.jpg) ;}");
      pw.println("tr.m-table { font-size:18 }");
      pw.println("table.m-table { width:95% ; border:0px ; border-style:none ; border-spacing:0px ; border-collapse:collapse ; }");
      pw.println("td.m-table { width:25% ; background-color:#dddddd ; text-align:center ; border:1px  ; border-style:solid ; border-spacing:1px ;}");
      pw.println("a.m-table:visited        { text-decoration:none ;  color:green ;}");
      pw.println("a.m-table-active:visited { text-decoration:none ; color:red ; }");
      pw.println("a.m-table:link        { text-decoration:none ;  color:green ; }");
      pw.println("a.m-table-active:link { text-decoration:none ; color:red ; }");
      pw.println("span.m-title { font-size:18 ; color=red ; }");
   }
}
