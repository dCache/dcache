package diskCacheV111.hsmControl.flush;

 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import java.io.PrintWriter;
 import java.io.Serializable;
 import java.text.SimpleDateFormat;
 import java.util.ArrayList;
 import java.util.Collections;
 import java.util.Date;
 import java.util.HashMap;
 import java.util.Iterator;
 import java.util.List;
 import java.util.Map;
 import java.util.NoSuchElementException;
 import java.util.StringTokenizer;
 import java.util.concurrent.TimeUnit;

 import diskCacheV111.pools.PoolCellInfo;
 import diskCacheV111.pools.PoolCostInfo;
 import diskCacheV111.pools.StorageClassFlushInfo;
 import diskCacheV111.util.CacheException;
 import diskCacheV111.util.TimeoutCacheException;

 import dmg.cells.nucleus.CellEndpoint;
 import dmg.cells.nucleus.CellMessage;
 import dmg.cells.nucleus.CellPath;
 import dmg.util.HttpException;
 import dmg.util.HttpRequest;
 import dmg.util.HttpResponseEngine;

 import org.dcache.cells.CellStub;

 import static java.util.concurrent.TimeUnit.SECONDS;

 public class HttpHsmFlushMgrEngineV1 implements HttpResponseEngine {

   private final static Logger _log =
       LoggerFactory.getLogger(HttpHsmFlushMgrEngineV1.class);

   private final CellEndpoint _endpoint;
   private long        _errorCounter;
   private long        _requestCounter;
   private String      _cssFile          = "/flushManager/css/default.css" ;
   private final List<String> _managerList      = new ArrayList<>() ;
   private final SimpleDateFormat _formatter   = new SimpleDateFormat ("MM.dd HH:mm:ss");

   private final HttpFlushManagerHelper.PoolEntryComparator  _poolCompare;
   private final HttpFlushManagerHelper.FlushEntryComparator _flushCompare;

   public HttpHsmFlushMgrEngineV1(CellEndpoint endpoint, String [] argsString ){
       _endpoint = endpoint ;

       for( int i = 0 ; i < argsString.length ; i++ ){
          _log.info("HttpPoolMgrEngineV3 : argument : "+i+" : "+argsString[i]);
          if( argsString[i].startsWith("css=") ){
              decodeCss( argsString[i].substring(4) ) ;
          }else if( argsString[i].startsWith("mgr=") ){
              decodeManager( argsString[i].substring(4) ) ;
          }
       }

      _poolCompare  = new HttpFlushManagerHelper.PoolEntryComparator() ;
      _poolCompare.setColumn(0) ;
      _flushCompare = new HttpFlushManagerHelper.FlushEntryComparator() ;
      _flushCompare.setColumn(1);

      if( _managerList.size() == 0 ) {
          _managerList.add("FlushManager");
      }

      _log.info("Using Manager  : "+_managerList ) ;
      _log.info("Using CSS file : "+_cssFile ) ;

   }
   private void decodeManager( String managers ){

      managers = managers.trim() ;

      for( StringTokenizer st = new StringTokenizer(managers,",") ; st.hasMoreTokens() ; ){
         _managerList.add( st.nextToken() ) ;
      }

   }
   private void decodeCss( String cssDetails ){

      cssDetails = cssDetails.trim() ;

      if(  ( cssDetails.length() > 0 ) && ! cssDetails.equals("default") ) {
          _cssFile = cssDetails;
      }

   }

   @Override
   public void queryUrl( HttpRequest request )  throws HttpException {

       PrintWriter pw       = request.getPrintWriter() ;
       String []   urlItems = request.getRequestTokens() ;

       request.printHttpHeader(0);
       _requestCounter ++ ;
       try{
          if( urlItems.length < 1 ) {
              return;
          }

          if( ( urlItems.length > 1 ) && ( urlItems[1].equals("css") ) ){
              //
              // the internal css stuff (if nothing else is specifed)
              //
              if( urlItems.length > 2 ) {
                  printCssFile(pw, urlItems[2]);
              }
              //
          }else if( ( urlItems.length > 1 ) && ( urlItems[1].equals("mgr") ) ){
             //
             // the parameter handler (dCache partitioning)
             //
             String flushManagerName = "FlushManager" ;
             Map<?,?>    optionsMap       = new HashMap<>() ;

             if( urlItems.length > 2 ) {
                 flushManagerName = urlItems[2];
             }
             if( urlItems.length > 3 ) {
                 optionsMap = createMap(urlItems[3]);
             }

             CellStub flushManager = new CellStub(_endpoint, new CellPath(flushManagerName), 20, SECONDS);

             _log.info("MAP -> "+optionsMap);

             printFlushHeader( pw ,  "Flush Info");
             printDirectory( pw ) ;

             if( !  flushManagerName.equals("*") ){

                StringBuffer result = new StringBuffer() ;
                doActionsIfNecessary(flushManager, optionsMap, result);

                String errorString = result.toString() ;
                if( errorString.length() > 0 ){
                   pw.println("<hr>");
                   pw.println("<h2>The following errors were reported</h2>");
                   pw.println("<pre>");
                   pw.println(errorString);
                   pw.println("</pre>");
                   pw.println("<hr>");
                }
                try{
                   printUpdateThis( pw , flushManagerName ) ;
                   printCellInfo(pw, flushManager);
                   printFlushManagerList(pw, flushManager, optionsMap);
                }catch(Exception ee ){
                   pw.println("<center><h2>Flush Manager "+flushManagerName+" seems not to be present</h2>");
                }
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
          pw.println( "<address>Last Modified : "+( new Date() )+ " $Id: HttpHsmFlushMgrEngineV1.java,v 1.3 2006-05-20 10:40:02 patrick Exp $</address>");
          pw.println( "</body>" ) ;
          pw.println( "</html>" ) ;
       }
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

   private void printUpdateThis( PrintWriter pw , String thisManager ){
     pw.println("<center><a class=\"big-link\" href=\"");
     pw.println(thisManager);
     pw.println("\"><span class=\"big-link\">");
     pw.println("Update this Flush Manager ("+thisManager+")");
     pw.println("</span></a></center><hr>");
   }
   private Map<String,Object> createMap( String message ){
      Map<String, Object> map = new HashMap<>() ;
      int     pos = message.indexOf('?');
      if( ( pos < 0 ) || ( pos == ( message.length() - 1 ) ) ){
         map.put("$MAIN$",message);
         return map ;
      }
      map.put("$MAIN$",message.substring(pos));
      StringTokenizer st = new StringTokenizer(message.substring(pos+1),"&") ;
      while( st.hasMoreTokens() ){
         StringTokenizer ss = new StringTokenizer( st.nextToken() , "=" ) ;
         try{
             String key   = ss.nextToken() ;
             String value = ss.hasMoreTokens() ? ss.nextToken() : "true" ;
             Object o  = map.get(key) ;
             if( o == null ){
                 map.put( key , value ) ;
             }else if( o instanceof List ){
                ((List)o).add(value) ;
             }else if( o instanceof String ){
                List l = new ArrayList() ;
                l.add( o ) ;
                l.add( value ) ;
                map.put( key , l ) ;
             }
         }catch(NoSuchElementException nsee ){}
      }
      return map ;

   }
   private void printCellInfo( PrintWriter pw , CellStub flushManager) throws Exception
   {
       try {
           FlushControlCellInfo info = flushManager.sendAndWait("xgetcellinfo", FlushControlCellInfo.class);
           prepareCellInfo(pw, flushManager.getDestinationPath().toString(), info);
       } catch (TimeoutCacheException e) {
           showTimeout(pw) ;
       } catch (CacheException e) {
           showProblem(pw, e.getMessage());
       }
   }
   private void prepareCellInfo( PrintWriter pw , String flushManagerName , FlushControlCellInfo info ){
      pw.println("<h2 class=\"s-table\">Manager : "+flushManagerName+"</h2>");
      pw.println("<center><table class=\"s-table\">");

      pw.println("<tr class=\"s-table\">");
      pw.println("<th class=\"s-table\">Cell Name</th>");
      pw.println("<th class=\"s-table\">Driver Class</th>");
      pw.println("<th class=\"s-table\">Control Type</th>");

      pw.println("</tr><tr class=\"s-table-b\">");

      pw.println("<td class=\"s-table\"><span class=\"s-table\">") ;
        pw.println(info.getCellName()+"@"+info.getDomainName());
      pw.println("</span></td>");
      pw.println("<td class=\"s-table\"><span class=\"s-table\">") ;
        pw.println(info.getDriverName());
      pw.println("</span></td>");
      pw.println("<td class=\"s-table\"><span class=\"s-table\">") ;
        pw.println(info.getIsControlled()?"Centrally Controlled":"Locally Controlled");
      pw.println("</span></td>");

      pw.println("</tr></table><br>");
      pw.println("<form action=\"submitonoff\" method=\"get\">" );
      pw.println("<input type=\"submit\" value=\"Control Locally\" name=\"command\">");
      pw.println("<input type=\"submit\" value=\"Control Centrally\" name=\"command\">");
      pw.println("</form>");
      pw.println("</center>");

      pw.println("<hr>");
   }

   private void printFlushManagerList(PrintWriter pw, CellStub flushManager, Map<?,?> options) throws Exception
   {
       try {
           List<HsmFlushControlCore.PoolDetails> list =
                   flushManager.sendAndWait("ls pool -l -binary", List.class);
           preparePoolList(pw, flushManager.getDestinationPath().toString(),  options, list);
       } catch (TimeoutCacheException e) {
           showTimeout(pw);
       } catch (CacheException e) {
           showProblem(pw, e.getMessage());
       }
   }
   private void  doActionsIfNecessary(CellStub flushManager, Map<?,?> options , StringBuffer output ){

       if( ( options == null ) || ( options.size() == 1 ) ) {
           return;
       }

       String command = (String)options.get("command") ;

       if( command == null ) {
           return;
       }

       if( command.startsWith("Control") ){

          boolean  central = command.contains("Centrally");
          String   remote  = "set control "+( central ? "on" : "off" ) ;

          sendCommand(flushManager , remote , output ) ;
       }else if( command.equals("Flush") ){

           Object o = options.get("storageclass") ;
           List<Object> list;
           if( o == null ) {
               return;
           }
           if( o instanceof String ){ list = new ArrayList<>() ; list.add( o ) ; }
           else if( o instanceof List ){ list = (List<Object>) o ; }
           else {
               return;
           }
           for (Object element : list) {
               StringTokenizer st = new StringTokenizer(element.toString(), "$");
               String poolName = st.nextToken();
               String storageClass = st.nextToken();
               String remote = "flush pool " + poolName + " " + storageClass;

               sendCommand(flushManager, remote, output);
           }

       }else if( command.startsWith( "Set" ) || command.startsWith( "Query" ) ){

           boolean rdOnly = command.contains("Only");
           boolean query  = command.contains("Query");
           Object o = options.get("pools") ;
           List<Object> list;
           if( o == null ) {
               return;
           }
           if( o instanceof String ){ list = new ArrayList<>() ; list.add( o ) ; }
           else if( o instanceof List ){ list = (List<Object>) o ; }
           else {
               return;
           }
           for (Object element : list) {
               String poolName = element.toString();
               String remote = query ? ("query pool mode " + poolName) :
                       "set pool " + poolName + " " + (rdOnly ? "rdonly" : "rw");

               sendCommand(flushManager, remote, output);
           }
       }
   }

   private void sendCommand(CellStub stub, String command, StringBuffer output)
   {
      try{
          stub.sendAndWait(command, Serializable.class);
      } catch (TimeoutCacheException e) {
          output.append("Command timed out : ").append(command).append("\n");
      } catch(InterruptedException | CacheException e) {
          output.append("Exception in command : ").append(command).append("\n") ;
          output.append("     ").append(e.getClass().getName()).
             append(" -> ").append( e.getMessage() ).append("\n") ;
          _log.warn(e.toString());
      }
   }

   private void preparePoolList( PrintWriter pw , String flushManagerName ,  Map<?,?> options , List<HsmFlushControlCore.PoolDetails> list ){

      List<HttpFlushManagerHelper.PoolEntry> pools  = new ArrayList<>() ;
      List<HttpFlushManagerHelper.FlushEntry> flushs = new ArrayList<>() ;

       for (HsmFlushControlCore.PoolDetails pool : list) {

           String poolName = pool.getName();
           boolean isReadOnly = pool.isReadOnly();
           PoolCellInfo cellInfo = pool.getCellInfo();

           if (cellInfo == null) {
               continue;
           }

           PoolCostInfo costInfo = cellInfo.getPoolCostInfo();

           if (costInfo == null) {
               continue;
           }

           PoolCostInfo.PoolSpaceInfo spaceInfo = costInfo.getSpaceInfo();
           PoolCostInfo.PoolQueueInfo queueInfo = costInfo.getStoreQueue();

           long totalSpace = spaceInfo.getTotalSpace();
           long preciousSpace = spaceInfo.getPreciousSpace();

           HttpFlushManagerHelper.PoolEntry pentry = new HttpFlushManagerHelper.PoolEntry();
           pentry._poolName = pool.getName();
           pentry._total = totalSpace;
           pentry._precious = preciousSpace;
           pentry._isReadOnly = isReadOnly;
           pentry._flushing = 0;

           pools.add(pentry);

           List<HsmFlushControlCore.FlushInfoDetails> flushes = pool.getFlushInfos();
           if ((flushes == null) || (flushes.size() == 0)) {
               continue;
           }

           for (Object flush1 : flushes) {

               HsmFlushControlCore.FlushInfoDetails flush = (HsmFlushControlCore.FlushInfoDetails) flush1;
               StorageClassFlushInfo info = flush.getStorageClassFlushInfo();

               HttpFlushManagerHelper.FlushEntry fentry = new HttpFlushManagerHelper.FlushEntry();
               fentry._poolName = pool.getName();
               fentry._storageClass = flush.getName();
               fentry._isFlushing = flush.isFlushing();
               fentry._total = totalSpace;
               fentry._precious = info.getTotalPendingFileSize();
               fentry._pending = info.getRequestCount();
               fentry._active = info.getActiveCount();
               fentry._failed = info.getFailedRequestCount();

               if (fentry._isFlushing) {
                   pentry._flushing++;
               }

               flushs.add(fentry);
           }
       }
      Collections.sort( pools , _poolCompare ) ;
      Collections.sort( flushs , _flushCompare ) ;
      printFlushingPools( pw , flushManagerName , options , pools ) ;
      printFlushingStorageClasses( pw , flushManagerName , options , flushs ) ;
   }
   private String [] _poolTableTitle =
        { "Action" , "PoolName" , "Pool Mode" ,  "Flushing" , "Total Size" ,
          "Precious Size" , "Fraction" } ;
   private void printFlushingPools( PrintWriter pw , String flushManagerName ,  Map<?,?> options , List<HttpFlushManagerHelper.PoolEntry> pools ){

      pw.println("<form action=\"submitpools\" method=\"get\">" );
      pw.println("<h2 class=\"s-table\">Flushing Pools</h2>");
      pw.println("<center><table class=\"s-table\">");
      pw.println("<tr class=\"s-table\">");
       for (String s : _poolTableTitle) {
           pw.print("<th class=\"s-table\">");
           pw.print(s);
           pw.println("</th>");
       }
      pw.println("</tr>");
      int row = 0 ;
      for( Iterator<HttpFlushManagerHelper.PoolEntry> it = pools.iterator() ; it.hasNext() ; row++ ){
          HttpFlushManagerHelper.PoolEntry pentry = it.next();

          /*
          pw.print(
                    pentry._isReadOnly || ( pentry._flushing > 0 ) ?
                    "<tr class=\"s-table-e\">" :
                    row%2 == 0 ? "<tr class=\"s-table-a\">" : "<tr class=\"s-table-b\">"
                  );
          */
          pw.print( row%2 == 0 ? "<tr class=\"s-table-a\">" : "<tr class=\"s-table-b\">" );
          pw.print("<td class=\"s-table\">");
            pw.print("<input type=\"checkbox\" name=\"pools\" value=\"");
            pw.print(pentry._poolName) ;
            pw.print("\">");
            pw.print("</input>");
          pw.println("</td>");

          pw.print("<td class=\"s-table\">");pw.print(pentry._poolName);pw.println("</td>");

          if( pentry._isReadOnly ){
             pw.print("<td class=\"s-table-e\">");
             pw.print("ReadOnly");
          }else{
             pw.print("<td class=\"s-table\">");
             pw.print("ReadWrite");
          }
          pw.println("</td>");

          pw.print(pentry._flushing>0?"<td class=\"s-table-e\">":"<td class=\"s-table\">");
            pw.print(pentry._flushing);
          pw.println("</td>");

          pw.print("<td class=\"s-table\">"); pw.print(pentry._total);pw.println("</td>");
          pw.print("<td class=\"s-table\">"); pw.print(pentry._precious);pw.println("</td>");
          pw.print("<td class=\"s-table\">");
             pw.print( (float)(((double)pentry._precious/(double)pentry._total)/100.0));
          pw.println("</td>");


          pw.println("</tr>");
      }

      pw.println("</table><br>");
      pw.println("<input type=\"submit\" value=\"Set Read Only\" name=\"command\">");
      pw.println("<input type=\"submit\" value=\"Set Read Write\" name=\"command\">");
      pw.println("<input type=\"submit\" value=\"Query\" name=\"command\">");
      pw.println("</center>");
      pw.println("</form>");
      pw.println("<hr>");
   }
   private String [] _flushTableTitle =
        { "Action" , "PoolName" , "StorageClass" ,  "Status" ,
          "Total Size" ,  "Precious Size" , "Fraction" ,
          "Active" , "Pending" , "Failed"
        } ;
   private void printFlushingStorageClasses( PrintWriter pw , String flushManagerName ,  Map<?,?> options , List<HttpFlushManagerHelper.FlushEntry> pools ){

      pw.println("<form action=\"submitstorageclass\" method=\"get\">" );
      pw.print("<h2 class=\"s-table\">Flushing Storage Classes</h2>");
      pw.print("<center><table class=\"s-table\">");
      pw.println("<tr class=\"s-table\">");
       for (String s : _flushTableTitle) {
           pw.print("<th class=\"s-table\">");
           pw.print(s);
           pw.println("</th>");
       }
      pw.println("</tr>");
      int row = 0 ;
      for( Iterator<HttpFlushManagerHelper.FlushEntry> it = pools.iterator() ; it.hasNext() ; row++ ){
          HttpFlushManagerHelper.FlushEntry fentry = it.next();

          pw.print(
                    fentry._isFlushing ?
                    "<tr class=\"s-table-e\">" :
                    row%2 == 0 ? "<tr class=\"s-table-a\">" : "<tr class=\"s-table-b\">"
                  );

          pw.print("<td class=\"s-table\">");
            pw.print("<input type=\"checkbox\" name=\"storageclass\" value=\"");
            pw.print(fentry._poolName+"$"+fentry._storageClass) ;
            pw.print("\">");
            pw.print("</input>");
          pw.println("</td>");

          pw.print("<td class=\"s-table\">"); pw.print(fentry._poolName);pw.println("</td>");
          pw.print("<td class=\"s-table\">"); pw.print(fentry._storageClass);pw.println("</td>");
          pw.print("<td class=\"s-table\">");
             pw.print(fentry._isFlushing?"Flushing":"Idle");
          pw.println("</td>");
          pw.print("<td class=\"s-table\">"); pw.print(fentry._total);pw.println("</td>");
          pw.print("<td class=\"s-table\">"); pw.print(fentry._precious);pw.println("</td>");
          pw.print("<td class=\"s-table\">");
             pw.print( (float)(((double)fentry._precious/(double)fentry._total)/100.0));
          pw.println("</td>");
          pw.print("<td class=\"s-table\">"); pw.print(fentry._active);pw.println("</td>");
          pw.print("<td class=\"s-table\">"); pw.print(fentry._pending);pw.println("</td>");
          pw.print("<td class=\"s-table\">"); pw.print(fentry._failed);pw.println("</td>");


          pw.println("</tr>");
      }
      pw.println("</table><br>");
      if( pools.size() > 0 ) {
          pw.println("<input type=\"submit\" value=\"Flush\" name=\"command\">");
      }
      pw.println("</center>");
      pw.println("</form>");
      pw.println("<hr>");
   }
   private void printFlushHeader( PrintWriter pw , String title ){
      pw.println( "<html>");
      pw.println( "<head><title>"+title+"</title>");
      pw.println("<link rel=\"stylesheet\" type=\"text/css\" href=\""+_cssFile+"\">");
      pw.println("</head>");
      pw.println( "<body class=\"flush-body\">") ;
      pw.println( "<table border=0 cellpadding=10 cellspacing=0 width=\"90%\">");
      pw.println( "<tr><td align=center valign=center width=\"1%\">" ) ;
      pw.println( "<a href=\"/\"><img border=0 src=\"/images/eagleredtrans.gif\"></a>");
      pw.println( "<br><font color=red>Birds Home</font>" ) ;
      pw.println( "</td><td align=center>" ) ;
      pw.println( "<h1 class=\"m-title\">"+title+"</font></h1>");
      pw.println( "</td></tr></table>");
   }
   private void printDirectory( PrintWriter pw ){
      printDirectory( pw , -1 ) ;
   }
   private void printDirectory( PrintWriter pw  , int position ){

      pw.println("<br><center><h2 class=\"m-table\">Flush Managers</h2><table class=\"m-table\">");
      pw.println("<tr class=\"m-table\">");

       for (Object o : _managerList) {
           String managerName = (String) o;
           printDirEntry(pw, managerName, false, "/flushManager/mgr/" + managerName + "/*");
       }
      pw.println("</tr>");

      pw.println("</table></center>");
      pw.println("<br><hr>");

   }
   private void printDirEntry( PrintWriter pw , String text , boolean inUse , String link ){
         String alternateClass=inUse?"class=\"m-table-active\"":"class=\"m-table\"" ;
         pw.print("<td width=\"25%\" class=\"m-table\"><span ");
         pw.print(alternateClass);
         pw.print("><a ");
         pw.print(alternateClass);
         pw.print(" href=\"") ;
         pw.print(link) ;
         pw.print("\">") ;
         pw.print(text) ;
         pw.println("</a></span></td>") ;
   }
   private void printCssFile( PrintWriter pw , String filename )
   {
       if( filename.equals("test.html") ){
       }else if( filename.equals("default.css") ){
          printInternalCssFile( pw ) ;
       }
   }
   private void printInternalCssFile( PrintWriter pw ){
      pw.println("body { background-color:orange ; }");
      pw.println("table.s-table { width:90% ; border:1px ; border-style:solid ; border-spacing:0px ; border-collapse:collapse ; }");
      pw.println("tr.s-table   { background-color:#115259 ; color:white; font-size:18 ; }");
      pw.println("tr.s-table-a { background-color:#bebebe ; text-align:center ; font-size:16 ; }");
      pw.println("tr.s-table-b { background-color:#efefef ; text-align:center ; font-size:16 ; }");
      pw.println("tr.s-table-e { background-color:red ; text-align:center ; font-size:16 ; }");
      pw.println("td.s-table {  border:1px  ; border-style:solid ; border-spacing:1px ; padding:3 ; }");
      pw.println("th.s-table {  border:1px  ; border-style:solid ; border-spacing:1px ;}");
      pw.println("td.s-table-disabled {  border:1px  ; border-style:solid ; border-spacing:0px ; padding:3 ;}");
      pw.println("td.s-table-e {  background-color:red ;}");
      pw.println("td.s-table-regular  {  border:1px  ; border-style:solid ; border-spacing:0px ; padding:3 ;}");
      pw.println("span.s-table-disabled { color:gray  ; }");
      pw.println("span.s-table-regular  { color:black  ; }");
      pw.println("a.s-table:visited  { text-decoration:none ; color:blue ; }");
      pw.println("a.s-table:link     { text-decoration:none ; color:blue ; }");
      pw.println("table.m-table { width:90% ; border:0px ; border-style:none ; border-spacing:0px ; border-collapse:collapse ; }");
      pw.println("td.m-table {  background-color:white ; text-align:center ; border:1px  ; border-style:solid ; border-spacing:1px ;}");
      pw.println("a.m-table:visited        { text-decoration:none ; }");
      pw.println("a.m-table-active:visited { text-decoration:none ; color:red ; }");
      pw.println("a.m-table:link        { text-decoration:none ; }");
      pw.println("a.m-table-active:link { text-decoration:none ; color:red ; }");
      pw.println("table.l-table { width:90% ; color:black ; table-layout:auto ;");
      pw.println("    border:1px ; border-style:none ; border-spacing:0px ; border-collapse:collapse ; }");
      pw.println("tr.l-table { background-color:green ; }");
      pw.println("td.l-table { background-color:white ; color:black ;");
      pw.println("width:10.5% ;");
      pw.println("padding:4 ; text-align:center ;");
      pw.println("border:1px ; border-style:solid ; border-spacing:0px ; border-collapse:collapse ; }");
      pw.println("span.l-table { font-size:16 ;}");
      pw.println("a.l-table:visited  { text-decoration:none ; color:blue ; }");
      pw.println("a.l-table:link     { text-decoration:none ; color:blue ; }");
      pw.println("table.f-table-a { width:90% ; color:black ; table-layout:auto ; background-color:white ;");
      pw.println("border:1px ; border-style:solid ; border-spacing:0px ; border-collapse:collapse ;}");
      pw.println("td.f-table-a { text-align:center ; padding:10 ; }");
      pw.println("span.f-table-a { text-align:center ; font-size:20px }");
      pw.println("table.f-table-b { width:100% ;  color:black ; table-layout:auto ; ");
      pw.println(" border:1px ; border-style:none ; border-spacing:0px ; border-collapse:collapse ; }");
      pw.println("th.f-table-b { width:20% ;}");
      pw.println("td.f-table-b { width:20% ; background-color:#eeeeee ; color:black ;");
      pw.println("padding:4 ; text-align:center ; border:1px ; border-style:solid ; border-spacing:0px ; }");
      pw.println("span.m-title { font-size:18 ; color=red ; }");
      pw.println("a.big-link:visited  { text-decoration:none ; color:red ; }");
      pw.println("a.big-link:link  { text-decoration:none ; color:red ; }");
      pw.println("span.big-link { font-size:24 ; text-align:center }");
   }
   private void showTimeout( PrintWriter pw ){
      pw.println("<font color=red><h1>Sorry, the request timed out</h1></font>");
   }
   private void showProblem( PrintWriter pw , String message ){
      pw.print("<font color=red><h1>") ;
      pw.print(message) ;
      pw.println("</h1></font>");
   }



}
