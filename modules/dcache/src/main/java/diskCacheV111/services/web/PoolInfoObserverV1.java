// $Id: PoolInfoObserverV1.java,v 1.6 2006-06-05 08:51:27 patrick Exp $Cg

package diskCacheV111.services.web ;

import java.util.* ;
import java.util.regex.Pattern ;
import java.text.* ;
import java.io.* ;
import dmg.cells.nucleus.* ;
import dmg.util.* ;


import diskCacheV111.pools.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PoolInfoObserverV1 extends CellAdapter implements Runnable {

    private final static Logger _log =
        LoggerFactory.getLogger(PoolInfoObserverV1.class);

   private CellNucleus _nucleus;
   private Args       _args;
   private HashMap<String, CellQueryInfo> _infoMap         = new HashMap<>() ;
   private Object     _lock            = new Object() ;
   private final Object     _infoLock        = new Object() ;
   private Thread     _collectThread;
   private final Thread _senderThread;
   private long       _interval        = 60000 ;
   private long       _counter;
   private String _dCacheInstance = "?" ;

   private final CellInfoContainer _container = new CellInfoContainer() ;

   private SimpleDateFormat _formatter = new SimpleDateFormat ("MM/dd hh:mm:ss");

   public static final String hh_x_addto_pgroup = "<groupName>  <name> [-pattern[=<pattern>}] [-class=<className>]";
   public String ac_x_addto_pgroup_$_2( Args args ) throws Exception {
       try{
           String groupClass = args.getOpt("class");
           groupClass = groupClass == null ? "default" : groupClass ;
           String groupName = args.argv(0) ;
           String name      = args.argv(1) ;
           String pattern   = args.getOpt("pattern");

           if( pattern == null ){
               _container.addPool( groupClass , groupName , name ) ;
           }else{
               if( pattern.length() == 0 ) {
                   pattern = name;
               }
               _container.addPattern( groupClass , groupName , name , pattern ) ;
           }

           return "" ;
       }catch(Exception e){
           _log.warn(e.toString(), e);
           throw e ;
       }
   }
   public static final String hh_x_add = "<pool> <poolValue> # debug only";
   public String ac_x_add_$_2( Args args ) throws Exception {
       try{
       _container.addInfo( args.argv(0) , args.argv(1) ) ;
       }catch(Exception ee ){
           _log.warn(ee.toString(), ee);
           throw ee;
       }
       return "" ;
   }
   public static final String hh_x_removefrom = "<poolGroup> [-class=<className>] <name> [-pattern]" ;
   public String ac_x_removefrom_$_2( Args args ) throws Exception {
       try{
           String groupClass = args.getOpt("class");
           groupClass = groupClass == null ? "default" : groupClass ;
           String poolGroup = args.argv(0) ;
           String name      = args.argv(1) ;
           if( !args.hasOption("pattern") ){
               _container.removePool( groupClass , poolGroup , name ) ;
           }else{
               _container.removePattern( groupClass , poolGroup , name ) ;
           }
       }catch(Exception ee ){
           _log.warn(ee.toString(), ee);
           throw ee ;
       }
       return "" ;
   }
   public String ac_x_info( Args args ) throws Exception {
       try{
          return _container.getInfo() ;

       }catch(Exception eee ){
           _log.warn(eee.toString(), eee);
           throw eee ;
       }
   }
   public static final String hh_scan_poolmanager = "[<poolManager>]" ;
   public String ac_scan_poolmanager_$_0_1( Args args ){
       final String poolManagerName = args.argc() == 0 ?
                                      "PoolManager":args.argv(0) ;

       _nucleus.newThread(
           new Runnable(){
              @Override
              public void run(){
                  _log.info("Starting pool manager ("+poolManagerName+") scan") ;
                  try{
                      collectPoolManagerPoolGroups(poolManagerName) ;
                  }catch(Exception ee ){
                      _log.warn("Problem in collectPoolManagerPoolGroups : "+ee);
                  }finally{
                      _log.info("collectPoolManagerPoolGroups done");
                  }
              }
           }
       ).start() ;
       return "Scan initialed (check pinboard for results)" ;
   }
   public static final String hh_addto_pgroup = "<poolGroup> [-class=<poolClass>] <poolName> | /poolNamePattern/ [...]" ;
   public String ac_addto_pgroup_$_2_999( Args args ){
       StringBuilder sb = new StringBuilder() ;
       String groupName = args.argv(0) ;
       String className = args.getOpt("class" ) ;

       if( className == null ) {
           className = "default";
       }
       if( className.length() == 0 ) {
           throw new
                   IllegalArgumentException("class name must not be \"\"");
       }

       synchronized( _container ){
           for( int i = 1 , n = args.argc() ; i < n ; i++ ){
               String name = args.argv(i) ;
               if( name.startsWith("/") ){
                   if( ( name.length() < 3  ) ||
                       ( ! name.endsWith("/")  ) ){ sb
                           .append("Not a valid pattern : ").append(name)
                           .append("\n") ; continue ;}
                   name = name.substring( 1 , name.length() - 1 ) ;
                   _container.addPattern( className , groupName , name , name ) ;
               }else{
                   _container.addPool( className , groupName , name ) ;
               }
           }
       }
       String result = sb.toString() ;
       if( result.length() != 0 ) {
           throw new
                   IllegalArgumentException(result);
       }

       return "";
   }
   private long _poolManagerTimeout = 30000L ;
   private void collectPoolManagerPoolGroups( String poolManager ) throws Exception {
       CellPath path = new CellPath(poolManager) ;
       CellMessage message = new CellMessage( path , "psux ls pgroup" ) ;
       message = sendAndWait( message , _poolManagerTimeout ) ;
       if( message == null ) {
           throw new
                   Exception("Request to " + poolManager + " timed out");
       }

       Object result = message.getMessageObject() ;
       if( result instanceof Exception ) {
           throw (Exception) result;
       }
       if( ! ( result instanceof Object [] ) ) {
           throw new
                   Exception("Illegal Reply on 'psux ls pgroup");
       }

       Object [] array = (Object [])result ;
       for (Object pgroup : array) {
           if (pgroup == null) {
               continue;
           }
           String pgroupName = pgroup.toString();
           String request = "psux ls pgroup " + pgroupName;
           message = new CellMessage(path, request);
           message = sendAndWait(message, _poolManagerTimeout);
           if (message == null) {
               _log.warn("Request to " + poolManager + " timed out");
               continue;
           }
           result = message.getMessageObject();
           if (!(result instanceof Object[])) {
               _log.warn("Illegal reply (1) on " + request + " " + result
                       .getClass().getName());
               continue;
           }
           Object[] props = (Object[]) result;
           if ((props.length < 3) ||
                   (!(props[0] instanceof String)) ||
                   (!(props[1] instanceof Object[]))) {
               _log.warn("Illegal reply (2) on " + request);
               continue;
           }
           Object[] list = (Object[]) props[1];
           synchronized (_container) {
               for (Object aList : list) {
                   _container.addPool("PoolManager", pgroupName, aList
                           .toString());
               }
           }
       }
   }
   private class CellInfoContainer {

       private Map<String, Map<String, Map<String, Object>>> _poolHash           = new HashMap<>() ;
       private Map<String, PatternEntry> _patternHash        = new HashMap<>() ;
       private Map<String, Map<String, Map<String, Object>>> _poolGroupClassHash = new HashMap<>() ;

       private void addInfo( String poolName , Object payload ){
           Map<String, Map<String, Object>> link = _poolHash.get(poolName);
           if( link != null ){
               for (Map<String, Object> table : link.values()) {
                   table.put(poolName, payload);
               }
           }
           for (PatternEntry patternEntry : _patternHash.values()) {
               if (patternEntry.pattern.matcher(poolName).matches()) {
                   link = patternEntry.linkMap;
                   for (Map<String, Object> table : link.values()) {
                       table.put(poolName, payload);
                   }
               }
           }
       }
       private void addPool( String groupClass , String group , String poolName ){
           Map<String, Map<String, Object>> poolGroupMap = _poolGroupClassHash.get(groupClass);
           if( poolGroupMap == null ) {
               _poolGroupClassHash
                       .put(groupClass, poolGroupMap = new HashMap<>());
           }

           Map<String, Object> table = poolGroupMap.get(group);
           if( table == null ) {
               poolGroupMap.put(group, table = new HashMap<>());
           }

           Map<String, Map<String, Object>> link = _poolHash.get(poolName);
           if( link == null ) {
               _poolHash.put(poolName, link = new HashMap<>());
           }

           link.put( groupClass+":"+group , table ) ;

       }
       private void removePool( String groupClass , String group , String poolName )
               throws NoSuchElementException , IllegalStateException {
           Map<String, Map<String, Object>> poolGroupMap = _poolGroupClassHash.get(groupClass);
           if( poolGroupMap == null ) {
               throw new
                       NoSuchElementException("groupClass not found : " + groupClass);
           }
           Map<String, Object> tableMap = poolGroupMap.get(group);
           if( tableMap == null ) {
               throw new
                       NoSuchElementException("group not found : " + group);
           }
           //
           //
           // now get the table map from the poolHash side
           //
           Map<String, Map<String, Object>> link = _poolHash.get(poolName);
           if( link == null ) {
               throw new
                       NoSuchElementException("pool not found : " + poolName);
           }

           tableMap = link.remove( groupClass+":"+group );
           if( tableMap == null ) {
               throw new
                       IllegalStateException("not found in link map : " + groupClass + ":" + group);
           }
           //
           // here we should check if both table maps are the same. But we wouldn't know
           // what to do if not.
           //
           // clear the possible content
           //
           tableMap.remove( poolName ) ;

       }
       //
       //   NOT FINISHED YET
       //
       public void removePoolGroup( String className , String groupName ){
           //
           // first remove pool group from poolGroupClass hash
           //
           Map<String, Map<String, Object>> groupMap = _poolGroupClassHash.get(className);
           if( groupMap == null ) {
               throw new
                       NoSuchElementException("not found : " + className);
           }

           Map<String, Object> tableMap = groupMap.remove(groupName);
           if( tableMap == null ) {
               throw new
                       NoSuchElementException("not found : " + groupName);
           }

           String d = className+":"+groupName ;

           for (Map.Entry<String, Map<String, Map<String, Object>>> entry :
                   _poolHash.entrySet()) {
               String poolName = entry.getKey();
               Map<String, Map<String, Object>> link = entry.getValue();

               for (Map.Entry<String, Map<String, Object>> domain :
                       link.entrySet()) {
                   String domainName = domain.getKey();
                   Map<String, Object> table = domain.getValue();
               }
           }
       }

       private class PatternEntry {
           private Map<String, Map<String, Object>> linkMap     = new HashMap<>() ;
           private Pattern pattern;
           private PatternEntry( Pattern pattern ){
               this.pattern = pattern ;
           }
           public String toString(){
               return pattern.pattern()+" "+linkMap.toString() ;
           }
       }
       private void addPattern( String groupClass , String group , String patternName , String pattern ){

           Map<String, Map<String, Object>> poolGroupMap = _poolGroupClassHash.get(groupClass);
           if( poolGroupMap == null ) {
               _poolGroupClassHash
                       .put(groupClass, poolGroupMap = new HashMap<>());
           }

           Map<String, Object> table = poolGroupMap.get(group);
           if( table == null ) {
               poolGroupMap.put(group, table = new HashMap<>());
           }

           PatternEntry patternEntry = _patternHash.get( patternName );

           if( patternEntry == null ){
               if( pattern == null ) {
                   throw new
                           IllegalArgumentException("patterName is new, so we need pattern");
               }

               _patternHash.put( patternName , patternEntry = new PatternEntry( Pattern.compile(pattern) ) ) ;
           }else{
               if( pattern != null ){
                   if( ! patternEntry.pattern.pattern().equals(pattern) ) {
                       throw new
                               IllegalArgumentException("Conflict in pattern (name in use with different pattern)");
                   }
               }
           }

           Map<String, Map<String, Object>> link = patternEntry.linkMap ;

           link.put( groupClass+":"+group , table ) ;

       }
       private void removePattern( String groupClass , String group , String patternName ){
           Map<String, Map<String, Object>> poolGroupMap = _poolGroupClassHash.get(groupClass);
           if( poolGroupMap == null ) {
               throw new
                       NoSuchElementException("groupClass not found : " + groupClass);
           }
           Map<String, Object> tableMap = poolGroupMap.get(group);
           if( tableMap == null ) {
               throw new
                       NoSuchElementException("group not found : " + group);
           }
           //
           //
           // now get the table map from the poolHash side
           //
           PatternEntry patternEntry = _patternHash.get( patternName );
           if( patternEntry == null ) {
               throw new
                       NoSuchElementException("patternName not found : " + patternName);
           }

           Map<String, Map<String, Object>> link = patternEntry.linkMap;

           tableMap = link.remove( groupClass+":"+group );
           if( tableMap == null ) {
               throw new
                       IllegalStateException("not found in link map : " + groupClass + ":" + group);
           }

           // if( link.size() == 0 )_patternHash.remove( patternName ) ;
           //
           // here we should check if both table maps are the same. But we wouldn't know
           // what to do if not.
           //
           // clear the possible content
           //
           List<String> toBeRemoved = new ArrayList<>() ;
           for (Object o : tableMap.keySet()) {
               String poolName = (String) o;
               if (patternEntry.pattern.matcher(poolName).matches()) {
                   toBeRemoved.add(poolName);
               }
           }
           for (String o : toBeRemoved) {
               tableMap.remove(o);
           }

       }
       private String getInfo(){
           StringBuffer sb = new StringBuffer() ;

           for (Map.Entry<String, Map<String, Map<String, Object>>> entry :
                   _poolGroupClassHash.entrySet()) {
               String className = entry.getKey();
               Map<String, Map<String, Object>> groupMap = entry.getValue();

               sb.append("Class : ").append(className).append("\n");

               for (Map.Entry<String, Map<String, Object>> groupEntry :
                       groupMap.entrySet()) {
                   String groupName = groupEntry.getKey();
                   Map<String, Object> tableMap = groupEntry.getValue();

                   sb.append("   Group : ").append(groupName).append("\n");

                   printTable(sb, "            ", tableMap);
               }
           }

           sb.append("PoolHash :\n");
           for (Map.Entry<String, Map<String, Map<String, Object>>> entry :
                   _poolHash.entrySet()) {
               String poolName = entry.getKey();
               Map<String, Map<String, Object>> link = entry.getValue();

               sb.append("  ").append(poolName).append("\n");

               for (Map.Entry<String, Map<String, Object>> domain :
                       link.entrySet()) {
                   String domainName = domain.getKey();
                   Map<String,Object> table = domain.getValue();

                   sb.append("     ").append(domainName).append("\n");

                   printTable(sb, "           ", table);
               }
           }
           sb.append("Pattern List :\n");
           for (Map.Entry<String, PatternEntry> entry : _patternHash.entrySet()) {
               String patternName = entry.getKey();
               PatternEntry patternEntry = entry.getValue();
               Pattern pattern = patternEntry.pattern;
               Map<String, Map<String, Object>> link = patternEntry.linkMap;

               sb.append("  ").append(patternName).append("(")
                       .append(pattern.pattern()).append(")").append("\n");

               for (Map.Entry<String, Map<String, Object>> domain : link
                       .entrySet()) {
                   String domainName = domain.getKey();
                   Map<String, Object> table = domain.getValue();

                   sb.append("     ").append(domainName).append("\n");

                   printTable(sb, "           ", table);
               }
           }
           return sb.toString();
       }
       private void printTable( StringBuffer sb , String prefix , Map<String, Object> table ){
           for (Map.Entry<String, Object> tableEntry :
                   table.entrySet()) {
               String pn = tableEntry.getKey();
               String tc = tableEntry.getValue().toString();

               sb.append(prefix).append(pn).append(" -> ").append(tc)
                       .append("\n");
           }
       }
   }
   private class RowInfoAdapter implements RowInfo {
       private String _primary;
       private String _secondary;
       private int [] [] _rows;
       private String _linkName;
       private RowInfoAdapter( String primary , String linkName , int [] [] rows ){
           _primary   = primary ;
           _linkName  = linkName ;
           _rows      = rows ;
       }

       @Override
       public String getPrimaryName() {
          return _primary ;
       }
       @Override
       public String getLinkName(){ return _linkName; }
       @Override
       public int[][] getRows() {
          return _rows ;
       }

       @Override
       public String getSecondaryName() {
          return _secondary ;
       }

   }
   private interface RowInfo {
       public int [] [] getRows() ;
       public String getPrimaryName() ;
       public String getSecondaryName() ;
       public String getLinkName();
   }
   private class CellQueryInfo implements RowInfo {

       private String   _destination;
       private long     _diff        = -1 ;
       private long     _start;
       private CellInfo _info;
       private CellMessage _message;
       private long        _lastMessage;
       private int [] []   _rows;

       private CellQueryInfo( String destination ){
           _destination = destination ;
	   //_destination = new CellPath(destination).getCellName();
           _message = new CellMessage( new CellPath(_destination) , "xgetcellinfo" ) ;
       }
       private String      getName(){ return _destination ; }
       private CellInfo    getCellInfo(){ return _info ; }
       private long        getPingTime(){ return _diff ; }
       private CellMessage getCellMessage(){
          _start = System.currentTimeMillis() ;
          return _message ;
       }
       private String getCellName(){ return _info.getCellName() ; }
       private String getDomainName(){ return _info.getDomainName() ; }
       private void infoArrived( CellInfo info ){
          _info = info ;
          _diff = ( _lastMessage = System.currentTimeMillis() ) - _start ;
          if( info instanceof PoolCellInfo ){
              PoolCellInfo poolInfo = (PoolCellInfo)info ;
              _rows =  decodePoolCostInfo( poolInfo.getPoolCostInfo() ) ;
          }
       }
       private boolean isOk(){
         return ( System.currentTimeMillis() - _lastMessage) < (3*_interval) ;
       }
       public String toString(){
           return "["+_destination+"("+(_diff/1000L)+")"+(_info==null?"NOINFO":_info.toString())+")]";
       }

       @Override
       public String getPrimaryName() { return getCellName() ; }
       @Override
       public int[][] getRows() { return _rows ; }
       @Override
       public String getSecondaryName() { return getDomainName() ; }
       @Override
       public String getLinkName() { return null ; }

   }

   public PoolInfoObserverV1( String name , String args )throws Exception {
      super( name ,PoolInfoObserverV1.class.getName(), args , false ) ;
      _args    = getArgs() ;
      _nucleus = getNucleus() ;
      try{
          String repeatString = null ;
          try{
              repeatString  = _args.getOpt("repeatHeader" ) ;
              _repeatHeader = Math.max( 0 , Integer.parseInt( repeatString ) ) ;
          }catch(Exception ee ){
              _log.warn( "Parsing error in repeatHader command : "+repeatString);
          }
          _log.info("Repeat header set to "+_repeatHeader);

          String instance = _args.getOpt("dCacheInstance");

          _dCacheInstance = ( instance == null ) ||
                            ( instance.length() == 0 ) ?
                            _dCacheInstance : instance ;

          for( int i = 0 ; i < _args.argc() ; i++ ) {
              addQuery(_args.argv(i));
          }

          ( _senderThread  = _nucleus.newThread( this , "sender" ) ).start() ;
          _log.info("Sender started" ) ;
          _log.info("Collector will be started a bit delayed" ) ;
          _nucleus.newThread(
              new Runnable(){
                 @Override
                 public void run(){
                     try {
                         Thread.currentThread().sleep(_interval/2);
                         ( _collectThread =
                           _nucleus.newThread( PoolInfoObserverV1.this , "collector" ) ).start() ;
                         _log.info("Collector now started as well");
                     } catch (InterruptedException e) {
                     }
                 }
              },
              "init"
          ).start() ;


      }catch(Exception ee ){
          _log.warn( "<init> of WebCollector reports : "+ee.getMessage(), ee);
          start() ;
          kill() ;
          throw ee ;
      }
      start() ;
   }

   private synchronized void addQuery( String destination ){
      if( _infoMap.get( destination ) != null ) {
          return;
      }
      _log.info( "Adding "+destination ) ;
      _infoMap.put( destination , new CellQueryInfo( destination ) ) ;
   }
   private synchronized void removeQuery( String destination ){
      _log.info( "Removing "+destination ) ;
      _infoMap.remove( destination ) ;
   }
   @Override
   public void run(){
      Thread x = Thread.currentThread() ;
      if( x == _senderThread ) {
          runSender();
      } else {
          runCollector();
      }
   }
   private void runCollector(){
     while( ! Thread.currentThread().interrupted() ){
        synchronized( _infoLock ){
            prepareTopologyMap( "topo-map.txt");
            preparePages() ;
        }
        try{
          Thread.currentThread().sleep(_interval) ;
        }catch(InterruptedException iie ){
           _log.info("Collector Thread interrupted" ) ;
           break ;
        }
     }

   }
   private void runSender(){
     while( ! Thread.currentThread().interrupted() ){
        _counter++ ;
        synchronized( _infoLock ){
            for (Object o : _infoMap.values()) {
                CellQueryInfo info = (CellQueryInfo) o;
                try {
                    sendMessage(info.getCellMessage());
                } catch (Exception ee) {

                }
            }
        }
        try{
          Thread.currentThread().sleep(_interval) ;
        }catch(InterruptedException iie ){
           _log.info("Sender Thread interrupted" ) ;
           break ;
        }
     }

   }
   @Override
   public void messageArrived( CellMessage message ){

      CellPath path = message.getSourcePath() ;

      String destination = path.getCellName();
      CellQueryInfo info = _infoMap.get(destination);
      if( info == null ){
         _log.debug("Unexpected reply arrived from : "+path ) ;
         return ;
      }
      Object reply = message.getMessageObject() ;
      //
      // generic cell info
      //
      if( reply instanceof CellInfo ){
          info.infoArrived((CellInfo)reply);
          synchronized( _container ){
              _container.addInfo( info.getName() , info ) ;
          }
      }
      //
      // special pool manager cell info (without the pool manager cellinfo
      // this cell won't do anything.
      //
      if( reply instanceof diskCacheV111.poolManager.PoolManagerCellInfo ){
        String [] poolList =
           ((diskCacheV111.poolManager.PoolManagerCellInfo)reply).getPoolList() ;
        synchronized( _infoLock ){
            for (String pool : poolList) {
                addQuery(pool);
            }
        }
      }
   }
   private int [] []  decodePoolCostInfo( PoolCostInfo costInfo ){

      try{

         PoolCostInfo.PoolQueueInfo mover     = costInfo.getMoverQueue() ;
         PoolCostInfo.PoolQueueInfo restore   = costInfo.getRestoreQueue() ;
         PoolCostInfo.PoolQueueInfo store     = costInfo.getStoreQueue() ;
         PoolCostInfo.PoolQueueInfo p2pServer = costInfo.getP2pQueue() ;
         PoolCostInfo.PoolQueueInfo p2pClient = costInfo.getP2pClientQueue() ;

         int [] [] rows = new int[5][] ;

         rows[0] = new int[3] ;
         rows[0][0] = mover.getActive() ;
         rows[0][1] = mover.getMaxActive() ;
         rows[0][2] = mover.getQueued() ;

         rows[1] = new int[3] ;
         rows[1][0] = restore.getActive() ;
         rows[1][1] = restore.getMaxActive() ;
         rows[1][2] = restore.getQueued() ;

         rows[2] = new int[3] ;
         rows[2][0] = store.getActive() ;
         rows[2][1] = store.getMaxActive() ;
         rows[2][2] = store.getQueued() ;

         if( p2pServer == null ){
            rows[3] = null ;
         }else{
            rows[3] = new int[3] ;
            rows[3][0] = p2pServer.getActive() ;
            rows[3][1] = p2pServer.getMaxActive() ;
            rows[3][2] = p2pServer.getQueued() ;
         }

         rows[4] = new int[3] ;
         rows[4][0] = p2pClient.getActive() ;
         rows[4][1] = p2pClient.getMaxActive() ;
         rows[4][2] = p2pClient.getQueued() ;

         return rows ;

      }catch( Exception e){
          _log.warn(e.toString(), e);
         return null ;
      }
   }

   @Override
   public void cleanUp(){
      _log.info( "Clean Up sequence started" ) ;
      //
      // wait for the worker to be done
      //
      _log.info( "Waiting for collector thread to be finished");
      if (_collectThread != null) {
          _collectThread.interrupt();
      }
      _senderThread.interrupt() ;
//      Dictionary context = _nucleus.getDomainContext() ;
//      context.remove( "cellInfoTable.html" ) ;

//      _log.info( "cellInfoTable.html removed from domain context" ) ;

      _log.info( "Clean Up sequence done" ) ;

   }
   @Override
   public void getInfo( PrintWriter pw ){
      pw.println("        Version : $Id: PoolInfoObserverV1.java,v 1.6 2006-06-05 08:51:27 patrick Exp $");
      pw.println("Update Interval : "+_interval+" [msec]");
      pw.println("        Updates : "+_counter);
      pw.println("       Watching : "+_infoMap.size()+" cells");
   }
   private void prepareTopologyMap( String topologyMapName ){

       StringBuilder sb = new StringBuilder() ;

       synchronized( _container ){
           for (Map.Entry<String, Map<String, Map<String, Object>>> entry :
                   _container._poolGroupClassHash.entrySet()) {
               String className = entry.getKey();
               Map<String, Map<String, Object>> groupMap = entry.getValue();

               sb.append(className).append("\n");

               for (Map.Entry<String, Map<String, Object>> groupEntry :
                       groupMap.entrySet()) {
                   String groupName = groupEntry.getKey();
                   Map<String, Object> tableMap = groupEntry.getValue();

                   sb.append("+").append(groupName).append("\n");

                   for (Object o2 : tableMap.keySet()) {
                       String poolName = o2.toString();
                       sb.append("++").append(poolName).append("\n");
                   }

               }
           }

       }
       _nucleus.setDomainContext( topologyMapName , sb.toString() ) ;

   }
   ///////////////////////////////////////////////////////////////////////////////////////////////////
   //
   //
   //       WEB PART
   //
   //
   private static final int HEADER_TOP    =   0 ;
   private static final int HEADER_MIDDLE =   1 ;
   private static final int HEADER_BOTTOM =   2 ;

   private String _bgImage    = "/images/bg.svg" ;
   private String _linkColor  = "red" ;
   private String _bgType     = "image" ;
   private String _bgColor    = "white" ;

   private String _webPrefix = "web" ;
   private int _repeatHeader = 20 ;
   private String [] _rowColors     = { "#efefef" , "#bebebe" } ;
   private String [] _rowTextColors      = { "black"  , "red"     , "#008080" } ;
   private String [] _rowTotalTextColors = { "black"  , "red"     , "white" } ;
   private String  _poolTableTotalColor = "#0000FF"  ;

   private String [] _poolTableHeaderColor = { "#0000FF" , "#0099FF" , "#00bbFF" , "#115259" } ;
   private String [] _poolTableHeaderTextColor = { "white" } ;
   private String [] _poolTableHeaderTitles = {
       "Movers" , "Restores" , "Stores" , "P2P-Server" , "P2P-Client"
   } ;
   private String _topIndex = "" ;
   /**
    * DO NOT TRY THESE COLORS
    *
    web set skin -rowtotalcolors=blue/green/yellow/#336688
    web set skin -rowcolors=blue/green/yellow/orange/black
    web set skin -headercolors=orange/blue/yellow/green/white
    *
    * DEFAULT COLORS
    *
    web set skin -rowcolors=black/red/#008080/#efefef/#bebebe
    web set skin -rowtotalcolors=black/red/white/#0000FF
    web set skin -headercolors=white/#0000FF/#0099FF/#00bbFF/#115259

    **/
   public static final String hh_web_set_skin = "[OPTIONS] ; please use 'help set skin' for more" ;
   public static final String fh_web_set_skin =
       "  web set skin [OPTIONS]\n"+
       "      OPTIONS\n"+
       "          -bgtype=image|color\n"+
       "          -bgcolor=&lt;background color&gt;\n"+
       "          -bgimage=&lt;background image&gt;\n"+
       "          -linkcolor=<link color>\n"+
       "          -rowcolors=<text1/text2/text3/bg1/bg2[/bg...]>\n" +
       "          -rowtotalcolors=<text1/text2/text3/bg1>\n" +
       "          -headercolors=<text1/bg1/bg2/bg3/bg4>\n" ;

   private String [] splitColorOptions( String colors ){
       StringTokenizer st = new StringTokenizer( colors , "/" ) ;
       ArrayList<String> list = new ArrayList<>() ;
       while( st.hasMoreTokens() ){
           list.add( st.nextToken() ) ;
       }
       return list.toArray(new String[list.size()]);
   }
   public String ac_web_set_skin_$_0( Args args ){

       String tmp = args.getOpt("bgtype") ;
       if( ( tmp != null ) && ( tmp.length() > 0 ) ) {
           _bgType = tmp;
       }

       tmp = args.getOpt("bgimage") ;
       if( ( tmp != null ) && ( tmp.length() > 0 ) ) {
           _bgImage = tmp;
       }

       tmp = args.getOpt("bgcolor") ;
       if( ( tmp != null ) && ( tmp.length() > 0 ) ) {
           _bgColor = tmp;
       }

       tmp = args.getOpt("linkcolor") ;
       if( ( tmp != null ) && ( tmp.length() > 0 ) ) {
           _linkColor = tmp;
       }

       tmp = args.getOpt("rowcolors") ;
       if( ( tmp != null ) && ( tmp.length() > 0 ) ){
           String [] x = splitColorOptions( tmp ) ;
           if( x.length < 5 ) {
               throw new
                       IllegalArgumentException("Not enought row colors (5)");
           }

           int base = 3;
           String [] y = new String[base] ;
           System.arraycopy(x, 0, y, 0, y.length);
           _rowTextColors = y ;

           int rowColorCount = x.length - y.length;
           y = new String[rowColorCount] ;
           for( int i = 0 , n = rowColorCount ; i < n ; i++ ) {
               y[i] = x[base + i];
           }
           _rowColors = y ;
       }

       tmp = args.getOpt("rowtotalcolors") ;
       if( ( tmp != null ) && ( tmp.length() > 0 ) ){
           String [] x = splitColorOptions( tmp ) ;
           if( x.length < 4 ) {
               throw new
                       IllegalArgumentException("Not enought row total colors (4)");
           }

           String [] y = new String[3] ;
           System.arraycopy(x, 0, y, 0, y.length);
           _rowTotalTextColors = y ;
           _poolTableTotalColor = x[y.length] ;
       }

       tmp = args.getOpt("headercolors") ;
       if( ( tmp != null ) && ( tmp.length() > 0 ) ){
           String [] x = splitColorOptions( tmp ) ;
           if( x.length < 5 ) {
               throw new
                       IllegalArgumentException("Not enought row colors (5)");
           }

           int base = 1 ;
           String [] y = new String[base] ;
           System.arraycopy(x, 0, y, 0, y.length);
           _poolTableHeaderTextColor = y ;

           int rowColorCount = x.length - y.length;
           y = new String[rowColorCount] ;
           for( int i = 0 , n = rowColorCount ; i < n ; i++ ) {
               y[i] = x[base + i];
           }
           _poolTableHeaderColor = y ;
       }
       return "" ;
   }
   private void preparePages(){
       try{
           prepareGroupPages() ;
       }catch(Exception ee ){
           _log.warn(ee.toString(), ee);
       }
   }
   private void printEagle( StringBuffer sb ){
       printEagle( sb , "dCache ONLINE" ) ;
   }

   private void printEagle( StringBuffer sb , String headTitle ){
      sb.append("<html>\n<head><title>").append(headTitle).append("</title></head>\n");
      sb.append("<body ") ;

      if( _bgType.equals("image") ) {
          sb.append(" background=\"").
                  append(_bgImage).
                  append("\" ");
      } else {
          sb.append(" bgcolor=\"").
                  append(_bgColor).
                  append("\" ");
      }


      String linkColor = "link=\""+_linkColor+"\"" ;
      sb.append(linkColor).
         append(" v").append(linkColor).
         append(" a").append(linkColor).
         append(">\n");

      sb.append( "<a href=\"/\"><img border=0 src=\"/images/eagleredtrans.gif\"></a>\n");
      sb.append( "<br><font color=red>Birds Home</font>\n" ) ;

      sb.append("<center><img src=\"/images/eagle-grey.gif\"></center>\n");
      sb.append("<p>\n");
   }
   private String createTopIndexTable( Map<String, String> classMap ){
       StringBuffer sb = new StringBuffer() ;
       printDirectoryTable( classMap , sb , 6 , " " ) ;
       return sb.toString() ;
   }
   private void prepareGroupPages(){

       Map<String, String> classMap = new HashMap<>() ;

       for (Map.Entry<String, Map<String, Map<String, Object>>> entry :
               _container._poolGroupClassHash.entrySet()) {
           String className = entry.getKey();
           Map<String, Map<String, Object>> groupMap = entry.getValue();

           Map<String, Object> totalGroupMap = new HashMap<>();
           for (Map.Entry<String, Map<String, Object>> groupEntry : groupMap.entrySet()) {
               String groupName = groupEntry.getKey();
               Map<String, Object> tableMap = groupEntry.getValue();

               int[][] total = calculateSum(tableMap, 5);

               StringBuffer sb = new StringBuffer();

               prepareBasicTable(sb, className, groupName, tableMap, total);

               String webPageName = storeWebPage(sb, className + "-" + groupName + ".html");

               totalGroupMap
                       .put(groupName, new RowInfoAdapter(groupName, webPageName, total));

           }
           StringBuffer sbi = new StringBuffer();


           int[][] classTotal = calculateSum(totalGroupMap, 5);

           prepareBasicTable(sbi, "index", className, totalGroupMap, classTotal);

           String indexWebName = storeWebPage(sbi, "index-" + className + ".html");

           classMap.put(className, indexWebName);

       }

       _topIndex = createTopIndexTable( classMap ) ;

       printMainIndexPage( classMap ) ;
   }
   private void printMainIndexPage( Map<String, String> map ){
       StringBuffer sb = new StringBuffer() ;
       printEagle( sb , "dCache ONLINE : Pool Info Main Index");

       sb.append( "<h1>Pool Info Summary Page for ").append(_dCacheInstance).append("</h1>\n");
       sb.append("<br><br><blockquote>");
       sb.append( "<h2>Pool Group Classes</h2>\n");

       printDirectoryTable( map , sb , 4 , null ) ;
       sb.append("</blockquote>\n");
       sb.append("<br><hr><br><address>").append(new Date().toString()).append("</address>");
       sb.append("</body></html>\n") ;

       storeWebPage( sb , "index.html" ) ;
   }
   private void printDirectoryTable( Map<String, String> map , StringBuffer sb , int rows , String extras ){
       sb.append("<table border=0 cellpadding=4 cellspacing=4 width=\"90%\">\n");
       int item = 0 ;
       int percent = 100 / rows ;
       extras = extras == null ? " bgcolor=\"gray\" " : extras ;
       String itemDeco = "<td width=\""+percent+"%\" "+extras+" align=center>" ;
       for( Iterator<Map.Entry<String,String>> n = map.entrySet().iterator() ; n.hasNext() ; item++){
           if( item % rows == 0 ) {
               sb.append("<tr>\n");
           }
           sb.append(itemDeco);

           Map.Entry<String, String> e = n.next();
           sb.append("<h3>").
              append("<a href=\"").
              append(e.getValue()).
              append("\" style=\"text-decoration: none\">").
              append(e.getKey()).append("</a></h3></td>\n");

           if( item % rows == (rows-1) ) {
               sb.append("</tr>\n");
           }
       }
       if( item%rows!= 0 ) {
           for (int i = item % rows; i < rows; i++) {
               sb.append(itemDeco).append("&nbsp;</td>\n");
               if (item % rows == (rows - 1)) {
                   sb.append("</tr>\n");
               }
           }
       }
       sb.append("</table>\n");
   }
   private String storeWebPage( StringBuffer sb , String pageName ){
       String webPageName = _webPrefix+"-"+pageName ;
       _nucleus.setDomainContext( webPageName , sb.toString() ) ;
       return webPageName ;
   }
   private void prepareBasicTable( StringBuffer sb , String className , String groupName , Map<String, Object> tableMap , int [] [] total ){

       _log.info("prepareBasicTable for "+className+" "+groupName+" map size "+tableMap.size() ) ;

       printEagle( sb , "dCache ONLINE : Pool Queues "+className+"/"+groupName) ;
       sb.append("<hr><center>").append(_topIndex).append("</center><hr>");
       sb.append("<center><table border=0 width=\"90%%\">\n").
          append("<tr><td><h1><font color=black>").append(groupName).append("</font></h1></td></tr>\n").
          append("<tr><td><h4><font color=black> class = ").append(className).append("</font></h4></td></tr>\n").
          append("</table></center>\n") ;


            printPoolQueueTable( sb , tableMap , total ) ;

       sb.append("<br><hr><br><address>").append(new Date().toString()).append("</address>");
       sb.append("</body>\n</html>\n") ;

       _log.info("prepareBasicTable : ready for "+className+" "+groupName);


   }
   private void printPoolActionTableHeader( StringBuffer sb , int position ){

      String [] colors      = _poolTableHeaderColor ;
      String [] textColors  = _poolTableHeaderTextColor ;
      String [] titles      = { "Active" , "Max" , "Queued" } ;
      String [] titleColors = { colors[1] , colors[1] , colors[2] } ;

      int [] regularProgram = { 0 , 1 , 2 , 3 } ;
      int [] reverseProgram = { 0 , 3 , 2 , 1 } ;
      int [] middleProgram  = { 0 , 3 , 2 , 1 , 2 , 3 } ;

      int [] program = position == HEADER_TOP    ? regularProgram :
                       position == HEADER_MIDDLE ? middleProgram  : reverseProgram ;

       for (int instruction : program) {

           switch (instruction) {
           case 0:
               int rowspan = program.length / 2;
               sb.append("<tr>\n");
               sb.append("<td rowspan=").append(rowspan)
                       .append(" valign=center bgcolor=\"").append(colors[3]).
                       append("\" align=center><font color=\"")
                       .append(textColors[0]).append("\">").
                       append("CellName").append("</font></td>\n");

               sb.append("<td rowspan=").append(rowspan)
                       .append(" valign=center bgcolor=\"").append(colors[3]).
                       append("\" align=center><font color=\"")
                       .append(textColors[0]).append("\">").
                       append("DomainName").append("</font></td>\n");
               break;
           case 1:
               for (String title : _poolTableHeaderTitles) {
                   sb.append("<td colspan=3 bgcolor=\"").append(colors[3]).
                           append("\" align=center><font color=\"")
                           .append(textColors[0]).
                           append("\">").append(title)
                           .append("</font></td>\n");
               }
               sb.append("</tr>\n");
               break;
           case 2:
               sb.append("<tr>");
               break;
           case 3:

               for (String title : _poolTableHeaderTitles) {
                   for (int m = 0; m < 3; m++) {
                       sb.append("<td bgcolor=\"").
                               append(titleColors[m]).
                               append("\" align=center><font color=\"").
                               append(textColors[0]).
                               append("\">").
                               append(titles[m]).
                               append("</font></td>\n");
                   }
               }
               sb.append("</tr>\n");
               break;
           }
       }
   }
   private int [] [] calculateSum( Map<String, Object> tableMap , int size ){
       int [] [] total = new int[size][] ;
       for( int j = 0 ; j < total.length ; j++ ) {
           total[j] = new int[3];
       }
       for (Object o : tableMap.values()) {
           int[][] status = ((RowInfo) o).getRows();
           if (status == null) {
               continue;
           }
           for (int j = 0; j < total.length; j++) {
               for (int l = 0; l < total[j].length; l++) {
                   if (status[j] != null) {
                       total[j][l] += status[j][l];
                   }
               }
           }
       }
       return total ;
   }
   private synchronized void printPoolQueueTable( StringBuffer sb ,  Map<String, Object> tableMap , int [][] total ){
       //
       TreeMap<String, Object> tree = new TreeMap<>( tableMap ) ;
       //
       //
       // table header
       //
       sb.append( "<center>\n<table border=1 cellpadding=4 cellspacing=0 width=\"90%\">\n");
       //
       //  header title
       //
       printPoolActionTableHeader( sb , HEADER_TOP ) ;
       //
       //  print sum of all rows
       //
       if( total != null ) {
           printPoolActionTableTotals(sb, total);
       }

       Iterator<Object> n = tree.values().iterator() ;
       int i;
       for( i = 1  ; n.hasNext() ; ){

           RowInfo e = (RowInfo) n.next();

           int [] [] rows = e.getRows() ;
           if( rows == null ) {
               continue;
           }

           printPoolActionRow( sb , e.getPrimaryName() , e.getSecondaryName() , e.getLinkName() ,
                               rows , _rowColors[i%_rowColors.length] ,
                               _rowTextColors) ;

           if( ( _repeatHeader != 0 ) && ( i % _repeatHeader ) == 0 ) {
               printPoolActionTableHeader(sb, HEADER_MIDDLE);
           }
           i++ ;
       }
       if( total != null ) {
           printPoolActionTableTotals(sb, total);
       }
       if( ( i % _repeatHeader ) > ( _repeatHeader / 2 ) ) {
           printPoolActionTableHeader(sb, HEADER_BOTTOM);
       }

       sb.append("</table></center>");
   }
   private void printPoolActionTableTotals( StringBuffer sb , int [] [] total ){
       printPoolActionRow( sb , "Total" , null , total , _poolTableTotalColor , _rowTotalTextColors ) ;
   }
   private void printPoolActionRow( StringBuffer sb ,
                                    String firstLabel , String secondLabel ,
                                    int [] [] rows ,
                                    String color  ,
                                    String [] rowTextColors ){


         printPoolActionRow( sb , firstLabel , secondLabel , null , rows , color , rowTextColors ) ;

   }
   private void printPoolActionRow( StringBuffer sb ,
                                    String firstLabel , String secondLabel , String labelLink ,
                                    int [] [] rows ,
                                    String color  ,
                                    String [] rowTextColors ){

     sb.append("<tr>\n");

     String colspan = secondLabel == null ? "colspan=2" : "" ;
     sb.append("<td bgcolor=\"").append(color).append("\" align=center ").append(colspan).append(">");
     if( labelLink == null ) {
         sb.append(firstLabel);
     } else {
         sb.append("<a href=\"").append(labelLink).append("\">")
                 .append(firstLabel).append("</a>");
     }

     sb.append("</td>\n") ;

     if( secondLabel != null ){
        sb.append("<td bgcolor=\"").append(color).append("\" align=center>")
                .append(secondLabel).append("</td>\n");
     }
     for( int j = 0 ; j < 5 ; j++ ) {
        if( rows[j] == null ){
           sb.append("<td bgcolor=").append(color)
                   .append(" align=center colspan=3>").
             append("<font color=\"").append(rowTextColors[1]).append("\">").
             append("Not available").
             append("</font>").
             append("</td>\n") ;

        }else{
           for( int i = 0 ; i < rows[j].length ; i++ ) {
               sb.append("<td bgcolor=").append(color).append(" align=center>").
                  append("<font color=\"").
                  append(i<2?rowTextColors[0]:rows[j][2]>0?rowTextColors[1]:rowTextColors[2]).
                  append("\">").append(rows[j][i]).append("</font>").
                  append("</td>\n") ;
           }
        }
     }

     sb.append("</tr>\n");


   }

}
