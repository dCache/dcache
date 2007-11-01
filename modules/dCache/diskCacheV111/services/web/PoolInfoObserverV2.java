// $Id: PoolInfoObserverV2.java,v 1.2 2006-06-08 15:23:27 patrick Exp $Cg

package diskCacheV111.services.web ;

import java.util.* ;
import java.util.regex.Pattern ;
import java.text.* ;
import java.io.* ;
import dmg.cells.nucleus.* ;
import dmg.util.* ;


import diskCacheV111.pools.* ;

public class PoolInfoObserverV2 extends CellAdapter implements Runnable {

   private CellNucleus _nucleus        = null ;
   private Args        _args           = null ;
   private HashMap     _infoMap        = new HashMap() ;
   private Object      _lock           = new Object() ;
   private Object      _infoLock       = new Object() ;
   private Thread      _collectThread  = null ;
   private Thread      _senderThread   = null ;
   private long        _interval       = 60000 ;
   private long        _counter        = 0 ;
   private boolean     _debug          = false ;
   private String      _dCacheInstance = "?" ;
   private File        _configFile     = null ;
   
   private long    _poolManagerTimeout     = 30000L ;
   private Object  _poolManagerUpdateLock  = new Object() ;
   private boolean _poolManagerUpdating    = false ;
   private long    _configFileLastModified = 0L ;
   private long    _poolManagerNextQuery   = 0L ;
   private long    _poolManagerUpdate      = 5L * _interval ;
   private String  _poolManagerName        = "PoolManager" ;
   
   
   private CellInfoContainer _container = new CellInfoContainer() ;
   private SimpleDateFormat  _formatter = new SimpleDateFormat ("MM/dd hh:mm:ss");
   
   public String hh_x_addto_pgroup = "<groupName>  <name> [-pattern[=<pattern>}] [-class=<className>]";
   public String ac_x_addto_pgroup_$_2( Args args ) throws Exception {
       try{
           String groupClass = (String)args.getOpt("view");
           groupClass = groupClass == null ? "default" : groupClass ;
           String groupName = args.argv(0) ; 
           String name      = args.argv(1) ;
           String pattern   = args.getOpt("pattern");
           
           if( pattern == null ){
               _container.addPool( groupClass , groupName , name ) ;           
           }else{
               if( pattern.length() == 0 )pattern = name ;
               _container.addPattern( groupClass , groupName , name , pattern ) ;
           }

           return "" ;
       }catch(Exception e){
           esay(e);
           throw e ;
       }
   }
   public String hh_x_add = "<pool> <poolValue> # debug only";
   public String ac_x_add_$_2( Args args ) throws Exception {
       try{
       _container.addInfo( args.argv(0) , args.argv(1) ) ;
       }catch(Exception ee ){
           esay(ee);
           throw ee;
       }
       return "" ;
   }
   public String hh_x_removefrom = "<poolGroup> [-class=<className>] <name> [-pattern]" ;
   public String ac_x_removefrom_$_2( Args args ) throws Exception {
       try{
           String groupClass = (String)args.getOpt("view");
           groupClass = groupClass == null ? "default" : groupClass ;
           String poolGroup = args.argv(0) ;
           String name      = args.argv(1) ;
           if( args.getOpt("pattern") == null ){
               _container.removePool( groupClass , poolGroup , name ) ;
           }else{
               _container.removePattern( groupClass , poolGroup , name ) ;
           }
       }catch(Exception ee ){
           esay(ee);
           throw ee ;
       }
       return "" ;
   }
   public String ac_x_info( Args args ) throws Exception {
       try{
          return _container.getInfo() ;
   
       }catch(Exception eee ){
           esay(eee);
           throw eee ;
       }
   }
   public String hh_scan_poolmanager = "[<poolManager>]" ;
   public String ac_scan_poolmanager_$_0_1( Args args ){
       final String poolManagerName = args.argc() == 0 ?
                                      "PoolManager":args.argv(0) ;
                                      
       _nucleus.newThread(
           new Runnable(){
              public void run(){
                  say("Starting pool manager ("+poolManagerName+") scan") ;
                  try{
                      collectPoolManagerPoolGroups(poolManagerName) ;
                  }catch(Exception ee ){
                      esay("Problem in collectPoolManagerPoolGroups : "+ee);
                  }finally{
                      say("collectPoolManagerPoolGroups done");
                  }
              }
           }
       ).start() ;
       return "Scan initialed (check pinboard for results)" ;
   }
   public String hh_addto_pgroup = "<poolGroup> [-class=<poolClass>] <poolName> | /poolNamePattern/ [...]" ;
   public String ac_addto_pgroup_$_2_999( Args args ){
       StringBuffer  sb = new StringBuffer() ;
       String groupName = args.argv(0) ;
       String className = args.getOpt("view" ) ;
       
       if( className == null )className = "default" ;
       if( className.length() == 0 )
           throw new
           IllegalArgumentException("class name must not be \"\"");
       
       synchronized( _container ){
           for( int i = 1 , n = args.argc() ; i < n ; i++ ){
               String name = args.argv(i) ;
               if( name.startsWith("/") ){
                   if( ( name.length() < 3  ) || 
                       ( ! name.endsWith("/")  ) ){ sb.append("Not a valid pattern : "+name ).append("\n") ; continue ;}
                   name = name.substring( 1 , name.length() - 1 ) ;
                   _container.addPattern( className , groupName , name , name ) ;
               }else{
                   _container.addPool( className , groupName , name ) ;
               }
           }
       }
       String result = sb.toString() ;
       if( result.length() != 0 )
           throw new
           IllegalArgumentException(result);
       
       return "";
   }
   private void collectPoolManagerPoolGroups( String poolManager ) throws Exception {
       synchronized( _poolManagerUpdateLock ){    
            if( _poolManagerUpdating ){
               say("PoolManager update already in progress");
               return ;
            }
            _poolManagerUpdating = true ;
       }
       try{
          _collectPoolManagerPoolGroups( poolManager );
       }finally{
          synchronized( _poolManagerUpdateLock ){
             _poolManagerUpdating = false ;
          }
       }
   }
   private void _collectPoolManagerPoolGroups( String poolManager ) throws Exception {
       CellPath path = new CellPath(poolManager) ;
       CellMessage message = new CellMessage( path , "psux ls pgroup" ) ;
       message = sendAndWait( message , _poolManagerTimeout ) ;
       if( message == null )
           throw new
           Exception("Request to "+poolManager+" timed out");
       
       Object result = message.getMessageObject() ;
       if( result instanceof Exception )throw (Exception)result ;
       if( ! ( result instanceof Object [] ) )
           throw new
           Exception("Illegal Reply on 'psux ls pgroup");
       
       Object [] array = (Object [])result ;
       for( int i = 0 , n = array.length ; i < n ; i++ ){
           if( array[i] == null )continue ;
           String pgroupName = array[i].toString() ;
           String request = "psux ls pgroup "+pgroupName ;
           message = new CellMessage( path , request ) ;
           message = sendAndWait( message , _poolManagerTimeout ) ;
           if( message == null ){
               esay("Request to "+poolManager+" timed out" ) ;
               continue ;
           }
           result = message.getMessageObject() ;
           if( ! ( result instanceof Object [] ) ){
               esay("Illegal reply (1) on "+request+ " "+result.getClass().getName() ) ;
               continue ;
           }
           Object [] props = (Object [])result ;
           if( ( props.length < 3 ) ||
               ( ! ( props[0] instanceof String ) ) ||
               ( ! ( props[1] instanceof Object [] ) ) ){                   
                  esay("Illegal reply (2) on "+request ) ;
                  continue ;
           }
           Object [] list = (Object [])props[1] ;
           synchronized( _container ){
               for( int j = 0 , m = list.length ; j < m ; j++ ){
                   _container.addPool( "PoolManager" , pgroupName ,  list[j].toString() ) ;
               }
           }
       }
   }
   private class CellQueryInfo {
   
       private String      _destination = null ;
       private long        _diff        = -1 ;
       private long        _start       = 0 ;
       private CellInfo    _info        = null ;
       private CellMessage _message     = null ;
       private long        _lastMessage = 0 ;
       
       private CellQueryInfo( String destination ){
           _destination = destination ;
           _message = new CellMessage( new CellPath(_destination) , "xgetcellinfo" ) ;
       }
       private String      getName(){ return _destination ; }
       private CellInfo    getCellInfo(){ return _info ; }
       private long        getPingTime(){ return _diff ; }
       private long        getArrivalTime(){ return _lastMessage ; }
       private CellMessage getCellMessage(){
          _start = System.currentTimeMillis() ;
          return _message ;
       }
       private String getCellName(){ return _info.getCellName() ; }
       private String getDomainName(){ return _info.getDomainName() ; }
       private void infoArrived( CellInfo info ){
          _info = info ;
          _diff = ( _lastMessage = System.currentTimeMillis() ) - _start ;
       }
       private boolean isOk(){ 
         return ( System.currentTimeMillis() - _lastMessage) < (3*_interval) ;
       }
       public String toString(){
           return "["+_destination+"("+(_diff/1000L)+")"+(_info==null?"NOINFO":_info.toString())+")]";
       }
       
   }

   public PoolInfoObserverV2( String name , String args )throws Exception {
      super( name ,PoolInfoObserverV1.class.getName(), args , false ) ;
      _args    = getArgs() ;
      _nucleus = getNucleus() ;
      try{
          _debug = _args.getOpt("debug") != null ;

          String instance = _args.getOpt("dCacheInstance");
      
          _dCacheInstance = ( instance == null ) || 
                            ( instance.length() == 0 ) ?
                            _dCacheInstance : instance ;
          
          String configName = _args.getOpt("config") ;
          if( ( configName != null ) && ( ! configName.equals("") ) )_configFile = new File(configName) ;
          
          String intervalString = _args.getOpt("pool-refresh-time") ;
          if( intervalString != null ){
             try{
                _interval = Long.parseLong(intervalString) * 1000L ;
             }catch(Exception iee){}
              
          }
          intervalString = _args.getOpt("poolManager-refresh-time") ;
          if( intervalString != null ){
             try{
                _poolManagerUpdate = Long.parseLong(intervalString) * 1000L ;
             }catch(Exception iee){}
              
          }
          for( int i = 0 ; i < _args.argc() ; i++ )addQuery( _args.argv(i) )  ;
          
          ( _senderThread  = _nucleus.newThread( this , "sender" ) ).start() ;
          
          say("Sender started" ) ;
          
          say("Collector will be started a bit delayed" ) ;
          
          _nucleus.newThread( new DoDelayedOnStartup() , "init" ).start() ;
                    
      }catch(Exception ee ){
          esay( "<init> of WebCollector reports : "+ee.getMessage());
          esay(ee);
          start() ;
          kill() ;
          throw ee ;
      }  
      start() ;
   }
   private class DoDelayedOnStartup implements Runnable {
       public void run(){
          /*
           * wait for awhile before starting startup processes
           */
          say("Collector will be delayed by "+_interval/2000L+" Seconds");
          
          try{ Thread.currentThread().sleep(_interval/2) ;}
          catch(Exception ee){ return ; }
          
          _collectThread =  _nucleus.newThread( PoolInfoObserverV2.this , "collector" ) ;
          _collectThread.start() ;
          
          say("Collector now started as well");
          say("Getting pool groups from PoolManager");
          
          try{
              collectPoolManagerPoolGroups("PoolManager") ;
          }catch(Exception ee ){
              esay("Problem in collectPoolManagerPoolGroups : "+ee);
          }
          say("collectPoolManagerPoolGroups done");
       }
   }
   private boolean loadConfigFile(){
   
      if( ( _configFile == null ) || ( ! _configFile.exists() ) || ( ! _configFile.canRead() ) )return false ;
      
      long accessTime = _configFile.lastModified() ;
      
      if( _configFileLastModified >= accessTime )return false ;
      
      /*
       * save current setup
       */
      CellInfoContainer c = _container ;
      /*
       * install new (empty) setup
       */
      _container = new CellInfoContainer() ;
      
      try{         
         BufferedReader br = new BufferedReader( new FileReader( _configFile ) ) ;
         try{
            String line = null ;
            while( (line = br.readLine() ) != null ){
                say(line);
                command(line);
            } 
         }catch(Exception ee ){
            esay( ee ) ;
         }finally{
            try{ br.close() ; }catch(Exception ie){}
         }
         _configFileLastModified = accessTime ;
      }catch(Exception eee ){
         esay("Couldn't open "+_configFile+" due to "+eee); 
         _container = c ;
         return false ;
      }
             
      return true ;
   }
   public void dsay( String message ){
      pin(message);
      if( _debug )super.say(message);
   }
   public void say( String message ){
      pin(message);
      super.say(message);
   }
   public void esay( String message ){
      pin(message);
      super.say(message);
   }
   private synchronized void addQuery( String destination ){
      if( _infoMap.get( destination ) != null )return ;
      say( "Adding "+destination ) ;
      _infoMap.put( destination , new CellQueryInfo( destination ) ) ;
      return ;
   }
   private synchronized void removeQuery( String destination ){
      say( "Removing "+destination ) ;
      _infoMap.remove( destination ) ;
      return ;
   }
   public void run(){
      Thread x = Thread.currentThread() ;
      if( x == _senderThread )runSender() ;
      else runCollector() ;
   }
   private void runCollector(){
     while( ! Thread.currentThread().interrupted() ){
        synchronized( _infoLock ){
            dsay("Updating info in context poolgroup-map.ser");
            flushTopologyMap( "poolgroup-map.ser");
            dsay("Updating info in context Done");
        }
        try{
          Thread.currentThread().sleep(_interval) ;
        }catch(InterruptedException iie ){
           say("Collector Thread interrupted" ) ;
           break ;
        }
     }
   
   }
   private void runSender(){
     
     _poolManagerNextQuery = System.currentTimeMillis() + _poolManagerUpdate ;

     while( ! Thread.currentThread().interrupted() ){
     
        _counter++ ;
        
        synchronized( _infoLock ){
        
           Iterator i = _infoMap.values().iterator() ;
           while( i.hasNext() ){
              CellQueryInfo info = (CellQueryInfo)i.next() ;
              try{
                 CellMessage cellMessage = info.getCellMessage() ;
                 dsay("Sending message to "+cellMessage.getDestinationPath() ) ;
                 sendMessage( info.getCellMessage() ) ;
              }catch( Exception ee ){
                 esay("Problem in sending message : "+ee ) ;
              }
           }
        }
        
        /*
         * if a new ConfigFile has been loaded we need to reget the
         * the poolManager pool groups.
         */
        long now = System.currentTimeMillis() ;
        if( loadConfigFile() || ( _poolManagerNextQuery < now ) ){
           try{
              dsay("collectPoolManagerPoolGroups started on "+_poolManagerName);
              collectPoolManagerPoolGroups(_poolManagerName);
              _poolManagerNextQuery = now + _poolManagerUpdate ;
           }catch(Exception ee ){
              esay("Problems reported by 'collectPoolManagerPoolGroups' : "+ee ) ;
           }
        }
        try{
          Thread.currentThread().sleep(_interval) ;
        }catch(InterruptedException iie ){
           say("Sender Thread interrupted" ) ;
           break ;
        }
     }
   
   }
   public String hh_show_context = "<contextName>" ;
   public String ac_show_context_$_1( Args args ){
      String contextName = args.argv(0) ;
      Object o = _nucleus.getDomainContext( contextName ) ;      
      if( o == null )
         throw new
         IllegalArgumentException("Context not found : "+contextName);
         
      return o.toString();
   }
   public void messageArrived( CellMessage message ){
       
      CellPath path = message.getSourcePath() ;
      Object  reply = message.getMessageObject() ;
      
      say("Message arrived : "+reply.getClass().getName()+" from "+path ) ;
      String destination = (String)path.getCellName() ;
      CellQueryInfo info = (CellQueryInfo)_infoMap.get(destination);
      if( info == null ){
         dsay("Unexpected reply arrived from : "+path ) ;
         return ;
      }
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
           for( int i = 0 ; i < poolList.length ; i++ )addQuery(poolList[i]);
        }
      }
   }

   public void cleanUp(){
      say( "Clean Up sequence started" ) ;
      //
      // wait for the worker to be done
      //
      say( "Waiting for collector thread to be finished");
      _collectThread.interrupt() ;
      _senderThread.interrupt() ;
      
      say( "Clean Up sequence done" ) ;
   
   }
   public void getInfo( PrintWriter pw ){
      pw.println("                    Version : $Id: PoolInfoObserverV2.java,v 1.2 2006-06-08 15:23:27 patrick Exp $");
      pw.println("       Pool Update Interval : "+_interval+" [msec]");
      pw.println("PoolManager Update Interval : "+_poolManagerUpdate+" [msec]");
      pw.println(" Update Counter : "+_counter);
      pw.println("       Watching : "+_infoMap.size()+" cells");
      pw.println("     Debug Mode : "+(_debug?"ON":"OFF"));
      
   }
   private Map scanTopologyMap( String topoMapString ){
      StringTokenizer st = new StringTokenizer( topoMapString , "\n" ) ;
      Map allClasses   = new HashMap() ;
      Map currentClass = null ;
      Map currentGroup = null ;
      while( st.hasMoreTokens() ){
         String line = st.nextToken() ;
         if( line.length() == 0 )continue ;
         if( line.startsWith("++") ){
             if( currentGroup == null )continue ;
             currentGroup.put( line.substring(2) , null ) ;
         }else if( line.startsWith("+") ){
             if( currentClass == null )continue ;
             currentClass.put( line.substring(1) , currentGroup = new HashMap() ) ;
         }else{
             allClasses.put( line.trim() , currentClass = new HashMap() ) ;
         }
      }
      return allClasses ;
   }
   private void flushTopologyMap( String topologyMapName ){

      PoolCellQueryContainer container = new PoolCellQueryContainer() ;
      
      synchronized( _infoLock ){
         for( Iterator it = _infoMap.values().iterator() ; it.hasNext() ; ){
         
             CellQueryInfo info     = (CellQueryInfo)it.next() ;
             CellInfo      cellInfo = info.getCellInfo() ;
             if( ! ( cellInfo instanceof PoolCellInfo ) )continue ;
             
             container.put( 
             
                info.getCellName() , 
                new PoolCellQueryContainer.PoolCellQueryInfo( (PoolCellInfo) cellInfo ,
                                       info.getPingTime() ,
                                       info.getArrivalTime() ) 
                                       
             ) ;
         }
      }
      Map allClasses = _container.createExternalTopologyMap() ;
      for( Iterator classes = allClasses.values().iterator() ; classes.hasNext() ; ){
           Map    groupMap  = (Map)classes.next() ;
           for( Iterator groups = groupMap.values().iterator() ; groups.hasNext() ; ){
              Map    tableMap  = (Map)groups.next() ;
              for( Iterator poolNames = tableMap.keySet().iterator() ; poolNames.hasNext() ; ){
                   String poolName = poolNames.next().toString() ;
                   tableMap.put( poolName , container.getInfoByName(poolName) ) ;
              }
              
           }
      }
      container.setTopology( allClasses ) ;
      _nucleus.setDomainContext( topologyMapName , container ) ;      
       
   }
 
}
