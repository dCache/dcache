// $Id: PoolManagerV5.java,v 1.32 2006-05-19 21:38:35 patrick Exp $ 

package diskCacheV111.poolManager ;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;
import  diskCacheV111.pools.PoolV2Mode;

import  dmg.cells.nucleus.*;
import  dmg.util.*;

import  java.text.*;
import  java.util.*;
import  java.io.*;
import  java.lang.reflect.* ;

public class PoolManagerV5 extends CellAdapter {
    
    private String      _cellName = null;
    private Args        _args     = null;
    private CellNucleus _nucleus  = null;
    
    private int  _writeThreads     = 0 ;
    private int  _readThreads      = 0 ;

    private int _counterPoolUp         = 0 ;
    private int _counterPoolDown       = 0 ;
    private int _counterSelectWritePool= 0 ;
    private int _counterSelectReadPool = 0 ;
    
    private String  _pnfsManagerName   = "PnfsManager";
    private String  _selectionUnitName = "diskCacheV111.poolManager.PoolSelectionUnitV2" ;
    private String  _setupFileName     = null ;
    private HashMap _readHandlerList   = new HashMap() ;
    private Object  _readHandlerLock   = new Object() ;
    
    private PnfsHandler       _pnfsHandler   = null ;
    private PoolSelectionUnit _selectionUnit = null ;
    private PoolMonitorV5     _poolMonitor   = null ;
    
    private long _interval         = 15 * 1000;
    private long _pnfsTimeout      = 15 * 1000;
    private long _readPoolTimeout  = 15 * 1000;
    private long _poolFetchTimeout = 5 * 24 * 3600 * 1000;
    private long _writePoolTimeout = 15 * 1000;
    private long _poolTimeout      = 15 * 1000;
    
    private CostModule   _costModule   = null ;
    private PoolOperator _poolOperator = null ;
    private CellPath     _poolStatusRelayPath = null ;
    private double _spaceCostFactor       = 1.0 ;
    private double _performanceCostFactor = 1.0 ;

    private Object _setupLock             = new Object() ;
    
    private RequestContainerV5 _requestContainer = null ;
    private WatchdogThread     _watchdog         = null ;
    private PartitionManager   _partitionManager = null ;
    
    private boolean _sendCostInfo = false ;                   //VP

    public PoolManagerV5( String cellName , String args ) throws Exception {
	super( cellName , PoolManagerV5.class.getName(), args , false );
        
	_cellName = cellName;
	_args     = getArgs();	
	_nucleus  = getNucleus();

        useInterpreter( true );
	
        try{
	
           if( _args.argc() == 0 )
              throw new
              IllegalArgumentException( "Usage : ... <setupFile>" ) ;
              
           _setupFileName = _args.argv(0) ;
           say("Using setupfile : "+_setupFileName);

           String tmp         = _args.getOpt( "selectionUnit" ) ;
           _selectionUnitName = tmp == null ? _selectionUnitName : tmp ;           
           _selectionUnit     = (PoolSelectionUnit)Class.forName( _selectionUnitName ).newInstance() ;
              
           addCommandListener( _selectionUnit ) ;
               
           say("Starting Cost module");
           _costModule = _poolOperator = new PoolOperator(this) ;
           say("Cost module sucessfully started");
             
           say("Cost module : "+_costModule);
           addCommandListener( _costModule );


           _partitionManager = new PartitionManager( this ) ;
           addCommandListener( _partitionManager ) ;

           String poolStatus = _args.getOpt("poolStatusRelay") ;
           if( poolStatus != null )_poolStatusRelayPath = new CellPath(poolStatus) ;
            
           _pnfsHandler      = new PnfsHandler( this , new CellPath(_pnfsManagerName) ) ;

           _poolMonitor      = new PoolMonitorV5( this , _selectionUnit , _pnfsHandler , _costModule , _partitionManager ) ;
           
           _requestContainer = new RequestContainerV5( this , _selectionUnit , _poolMonitor , _partitionManager ) ;
           addCommandListener( _requestContainer ) ;
           
           //
           //  additional info about cost
           //
           String sendCostString = _args.getOpt("sendCostInfoMessages" ) ;              //VP
           if( sendCostString != null ) _sendCostInfo = sendCostString.equals("yes") ;  //VP
           say( "send CostInfoMessages : "+(_sendCostInfo?"yes":"no") ) ;               //VP
           _requestContainer.setSendCostInfo(_sendCostInfo) ;                           //VP
           

           synchronized( _setupLock ){
              runSetupFile() ;
           }
            
	}catch(Exception ee ){
           ee.printStackTrace();
           start() ;
           kill() ;
           esay(ee);
           throw ee ;
        }
	
        getNucleus().export();
        
	new MessageTimeoutThread();
        
        String watchdogParam = _args.getOpt("watchdog") ;
        if( watchdogParam != null ){
            _watchdog = watchdogParam.length() > 0 ? new WatchdogThread( watchdogParam ) :  new WatchdogThread() ;
            say("Watchdog : "+_watchdog);
        }
	start();
    } 
    private void runSetupFile() throws Exception {
      runSetupFile(null);
    }
    private void runSetupFile( StringBuffer sb ) throws Exception {
       File setupFile = new File( _setupFileName ) ;
       if( ! setupFile.exists() )
          throw new
          IllegalArgumentException( "Setup File not found : "+_setupFileName ) ;
          
       BufferedReader reader = 
          new BufferedReader( new FileReader( setupFile ) ) ;
       try{
          

          String line = null ;
          while( ( line = reader.readLine() ) != null ){
             if( line.length() == 0 )continue ;
             if( line.charAt(0) == '#' )continue ;
             try{
                say( "Executing : "+line ) ;
                String answer = command( line ) ;
                if( answer.length() > 0 )say( "Answer    : "+answer ) ;
             }catch( Exception ee ){
                esay("Exception : "+ee.toString() ) ;
                if( sb != null )
                   sb.append(line).
                   append(" -> ").
                   append(ee.toString()).
                   append("\n");
             }
          }
       }finally{
          try{ reader.close() ; }catch(Exception ee){}
       }
       
    }
    public CellVersion getCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision: 1.32 $" ); }
    private void dumpSetup() throws Exception {
      
       File setupFile = new File( _setupFileName ).getCanonicalFile() ;
       File tmpFile   = new File( setupFile.getParent() , "."+setupFile.getName() ) ;
         
       PrintWriter writer =
          new PrintWriter( new FileWriter( tmpFile ) ) ;
          
       try{
          writer.print( "#\n# Setup of " ) ;
          writer.print(_nucleus.getCellName() ) ;
          writer.print(" (") ;
          writer.print(this.getClass().getName()) ;
          writer.print(") at ") ;
          writer.println( new Date().toString() ) ;
          writer.println( "#") ;
          writer.print("set timeout pool ");
          writer.println(""+(_poolMonitor.getPoolTimeout()/1000L));
          writer.println( "#" ) ;
          
          StringBuffer sb = new StringBuffer(16*1024) ;
          
          _selectionUnit.dumpSetup(sb) ;
          _requestContainer.dumpSetup(sb);
          _partitionManager.dumpSetup(sb);
          
          writer.println(sb.toString());
          
       }catch(Exception ee){
          tmpFile.delete() ;
          throw ee ;
       }finally{
          try{ writer.close() ; }catch(Exception eee ){}
       }
       if( ! tmpFile.renameTo( setupFile ) ){
       
          tmpFile.delete() ;
          
          throw new
          IllegalArgumentException( "Rename failed : "+_setupFileName ) ;
           
       }
       return ;
    }
    private class WatchdogThread implements Runnable {
        private long _deathDetected = 10L * 60L * 1000L ;
        private long _sleepTimer    = 1L  * 60L * 1000L ; 
        private long _watchdogSequenceCounter = 0L ;
        
	public WatchdogThread(  ){
            _nucleus.newThread( this , "watchdog" ).start() ;
            say("WatchdogThread initialized with : "+this);
        }
	public WatchdogThread( String parameter ){
            //
            //    [<deathDetection>]:[<sleeper>]
            //
            long deathDetected = 0 ;
            long sleeping      = 0 ;
            try{
                StringTokenizer st = new StringTokenizer( parameter , ":");
                String tmp = null  ;
                if( st.hasMoreTokens() ){
                    tmp = st.nextToken() ;
                    if(tmp.length() > 0 )deathDetected = Long.parseLong( tmp );
                }
                if( st.hasMoreTokens() ){
                    tmp = st.nextToken() ;
                    if(tmp.length() > 0 )sleeping = Long.parseLong( tmp );
                }

                if( ( deathDetected < 10 ) || ( sleeping < 10 ) )
                     throw new
                     IllegalArgumentException("Timers to small : "+parameter);
                
                if( deathDetected > 0L )_deathDetected = deathDetected * 1000L ;
                if( sleeping > 0L )_sleepTimer = sleeping * 1000L ;
                
            }catch(Exception ee ){
                esay("WatchdogThread : illegal arguments ["+parameter+"] (using defaults) "+ee.getMessage());
            }
            _nucleus.newThread( this , "watchdog" ).start() ;
            say("WatchdogThread initialized with : "+this);
        }
	public void run() {
            say( "watchdog thread activated" ) ;
	    while (true){
		try {
                   Thread.sleep(_sleepTimer);
		} catch (InterruptedException e){
                    say( "watchdog thread interrupted" ) ;
                    break ;
		}
	        runWatchdogSequence(_deathDetected);
                _watchdogSequenceCounter++;
	    }
            say( "watchdog finished" ) ;
	}
        public String toString(){
            return "DeathDetection="+(_deathDetected/1000L)+
                    ";Sleep="+(_sleepTimer/1000L)+
                    ";Counter="+_watchdogSequenceCounter+";";
        }
    }
    private void handlePoolMode( PoolManagerPoolModeMessage msg , CellMessage message ){
    
        PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool( msg.getPoolName() );
        if( pool == null ){
           msg.setFailed( 563 , "Pool not found : "+msg.getPoolName() ) ;
        }else if( msg.getPoolMode() == PoolManagerPoolModeMessage.UNDEFINED ){
          //
          // get pool mode
          //
          msg.setPoolMode( PoolManagerPoolModeMessage.READ |
                           ( pool.isReadOnly() ? 0 : PoolManagerPoolModeMessage.WRITE ) ) ;
        }else{
          // 
          // set pool mode
          //
          pool.setReadOnly( ( msg.getPoolMode() & PoolManagerPoolModeMessage.WRITE ) == 0 ) ;
       }
   
        if( ! msg.getReplyRequired() )return ;
        try{
            say("Sending reply "+message);
            message.revertDirection();
            sendMessage(message);
        }catch (Exception e){
            esay("Can't reply message : "+e);
        }

    }
    private void runWatchdogSequence( long deathDetectedTimer ) {
        String [] definedPools = _selectionUnit.getDefinedPools(false);

        for( int i = 0 , n = definedPools.length ; i < n ; i++ ){

            String poolName = definedPools[i] ;

            PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool( poolName );
            if( pool == null )continue ;

            if( ( pool.getActive() > deathDetectedTimer ) && pool.setSerialId( 0L )  ){
                      
                 _requestContainer.poolStatusChanged(poolName);
                 sendPoolStatusRelay( poolName , PoolStatusChangedMessage.DOWN , null , 666 , "DEAD" ) ;

            }

        }
    }
    private class MessageTimeoutThread implements Runnable {
	public MessageTimeoutThread(){
            _nucleus.newThread( this , "messageTimeout" ).start() ;
        }
	public void run() {
	    while (true){
		_nucleus.updateWaitQueue();
		try {
		    Thread.sleep(_interval);
		} catch (InterruptedException e){
                    say( "Message timeout thread interrupted" ) ;
                    break ;
		}
	    }
            say( "Message timeoutthread finished" ) ;
	}
    }
    
    public void getInfo( PrintWriter pw ){
	pw.println("PoolManager V [$Id: PoolManagerV5.java,v 1.32 2006-05-19 21:38:35 patrick Exp $]");
        pw.println(" SelectionUnit : "+_selectionUnit.getVersion() ) ;
        pw.println(" Write Threads : "+_writeThreads) ;
        pw.println(" Read  Threads : "+_readThreads) ;
        pw.println("  Pool Timeout : "+_poolMonitor.getPoolTimeout()/1000L) ;
        pw.println(" Decision : space="+_spaceCostFactor+
                              " cpu="+_performanceCostFactor ) ;
        pw.println("Message counts") ;
        pw.println("           PoolUp : "+_counterPoolUp ) ;
        pw.println("         PoolDown : "+_counterPoolDown ) ;
        pw.println("   SelectReadPool : "+_counterSelectReadPool ) ;
        pw.println("  SelectWritePool : "+_counterSelectWritePool ) ;
        if( _watchdog == null ){
             pw.println("         Watchdog : disabled" ) ;
        }else{
             pw.println("         Watchdog : "+_watchdog ) ;
        }
        if( _requestContainer != null )_requestContainer.getInfo( pw ) ;
        _costModule.getInfo(pw);
    }
    public CellInfo getCellInfo(){
        PoolManagerCellInfo info = new PoolManagerCellInfo(  super.getCellInfo() ) ;
        info.setPoolList( _selectionUnit.getActivePools() ) ;
        return info ;
    }
    public String hh_set_max_threads = " # DEPRICATED 	" ;
    public String ac_set_max_threads_$_1( Args args )throws CommandException{
      return "" ;
    }
    public String hh_save = " # make setup permanent" ;
    public String ac_save( Args args )throws Exception {
       dumpSetup() ;
       return "" ;
    }
    public String hh_set_timeout_pool = "[-read] [-write] <timeout/secs>" ;
    public String ac_set_timeout_pool_$_1( Args args )throws CommandException{
       boolean isWrite = args.getOpt("write") != null ;
       boolean isRead  = args.getOpt("read")  != null ;
       long    timeout = Integer.parseInt(args.argv(0)) * 1000 ;
       if( ( ! isWrite ) && ( ! isRead ) ){
          _readPoolTimeout = _writePoolTimeout = timeout ;
          _poolMonitor.setPoolTimeout(_readPoolTimeout);
          return "" ;
       }
       if( isWrite )_writePoolTimeout = timeout ;
       if( isRead  ){
          _readPoolTimeout = timeout ;
          _poolMonitor.setPoolTimeout(_readPoolTimeout);
       }
       return "" ;
    }
    public String hh_set_timeout_pnfs = "<timeout/secs>" ;
    public String ac_set_timeout_pnfs_$_1( Args args )throws CommandException{
       _pnfsTimeout = Integer.parseInt(args.argv(0)) * 1000 ;
       return "" ;
    }
    public String hh_set_timeout_fetch = "<timeout/min>" ;
    public String ac_set_timeout_fetch_$_1( Args args )throws CommandException{
       _poolFetchTimeout = Integer.parseInt(args.argv(0)) * 1000 * 60 ;
       return "" ;
    }
    public void say( String str ){ pin( str ) ; super.say( str ) ; }
    public void esay( String str ){ pin( str ) ; super.esay( str ) ; }
    public void esay( Throwable t ){ super.esay( t ) ; }
    
    private synchronized 
       void poolUp( PoolManagerPoolUpMessage poolMessage , CellPath poolPath ){
            
        poolPath.revert() ;
        String poolName = poolMessage.getPoolName() ;
        PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool(poolName,true) ;
        if( pool == null ){
           esay( "poolUP : pool not found : "+poolName ) ;
           return ;
        }
        pool.setActive( true ) ;
        if( pool.setSerialId( poolMessage.getSerialId() ) ){
        
           _requestContainer.poolStatusChanged(poolName);
           
           sendPoolStatusRelay( poolName , PoolStatusChangedMessage.RESTART ) ;
           
        }
		
    }
    private synchronized 
       void poolDown(PoolManagerPoolDownMessage poolMessage){
        String poolName = poolMessage.getPoolName() ;
        PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool(poolName) ;
        if( pool == null ){
           esay( "poolDown : pool not found : "+poolName ) ;
        }else{
           pool.setActive( false ) ;
           pool.setSerialId(0L);
           _requestContainer.poolStatusChanged(poolName);
        }
        sendPoolStatusRelay( poolName , PoolStatusChangedMessage.DOWN , 
                             poolMessage.getPoolMode(),
                             poolMessage.getDetailCode() ,
                             poolMessage.getDetailMessage() ) ;
    }
    private void sendPoolStatusRelay( String poolName , int status ){
       sendPoolStatusRelay( poolName , status , null , 0 , null ) ;
    }
    private void sendPoolStatusRelay( String poolName , int status , 
                                      PoolV2Mode poolMode ,
                                      int statusCode , String statusMessage ){
    
       if( _poolStatusRelayPath == null )return ;
       
       try{
       
          PoolStatusChangedMessage msg = new PoolStatusChangedMessage( poolName , status ) ;
          msg.setPoolMode( poolMode ) ;
          msg.setDetail( statusCode , statusMessage ) ;
          say("sendPoolStatusRelay : "+msg);
          sendMessage(
               new CellMessage( _poolStatusRelayPath , msg ) 
                     ) ;
                         
       }catch(Exception ee ){
          esay("Failed to send poolStatus changed message : "+ee ) ;
       }
    }
    public void messageToForward(  CellMessage cellMessage ){
         
        _costModule.messageArrived(cellMessage);
        
        super.messageToForward(cellMessage);
    }
    public void messageArrived( CellMessage cellMessage ){
	
        Object message  = cellMessage.getMessageObject();
        synchronized( _setupLock ){
        
           _costModule.messageArrived( cellMessage ) ;

           if( message instanceof PoolManagerPoolUpMessage ){

               _counterPoolUp ++ ;
               poolUp(  (PoolManagerPoolUpMessage)message ,
                        cellMessage.getSourcePath() ) ;

           }else if( message instanceof PoolManagerPoolDownMessage ){

               _counterPoolDown ++ ;
               poolDown( (PoolManagerPoolDownMessage)message ) ;

	   }else if (message instanceof PoolMgrSelectPoolMsg){


                 if( message instanceof PoolMgrSelectReadPoolMsg ){
                     _counterSelectReadPool ++ ;

                     _requestContainer.addRequest( cellMessage ) ;
                 }else{
                     _counterSelectWritePool ++ ;

                     choseWritePool( cellMessage ) ;
                 }

	   }else if( message instanceof PoolMgrQueryPoolsMsg ){

                  queryPools( (PoolMgrQueryPoolsMsg)message ,
                               cellMessage ) ;

	   }else if( message instanceof PoolManagerGetPoolListMessage ){

                  getPoolList( (PoolManagerGetPoolListMessage)message ,
                               cellMessage ) ;

	   }else if( message instanceof PoolManagerPoolModeMessage ){

                  handlePoolMode( (PoolManagerPoolModeMessage)message ,
                                  cellMessage ) ;

           }else{
               _requestContainer.messageArrived( cellMessage ) ;
	   }
           
        }
    }
    private void getPoolList( PoolManagerGetPoolListMessage poolMessage ,
                              CellMessage cellMessage ){

       String [] pools = _selectionUnit.getActivePools() ;
       ArrayList l = new ArrayList(pools.length);
       for( int i = 0 , j = pools.length ; i < j ; i++ )l.add(pools[i]);
       poolMessage.setPoolList(l) ;
       poolMessage.setReply();

       cellMessage.revertDirection() ;
       try{
          sendMessage( cellMessage ) ;
       }catch(Exception ee ){
          esay( "Problem replying to queryPool Request : "+ee ) ;
       }
    }
    private void queryPools( PoolMgrQueryPoolsMsg poolQueryMessage ,
                             CellMessage cellMessage ){
       String accessType = poolQueryMessage.getAccessType() ;
       List poolList     = null ;
       if( accessType == null ){
          poolQueryMessage.setReply( 101 , "AccessType == null" ) ;
       }else{
          try{
             poolQueryMessage.setPoolList(
               PoolPreferenceLevel.fromPoolPreferenceLevelToList(
                 _selectionUnit.match( 
                        accessType ,
                        poolQueryMessage.getStoreUnitName() ,
                        poolQueryMessage.getDCacheUnitName() ,
                        poolQueryMessage.getNetUnitName() ,
                        poolQueryMessage.getProtocolUnitName() ,
                        poolQueryMessage.getVariableMap()
                                     ) 
                )
              ) ;
          }catch(Exception ee){
             poolQueryMessage.setReply( 102 , ee ) ;
          }
       }
       cellMessage.revertDirection() ;
       try{
          sendMessage( cellMessage ) ;
       }catch(Exception ee ){
          esay( "Problem replying to queryPool Request : "+ee ) ;
       }
    }
    private class XProtocolInfo implements IpProtocolInfo {
       private String [] _host = new String[1] ;
       
       private static final long serialVersionUID = -5817364111427851052L;
       
       private XProtocolInfo( String hostName ){
          _host[0] = hostName ;
       }
       public String getProtocol(){ return "DCap" ; }
       public int    getMinorVersion(){ return 0 ; }
       public int    getMajorVersion(){ return 0 ; }
       public String getVersionString(){ return "0.0" ; }
       public String [] getHosts(){ return _host ; }
       public int       getPort(){ return 0 ; }
       public boolean isFileCheckRequired() { return true; }
    }
    private class XStorageInfo implements StorageInfo {
       private String _hsm = null ;
       private String _storageClass = null ;
       
       private static final long serialVersionUID = -6624549402952279903L;
       
       private XStorageInfo( String hsm , String storageClass ){
          _hsm = hsm ;
          _storageClass = storageClass ;
       }
       public String getStorageClass(){ return _storageClass ; }
       public void setBitfileId( String bfid ){}
       public String getBitfileId(){ return "" ; }
       public String getCacheClass(){ return null ; }
       public String getHsm(){ return _hsm ; }
       public long   getFileSize(){ return 100 ; }
       public void   setFileSize( long fileSize ){}
       public boolean isCreatedOnly(){ return false ; }
       public boolean isStored(){ return true ; }
       public String  getKey( String key ){ return null ; }
       public void    setKey( String key , String value ){}
       public Map     getMap(){ return new HashMap() ; }
    }
    public String hh_get_av_pools = "<pnfsId> <hsm> <storageClass> <host>" ;
    public String ac_get_av_pools_$_4( Args args ) throws Exception {
       try{
          PnfsId pnfsId = new PnfsId( args.argv(0) ) ;
          XStorageInfo storageInfo = new XStorageInfo( args.argv(1) , args.argv(2) ) ;
          XProtocolInfo protocolInfo = new XProtocolInfo( args.argv(3) ) ;

          PoolMonitorV5.PnfsFileLocation  _pnfsFileLocation = 
                    _poolMonitor.getPnfsFileLocation( pnfsId , 
                                                      storageInfo , 
                                                      protocolInfo ) ;

          List available = _pnfsFileLocation.getFileAvailableMatrix() ;
       
          Iterator i = ((List)available.get(0)).iterator() ;
          StringBuffer sb = new StringBuffer() ;
          sb.append("Available and allowed\n");
          while( i.hasNext() ){
             sb.append("  ").append( i.next().toString() ).append("\n");
          }
          sb.append("Allowed (not available)\n");
          if( ( available = _pnfsFileLocation.getAllowedButNotAvailable() ) != null ){
             i = available.iterator() ;
             while( i.hasNext() ){
                sb.append("  ").append( i.next().toString() ).append("\n");
             }
          }
          return sb.toString() ;
       
       }catch( Exception ee ){
          
          ee.printStackTrace() ;
          throw ee ;
       }
    }
    /*
    public String hh_get_pools = "<hsm> <storageClass> <host>"+
                                 " [-size=<size>] [-mode=stage|store]" ;
    public String ac_get_pools_$_3( Args args ) throws Exception {
       String mode = args.getOpt("mode") ;
       mode = mode == null ? "stage" : mode ;
       long size = 0L ;
       String sizeString = args.getOpt("size") ;
       if( sizeString != null )size = Long.parseLong(sizeString);
       try{
          XStorageInfo storageInfo = new XStorageInfo( args.argv(0) , args.argv(1) ) ;
          XProtocolInfo protocolInfo = new XProtocolInfo( args.argv(2) ) ;
          
          List list = mode.equals("stage") ?
                      _poolMonitor.getStagePoolList( storageInfo , protocolInfo , size ) :
                      _poolMonitor.getStorePoolList( storageInfo , protocolInfo , size ) ;

          Iterator i = list.iterator() ;
          StringBuffer sb = new StringBuffer() ;
          while( i.hasNext() ){
             sb.append( i.next().toString() ).append("\n");
          }
          return sb.toString() ;
       
       }catch( Exception ee ){
          
          ee.printStackTrace() ;
          throw ee ;
       }
    }
    */
    public String hh_reload = "[-yes]  # reloads the setup from disk" ;
    public String ac_reload( Args args )throws Exception {
       if( args.getOpt("yes") == null ){
          return " This Command destroys the current setup\n"+
                 " and replaces it by the setup on disk\n"+
                 " Please use 'reload -yes' if you ready want\n"+
                 " to do that\n" ;
       }
       synchronized( _setupLock ){
          _selectionUnit.clear() ;
          _partitionManager.clear() ;
          StringBuffer sb = new StringBuffer() ;
          runSetupFile(sb) ;
          sb.append("\n");
          return sb.toString() ;
       }
    }
    ///////////////////////////////////////////////////////////////
    //
    // tthe write io request handler
    //
    private void choseWritePool( CellMessage cellMessage ){
       new WriteRequestHandler( cellMessage ) ;
    }
    public class WriteRequestHandler implements Runnable {
    
       private CellMessage               _cellMessage = null ;
       private PoolMgrSelectWritePoolMsg _request     = null ;
       private PnfsId                    _pnfsId      = null ;
       
       public WriteRequestHandler( CellMessage cellMessage ){
       
           _cellMessage = cellMessage ;
           _request     =  (PoolMgrSelectWritePoolMsg)_cellMessage.getMessageObject() ;
           _pnfsId      = _request.getPnfsId();
           _nucleus.newThread( this , "writeHandler" ).start() ;
       }
       public void run(){
                
           StorageInfo  storageInfo  = _request.getStorageInfo() ;
           ProtocolInfo protocolInfo = _request.getProtocolInfo() ;
           
           say( _pnfsId.toString()+" write handler started" );
           long started = System.currentTimeMillis();
           
           if( storageInfo == null ){
              requestFailed( 21 , "Storage info not available for write request : "+_pnfsId ) ;
              return ;
           }else if( protocolInfo == null ){
              requestFailed( 22 , "Protocol info not available for write request : "+_pnfsId ) ;
              return ;
           }
           String expectedLengthString = storageInfo.getKey("alloc-size") ;
           long expectedLength = 0L ;
           if( expectedLengthString != null ){
              try{
                 expectedLength = Long.parseLong(expectedLengthString) ; 
              }catch(Exception ee ){}
           }
           try{
           
              List storeList = _poolMonitor.
                               getPnfsFileLocation( _pnfsId , storageInfo , protocolInfo ).
                               getStorePoolList( expectedLength ) ;
              /* 
              List storeList = 
                  _poolMonitor.getStorePoolList(  storageInfo ,
                                                  protocolInfo ,
                                                  expectedLength );
              */
              String poolName = ((PoolCheckable)storeList.get(0)).getPoolName() ;
              
              if (_sendCostInfo)
                    _requestContainer.sendCostMsg(
                             _pnfsId, (PoolCostCheckable)storeList.get(0), true
                                                 );        //VP
                                                 
              say(_pnfsId+" write handler selected "+poolName+" after "+
                  ( System.currentTimeMillis() - started ) );
              requestSucceeded( poolName ) ;
              
           }catch(CacheException ce ){
              requestFailed( ce.getRc() , ce.getMessage() ) ;
           }catch(Exception ee ){
              requestFailed( 17 , ee.getMessage() ) ;
           }
       }
       protected void requestFailed(int errorCode, String errorMessage){
	   _request.setFailed(errorCode, errorMessage);
	   try {
	       _cellMessage.revertDirection();
	       sendMessage(_cellMessage);
	   } catch (Exception e){
	       esay("Exception requestFailed : "+e);
               esay(e);
	   }	    
       }
       protected void requestSucceeded(String poolName){
	   _request.setPoolName(poolName);
	   _request.setSucceeded();
	   try{
	       _cellMessage.revertDirection();
	       sendMessage(_cellMessage);
               _costModule.messageArrived(_cellMessage);
	   }catch (Exception e){
	       esay("Exception in requestSucceeded : "+e);
	       esay(e);
	   }
       }
    }
} 
