// $Id: PoolManagerV3.java,v 1.16 2006-04-22 15:52:11 patrick Exp $ 

package diskCacheV111.poolManager ;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;

import  dmg.cells.nucleus.*;
import  dmg.util.*;

import  java.util.*;
import  java.io.*;

public class PoolManagerV3 extends CellAdapter {
    
    private String      _cellName = null;
    private Args        _args     = null;
    private CellNucleus _nucleus  = null;
    
    private int  _writeThreads     = 0 ;
    private int  _readThreads      = 0 ;
    private int  _maxReadThreads   = 3 ;
    private int  _maxWriteThreads  = 3 ;

    private int _counterPoolUp         = 0 ;
    private int _counterPoolDown       = 0 ;
    private int _counterSelectWritePool= 0 ;
    private int _counterSelectReadPool = 0 ;
    
    private String _pnfsManagerName   = "PnfsManager";
    private String _selectionUnitName = "diskCacheV111.poolManager.PoolSelectionUnitV1" ;
    private String _setupFileName     = null ;
    
    private PoolSelectionUnit _selectionUnit = null ;

    private long _interval         = 15 * 1000;
    private long _pnfsTimeout      = 15 * 1000;
    private long _readPoolTimeout  = 15 * 1000;
    private long _poolFetchTimeout = 5 * 24 * 3600 * 1000;
    private long _writePoolTimeout = 15 * 1000;
    private long _poolTimeout      = 15 * 1000;

    private double _spaceCostFactor       = 1.0 ;
    private double _performanceCostFactor = 1.0 ;
    private double _costCut               = 1.0 ;
    
    public PoolManagerV3( String cellName , String args ) throws Exception {
	super( cellName , args , false );
        
	_cellName = cellName;
	_args     = getArgs();	
	_nucleus  = getNucleus();

        useInterpreter( true );
	
        try{
	
           if( _args.argc() == 0 )
              throw new
              IllegalArgumentException( "Usage : ... <setupFile>" ) ;
              
           _setupFileName = _args.argv(0) ;

           String tmp         = _args.getOpt( "selectionUnit" ) ;
           _selectionUnitName = tmp == null ? _selectionUnitName : tmp ;           
           _selectionUnit     = (PoolSelectionUnit)Class.forName( _selectionUnitName ).newInstance() ;
              
           addCommandListener( _selectionUnit ) ;
               
               
           runSetupFile() ;
           
	}catch(Exception ee ){
           start() ;
           kill() ;
           throw ee ;
        }
	
        getNucleus().export();
	new MessageTimeoutThread();
	start();
    } 
    private void runSetupFile() throws Exception {
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
                say( "Answer    : "+answer ) ;
             }catch( Exception ee ){
                esay("Exception : "+ee.toString() ) ;
                
             }
          }
       }finally{
          try{ reader.close() ; }catch(Exception ee){}
       }
       
    }
    private void dumpSetup() throws Exception {
       File setupFile = new File( _setupFileName ) ;
       File tmpFile   = new File( setupFile.getParent() , "."+setupFile.getName() ) ;
       
//       if( ! tmpFile.canWrite() )
//         throw new
//         IOException( "Can't write to "+tmpFile ) ;
         
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
          writer.println( "set max threads -read "+_maxReadThreads ) ;
          writer.println( "set max threads -write "+_maxWriteThreads ) ;
          writer.print( "set pool decision ") ;
          writer.print( " -spacecostfactor="+_spaceCostFactor ) ;
          writer.print( " -cpucostfactor="+_performanceCostFactor ) ;
          writer.println( " -costcut="+_costCut ) ;
          writer.println( "#" ) ;
          StringBuffer sb = new StringBuffer(16*1024) ;
          _selectionUnit.dumpSetup(sb) ;
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
    private class MessageTimeoutThread implements Runnable {
	public MessageTimeoutThread(){
            _nucleus.newThread( this , "messageTimeout" ).start() ;
        }
	public void run() {
	    while (true){
		_nucleus.updateWaitQueue();
		try {
		    Thread.currentThread().sleep(_interval);
		} catch (InterruptedException e){
                    say( "Message timeout thread interrupted" ) ;
                    break ;
		}
	    }
            say( "Message timeoutthread finished" ) ;
	}
    }
    
    public void getInfo( PrintWriter pw ){
	pw.println("PoolManager (VERSION III)[$Id: PoolManagerV3.java,v 1.16 2006-04-22 15:52:11 patrick Exp $]");
        pw.println(" SelectionUnit : "+_selectionUnit.getVersion() ) ;
        pw.println(" Write Threads : "+_writeThreads) ;
        pw.println(" Read  Threads : "+_readThreads) ;
        pw.println(" Decision : space="+_spaceCostFactor+
                              " cpu="+_performanceCostFactor+
                              " cut="+_costCut ) ;
        pw.println("Message counts") ;
        pw.println("          PoolUp : "+_counterPoolUp ) ;
        pw.println("        PoolDown : "+_counterPoolDown ) ;
        pw.println("  SelectReadPool : "+_counterSelectReadPool ) ;
        pw.println(" SelectWritePool : "+_counterSelectWritePool ) ;
//        pw.println(" Pools available : " ) ;
    }
    public CellInfo getCellInfo(){
        PoolManagerCellInfo info = new PoolManagerCellInfo(  super.getCellInfo() ) ;
        info.setPoolList( _selectionUnit.getActivePools() ) ;
        return info ;
    }
    public String hh_save = " # make setup permanent" ;
    public String ac_save( Args args )throws Exception {
       dumpSetup() ;
       return "" ;
    }
    public String hh_dump_setup = " # depricated, use save instead" ;
    public String ac_dump_setup( Args args )throws Exception {
       dumpSetup() ;
       return "" ;
    }
    public String hh_set_pool_decision = 
       "[-spacecostfactor=<scf>] [-cpucostfactor=<ccf>] [-costcut=<cc>]" ;
       
    public String ac_set_pool_decision( Args args )throws Exception {
       String tmp = args.getOpt("spacecostfactor") ;
       if( tmp != null )_spaceCostFactor = Double.parseDouble(tmp) ;
       tmp = args.getOpt("cpucostfactor") ;
       if( tmp != null )_performanceCostFactor = Double.parseDouble(tmp) ;
       tmp = args.getOpt("costcut") ;
       if( tmp != null )_costCut = Double.parseDouble(tmp) ;
       return "scf="+_spaceCostFactor+";ccf="+_performanceCostFactor+";cc="+_costCut ;
    }
    public String hh_set_max_threads = "[-read] [-write] <#threads>" ;
    public String ac_set_max_threads_$_1( Args args )throws CommandException{
       boolean isWrite = args.getOpt("write") != null ;
       boolean isRead  = args.getOpt("read")  != null ;
       int     count   = Integer.parseInt(args.argv(0)) ;
       if( ( ! isWrite ) && ( ! isRead ) ){
          _maxWriteThreads = _maxReadThreads = count ;
          return "" ;
       }
       if( isWrite )_maxWriteThreads = count ;
       if( isRead  )_maxReadThreads  = count ;
       return "" ;
    }
    public String hh_set_timeout_pool = "[-read] [-write] <timeout/secs>" ;
    public String ac_set_timeout_pool_$_1( Args args )throws CommandException{
       boolean isWrite = args.getOpt("write") != null ;
       boolean isRead  = args.getOpt("read")  != null ;
       long    timeout = Integer.parseInt(args.argv(0)) * 1000 ;
       if( ( ! isWrite ) && ( ! isRead ) ){
          _readPoolTimeout = _writePoolTimeout = timeout ;
          return "" ;
       }
       if( isWrite )_writePoolTimeout = timeout ;
       if( isRead  )_readPoolTimeout = timeout ;
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
        PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool(poolName) ;
        if( pool == null ){
           esay( "poolUP : pool not found : "+poolName ) ;
           return ;
        }
        pool.setActive( true ) ;
        pool.setSerialId( poolMessage.getSerialId() ) ;
		
    }
    private synchronized 
       void poolDown(PoolManagerPoolDownMessage poolMessage){
        String poolName = poolMessage.getPoolName() ;
        PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool(poolName) ;
        if( pool == null ){
           esay( "poolUP : pool not found : "+poolName ) ;
           return ;
        }
        pool.setActive( false ) ;
    }
    public void messageArrived( CellMessage cellMessage ){
	
        Object poolManagerMessage  = cellMessage.getMessageObject();
	
	if( ! ( poolManagerMessage instanceof Message ) ){
	    say("Unexpected message class 6 "+poolManagerMessage.getClass());
	    say("source = "+cellMessage.getSourceAddress());
	    return;
	}
	
        if( poolManagerMessage instanceof PoolManagerPoolUpMessage ){
        
            _counterPoolUp ++ ;
            poolUp(  (PoolManagerPoolUpMessage)poolManagerMessage ,
                     cellMessage.getSourcePath() ) ;
        
        }else if (poolManagerMessage instanceof PoolManagerPoolDownMessage){
            
            _counterPoolDown ++ ;
            poolDown( (PoolManagerPoolDownMessage)poolManagerMessage ) ;
	
	}else if (poolManagerMessage instanceof PoolMgrSelectReadPoolMsg){
        
            _counterSelectReadPool ++ ;
	    new ReadPoolRequestHandler( 
                   (PoolMgrSelectReadPoolMsg)poolManagerMessage, 
                   cellMessage         );
	}else if (poolManagerMessage instanceof PoolMgrSelectWritePoolMsg){
            _counterSelectWritePool ++ ;
	    new WritePoolRequestHandler( 
	           (PoolMgrSelectWritePoolMsg)poolManagerMessage, 
                   cellMessage         );
	
	}else if (poolManagerMessage instanceof PoolMgrQueryPoolsMsg){
            queryPools( (PoolMgrQueryPoolsMsg)poolManagerMessage ,
                         cellMessage ) ;
        }else {
	    say("Unexpected message class 7 "+poolManagerMessage.getClass());
	    say("source = "+cellMessage.getSourceAddress());
	    return;
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
                        null ,
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
    private List getCacheLocations( PnfsId pnfsId ) throws Exception {
	CellPath    cellPath;
	CellMessage cellMessage;
	PnfsGetCacheLocationsMessage pnfsMessage;
	cellPath    = new CellPath(_pnfsManagerName);
	pnfsMessage = new PnfsGetCacheLocationsMessage(pnfsId);
	cellMessage = new CellMessage(cellPath, pnfsMessage);

        SpreadAndWait control = new SpreadAndWait( _nucleus, _pnfsTimeout ) ;

	say("Sending pnfs lookup request") ;
        control.send( cellMessage ) ;
        control.waitForReplies() ;
        
        List list = control.getReplyList() ;

        if( list.size() == 0 )
          throw new 
          HandlerException( 11 , "PnfsRequest timed out" ) ;

        cellMessage    = (CellMessage)list.get(0) ;
	Object message = cellMessage.getMessageObject();

	if( ! ( message instanceof PnfsGetCacheLocationsMessage ) )
            throw new 
            HandlerException(  102 , 
                               "Unexpected message from ("+
                               cellMessage.getSourcePath()+") : "+
                               message.getClass());


	pnfsMessage = (PnfsGetCacheLocationsMessage)message;
	say ("Got reply from pnfs manager "+pnfsMessage);

	if( pnfsMessage.getReturnCode() != 0 )
            throw new
            HandlerException( 13 , 
		              "Pnfs request failed : "+
                              pnfsMessage.getErrorObject() ) ;

	return new ArrayList( pnfsMessage.getCacheLocations() );
    }
    private void clearCacheLocation(PnfsId pnfsId, String poolName){
	
	try {
	    sendMessage( 
               new CellMessage(
                  new CellPath(_pnfsManagerName),
                  new PnfsClearCacheLocationMessage(pnfsId,poolName) ) ) ;
	} catch (Exception e){
	    esay("Cannot send messge to pnfs manager "+e);
            esay(e) ;
	}
    }
    //------------------------------------------------------------------------------
    //
    //  'queryPoolsForPnfsId' sends PoolCheckFileMessages to all pools
    //  specified in the pool iterator. It waits until all replies
    //  have arrived, the global timeout has expired or the thread
    //  was interrupted.
    //
    private List queryPoolsForPnfsId( Iterator pools , PnfsId pnfsId ) throws Exception {

        SpreadAndWait control = 
              new SpreadAndWait( _nucleus , _poolTimeout ) ;

	while( pools.hasNext() ){

	    String poolName = (String)pools.next();
            //
            // deselection inactive and disabled pools
            //
            PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool(poolName) ;
            
            if( ( pool == null                    ) ||
                ( ! pool.isEnabled()              ) ||
                ( pool.getActive() > (10*60*1000) ) )continue ;

            say( "queryPoolsForPnfsId : PoolCheckFileRequest to : "+poolName);
            //
            // send query
            //
	    CellMessage  cellMessage = 
                   new CellMessage(  new CellPath(poolName), 
                                     new PoolCheckFileMessage(poolName,pnfsId) 
                                  );

	    try{
               control.send( cellMessage ) ;
	    }catch(Exception exc){
               // 
               // here we don't care about exceptions
               //
	       esay ("Exception sending PoolCheckFileRequest to "+poolName+" : "+exc);
	    }
        }
        
        //
        // scan the replies
        //
        CellMessage answer = null ; 
        ArrayList   list   = new ArrayList() ;

        while( ( answer = control.next() ) != null ){

           Object message = answer.getMessageObject();

	   if( ! (message instanceof PoolCheckFileMessage)){
	      esay("queryPoolsForPnfsId : Unexpected message from ("+
                   answer.getSourcePath()+") "+message.getClass());
              continue ;
	   } 
	   PoolCheckFileMessage poolMessage = (PoolCheckFileMessage)message;
           say( "queryPoolsForPnfsId : reply : "+poolMessage ) ;
	   boolean have     = poolMessage.getHave();
	   boolean waiting  = poolMessage.getWaiting();
	   String  poolName = poolMessage.getPoolName();
	   if( have || waiting){
              list.add( poolMessage ) ;
	   }else{ 
              esay("queryPoolsForPnfsId : clearingCacheLocation for pnfsId "+
                    pnfsId+" at pool "+poolName ) ;
              clearCacheLocation( pnfsId , poolName ) ;
	   }
        }
        say( "queryPoolsForPnfsId : number of valid replies : "+list.size() );
        return list ;

    }
    private List queryPoolsForCost( Iterator pools , long filesize ) throws Exception {
    
        SpreadAndWait control = 
              new SpreadAndWait( _nucleus , _poolTimeout ) ;

	while( pools.hasNext() ){

	    String poolName = (String)pools.next();
            say( "queryPoolsForCost : PoolCheckFileRequest to : "+poolName);
            //
            // send query
            //
	    CellMessage  cellMessage = 
                   new CellMessage(  new CellPath(poolName), 
                                     new PoolCheckCostMessage(poolName,filesize) 
                                  );

	    try{
               control.send( cellMessage ) ;
	    }catch(Exception exc){
               // 
               // here we don't care about exceptions
               //
	       esay ("Exception sending PoolCheckFileRequest to "+poolName+" : "+exc);
	    }
    
        }
        //
        // scan the replies
        //
        CellMessage answer = null ; 
        ArrayList   list   = new ArrayList() ;

        while( ( answer = control.next() ) != null ){

           Object message = answer.getMessageObject();

	   if( ! (message instanceof PoolCheckCostMessage)){
	      esay("queryPoolsForCost : Unexpected message from ("+
                   answer.getSourcePath()+") "+message.getClass());
              continue ;
	   } 
	   PoolCheckCostMessage poolMessage = (PoolCheckCostMessage)message;
           say( "queryPoolsForCost : reply : "+poolMessage ) ;
	   String  poolName = poolMessage.getPoolName();
           list.add( poolMessage ) ;
        }
        say( "queryPoolsForCost : number of valid replies : "+list.size() );
        return list ;
    }
    private String runCostDecision( List list ) throws HandlerException {
       double cost = 1000000.0 , minCost = 10000000.0 ;
       String result   = null ;

       for( Iterator i = list.iterator() ; i.hasNext() ;  ){

          PoolCheckCostMessage m = (PoolCheckCostMessage)i.next() ;
          
          
          cost = Math.abs(m.getSpaceCost())       * _spaceCostFactor + 
                 Math.abs(m.getPerformanceCost()) * _performanceCostFactor ;
                 
                 
          if( cost < minCost ){
             minCost = cost ;
             result  = m.getPoolName() ;
          }
       }

       if( ( result == null ) || ( minCost >= _costCut ) ){
          throw new
          HandlerException( 34 , "Min Cost too high "+minCost ) ;
       }
       say( "runCostDecision : Chosen ["+result+"] out of "+list ) ;
       return result ;
    }
    private class HandlerException extends Exception {
        private int _rc = 666 ;
        
        private static final long serialVersionUID = -6320998693593883949L;
        
        public HandlerException( String msg ){ super( msg ) ; }
        public HandlerException( int rc , String msg ){
            super(msg) ;
            _rc = rc ;
        }
        public int getRc(){ return _rc ; }
    }
    ///////////////////////////////////////////////////////////////
    //
    // template (base class) for the io request handler
    //
    private class PoolRequestHandler  {
	
	protected PoolMgrSelectPoolMsg _poolManagerMessage;
	protected PnfsId       _pnfsId;
	protected StorageInfo  _storageInfo ;
	protected ProtocolInfo _protocolInfo;
        protected Thread       _workerThread ;
	protected CellMessage  _originalCellMessage ; 
                                                   
                                                   
	
        public PoolRequestHandler(
                  PoolMgrSelectPoolMsg poolManagerMessage,
                  CellMessage          originalCellMessage ){
                  
	    _pnfsId              = poolManagerMessage.getPnfsId();
	    _storageInfo         = poolManagerMessage.getStorageInfo();
	    _protocolInfo        = poolManagerMessage.getProtocolInfo();
	    _poolManagerMessage  = poolManagerMessage;
	    _originalCellMessage = originalCellMessage;
	    
	}
        protected void requestFailed(int errorCode, String errorMessage){
	    _poolManagerMessage.setFailed(errorCode, errorMessage);
	    try {
	        _originalCellMessage.revertDirection();
	        sendMessage(_originalCellMessage);
	    } catch (Exception e){
	        esay("Exception requestFailed : "+e);
                esay(e);
	    }	    
        }
        protected void requestSucceeded(String poolName){
	    _poolManagerMessage.setPoolName(poolName);
	    _poolManagerMessage.setSucceeded();
	    try {
	        _originalCellMessage.revertDirection();
	        sendMessage(_originalCellMessage);
	    } catch (Exception e){
	        esay("Exception requestSucceeded : "+e);
	        esay(e);
	    }
        }
        
    }
    ///////////////////////////////////////////////////////////////
    //
    //             R E A D
    //
    //    the readpoolrequest handler
    //    it is created and started (thread) for each
    //    read request. it runs the initial message protocol
    //    until the actual fetch request was sent. then
    //    the thread dies. the respond is received 
    //    asynchronously.
    //
    private class      ReadPoolRequestHandler 
            extends    PoolRequestHandler
            implements CellMessageAnswerable,
                       Runnable                    {
	
	
        public ReadPoolRequestHandler(
                  PoolMgrSelectReadPoolMsg poolManagerMessage,
                  CellMessage originalCellMessage                   ){
                  
	    super( poolManagerMessage , originalCellMessage ) ;
	    
            say("Read pool request handler created (2)");
            
            synchronized( this.getClass() ){ _readThreads++ ; }
            _workerThread = _nucleus.newThread( this , "readPool" ) ;
            _workerThread.start() ;
	}
	
	public void run(){
           try{
              String hsm          = _storageInfo.getHsm() ;
              String storageClass = _storageInfo.getStorageClass() ;
              String  hostName    = 
                         _protocolInfo instanceof IpProtocolInfo ?
                         ((IpProtocolInfo)_protocolInfo).getHosts()[0] :
                         null ;
              //
              // will ask the PnfsManager for a hint
              // about the pool locations of this
              // pnfsId. Returns an enumeration of
              // the possible pools.
              //
              List expectedPools = getCacheLocations( _pnfsId ) ;
              say( "ReadPoolRequestHandler : expectedPools : "+expectedPools ) ;
              //
              //  get the prioratized list of allowed pools for this
              //  request. (We are only allowed to use the level-1
              //  pools.
              //       
              PoolPreferenceLevel [] level = 
                  _selectionUnit.match( "read" , 
                                        storageClass+"@"+hsm ,
                                        null , // no dCacheClass yet.
                                        hostName ,
                                        null ,
                                        null      ) ; // doing the HashMap later.
              List [] prioPools = new ArrayList[level.length] ;
              for( int i = 0 ; i < level.length ; i++ )prioPools[i] = level[i].getPoolList() ;
              //
              //  merge the prioPools with the expected pools.
              //  
              int  maxDepth      = 9999 ;
              List fileAvailable = new ArrayList() ;
              for( int prio = 0 ; prio < Math.min( maxDepth , prioPools.length ) ; prio++ ){                       
                 HashSet  allowedPools = new HashSet( prioPools[prio] ) ;   
                 List     result       = new ArrayList( allowedPools.size() ) ;                 
                 Iterator ep           = expectedPools.iterator() ;
                 say( "ReadPoolRequestHandler : allowedPools("+prio+") : "+allowedPools ) ;
                 while( ep.hasNext() ){
                    String p = (String)ep.next() ;
                    if( allowedPools.contains( p ) )result.add( p ) ;
                 }
                 say( "ReadPoolRequestHandler : result("+prio+") : "+result ) ;
                 Iterator pools = result.iterator() ;                
                 //
                 // sends 'do you have' queries to all
                 // pools in the enumeration simultaniously.
                 //  
                 fileAvailable = queryPoolsForPnfsId( pools , _pnfsId ) ;
                 if( fileAvailable.size() > 0 )break ;
              }
              if( fileAvailable.size() > 0 ){
                 int pos = (int) ( ( System.currentTimeMillis() & 0xffffff ) % fileAvailable.size() ) ;
                 requestSucceeded( 
                      ((PoolMessage)fileAvailable.get(pos)).getPoolName() ) ; 
                 return ;
              }
              //              
	      //  couldn't find the file in any of the allowed pools.
              //  look for the 'cache' pools.
              //
              level = 
                  _selectionUnit.match( "cache" , 
                                        storageClass+"@"+hsm ,
                                        null ,      // no dCacheClass yet.
                                        hostName ,
                                        null ,
                                        null ) ;    // doing the HashMap later.
                                        
              prioPools = new ArrayList[level.length] ;
              for( int i = 0 ; i < level.length ; i++ )prioPools[i] = level[i].getPoolList() ;
              //
              // this is the final knock out.
              //
              if( prioPools.length == 0 )
                 throw new
                 HandlerException( 19 ,
                                  "No read pools available for <"+
                                  storageClass+"@"+hsm+">" ) ;
                  
              
              List costs = new ArrayList() ;  
              for( int prio = 0 ; prio < Math.min( maxDepth , prioPools.length ) ; prio++ ){                       
                 costs = queryPoolsForCost( prioPools[prio].iterator() , 0L ) ;
                 if( costs.size() != 0 )break ;
              }  
              if( costs.size() > 0 ){
                 sendFetchRequest( runCostDecision( costs ) ) ; 
              }else{
                 StringBuffer sb = new StringBuffer() ;
                 for( int prio = 0 ; prio < Math.min( maxDepth , prioPools.length ) ; prio++ ){    
                    Iterator n = prioPools[prio].iterator() ;
                    while( n.hasNext() )sb.append(n.next().toString()).append(",") ;
                    sb.append("/");                   
                 }  
                 throw new
                 HandlerException( 20 ,
                                  "No reply from costcheck for <"+
                                  storageClass+"@"+hsm+"> "+sb.toString() ) ;
              }   
	   }catch(HandlerException hexc){
	      esay ("ReadHandler thread Problem : "+
                    hexc.getRc()+" : "+hexc.getMessage());
	      requestFailed( hexc.getRc(), hexc.getMessage() );
	   }catch(Exception exc){
	      esay ("ReadHandler thread Exception : "+exc);
              esay(exc);
	      requestFailed(11, exc.toString() );
	   }finally{
              say( "ReadHandler thread finished" ) ;
           }
        
        }
        //
        //
	private void sendFetchRequest(String poolName) throws Exception {
	    PoolFetchFileMessage poolMessage;
	    CellMessage          cellMessage;
	    
	    poolMessage = new PoolFetchFileMessage(
                                 poolName,
                                 _storageInfo,
                                 _pnfsId);
	    cellMessage = new CellMessage(
                                new CellPath(poolName), 
                                poolMessage);
            
            sendMessage( cellMessage , true , true , 
                         this , 
                         _poolFetchTimeout);
	}
	
	public void answerArrived(CellMessage request, CellMessage answer){
           Object obj = answer.getMessageObject() ;
           if( obj instanceof PoolFetchFileMessage ){
              PoolFetchFileMessage pm = (PoolFetchFileMessage)obj ;
	      
              if( pm.getReturnCode() != 0 ){
		  requestFailed( 17, pm.getErrorObject().toString() );
	      }else{
		  requestSucceeded( pm.getPoolName() );
	      }
              synchronized( this.getClass() ){ _readThreads-- ; }
           }

        }
	public void exceptionArrived(CellMessage request, Exception exception){
           Object obj = request.getMessageObject();
           if( obj instanceof PoolFetchFileMessage ){
              esay( "ExceptionArrived for FetchRequest : "+exception ) ;
              esay(exception) ;
              requestFailed(20,exception.toString()) ;
           }else{
              esay( "ExceptionArrived for request "+
                     obj.getClass()+" : "+exception ) ;
           }
	}
	
	public void answerTimedOut(CellMessage request){
           Object obj = request.getMessageObject();
           if( obj instanceof PoolFetchFileMessage ){
              esay( "AnswerTimedOut for FetchRequest" ) ;
              requestFailed(21,"AnswerTimedOut for FetchRequest") ;
           }else{
              esay( "AnswerTimedOut for "+obj.getClass() ) ;
           }
	}
    }    
    ///////////////////////////////////////////////////////////////
    //
    //             W R I T E
    //
    //
    private class      WritePoolRequestHandler 
            extends    PoolRequestHandler
            implements Runnable                    {
	
	
        public WritePoolRequestHandler(
                  PoolMgrSelectWritePoolMsg poolManagerMessage,
                  CellMessage               originalCellMessage ){
                  
	    super( poolManagerMessage , originalCellMessage ) ;
	    
            say("Write pool request handler created (2)");
            
            synchronized( this.getClass() ){ _writeThreads++ ; }
            _workerThread = _nucleus.newThread( this , "writePool" ) ;
            _workerThread.start() ;
	}
	
	public void run(){
           try{
              String hsm          = _storageInfo.getHsm() ;
              String storageClass = _storageInfo.getStorageClass() ;
              String  hostName    = 
                         _protocolInfo instanceof IpProtocolInfo ?
                         ((IpProtocolInfo)_protocolInfo).getHosts()[0] :
                         null ;
              //
              //  get the prioratized list of allowed pools for this
              //  request. (We are only allowed to use the level-1
              //  pools.
              //       
              PoolPreferenceLevel [] level = 
                  _selectionUnit.match( "write" , 
                                        storageClass+"@"+hsm ,
                                        null , // no dCacheClass yet.
                                        hostName ,
                                        null ,
                                        null      ) ; // doing the HashMap later.
                                        
              List [] prioPools = new ArrayList[level.length] ;
              for( int i = 0 ; i < level.length ; i++ )prioPools[i] = level[i].getPoolList() ;
              //
              // this is the final knock out.
              //
              if( prioPools.length == 0 )
                 throw new
                 HandlerException( 19 ,
                                  "No write pools available for <"+
                                  storageClass+"@"+hsm+">" ) ;
                  
              int maxDepth = 9999 ;
              List costs = new ArrayList() ;  
              for( int prio = 0 ; prio < Math.min( maxDepth , prioPools.length ) ; prio++ ){
                 costs = queryPoolsForCost( prioPools[prio].iterator() , 0L ) ;
                 if( costs.size() != 0 )break ;
              }  
              if( costs.size() > 0 ){
                 requestSucceeded( runCostDecision( costs ) ) ; 
              }else{
                 StringBuffer sb = new StringBuffer() ;
                 for( int prio = 0 ; prio < Math.min( maxDepth , prioPools.length ) ; prio++ ){
                    Iterator n = prioPools[prio].iterator() ;
                    while( n.hasNext() )sb.append(n.next().toString()).append(",") ;
                    sb.append("/");                   
                 }  
                 throw new
                 HandlerException( 20 ,
                                  "No reply from costcheck for <"+
                                  storageClass+"@"+hsm+"> "+sb.toString() ) ;
              }   
	   }catch(HandlerException hexc){
	      esay ("WriteHandler thread Problem : "+
                    hexc.getRc()+" : "+hexc.getMessage());
	      requestFailed( hexc.getRc(), hexc.getMessage() );
	   }catch(Exception exc){
	      esay ("WriteHandler thread Exception : "+exc);
              esay(exc);
	      requestFailed(11, exc.toString() );
	   }finally{
              say( "WriteHandler thread finished" ) ;
              synchronized( this.getClass() ){ _writeThreads-- ; }
           }
        
        }
	
    }    
    
} 
