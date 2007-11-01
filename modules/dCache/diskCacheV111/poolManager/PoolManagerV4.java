// $Id: PoolManagerV4.java,v 1.21 2006-05-12 20:47:12 tigran Exp $ 

package diskCacheV111.poolManager ;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;

import  dmg.cells.nucleus.*;
import  dmg.util.*;

import  java.text.*;
import  java.util.*;
import  java.io.*;

public class PoolManagerV4 extends CellAdapter {
    
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
    private String  _selectionUnitName = "diskCacheV111.poolManager.PoolSelectionUnitV1" ;
    private String  _setupFileName     = null ;
    private HashMap _readHandlerList   = new HashMap() ;
    private Object  _readHandlerLock   = new Object() ;
    private PnfsHandler _pnfsHandler   = null ;
    private HashMap     _messageHash   = new HashMap() ;
    private Object      _senderLock    = new Object() ;
    private PoolSelectionUnit _selectionUnit = null ;

    private long _interval         = 15 * 1000;
    private long _pnfsTimeout      = 15 * 1000;
    private long _readPoolTimeout  = 15 * 1000;
    private long _poolFetchTimeout = 5 * 24 * 3600 * 1000;
    private long _writePoolTimeout = 15 * 1000;
    private long _poolTimeout      = 15 * 1000;

    private double _spaceCostFactor       = 1.0 ;
    private double _performanceCostFactor = 1.0 ;
    private double _costCut               = 10000.0 ;

    private RequestContainer _requestContainer = null ;

    private SimpleDateFormat formatter
         = new SimpleDateFormat ("MM.dd HH:mm:ss");
    
    public PoolManagerV4( String cellName , String args ) throws Exception {
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
               
           
           
           _requestContainer = new RequestContainer();
           
           addCommandListener( _requestContainer ) ;
               
           runSetupFile() ;
           
           _pnfsHandler      = new PnfsHandler( this , new CellPath(_pnfsManagerName) ) ;
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
                if( answer.length() > 0 )say( "Answer    : "+answer ) ;
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
          writer.print( "set pool decision ") ;
          writer.print( " -spacecostfactor="+_spaceCostFactor ) ;
          writer.print( " -cpucostfactor="+_performanceCostFactor ) ;
          writer.println( " -costcut="+_costCut ) ;
          writer.println( "#" ) ;
          StringBuffer sb = new StringBuffer(16*1024) ;
          _selectionUnit.dumpSetup(sb) ;
          _requestContainer.dumpSetup(sb);
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
	pw.println("PoolManager VI [$Id: PoolManagerV4.java,v 1.21 2006-05-12 20:47:12 tigran Exp $]");
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
        if( pool.setSerialId( poolMessage.getSerialId() ) )
           _requestContainer.poolRestarted(poolName);
		
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
	
        Object message  = cellMessage.getMessageObject();
	
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

        }else{
            synchronized( _senderLock ){
            
                UOID uoid = cellMessage.getLastUOID() ;
                
                PoolRequestHandler handler = 
                    (PoolRequestHandler)_messageHash.remove( uoid ) ;
                if( handler == null ){
	           say("Unexpected message class 7 "+
                       message.getClass()+" from source = "+
                       cellMessage.getSourceAddress() );
	           return;
                }
                
                handler.mailForYou( message ) ;
                
            }
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
    //------------------------------------------------------------------------------
    //
    //  'queryPoolsForPnfsId' sends PoolCheckFileMessages to all pools
    //  specified in the pool iterator. It waits until all replies
    //  have arrived, the global timeout has expired or the thread
    //  was interrupted.
    //
    private List queryPoolsForPnfsId( Iterator pools , PnfsId pnfsId , long filesize ) 
            throws Exception {

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
                ( pool.getActive() > (5*60*1000) ) )continue ;

            say( "queryPoolsForPnfsId : PoolCheckFileRequest to : "+poolName);
            //
            // send query
            //
	    CellMessage  cellMessage = 
                   new CellMessage(  new CellPath(poolName), 
                                     new PoolCheckFileCostMessage(
                                          poolName,pnfsId,filesize) 
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

	   if( ! (message instanceof PoolFileCheckable)){
	      esay("queryPoolsForPnfsId : Unexpected message from ("+
                   answer.getSourcePath()+") "+message.getClass());
              continue ;
	   } 
	   PoolFileCheckable poolMessage = (PoolFileCheckable)message;
           say( "queryPoolsForPnfsId : reply : "+poolMessage ) ;
	   boolean have     = poolMessage.getHave();
	   String  poolName = poolMessage.getPoolName();
	   if( have ){
              list.add( poolMessage ) ;
	   }else{ 
              esay("queryPoolsForPnfsId : clearingCacheLocation for pnfsId "+
                    pnfsId+" at pool "+poolName ) ;
              _pnfsHandler.clearCacheLocation( pnfsId , poolName ) ;
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

	   if( ! ( message instanceof PoolCostCheckable )){
	      esay("queryPoolsForCost : Unexpected message from ("+
                   answer.getSourcePath()+") "+message.getClass());
              continue ;
	   } 
	   PoolCostCheckable poolMessage = (PoolCostCheckable)message;
           say( "queryPoolsForCost : reply : "+poolMessage ) ;
	   String  poolName = poolMessage.getPoolName();
           list.add( poolMessage ) ;
        }
        say( "queryPoolsForCost : number of valid replies : "+list.size() );
        return list ;
    }
    private class XProtocolInfo implements IpProtocolInfo {
       private String [] _host = new String[1] ;
       
       private static final long serialVersionUID = -4233684717483056106L;
       
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
       
       private static final long serialVersionUID = -5437029688035829010L;
       
       private XStorageInfo( String hsm , String storageClass ){
          _hsm = hsm ;
          _storageClass = storageClass ;
       }
       public String getStorageClass(){ return _storageClass ; }
       public void setBitfileId( String bfid ){ } ;
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
          List list = getFileAvailableList( pnfsId , storageInfo , protocolInfo ) ;

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
                      getStagePoolList( storageInfo , protocolInfo , size ) :
                      getStorePoolList( storageInfo , protocolInfo , size ) ;

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
    private List getStorePoolList(  StorageInfo  storageInfo ,
                                    ProtocolInfo protocolInfo ,
                                    long         filesize )
            throws Exception {
            
       String hsm          = storageInfo.getHsm() ;
       String storageClass = storageInfo.getStorageClass() ;
       String cacheClass   = storageInfo.getCacheClass() ;
       String  hostName    = 
                  protocolInfo instanceof IpProtocolInfo ?
                  ((IpProtocolInfo)protocolInfo).getHosts()[0] :
                  null ;
       int  maxDepth      = 9999 ;
       List [] prioPools = PoolPreferenceLevel.fromPoolPreferenceLevelToList(
           _selectionUnit.match( "write" , 
                                 storageClass+"@"+hsm ,
                                 cacheClass ,
                                 hostName ,
                                 null ,
                                 null ) );    // doing the HashMap later.
       
       //
       // this is the final knock out.
       //
       if( prioPools.length == 0 )
          throw new
          CacheException( 19 ,
                           "No write pools available for <"+
                           storageClass+"@"+hsm+">" ) ;


       List costs = new ArrayList() ;  
       for( int prio = 0 ; prio < Math.min( maxDepth , prioPools.length ) ; prio++ ){                       
          costs = queryPoolsForCost( prioPools[prio].iterator() , filesize ) ;
          if( costs.size() != 0 )break ;
       }  
       if( costs.size() == 0 )
          throw new
          CacheException( 20 ,
                           "No reply from costcheck for <"+
                           storageClass+"@"+hsm+">" ) ;
                           
       TreeSet order = new TreeSet( new CostComparator() ) ;
       order.addAll( costs ) ;
       
       return new ArrayList( order ) ;              
    }
    private List getStagePoolList(  StorageInfo  storageInfo ,
                                    ProtocolInfo protocolInfo ,
                                    long         filesize  )
            throws Exception {
            
       String hsm          = storageInfo.getHsm() ;
       String storageClass = storageInfo.getStorageClass() ;
       String cacheClass   = storageInfo.getCacheClass() ;
       String  hostName    = 
                  protocolInfo instanceof IpProtocolInfo ?
                  ((IpProtocolInfo)protocolInfo).getHosts()[0] :
                  null ;
       int  maxDepth      = 9999 ;
       List [] prioPools = PoolPreferenceLevel.fromPoolPreferenceLevelToList(
           _selectionUnit.match( "cache" , 
                                 storageClass+"@"+hsm ,
                                 cacheClass ,
                                 hostName ,
                                 null ,
                                 null ) );    // doing the HashMap later.
       //
       // this is the final knock out.
       //
       if( prioPools.length == 0 )
          throw new
          CacheException( 19 ,
                           "No read pools available for <"+
                           storageClass+"@"+hsm+">" ) ;


       List costs = new ArrayList() ;  
       for( int prio = 0 ; prio < Math.min( maxDepth , prioPools.length ) ; prio++ ){                       
          costs = queryPoolsForCost( prioPools[prio].iterator() , filesize ) ;
          if( costs.size() != 0 )break ;
       }  
       if( costs.size() == 0 )
          throw new
          CacheException( 20 ,
                           "No reply from costcheck for <"+
                           storageClass+"@"+hsm+">" ) ;
                           
       TreeSet order = new TreeSet( new CostComparator() ) ;
       order.addAll( costs ) ;
       
       return new ArrayList( order ) ;              
    }
    private List getFileAvailableList( PnfsId pnfsId ,
                                       StorageInfo storageInfo ,
                                       ProtocolInfo protocolInfo )
            throws Exception {
    
       String hsm          = storageInfo.getHsm() ;
       String storageClass = storageInfo.getStorageClass() ;
       String cacheClass   = storageInfo.getCacheClass() ;
       String hostName     = 
                  protocolInfo instanceof IpProtocolInfo ?
                  ((IpProtocolInfo)protocolInfo).getHosts()[0] :
                  null ;
       //
       // will ask the PnfsManager for a hint
       // about the pool locations of this
       // pnfsId. Returns an enumeration of
       // the possible pools.
       //
       List expectedPools = _pnfsHandler.getCacheLocations( pnfsId ) ;
       say( "ReadPoolRequestHandler : expectedPools : "+expectedPools ) ;
       //
       //  get the prioratized list of allowed pools for this
       //  request. (We are only allowed to use the level-1
       //  pools.
       //       
       List [] prioPools = PoolPreferenceLevel.fromPoolPreferenceLevelToList(
           _selectionUnit.match( "read" , 
                                 storageClass+"@"+hsm ,
                                 cacheClass ,
                                 hostName ,
                                 null ,
                                 null      ) ) ; // doing the HashMap later.
       //
       //  merge the prioPools with the expected pools.
       //  
       int  maxDepth      = 9999 ;
       List fileAvailable = new ArrayList() ;
       HashSet allPools   = new HashSet() ;
       for( int prio = 0 ; prio < Math.min( maxDepth , prioPools.length ) ; prio++ ){  
          allPools.addAll( prioPools[prio] ) ;                     
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
          fileAvailable = queryPoolsForPnfsId( pools , pnfsId , 0 ) ;
          if( fileAvailable.size() > 0 )break ;
       }
       if( fileAvailable.size() <= 1 )return fileAvailable ;
       
       TreeSet order = new TreeSet( new CostComparator() ) ;
       order.addAll( fileAvailable ) ;
       
       return new ArrayList( order ) ;              
    }
    private String selectFuzzy( List costList ){
       return ((PoolCheckable)costList.get(0)).getPoolName() ;
    }
    private class CostComparator implements Comparator {
       public int compare( Object o1 , Object o2 ){
          PoolCostCheckable check1 = (PoolCostCheckable)o1 ;
          PoolCostCheckable check2 = (PoolCostCheckable)o2 ;
          Double d1 = new Double( calculateCost( check1 ) ) ;
          Double d2 = new Double( calculateCost( check2 ) ) ;
          int c = d1.compareTo( d2 ) ;
          if( c != 0 )return c ;
          return check1.getPoolName().compareTo( check2.getPoolName() ) ;
       }
    }
    private double calculateCost( PoolCostCheckable checkable ){
       return Math.abs(checkable.getSpaceCost())       * _spaceCostFactor + 
              Math.abs(checkable.getPerformanceCost()) * _performanceCostFactor ;
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
       
       public WriteRequestHandler( CellMessage cellMessage ){
       
           _cellMessage = cellMessage ;
           _request     =  (PoolMgrSelectWritePoolMsg)_cellMessage.getMessageObject() ;
           _nucleus.newThread( this , "writeHandler" ).start() ;
       }
       public void run(){
                
           StorageInfo  storageInfo  = _request.getStorageInfo() ;
           ProtocolInfo protocolInfo = _request.getProtocolInfo() ;
           
           try{
              List storeList = getStorePoolList(  storageInfo ,
                                                  protocolInfo ,
                                                  50000000L );
              
              requestSucceeded( selectFuzzy( storeList ) ) ;
              
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
	   }catch (Exception e){
	       esay("Exception in requestSucceeded : "+e);
	       esay(e);
	   }
       }
    }
    private long    _retryTimer = 15 * 60 * 1000 ;
    public class RequestContainer implements Runnable {
       private HashMap _map = new HashMap() ;
       private String  _warningPath       = "billing" ;
       public RequestContainer(){
          _nucleus.newThread(this,"Container-ticker").start();
       }
       public void run(){
         try{
            while( ! Thread.currentThread().interrupted() ){
               Thread.currentThread().sleep(60000) ;
               synchronized(this){
                 Iterator i = _map.values().iterator() ;
                 while( i.hasNext() ){
                     PoolRequestHandler h = (PoolRequestHandler)i.next() ;
                     h.alive() ;
                 }
               }
            }
         }catch(InterruptedException ie ){
            esay("Container-ticker done");
         }
       }
       public void poolRestarted( String poolName ){
          try{
             synchronized( this ){
                Iterator it = _map.values().iterator() ;
                while( it.hasNext() ){
                   PoolRequestHandler rph = (PoolRequestHandler)it.next() ;
                   if( rph.getPoolCandidate().equals(poolName) ){
                      say("Restore Manager : retrying : "+rph ) ;
                      rph.retry() ;
                   }
                }
             }
          }catch(Exception ee ){
             esay("Problem retrying pool "+poolName+" ("+ee+")");
          }
       }
       public void dumpSetup( StringBuffer sb ){
          sb.append("#\n# Submodule [rc] : ").append(this.getClass().toString()).append("\n#\n");
          sb.append("rc set retry ").append(_retryTimer/1000).append("\n");
          sb.append("rc set warning path ").append(_warningPath).append("\n");
       }
       public String hh_rc_set_warning_path = " # where to send the warnings to" ;
       public String ac_rc_set_warning_path_$_0_1( Args args ){
          if( args.argc() > 0 ){
             _warningPath = args.argv(0) ;
          }
          return _warningPath ;
       }
       public String hh_rc_set_retry = "<retryTimer/seconds>" ;
       public String ac_rc_set_retry_$_1(Args args ){
          _retryTimer = 1000L * Long.parseLong(args.argv(0));
          return "" ;
       }
       public String hh_rc_retry = "<pnfsId>|*" ;
       public String ac_rc_retry_$_1( Args args ) throws CacheException {
          if( args.argv(0).equals("*") ){
             synchronized( this ){
                Iterator it = _map.values().iterator() ;
                while( it.hasNext() ){
                   PoolRequestHandler rph = (PoolRequestHandler)it.next() ;
                   rph.retry() ;
                }
             }
          }else{
             synchronized( this ){
                PoolRequestHandler rph = (PoolRequestHandler)_map.get(args.argv(0));
                if( rph == null )
                   throw new
                   IllegalArgumentException("Not found : "+args.argv(0) ) ;
                rph.retry() ;             
             }
          }
          return "Done" ;
       }
       public String hh_rc_failed = "<pnfsId> [<errorNumber> [<errorMessage>]]" ;
       public String ac_rc_failed_$_1_3( Args args ) throws CacheException {
//          PnfsId pnfsId = new PnfsId( args.argv(0) ) ;
          int    errorNumber = args.argc() > 1 ? Integer.parseInt(args.argv(1)) : 1;
          String errorString = args.argc() > 2 ? args.argv(2) : "Operator Intervention" ;
          synchronized( this ){
             PoolRequestHandler rph = (PoolRequestHandler)_map.get(args.argv(0));
             rph.failed(errorNumber,errorString) ;             
          }
          return "Done" ;
       }
       public String hh_rc_ls = " [-w] # lists pending requests" ;
       public String ac_rc_ls( Args args ){
          StringBuffer sb = new StringBuffer() ;
          if( args.getOpt("w") == null ){
             synchronized(this){
               Iterator i = _map.values().iterator() ;
               while( i.hasNext() ){
                   PoolRequestHandler h = (PoolRequestHandler)i.next() ;
                   sb.append(h.toString()).append("\n");
               }
             }
          }else{
             synchronized(_senderLock){
               Iterator i = _messageHash.keySet().iterator() ;
               while( i.hasNext() ){
                   UOID uoid = (UOID)i.next() ;
                   PoolRequestHandler h = (PoolRequestHandler)_messageHash.get(uoid) ;
                   sb.append(uoid.toString()).append(" ").append(h.toString()).append("\n");
               }
             }
          }
          return sb.toString();
       }
       public String hh_xrc_ls = " # lists pending requests (binary)" ;
       public Object ac_xrc_ls( Args args ){
          ArrayList list = new ArrayList() ;
          synchronized(this){
            Iterator i = _map.values().iterator() ;
            while( i.hasNext() ){
                list.add(((PoolRequestHandler)i.next()).getRestoreHandlerInfo()) ;
            }
          }
	  return list.toArray( new RestoreHandlerInfo[0] ) ;
       }
       private synchronized void addRequest( CellMessage message ){
           PoolMgrSelectPoolMsg request = 
              (PoolMgrSelectPoolMsg)message.getMessageObject() ;
           
           PnfsId       pnfsId       = request.getPnfsId() ;
           ProtocolInfo protocolInfo = request.getProtocolInfo() ;
           String  hostName    = 
                  protocolInfo instanceof IpProtocolInfo ?
                  ((IpProtocolInfo)protocolInfo).getHosts()[0] :
                  "NoSuchHost" ;
                  
           String netName = _selectionUnit.getNetIdentifier(hostName);
           
           String canonicalName = pnfsId +"@"+netName ;
           
           PoolRequestHandler handler = (PoolRequestHandler)_map.get(canonicalName);
           if( handler == null ){
              _map.put( canonicalName , handler = new PoolRequestHandler( pnfsId , canonicalName) ) ;
           }
           handler.addRequest(message) ;
       }
       
       private synchronized void freeMe( String canonicalName ){
           _map.remove( canonicalName ) ;
       }
       private void sendInfoMessage( PnfsId pnfsId , 
                                     StorageInfo storageInfo , 
                                     int rc , String infoMessage ){
         try{
           WarningPnfsFileInfoMessage info = 
               new WarningPnfsFileInfoMessage(  
                                       "PoolManager","PoolManager",pnfsId ,
                                       rc , infoMessage )  ;
               info.setStorageInfo( storageInfo ) ;
               
           _nucleus.sendMessage(
            new CellMessage( new CellPath(_warningPath), info )
                                ) ;

         }catch(Exception ee){
            esay("Coudn't send WarningInfoMessage : "+ee ) ;
         }
       }
    }
    ///////////////////////////////////////////////////////////////
    //
    // tthe read io request handler
    //
    private class PoolRequestHandler implements Runnable {
	
	protected PnfsId       _pnfsId;
        protected ArrayList    _messages = new ArrayList() ;
        protected boolean      _active   = false ;
        protected Thread       _finder   = null ;
        protected int          _retryCounter = 0 ;
        private   String       _poolCandidate = null ;
        private   UOID         _waitingFor    = null ;
        private   long         _waitUntil     = 0 ;
        private   boolean      _failOk        = false ;
        private   String       _state         = "[<idle>]";
        private   int          _currentRc     = 0 ;
        private   String       _currentRm     = "" ;
        private   boolean      _allowInterrupt= false ;
        private   boolean      _mailSent      = false ;
        private   long         _started       = System.currentTimeMillis() ;
        private   StorageInfo  _storageInfo   = null ;
        private   String       _name          = null ;
        public PoolRequestHandler( PnfsId pnfsId , String canonicalName ){
                  
	    _pnfsId  = pnfsId ;
	    _name    = canonicalName ;
	}
        public String getPoolCandidate(){ 
          return _poolCandidate==null?"<unknown>":_poolCandidate  ; 
        }
	public RestoreHandlerInfo getRestoreHandlerInfo(){
	   return new RestoreHandlerInfo(
	          _name,
		  _messages.size(),
		  _retryCounter ,
                  _started ,
		  (_poolCandidate==null?"<unknown>":_poolCandidate) ,
		  _state ,
		  _currentRc ,
		  _currentRm ) ;
	}
        public String toString(){
           return _name+
                  " m="+_messages.size()+
                  " ["+_retryCounter+"] "+
                  " ["+(_poolCandidate==null?"<unknown>":_poolCandidate)+"] "+
                  _state+
                  " {"+_currentRc+","+_currentRm+"}" ;
        }
        public void say(String message){
           PoolManagerV4.this.say(_pnfsId.toString()+" : "+message) ;
        }
        public void esay(String message){
           PoolManagerV4.this.esay(_pnfsId.toString()+" : "+message) ;
        }
        public void esay(Exception e){
           PoolManagerV4.this.esay(e) ;
        }
        private synchronized void mailForYou( Object message ){
           int    rc = 0 ;
           String rm = null ;
           if( message instanceof PoolFetchFileMessage ){
              PoolFetchFileMessage m = (PoolFetchFileMessage)message ;
              rc = m.getReturnCode() ;
              rm = m.getErrorObject() == null ? "Failed : " : m.getErrorObject().toString() ;
              if( rc == 0 ){
                 requestSucceeded( m.getPoolName() ) ;             
                 return ;                 
              }
           }else if( message instanceof NoRouteToCellException ){
              rc = 184 ;
              rm = message.toString() ;
           }
           _currentRc = rc ;
           _currentRm = rm ;
           esay( "MailForYou ["+_retryCounter+"] ["+rc+"] "+rm ) ;
           if( _failOk )requestFailed( rc , rm ) ;
           else nextToDo() ;
        }
        private void alive(){
           if( _poolCandidate != null ){
              PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool(_poolCandidate) ;
              
              if( ( pool != null ) && ( pool.getActive() > 10L*60L*1000L ) ){
                 say("Restore Manager : retry due to inactive pool : "+_poolCandidate);
                 try{ 
                   retry() ;
                   return ;
                 }catch(Exception ee ){
                    esay("Restore Manager : problem in retry : "+ee ) ;
                 }             
              }
           }

           long now = System.currentTimeMillis() ;
           if( ( _waitUntil > 0 ) &&  ( _waitUntil < now ) ){
                 _waitUntil = 0 ;
                 nextToDo();
                 
           }
           if( ( ! _mailSent ) && ( now > ( _started + (long)(1*3600*1000) ) ) ){
              _requestContainer.sendInfoMessage(_pnfsId,_storageInfo,3,"Rottening") ;
              _mailSent = true ;
           }
        }
        private synchronized void retry() throws CacheException {
//           if( ! _allowInterrupt )
//              throw new
//              CacheException("IllegalState Exception : can't be interrupted : "+_state);
              
            synchronized( _senderLock ){
               if( _waitingFor != null )_messageHash.remove(_waitingFor) ;
            }
            _retryCounter = 0 ;
            nextToDo() ;
            return ;
        }
        private synchronized void failed( int errorNumber , String errorMessage ) 
                throws CacheException {
//           if( ! _allowInterrupt )
//              throw new
//              CacheException("IllegalState Exception : can't be interrupted : "+_state);
              
            synchronized( _senderLock ){
               if( _waitingFor != null )_messageHash.remove(_waitingFor) ;
            }
            requestFailed( errorNumber , errorMessage ) ;
            return ;
        }
        private void nextToDo(){ nextToDo(0) ; }
        private synchronized void nextToDo( int skip ){
           _retryCounter += ( skip + 1 ) ;
           say("nextToDo State = "+_retryCounter );
           switch( _retryCounter){ 
           
             case 1 :  _allowInterrupt = false ;
                       _poolCandidate  = null ;
                      runFinder() ;
                      _state = "RunningFinder(2)" ;
                      return ; 
                      
             case 2 :  _allowInterrupt = true ;
                      _waitUntil = System.currentTimeMillis() + _retryTimer ;
                      _state = "Waiting ... " ;
                      return ; 
                      
             case 3 :  _allowInterrupt = false ;
                      _poolCandidate = null ;
                      runFinder() ;
                      _state = "RunningFinder(3)" ;
                      return ; 
                      
             default : _state = "Suspended" ;
                       _allowInterrupt = true ;
                       _requestContainer.sendInfoMessage( _pnfsId , _storageInfo,  2 , "Suspended" );
           }
           
           
        }
        public void run(){
           //
           //
           long starting = System.currentTimeMillis() ;
           
           CellMessage m = (CellMessage)_messages.get(0) ;
           
           PoolMgrSelectReadPoolMsg request = 
                (PoolMgrSelectReadPoolMsg)m.getMessageObject() ;
                
           StorageInfo  storageInfo  = _storageInfo = request.getStorageInfo() ;
           ProtocolInfo protocolInfo = request.getProtocolInfo() ;
           try{
              List av = getFileAvailableList( _pnfsId , storageInfo , protocolInfo );
              if( av.size() > 0 ){
                 PoolCostCheckable cost = (PoolCostCheckable)av.get(0) ;
                 say( ""+av.size()+" av candidates found, chosing : "+cost.getPoolName() ) ;
                 if( cost.getPerformanceCost() < _costCut ){
                    requestSucceeded( cost.getPoolName() ) ;
                    say( "Selection av took : "+(System.currentTimeMillis()-starting));
                    return ;
                 }else{
                    esay( "Cost too high for "+cost.getPoolName() ) ;
                 }
              }
           }catch(Exception ee ){
              esay("Exception in getFileAvailableList : "+ee ) ;
              //
              // now we can try to get it staged
              //
           }
           try{
              List staged = getStagePoolList( storageInfo ,
                                              protocolInfo ,
                                              storageInfo.getFileSize()  ) ;
              
              int pool = 0 ;
              if( _poolCandidate != null ){
                 say("Second shot excluding : "+_poolCandidate ) ;
                 //
                 // find a pool which is not identical to the first candidate
                 //
                 for( ; ( pool < staged.size() ) &&
                        ( ((PoolCostCheckable)staged.get(pool)).getPoolName().
                             equals(_poolCandidate) ) ; pool++ ) ; 
                 
                 if( pool == staged.size() )
                    throw new 
                    CacheException( 183 , "No pool left in retry (2)" ) ;
                                       
              }
              
              PoolCostCheckable cost = (PoolCostCheckable)staged.get(pool) ;
              say( ""+staged.size()+" stage candidates found, chosing : "+cost.getPoolName() ) ;
              sendFetchRequest( _poolCandidate = cost.getPoolName() , storageInfo ) ;
              say( "Selection stage took : "+(System.currentTimeMillis()-starting));
              
           }catch( CacheException ce ){
              if( _failOk )requestFailed( ce.getRc() , ce.getMessage() ) ;
              esay(ce.toString()) ;
              say( "Selection stage took : "+(System.currentTimeMillis()-starting));
              nextToDo() ;
              return ;
           }catch( Exception ee ){
              if( _failOk )requestFailed( 182 , ee.getMessage() ) ;
              esay(ee.toString()) ;
              say( "Selection stage took : "+(System.currentTimeMillis()-starting));
              nextToDo() ;
              return ;
           }
           
           return ;
        }
        public void runFinder(){
           _finder = _nucleus.newThread( this , "finder-"+_pnfsId ) ;
           _finder.start() ;
        
        }
	private void sendFetchRequest( String poolName , StorageInfo storageInfo ) 
                throws Exception {
                
	    CellMessage cellMessage = new CellMessage(
                                new CellPath( poolName ), 
	                        new PoolFetchFileMessage(
                                        poolName,
                                        storageInfo,
                                        _pnfsId          )
                                );
            synchronized( _senderLock ){
                sendMessage( cellMessage );
                _messageHash.put( _waitingFor = cellMessage.getUOID() , this ) ;
                _state = "[Staging "+formatter.format(new Date())+"]" ;
            }
	}
        public synchronized void addRequest( CellMessage message ){
           _messages.add(message);
           if( _active )return ;
           _active = true ;
           runFinder() ;
        }
        protected void requestFailed(int errorCode, String errorMessage){
           synchronized( _requestContainer ){

              Iterator messages = _messages.iterator() ;
              while( messages.hasNext() ){
                 CellMessage m = (CellMessage)messages.next() ;
                 PoolMgrSelectPoolMsg rpm =
                    (PoolMgrSelectPoolMsg)m.getMessageObject() ;
                 rpm.setFailed( errorCode , errorMessage);
                 try{
	             m.revertDirection();
	             sendMessage(m);
	         }catch (Exception e){
                     esay("Exception requestFailed : "+e);
                     esay(e);
	         }
              }
              _requestContainer.freeMe(_name);
           } 
        }
        protected void requestSucceeded(String poolName){
           synchronized( _requestContainer ){
              Iterator messages = _messages.iterator() ;
              while( messages.hasNext() ){
                 CellMessage m = (CellMessage)messages.next() ;
                 PoolMgrSelectPoolMsg rpm =
                    (PoolMgrSelectPoolMsg)m.getMessageObject() ;
	         rpm.setPoolName(poolName);
	         rpm.setSucceeded();
	         try{
                     m.revertDirection();
                     sendMessage(m);
	         }catch (Exception e){
                     esay("Exception requestSucceeded : "+e);
	             esay(e);
	         }
              }
              _requestContainer.freeMe(_name);
           } 
        }
        
    }
} 
