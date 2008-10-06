// $Id: PoolManagerV5.java,v 1.48 2007-10-10 08:05:34 tigran Exp $

package diskCacheV111.poolManager ;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.dcache.poolmanager.Utils;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.PoolCostCheckable;
import diskCacheV111.vehicles.PoolLinkGroupInfo;
import diskCacheV111.vehicles.PoolManagerGetPoolListMessage;
import diskCacheV111.vehicles.PoolManagerPoolModeMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.PoolMgrGetPoolByLink;
import diskCacheV111.vehicles.PoolMgrGetPoolLinkGroups;
import diskCacheV111.vehicles.PoolMgrQueryPoolsMsg;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.QuotaMgrCheckQuotaMessage;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import dmg.util.Args;
import dmg.util.CommandException;

public class PoolManagerV5 extends CellAdapter {

    private final String      _cellName;
    private final Args        _args    ;
    private final CellNucleus _nucleus ;

    private int  _writeThreads     = 0 ;
    private int  _readThreads      = 0 ;

    private int _counterPoolUp         = 0 ;
    private int _counterSelectWritePool= 0 ;
    private int _counterSelectReadPool = 0 ;

    private String  _pnfsManagerName   = "PnfsManager";
    private String  _selectionUnitName = "diskCacheV111.poolManager.PoolSelectionUnitV2" ;
    private final String  _setupFileName  ;
    private Map _readHandlerList   = new HashMap() ;
    private final Object  _readHandlerLock   = new Object() ;

    private final PnfsHandler       _pnfsHandler  ;
    private final PoolSelectionUnit _selectionUnit ;
    private final PoolMonitorV5     _poolMonitor   ;

    private long _interval         = 15 * 1000;
    private long _pnfsTimeout      = 15 * 1000;
    private long _readPoolTimeout  = 15 * 1000;
    private long _poolFetchTimeout = 5 * 24 * 3600 * 1000;
    private long _writePoolTimeout = 15 * 1000;
    private long _poolTimeout      = 15 * 1000;

    private final CostModule   _costModule  ;
    private PoolOperator _poolOperator = null ;
    private CellPath     _poolStatusRelayPath = null ;

    private final Object _setupLock             = new Object() ;

    private final RequestContainerV5 _requestContainer ;
    private WatchdogThread     _watchdog         = null ;
    private final PartitionManager   _partitionManager ;

    private boolean _sendCostInfo  = false ;                   //VP
    private boolean _quotasEnabled = false ;
    private String  _quotaManager  = "QuotaManager" ;


    private final static Logger _logPoolMonitor = Logger.getLogger("logger.org.dcache.poolmonitor." + PoolManagerV5.class.getName());


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
           say("Cost module successfully started");

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
           // Quota settings
           //
           _quotasEnabled = false ;
           _quotaManager  = "QuotaManager" ;
           if( ( tmp = _args.getOpt("quotaManager") ) != null ){
               if( tmp.length() == 0 ){
                   _quotasEnabled = true ;
               }else{
                   if( tmp.equals("none" ) ){
                      _quotasEnabled = false ;
                   }else{
                      _quotasEnabled = true ;
                      _quotaManager  = tmp ;
                   }
               }
           }
           if( _quotasEnabled ){
              say("Quotas enabled ; QuotaManager = <"+_quotaManager+">");
           }else{
              say("Quotas disabled");
           }
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
    private void runSetupFile(StringBuffer sb) throws Exception {
        File setupFile = new File(_setupFileName);
        if (!setupFile.exists())
            throw new IllegalArgumentException("Setup File not found : "
                    + _setupFileName);

        BufferedReader reader = new BufferedReader(new FileReader(setupFile));
        try {

            int lineCounter = 0;
            String line = null;
            while ((line = reader.readLine()) != null) {
                ++lineCounter;
                line = line.trim();
                if (line.length() == 0)
                    continue;
                if (line.charAt(0) == '#')
                    continue;
                try {
                    say("Executing : " + line);
                    String answer = command(line);
                    if (answer.length() > 0)
                        say("Answer    : " + answer);
                } catch (Exception ee) {
                    esay("Exception at line " +lineCounter + " : " + ee.toString());
                    if (sb != null)
                        sb.append(line).append(" -> ").append(ee.toString())
                                .append("\n");
                }
            }
        } finally {
            reader.close();
        }

    }
    @Override
    public CellVersion getCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision$" ); }
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
          writer.close() ;
       }
       if( ! tmpFile.renameTo( setupFile ) ){

          tmpFile.delete() ;

          throw new
          IllegalArgumentException( "Rename failed : "+_setupFileName ) ;

       }
       return ;
    }
    private class WatchdogThread implements Runnable {
        private long _deathDetected = 10L * 60L * 1000L; // 10 minutes
        private long _sleepTimer = 1L * 60L * 1000L; // 1 minute
        private long _watchdogSequenceCounter = 0L;

        public WatchdogThread() {
            _nucleus.newThread(this, "watchdog").start();
            say("WatchdogThread initialized with : " + this);
        }

        public WatchdogThread(String parameter) {
            //
            // [<deathDetection>]:[<sleeper>]
            //
            long deathDetected = 0;
            long sleeping = 0;
            try {
                StringTokenizer st = new StringTokenizer(parameter, ":");
                String tmp = null;
                if (st.hasMoreTokens()) {
                    tmp = st.nextToken();
                    if (tmp.length() > 0)
                        deathDetected = Long.parseLong(tmp);
                }
                if (st.hasMoreTokens()) {
                    tmp = st.nextToken();
                    if (tmp.length() > 0)
                        sleeping = Long.parseLong(tmp);
                }

                if ((deathDetected < 10) || (sleeping < 10))
                    throw new IllegalArgumentException("Timers to small : " + parameter);

                if (deathDetected > 0L)
                    _deathDetected = deathDetected * 1000L;
                if (sleeping > 0L)
                    _sleepTimer = sleeping * 1000L;

            } catch (Exception ee) {
                esay("WatchdogThread : illegal arguments [" + parameter + "] (using defaults) " + ee.getMessage());
            }
            _nucleus.newThread(this, "watchdog").start();
            say("WatchdogThread initialized with : " + this);
        }

        public void run() {
            say("watchdog thread activated");
            while (true) {
                try {
                    Thread.sleep(_sleepTimer);
                } catch (InterruptedException e) {
                    say("watchdog thread interrupted");
                    break;
                }
                runWatchdogSequence(_deathDetected);
                _watchdogSequenceCounter++;
            }
            say("watchdog finished");
        }

        @Override
        public String toString() {
            return "DeathDetection=" + (_deathDetected / 1000L) + ";Sleep="
                    + (_sleepTimer / 1000L) + ";Counter="
                    + _watchdogSequenceCounter + ";";
        }
    }

    private void handlePoolMode(PoolManagerPoolModeMessage msg,
            CellMessage message) {

        PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool(msg
                .getPoolName());
        if (pool == null) {
            msg.setFailed(563, "Pool not found : " + msg.getPoolName());
        } else if (msg.getPoolMode() == PoolManagerPoolModeMessage.UNDEFINED) {
            //
            // get pool mode
            //
            msg.setPoolMode(PoolManagerPoolModeMessage.READ | (pool.isReadOnly() ? 0 : PoolManagerPoolModeMessage.WRITE));
        } else {
            //
            // set pool mode
            //
            pool.setReadOnly((msg.getPoolMode() & PoolManagerPoolModeMessage.WRITE) == 0);
        }

        if (!msg.getReplyRequired())
            return;
        try {
            say("Sending reply " + message);
            message.revertDirection();
            sendMessage(message);
        } catch (Exception e) {
            esay("Can't reply message : " + e);
        }

    }

    private void runWatchdogSequence(long deathDetectedTimer)
    {
        for (String name : _selectionUnit.getDefinedPools(false)) {
            PoolSelectionUnit.SelectionPool pool = _selectionUnit.getPool(name);
            if (pool != null) {
                if (pool.getActive() > deathDetectedTimer
                    && pool.setSerialId(0L)) {

                    if( _logPoolMonitor.isDebugEnabled() ) {
                        _logPoolMonitor.debug("Pool " + name + " declared as DOWN (no ping in " + deathDetectedTimer/1000 +" seconds).");
                    }
                    _requestContainer.poolStatusChanged(name, PoolStatusChangedMessage.DOWN);
                    sendPoolStatusRelay(name, PoolStatusChangedMessage.DOWN,
                                        null, 666, "DEAD");
                }
            }
        }
    }

    private class MessageTimeoutThread implements Runnable {
        public MessageTimeoutThread() {
            _nucleus.newThread(this, "messageTimeout").start();
        }

        public void run() {
            while (true) {
                _nucleus.updateWaitQueue();
                try {
                    Thread.sleep(_interval);
                } catch (InterruptedException e) {
                    say("Message timeout thread interrupted");
                    break;
                }
            }
            say("Message timeout thread finished");
        }
    }

    @Override
    public void getInfo( PrintWriter pw ){
	pw.println("PoolManager V [$Id: PoolManagerV5.java,v 1.48 2007-10-10 08:05:34 tigran Exp $]");
        pw.println(" SelectionUnit : "+_selectionUnit.getVersion() ) ;
        pw.println(" Write Threads : "+_writeThreads) ;
        pw.println(" Read  Threads : "+_readThreads) ;
        pw.println("  Pool Timeout : "+_poolMonitor.getPoolTimeout()/1000L) ;
        pw.println("Message counts") ;
        pw.println("           PoolUp : "+_counterPoolUp ) ;
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
    @Override
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
    public String hh_getpoolsbylink = "<linkName> [-size=<filesize>]" ;
    public String ac_getpoolsbylink_$_1( Args args )throws Exception {
       String sizeString = args.getOpt("size") ;
       long size = sizeString == null ? 50000000L : Long.parseLong( sizeString ) ;
       String linkName = args.argv(0) ;

       List list = _poolMonitor.queryPoolsByLinkName( linkName , size ) ;

       StringBuffer sb = new StringBuffer() ;
       for( Iterator i = list.iterator() ; i.hasNext() ; ){
          sb.append( i.next().toString() ).append("\n");
       }
       return sb.toString() ;
    }

    private synchronized
       void poolUp(PoolManagerPoolUpMessage poolMessage, CellPath poolPath)
    {
        poolPath.revert();
        String poolName = poolMessage.getPoolName();
        PoolSelectionUnit.SelectionPool pool =
            _selectionUnit.getPool(poolName, true);

        PoolV2Mode newMode = poolMessage.getPoolMode();
        PoolV2Mode oldMode = pool.getPoolMode();

        if (_logPoolMonitor.isDebugEnabled()) {
            _logPoolMonitor.debug("PoolUp message from " + poolName
                                  + " with mode " + newMode
                                  + " and serialId " + poolMessage.getSerialId());
        }

        /* For compatibility with previous versions of dCache, a pool
         * marked DISABLED, but without any other DISABLED_ flags set
         * is considered fully disabled.
         */
        boolean disabled =
            newMode.getMode() == PoolV2Mode.DISABLED
            || newMode.isDisabled(PoolV2Mode.DISABLED_DEAD)
            || newMode.isDisabled(PoolV2Mode.DISABLED_STRICT);

        /* By convention, the serial number is set to zero when a pool
         * is disabled. This is used by the watchdog to identify, that
         * we have already announced that the pool is down.
         */
        long serial = disabled ? 0 : poolMessage.getSerialId();

        /* Any change in the kind of operations a pool might be able
         * to perform has to be propagated to a number of other
         * components.
         *
         * Notice that calling setSerialId has a side-effect, which is
         * why we call it first.
         */
        boolean changed =
            pool.setSerialId(serial)
            || (newMode.getMode() != oldMode.getMode())
            || !pool.getHsmInstances().equals(poolMessage.getHsmInstances());

        pool.setPoolMode(newMode);
        pool.setHsmInstances(poolMessage.getHsmInstances());
        pool.setActive(!disabled);

        /* Notify others in case the pool status has changed. Due to
         * limitations of the PoolStatusChangedMessage, we will often
         * send a RESTART notification, when in fact only the pool
         * mode has changed.
         */
        if (changed) {
            _logPoolMonitor.info("Pool " + poolName + " changed from mode "
                                 + oldMode + " to " + newMode);

            if (disabled) {
                _requestContainer.poolStatusChanged(poolName,
                                                    PoolStatusChangedMessage.DOWN);
                sendPoolStatusRelay(poolName, PoolStatusChangedMessage.DOWN,
                                    poolMessage.getPoolMode(),
                                    poolMessage.getCode(),
                                    poolMessage.getMessage());
            } else {
                _requestContainer.poolStatusChanged(poolName,
                                                    PoolStatusChangedMessage.UP);
                sendPoolStatusRelay(poolName, PoolStatusChangedMessage.RESTART);
            }
        }
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
    @Override
    public void messageToForward(  CellMessage cellMessage ){

        _costModule.messageArrived(cellMessage);

        super.messageToForward(cellMessage);
    }
    @Override
    public void messageArrived( CellMessage cellMessage ){

        Object message  = cellMessage.getMessageObject();
        synchronized( _setupLock ){

           _costModule.messageArrived( cellMessage ) ;

           if( message instanceof PoolManagerPoolUpMessage ){

               _counterPoolUp ++ ;
               poolUp(  (PoolManagerPoolUpMessage)message ,
                        cellMessage.getSourcePath() ) ;

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
	   }else if( message instanceof PoolMgrGetPoolLinkGroups ){
		   	      getLinkGroups( (PoolMgrGetPoolLinkGroups)message ,
				                  cellMessage);
	   }else if( message instanceof PoolMgrGetPoolByLink ){

                  getPoolByLink( (PoolMgrGetPoolByLink)message ,
                                  cellMessage ) ;

           }else{
               _requestContainer.messageArrived( cellMessage ) ;
	   }

        }
    }

    private void getLinkGroups(PoolMgrGetPoolLinkGroups poolMessage,CellMessage cellMessage ){

        Collection<PoolLinkGroupInfo> linkGroupInfos = Utils.linkGroupInfos(_selectionUnit, _costModule).values();

    	PoolLinkGroupInfo[] poolLinkGroupInfos = linkGroupInfos.toArray(new PoolLinkGroupInfo[linkGroupInfos.size()]);
    	poolMessage.setPoolLinkGroupInfos(poolLinkGroupInfos);
        poolMessage.setReply();

        cellMessage.revertDirection() ;
        try{
           sendMessage( cellMessage ) ;
        }catch(Exception ee ){
           esay( "Problem replying to getLinkGroups Request : "+ee ) ;
        }
    }

    private void getPoolList( PoolManagerGetPoolListMessage poolMessage ,
                              CellMessage cellMessage ){

       String [] pools = _selectionUnit.getActivePools() ;

       poolMessage.setPoolList(Arrays.asList(pools)) ;
       poolMessage.setReply();

       cellMessage.revertDirection() ;
       try{
          sendMessage( cellMessage ) ;
       }catch(Exception ee ){
          esay( "Problem replying to getPoolList Request : "+ee ) ;
       }
    }
    private void getPoolByLink( PoolMgrGetPoolByLink poolMessage ,
                                CellMessage cellMessage ){

       try{
          String linkName = poolMessage.getLinkName() ;
          long   filesize = poolMessage.getFilesize() ;

          List<PoolCostCheckable> pools = _poolMonitor.queryPoolsByLinkName( linkName , filesize ) ;

          if( ( pools == null ) ||  pools.isEmpty() )
             throw new
             NoSuchElementException("No appropriate pools found for link : "+linkName ) ;

          poolMessage.setPoolName( pools.get(0).getPoolName() ) ;

       }catch(Exception ee ){
          poolMessage.setFailed( 57 , ee.getMessage() ) ;
       }

       poolMessage.setReply();

       cellMessage.revertDirection() ;
       try{
          sendMessage( cellMessage ) ;
       }catch(Exception ee ){
          esay( "Problem replying to getPoolByLink Request : "+ee ) ;
       }
    }
    private void queryPools( PoolMgrQueryPoolsMsg poolQueryMessage ,
                             CellMessage cellMessage ){
       DirectionType accessType = poolQueryMessage.getAccessType() ;

          try{
             poolQueryMessage.setPoolList(
               PoolPreferenceLevel.fromPoolPreferenceLevelToList(
                 _selectionUnit.match(
                        accessType ,
                        poolQueryMessage.getStoreUnitName() ,
                        poolQueryMessage.getDCacheUnitName() ,
                        poolQueryMessage.getNetUnitName() ,
                        poolQueryMessage.getProtocolUnitName() ,
                        poolQueryMessage.getStorageInfo(),
                        null             )
                )
              ) ;
          }catch(Exception ee){
             poolQueryMessage.setReply( 102 , ee ) ;
          }

       cellMessage.revertDirection() ;
       try{
          sendMessage( cellMessage ) ;
       }catch(Exception ee ){
          esay( "Problem replying to queryPool Request : "+ee ) ;
       }
    }
    private static class XProtocolInfo implements IpProtocolInfo {
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
    private static class XStorageInfo extends GenericStorageInfo {

       private static final long serialVersionUID = -6624549402952279903L;

       private XStorageInfo( String hsm , String storageClass ){
    	   super(hsm,storageClass);
       }
       @Override
    public String getBitfileId(){ return "" ; }
       @Override
    public long   getFileSize(){ return 100 ; }
       @Override
    public void   setFileSize( long fileSize ){}
       @Override
    public boolean isStored(){ return true ; }

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
                                                      protocolInfo, null ) ;

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
    private boolean quotasExceeded( StorageInfo info ){

       String storageClass = info.getStorageClass()+"@"+info.getHsm() ;

       QuotaMgrCheckQuotaMessage quotas = new QuotaMgrCheckQuotaMessage( storageClass ) ;
       CellMessage msg = new CellMessage( new CellPath(_quotaManager) , quotas ) ;
       try{
           msg = sendAndWait( msg , 20000L ) ;
           if( msg == null ){
              esay("quotasExceeded of "+storageClass+" : request timed out");
              return false ;
           }
           Object obj = msg.getMessageObject() ;
           if( ! (obj instanceof QuotaMgrCheckQuotaMessage ) ){
              esay("quotasExceeded of "+storageClass+" : unexpected object arrived : "+obj.getClass().getName());
              return false ;
           }

           return ((QuotaMgrCheckQuotaMessage)obj).isHardQuotaExceeded() ;

       }catch(Exception ee ){

           esay( "quotasExceeded of "+storageClass+" : Exception : "+ee);
           esay(ee);
           return false ;
       }

    }
    ///////////////////////////////////////////////////////////////
    //
    // the write io request handler
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
           if( _quotasEnabled && quotasExceeded( storageInfo ) ){
              requestFailed( 55 , "Quotas Exceeded for StorageClass : "+storageInfo.getStorageClass() ) ;
              return ;
           }
           String expectedLengthString = storageInfo.getKey("alloc-size") ;
           long expectedLength = 0L ;
           if( expectedLengthString != null ){
              try{
                 expectedLength = Long.parseLong(expectedLengthString) ;
              }catch(NumberFormatException ee ){
                  // bad values are ignored
              }
           }
           try{

              List<PoolCostCheckable> storeList = _poolMonitor.
                               getPnfsFileLocation( _pnfsId , storageInfo , protocolInfo, _request.getLinkGroup() ).
                               getStorePoolList( expectedLength ) ;
              /*
              List storeList =
                  _poolMonitor.getStorePoolList(  storageInfo ,
                                                  protocolInfo ,
                                                  expectedLength );
              */
              String poolName = storeList.get(0).getPoolName() ;

              if (_sendCostInfo)
                    _requestContainer.sendCostMsg(
                             _pnfsId, storeList.get(0), true
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

    public String ac_free_$_0(Args args) {


    	Map<String, PoolLinkGroupInfo> linkGroupSize = Utils.linkGroupInfos(_selectionUnit, _costModule);

    	StringBuilder sb = new StringBuilder();

    	for(Map.Entry<String, PoolLinkGroupInfo> linkGourp: linkGroupSize.entrySet() ) {
    		sb.append(linkGourp.getKey()).append(" : ")
    			.append(linkGourp.getValue().getAvailableSpaceInBytes() ).append("\n");
    	}

    	return sb.toString();

    }

}
