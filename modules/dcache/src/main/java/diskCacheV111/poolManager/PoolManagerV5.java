package diskCacheV111.poolManager ;

import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.PoolCostCheckable;
import diskCacheV111.vehicles.PoolLinkGroupInfo;
import diskCacheV111.vehicles.PoolManagerGetPoolListMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;
import diskCacheV111.vehicles.PoolManagerGetPoolsByLinkMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolsByNameMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolsByPoolGroupMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import diskCacheV111.vehicles.PoolManagerPoolModeMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.PoolMgrGetPoolByLink;
import diskCacheV111.vehicles.PoolMgrGetPoolLinkGroups;
import diskCacheV111.vehicles.PoolMgrQueryPoolsMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.QuotaMgrCheckQuotaMessage;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.Partition;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.Utils;
import org.dcache.util.Version;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PoolManagerSelectLinkGroupForWriteMessage;

import static com.google.common.collect.Iterables.transform;

public class PoolManagerV5
    extends AbstractCellComponent
    implements CellCommandListener,
               CellMessageReceiver
{
    private static final Version VERSION = Version.of(PoolManagerV5.class);
    private int  _writeThreads     = 0 ;
    private int  _readThreads      = 0 ;

    private int _counterPoolUp         = 0 ;
    private int _counterSelectWritePool= 0 ;
    private int _counterSelectReadPool = 0 ;

    private PoolSelectionUnit _selectionUnit ;
    private PoolMonitorV5     _poolMonitor   ;

    private CostModule   _costModule  ;
    private CellPath     _poolStatusRelayPath = null ;
    private PnfsHandler _pnfsHandler;

    private RequestContainerV5 _requestContainer ;
    private WatchdogThread     _watchdog         = null ;

    private boolean _quotasEnabled = false ;
    private String  _quotaManager  = "none";


    private final static Logger _log = LoggerFactory.getLogger(PoolManagerV5.class);
    private final static Logger _logPoolMonitor = LoggerFactory.getLogger("logger.org.dcache.poolmonitor." + PoolManagerV5.class.getName());


    public PoolManagerV5()
    {
    }

    public void setPoolSelectionUnit(PoolSelectionUnit selectionUnit)
    {
        _selectionUnit = selectionUnit;
    }

    public void setCostModule(CostModule costModule)
    {
        _costModule = costModule;
    }

    public void setPoolMonitor(PoolMonitorV5 poolMonitor)
    {
        _poolMonitor = poolMonitor;
    }

    public void setRequestContainer(RequestContainerV5 requestContainer)
    {
        _requestContainer = requestContainer;
    }

    public void setPoolStatusRelayPath(CellPath poolStatusRelayPath)
    {
        _poolStatusRelayPath =
            (poolStatusRelayPath.hops() == 0)
            ? null
            : poolStatusRelayPath;
    }

    public void setQuotaManager(String quotaManager)
    {
        _quotaManager = quotaManager;
        _quotasEnabled = !_quotaManager.equals("none");
    }

    public void setPnfsHandler(PnfsHandler pnfsHandler)
    {
        _pnfsHandler = pnfsHandler;
    }

    public void init()
    {
        String watchdogParam = getArgs().getOpt("watchdog");
        if (watchdogParam != null && watchdogParam.length() > 0) {
            _watchdog = new WatchdogThread(watchdogParam);
        } else {
            _watchdog = new WatchdogThread();
        }
        _log.info("Watchdog : " + _watchdog);
    }

    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        PoolManagerCellInfo pminfo = new PoolManagerCellInfo(info);
        pminfo.setCellVersion(new CellVersion(VERSION));
        pminfo.setPoolList(_selectionUnit.getActivePools());
        return pminfo;
    }

    @Override
    public void printSetup(PrintWriter writer)
    {
        writer.print("#\n# Setup of ");
        writer.print(getCellName());
        writer.print(" (");
        writer.print(getClass().getName());
        writer.print(") at ");
        writer.println(new Date().toString());
        writer.println("#");
    }

    private class WatchdogThread implements Runnable {
        private long _deathDetected = 10L * 60L * 1000L; // 10 minutes
        private long _sleepTimer = 1L * 60L * 1000L; // 1 minute
        private long _watchdogSequenceCounter = 0L;

        public WatchdogThread() {
            new Thread(this, "watchdog").start();
            _log.info("WatchdogThread initialized with : " + this);
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
                _log.warn("WatchdogThread : illegal arguments [" + parameter + "] (using defaults) " + ee.getMessage());
            }
            new Thread(this, "watchdog").start();
            _log.info("WatchdogThread initialized with : " + this);
        }

        @Override
        public void run() {
            _log.info("watchdog thread activated");
            while (true) {
                try {
                    Thread.sleep(_sleepTimer);
                } catch (InterruptedException e) {
                    _log.info("watchdog thread interrupted");
                    break;
                }
                runWatchdogSequence(_deathDetected);
                _watchdogSequenceCounter++;
            }
            _log.info("watchdog finished");
        }

        @Override
        public String toString() {
            return "DeathDetection=" + (_deathDetected / 1000L) + ";Sleep="
                    + (_sleepTimer / 1000L) + ";Counter="
                    + _watchdogSequenceCounter + ";";
        }
    }

    public PoolManagerPoolModeMessage
        messageArrived(PoolManagerPoolModeMessage msg)
    {
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

        msg.setSucceeded();
        return msg;
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

    @Override
    public void getInfo( PrintWriter pw ){
	pw.println("PoolManager V [$Id: PoolManagerV5.java,v 1.48 2007-10-10 08:05:34 tigran Exp $]");
        pw.println(" SelectionUnit : "+_selectionUnit.getVersion() ) ;
        pw.println(" Write Threads : "+_writeThreads) ;
        pw.println(" Read  Threads : "+_readThreads) ;
        pw.println("Message counts") ;
        pw.println("           PoolUp : "+_counterPoolUp ) ;
        pw.println("   SelectReadPool : "+_counterSelectReadPool ) ;
        pw.println("  SelectWritePool : "+_counterSelectWritePool ) ;
        if( _watchdog == null ){
             pw.println("         Watchdog : disabled" ) ;
        }else{
             pw.println("         Watchdog : "+_watchdog ) ;
        }
    }
    public final static String hh_set_max_threads = "# OBSOLETE";
    public String ac_set_max_threads_$_1(Args args)
    {
        return "'set max threads' is obsolete";
    }

    public final static String hh_set_timeout_pool = "# OBSOLETE";
    public String ac_set_timeout_pool_$_1(Args args)
    {
        return "'set timeout pool' is obsolete";
    }

    public final static String hh_getpoolsbylink =
        "<linkName> [-size=<filesize>]";
    public String ac_getpoolsbylink_$_1(Args args)
        throws NumberFormatException
    {
       String sizeString = args.getOpt("size");
       long size = (sizeString == null) ? 50000000L : Long.parseLong(sizeString);
       String link = args.argv(0);
       StringBuilder sb = new StringBuilder();
       for (PoolCostCheckable pool: _poolMonitor.queryPoolsByLinkName(link, size)) {
           sb.append(pool).append("\n");
       }
       return sb.toString();
    }

    public synchronized
        void messageArrived(PoolManagerPoolUpMessage poolMessage)
    {
        _counterPoolUp++;

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
            || pool.isActive() == disabled
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
            _logPoolMonitor.warn("Pool " + poolName + " changed from mode "
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
          _log.info("sendPoolStatusRelay : "+msg);
          sendMessage(
               new CellMessage( _poolStatusRelayPath , msg )
                     ) ;

       }catch(Exception ee ){
          _log.warn("Failed to send poolStatus changed message : "+ee ) ;
       }
    }

    public PoolMgrGetPoolLinkGroups
        messageArrived(PoolMgrGetPoolLinkGroups msg)
    {
        Collection<PoolLinkGroupInfo> linkGroupInfos = Utils.linkGroupInfos(_selectionUnit, _costModule).values();

    	PoolLinkGroupInfo[] poolLinkGroupInfos = linkGroupInfos.toArray(new PoolLinkGroupInfo[linkGroupInfos.size()]);
    	msg.setPoolLinkGroupInfos(poolLinkGroupInfos);
        msg.setSucceeded();
        return msg;
    }

    public PoolManagerGetPoolListMessage
        messageArrived(PoolManagerGetPoolListMessage msg)
    {
       String [] pools = _selectionUnit.getActivePools() ;
       msg.setPoolList(Arrays.asList(pools)) ;
       msg.setSucceeded();
       return msg;
    }

    public PoolMgrGetPoolByLink messageArrived(PoolMgrGetPoolByLink msg)
        throws CacheException
    {
        String linkName = msg.getLinkName();
        long filesize = msg.getFilesize();

        Function<PoolSelectionUnit.SelectionPool,String> getName =
            new Function<PoolSelectionUnit.SelectionPool,String>() {
                public String apply(PoolSelectionUnit.SelectionPool pool) {
                    return pool.getName();
                }
            };

        PoolSelectionUnit.SelectionLink link =
            _selectionUnit.getLinkByName(linkName);
        List<PoolInfo> pools =
            _costModule.getPoolInfo(transform(link.pools(), getName));
        if (pools.isEmpty()) {
            throw new CacheException(57, "No appropriate pools found for link: " + linkName);
        }

        FileAttributes fileAttributes = new FileAttributes();
        fileAttributes.setSize(filesize);
        Partition partition =
            _poolMonitor.getPartitionManager().getPartition(link.getTag());
        msg.setPoolName(partition.selectWritePool(_costModule, pools, fileAttributes).getName());
        msg.setSucceeded();
        return msg;
    }

    public PoolManagerGetPoolsByNameMessage
        messageArrived(PoolManagerGetPoolsByNameMessage msg)
        throws CacheException
    {
        try {
            List<PoolManagerPoolInformation> pools = new ArrayList<PoolManagerPoolInformation>();
            for (String name: msg.getPoolNames()) {
                try {
                    pools.add(_poolMonitor.getPoolInformation(name));
                } catch (NoSuchElementException e) {
                    /* Don't include a pool that doesn't exist.
                     */
                }
            }
            msg.setPools(pools);
            msg.setSucceeded();
        } catch (InterruptedException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     "Pool manager is shutting down");
        }
        return msg;
    }

    public PoolManagerGetPoolsByLinkMessage
        messageArrived(PoolManagerGetPoolsByLinkMessage msg)
        throws CacheException
    {
        try {
            msg.setPools(_poolMonitor.getPoolsByLink(msg.getLink()));
            msg.setSucceeded();
        } catch (InterruptedException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     "Pool manager is shutting down");
        } catch (NoSuchElementException e) {
            Collection<PoolManagerPoolInformation> empty =
                Collections.emptyList();
            msg.setPools(empty);
            msg.setSucceeded();
        }
        return msg;
    }

    public PoolManagerGetPoolsByPoolGroupMessage
        messageArrived(PoolManagerGetPoolsByPoolGroupMessage msg)
        throws CacheException
    {
        try {
            msg.setPools(_poolMonitor.getPoolsByPoolGroup(msg.getPoolGroup()));
            msg.setSucceeded();
        } catch (InterruptedException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     "Pool manager is shutting down");
        } catch (NoSuchElementException e) {
            Collection<PoolManagerPoolInformation> empty =
                Collections.emptyList();
            msg.setPools(empty);
            msg.setSucceeded();
        }
        return msg;
    }

    public PoolMgrQueryPoolsMsg
        messageArrived(PoolMgrQueryPoolsMsg msg)
    {
        DirectionType accessType = msg.getAccessType();
        msg.setPoolList(PoolPreferenceLevel.fromPoolPreferenceLevelToList(
           _selectionUnit.match(accessType,
                                msg.getNetUnitName(),
                                msg.getProtocolUnitName(),
                                msg.getStorageInfo(),
                                null)));
        msg.setSucceeded();
        return msg;
    }

    private static class XProtocolInfo implements IpProtocolInfo {
       private String [] _host = new String[1] ;

       private static final long serialVersionUID = -5817364111427851052L;

       private XProtocolInfo( String hostName ){
          _host[0] = hostName ;
       }

       @Override
       public String getProtocol()
       {
           return "DCap";
       }

       @Override
       public int getMinorVersion()
       {
           return 0;
       }

       @Override
       public int getMajorVersion()
       {
           return 0;
       }

       @Override
       public String getVersionString()
       {
           return "0.0";
       }

       @Override
       public String[] getHosts()
       {
           return _host;
       }

       @Override
       public int getPort()
       {
           return 0;
       }

       @Override
       public InetSocketAddress getSocketAddress() {
           // enforced by interface
           return null;
       }
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

          FileAttributes fileAttributes =
              _pnfsHandler.getFileAttributes(pnfsId, EnumSet.of(FileAttribute.LOCATIONS));
          fileAttributes.setPnfsId(pnfsId);
          fileAttributes.setStorageInfo(storageInfo);

          PoolMonitorV5.PnfsFileLocation pnfsFileLocation =
              _poolMonitor.getPnfsFileLocation(fileAttributes,
                                               protocolInfo, null ) ;

          List<List<PoolInfo>> available = pnfsFileLocation.getReadPools();

          StringBuffer sb = new StringBuffer() ;
          sb.append("Available and allowed\n");
          for (PoolInfo pool: available.get(0)) {
              sb.append("  ").append(pool).append("\n");
          }
          sb.append("Allowed (not available)\n");

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

    private boolean quotasExceeded( StorageInfo info ){

       String storageClass = info.getStorageClass()+"@"+info.getHsm() ;

       QuotaMgrCheckQuotaMessage quotas = new QuotaMgrCheckQuotaMessage( storageClass ) ;
       CellMessage msg = new CellMessage( new CellPath(_quotaManager) , quotas ) ;
       try{
           msg = sendAndWait( msg , 20000L ) ;
           if( msg == null ){
              _log.warn("quotasExceeded of "+storageClass+" : request timed out");
              return false ;
           }
           Object obj = msg.getMessageObject() ;
           if( ! (obj instanceof QuotaMgrCheckQuotaMessage ) ){
              _log.warn("quotasExceeded of "+storageClass+" : unexpected object arrived : "+obj.getClass().getName());
              return false ;
           }

           return ((QuotaMgrCheckQuotaMessage)obj).isHardQuotaExceeded() ;

       }catch(Exception ee ){

           _log.warn( "quotasExceeded of "+storageClass+" : Exception : "+ee);
           _log.warn(ee.toString());
           return false ;
       }

    }

    private long determineExpectedFileSize(long expectedLength, StorageInfo storageInfo)
    {
        if (expectedLength > 0) {
            return expectedLength;
        }

        if (storageInfo.getFileSize() > 0) {
            return storageInfo.getFileSize();
        }

        String s = storageInfo.getKey("alloc-size");
        if (s != null) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                // bad values are ignored
            }
        }

        return 0;
    }

    public DelayedReply messageArrived(PoolManagerSelectLinkGroupForWriteMessage message)
        throws CacheException
    {
        if (message.getStorageInfo() == null) {
            throw new IllegalArgumentException("Storage info is missing");
        }
        if (message.getProtocolInfo() == null ){
            throw new IllegalArgumentException("Protocol info is missing");
        }

        return new LinkGroupSelectionTask(message);
    }

    /**
     * Task for processing link group selection messages.
     */
    public class LinkGroupSelectionTask
        extends DelayedReply
        implements Runnable
    {
        private final PoolManagerSelectLinkGroupForWriteMessage _message;
        private final CDC _cdc;

        public LinkGroupSelectionTask(PoolManagerSelectLinkGroupForWriteMessage message)
        {
            _message = message;
            _cdc = new CDC();
            new Thread(this, "LinkGroupSelectionTask").start();
        }

        @Override
        public void run()
        {
            long started = System.currentTimeMillis();
            _cdc.restore();
            try {
                _log.info("Select link group handler started");

                _message.setLinkGroups(selectLinkGroups());
                _message.setSucceeded();

                _log.info("Select link group handler finished after {} ms",
                          (System.currentTimeMillis() - started));
            } catch (Exception e) {
                _message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.getMessage());
            } finally {
                try {
                    send(_message);
                } catch (NoRouteToCellException e) {
                    _log.error("Failed to send reply: " + e.getMessage());
                } catch (InterruptedException e) {
                    _log.warn("Link group selection handler was interrupted");
                } finally {
                    CDC.clear();
                }
            }
        }

        protected List<String> selectLinkGroups()
        {
            StorageInfo storageInfo = _message.getStorageInfo();
            ProtocolInfo protocolInfo = _message.getProtocolInfo();
            String protocol =
                protocolInfo.getProtocol() + "/" + protocolInfo.getMajorVersion();
            String hostName =
                (protocolInfo instanceof IpProtocolInfo)
                ? ((IpProtocolInfo) protocolInfo).getHosts()[0]
                : null;

            Collection<String> linkGroups = _message.getLinkGroups();
            if (linkGroups == null) {
                linkGroups =
                    Utils.linkGroupInfos(_selectionUnit, _costModule).keySet();
            }

            List<String> outputLinkGroups =
                new ArrayList<String>(linkGroups.size());

            for (String linkGroup: linkGroups) {
                PoolPreferenceLevel [] level =
                    _selectionUnit.match(DirectionType.WRITE,
                                         hostName,
                                         protocol,
                                         storageInfo,
                                         linkGroup);
                if (level.length > 0) {
                    outputLinkGroups.add(linkGroup);
                }
            }

            return outputLinkGroups;
        }
    }

    public PoolManagerGetPoolMonitor
        messageArrived(PoolManagerGetPoolMonitor msg)
    {
        msg.setPoolMonitor(_poolMonitor);
        return msg;
    }

    ///////////////////////////////////////////////////////////////
    //
    // the write io request handler
    //
    public DelayedReply messageArrived(CellMessage envelope,
                                       PoolMgrSelectWritePoolMsg msg)
    {
        return new WriteRequestHandler(envelope, msg);
    }

    public class WriteRequestHandler extends DelayedReply implements Runnable
    {
        private CellMessage _envelope;
        private PoolMgrSelectWritePoolMsg _request;
        private PnfsId _pnfsId;

        public WriteRequestHandler(CellMessage envelope,
                                   PoolMgrSelectWritePoolMsg msg)
        {
            _envelope = envelope;
            _request = msg;
            _pnfsId = _request.getPnfsId();
            new Thread(this, "writeHandler").start();
        }

       @Override
       public void run(){

           StorageInfo  storageInfo  = _request.getStorageInfo() ;
           ProtocolInfo protocolInfo = _request.getProtocolInfo() ;

           _log.info( _pnfsId.toString()+" write handler started" );
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

           long expectedLength =
               determineExpectedFileSize(_request.getFileSize(), storageInfo);

           /* The cost module relies on the expected file size.
            */
           _request.setFileSize(expectedLength);

           try{

               FileAttributes fileAttributes = new FileAttributes();
               fileAttributes.setPnfsId(_pnfsId);
               fileAttributes.setStorageInfo(storageInfo);
               fileAttributes.setSize(expectedLength);
               String poolName =
                   _poolMonitor
                   .getPnfsFileLocation(fileAttributes, protocolInfo, _request.getLinkGroup())
                   .selectWritePool()
                   .getName();

              _log.info("{} write handler selected {} after {} ms",
                        new Object[] { _pnfsId, poolName, System.currentTimeMillis() - started });
              requestSucceeded(poolName);

           }catch(CacheException ce ){
              requestFailed( ce.getRc() , ce.getMessage() ) ;
           }catch(Exception ee ){
              requestFailed( 17 , ee.getMessage() ) ;
           }
       }

        protected void requestFailed(int errorCode, String errorMessage)
        {
            _request.setFailed(errorCode, errorMessage);
            try {
                send(_request);
            } catch (Exception e) {
                _log.warn("Exception requestFailed : " + e, e);
            }
        }

        protected void requestSucceeded(String poolName)
        {
            _request.setPoolName(poolName);
            _request.setSucceeded();
            try {
                send(_request);
                if (!_request.getSkipCostUpdate()) {
                    _costModule.messageArrived(_envelope);
                }
            } catch (Exception e) {
                _log.warn("Exception in requestSucceeded : " + e, e);
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
