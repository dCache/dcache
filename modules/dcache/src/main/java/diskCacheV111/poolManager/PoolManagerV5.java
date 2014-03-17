package diskCacheV111.poolManager ;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.IpProtocolInfo;
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
import diskCacheV111.vehicles.PoolMgrQueryPoolsMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.QuotaMgrCheckQuotaMessage;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.DelayedReply;

import org.dcache.cells.CellStub;
import org.dcache.poolmanager.Partition;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.poolmanager.PoolSelector;
import org.dcache.poolmanager.Utils;
import org.dcache.util.Args;
import org.dcache.util.Version;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.collect.Iterables.transform;

public class PoolManagerV5
    extends AbstractCellComponent
    implements CellCommandListener,
               CellMessageReceiver
{
    private static final Version VERSION = Version.of(PoolManagerV5.class);
    private int  _writeThreads;
    private int  _readThreads;

    private int _counterPoolUp;
    private int _counterSelectWritePool;
    private int _counterSelectReadPool;

    private PoolSelectionUnit _selectionUnit ;
    private PoolMonitor _poolMonitor;

    private CostModule   _costModule  ;
    private CellStub _broadcast;
    private PnfsHandler _pnfsHandler;

    private RequestContainerV5 _requestContainer ;
    private WatchdogThread     _watchdog;

    private boolean _quotasEnabled;
    private CellStub _quotaManager;


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

    public void setPoolMonitor(PoolMonitor poolMonitor)
    {
        _poolMonitor = poolMonitor;
    }

    public void setRequestContainer(RequestContainerV5 requestContainer)
    {
        _requestContainer = requestContainer;
    }

    public void setBroadcastStub(CellStub stub)
    {
        _broadcast = stub;
    }

    public void setQuotaManager(CellStub stub)
    {
        if (stub == null) {
            _quotasEnabled = false;
            _quotaManager = null;
        } else {
            _quotasEnabled = true;
            _quotaManager = stub;
        }
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
        ImmutableBiMap.Builder<String,CellAddressCore> builder =
                ImmutableBiMap.builder();
        for (String pool: _selectionUnit.getActivePools()) {
            builder.put(pool, _selectionUnit.getPool(pool).getAddress());
        }

        PoolManagerCellInfo pminfo = new PoolManagerCellInfo(info);
        pminfo.setCellVersion(new CellVersion(VERSION));
        pminfo.setPools(builder.build());
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
        private long _watchdogSequenceCounter;

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
                String tmp;
                if (st.hasMoreTokens()) {
                    tmp = st.nextToken();
                    if (tmp.length() > 0) {
                        deathDetected = Long.parseLong(tmp);
                    }
                }
                if (st.hasMoreTokens()) {
                    tmp = st.nextToken();
                    if (tmp.length() > 0) {
                        sleeping = Long.parseLong(tmp);
                    }
                }

                if ((deathDetected < 10) || (sleeping < 10)) {
                    throw new IllegalArgumentException("Timers to small : " + parameter);
                }

                if (deathDetected > 0L) {
                    _deathDetected = deathDetected * 1000L;
                }
                if (sleeping > 0L) {
                    _sleepTimer = sleeping * 1000L;
                }

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

    public static final String hh_getpoolsbylink = "<linkName>";
    public String ac_getpoolsbylink_$_1(Args args)
    {
       String link = args.argv(0);
       StringBuilder sb = new StringBuilder();
       for (PoolCostInfo pool: _poolMonitor.queryPoolsByLinkName(link)) {
           sb.append(pool).append("\n");
       }
       return sb.toString();
    }

    public synchronized
        void messageArrived(CellMessage envelope, PoolManagerPoolUpMessage poolMessage)
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

        pool.setAddress(envelope.getSourcePath().getSourceAddress());
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
       try {
          PoolStatusChangedMessage msg = new PoolStatusChangedMessage( poolName , status ) ;
          msg.setPoolMode( poolMode ) ;
          msg.setDetail( statusCode , statusMessage ) ;
          _log.trace("sendPoolStatusRelay: {}", msg);
          _broadcast.notify(msg);
       }catch(Exception ee ){
          _log.warn("Failed to send poolStatus changed message: {}", ee.toString());
       }
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
                @Override
                public String apply(PoolSelectionUnit.SelectionPool pool) {
                    return pool.getName();
                }
            };

        PoolSelectionUnit.SelectionLink link =
            _selectionUnit.getLinkByName(linkName);
        List<PoolInfo> pools =
            _costModule.getPoolInfo(transform(link.getPools(), getName));
        if (pools.isEmpty()) {
            throw new CacheException(57, "No appropriate pools found for link: " + linkName);
        }

        Partition partition =
            _poolMonitor.getPartitionManager().getPartition(link.getTag());
        msg.setPoolName(partition.selectWritePool(_costModule, pools, new FileAttributes(), filesize).getName());
        msg.setSucceeded();
        return msg;
    }

    public PoolManagerGetPoolsByNameMessage
        messageArrived(PoolManagerGetPoolsByNameMessage msg)
    {
        List<PoolManagerPoolInformation> pools = new ArrayList<>();
        for (String name: msg.getPoolNames()) {
            PoolManagerPoolInformation pool = _poolMonitor.getPoolInformation(name);
            if (pool != null) {
                pools.add(pool);
            }
        }
        msg.setPools(pools);
        msg.setSucceeded();
        return msg;
    }

    public PoolManagerGetPoolsByLinkMessage
        messageArrived(PoolManagerGetPoolsByLinkMessage msg)
    {
        try {
            msg.setPools(_poolMonitor.getPoolsByLink(msg.getLink()));
            msg.setSucceeded();
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
    {
        try {
            List<PoolManagerPoolInformation> pools = new ArrayList<>();
            for (String poolGroup : msg.getPoolGroups()) {
                pools.addAll(_poolMonitor.getPoolsByPoolGroup(poolGroup));
            }
            msg.setPools(pools);
            msg.setSucceeded();
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
                                msg.getFileAttributes(),
                                null)));
        msg.setSucceeded();
        return msg;
    }

    private static class XProtocolInfo implements IpProtocolInfo {
       private final InetSocketAddress _addr;

       private static final long serialVersionUID = -5817364111427851052L;

       private XProtocolInfo( InetSocketAddress addr ){
          _addr = addr ;
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
       public InetSocketAddress getSocketAddress() {
           return _addr;
       }
    }

    private static class XStorageInfo extends GenericStorageInfo {
        private static final long serialVersionUID = -6624549402952279903L;

        private XStorageInfo(String hsm, String storageClass) {
            super(hsm, storageClass);
        }

        @Override
        public String getBitfileId() {
            return "";
        }

        @Override
        public boolean isStored() {
            return true;
        }
    }

    public static final String hh_get_av_pools = "<pnfsId> <hsm> <storageClass> <host>";

    public String ac_get_av_pools_$_4(Args args) throws CacheException {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        XStorageInfo storageInfo = new XStorageInfo(args.argv(1), args.argv(2));
        XProtocolInfo protocolInfo = new XProtocolInfo(new InetSocketAddress(args.argv(3), 0));
        FileAttributes fileAttributes =
                _pnfsHandler.getFileAttributes(pnfsId, PoolMgrSelectReadPoolMsg.getRequiredAttributes());
        fileAttributes.setStorageInfo(storageInfo);
        PoolSelector poolSelector = _poolMonitor.getPoolSelector(fileAttributes, protocolInfo, null);
        List<List<PoolInfo>> available = poolSelector.getReadPools();
        StringBuilder sb = new StringBuilder();
        sb.append("Available and allowed\n");
        for (PoolInfo pool : Iterables.getFirst(available, Collections.<PoolInfo>emptyList())) {
            sb.append("  ").append(pool).append("\n");
        }
        return sb.toString();
    }
    /*
    public static final String hh_get_pools = "<hsm> <storageClass> <host>"+
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

    private boolean quotasExceeded(FileAttributes fileAttributes) {
        StorageInfo info = fileAttributes.getStorageInfo();
        String storageClass = info.getStorageClass() + "@" + info.getHsm() ;
        try {
            QuotaMgrCheckQuotaMessage quotas = new QuotaMgrCheckQuotaMessage(storageClass);
           return _quotaManager.sendAndWait(quotas).isHardQuotaExceeded();
        } catch (Exception e) {
            _log.warn("quotasExceeded of " + storageClass + " : Exception : {}", e.toString());
            return false;
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
           FileAttributes fileAttributes = _request.getFileAttributes();
           StorageInfo storageInfo = fileAttributes.getStorageInfo();
           ProtocolInfo protocolInfo = _request.getProtocolInfo();

           _log.info("{} write handler started", _pnfsId);
           long started = System.currentTimeMillis();

           if( _quotasEnabled && quotasExceeded(fileAttributes) ){
              requestFailed( 55 , "Quotas Exceeded for StorageClass : "+storageInfo.getStorageClass() ) ;
              return ;
           }

           try {
               PoolInfo pool =
                   _poolMonitor
                   .getPoolSelector(fileAttributes, protocolInfo, _request.getLinkGroup())
                   .selectWritePool(_request.getPreallocated());

              _log.info("{} write handler selected {} after {} ms", _pnfsId, pool.getName(),
                      System.currentTimeMillis() - started);
              requestSucceeded(pool);

           }catch(CacheException ce ){
              requestFailed( ce.getRc() , ce.getMessage() ) ;
           }catch(Exception ee ){
              requestFailed( 17 , ee.getMessage() ) ;
           }
       }

        protected void requestFailed(int errorCode, String errorMessage)
        {
            _request.setFailed(errorCode, errorMessage);
            reply(_request);
        }

        protected void requestSucceeded(PoolInfo pool)
        {
            _request.setPoolName(pool.getName());
            _request.setPoolAddress(pool.getAddress());
            _request.setSucceeded();
            reply(_request);
            if (!_request.getSkipCostUpdate()) {
                _costModule.messageArrived(_envelope);
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
