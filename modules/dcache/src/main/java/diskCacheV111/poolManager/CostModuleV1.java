// $Id: CostModuleV1.java,v 1.21 2007-08-01 20:19:23 tigran Exp $

package diskCacheV111.poolManager ;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import diskCacheV111.pools.CostCalculatable;
import diskCacheV111.pools.CostCalculationEngine;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCostInfo.NamedPoolQueueInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.CostModulePoolInfoTable;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolCostCheckable;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.util.Args;

import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellInfoProvider;
import org.dcache.cells.CellMessageDispatcher;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.CellSetupProvider;
import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.vehicles.FileAttributes;

public class CostModuleV1
    implements Serializable,
               CostModule,
               CellCommandListener,
               CellMessageReceiver,
               CellInfoProvider,
               CellSetupProvider
{
    private final static Logger _log =
        LoggerFactory.getLogger(CostModuleV1.class);

    /**
     * The file size used when calculating performance cost ranked
     * percentile.
     */
    public static final long PERCENTILE_FILE_SIZE = 104857600;

    private static final long serialVersionUID = -267023006449629909L;

    private final Map<String, Entry> _hash = new HashMap<>() ;
    private boolean _isActive = true ;
    private boolean _update = true ;
    private boolean _magic = true ;
    private boolean _debug;
    private boolean _cachedPercentileCostCutIsValid;
    private double _cachedPercentileCostCut;
    private double _cachedPercentileFraction;
    private transient CellMessageDispatcher _handlers =
        new CellMessageDispatcher("messageToForward");


    /**
     * Information about some specific pool.
     */
   private static class Entry implements Serializable
   {
       private static final long serialVersionUID = -6380756950554320179L;

       private final long timestamp;
       private final PoolCostInfo _info;
       private double _fakeCpu = -1.0;
       private double _fakeSpace = -1.0;
       private final ImmutableMap<String,String> _tagMap;
       private final CellAddressCore _address;

       public Entry(CellAddressCore address, PoolCostInfo info, Map<String,String> tagMap)
       {
           timestamp = System.currentTimeMillis();
           _address = address;
           _info = info;
           _tagMap =
               (tagMap == null)
               ? ImmutableMap.<String,String>of()
               : ImmutableMap.copyOf(tagMap);
       }

       public boolean isValid()
       {
           return (System.currentTimeMillis() - timestamp) < 5*60*1000L;
       }

       public PoolCostInfo getPoolCostInfo()
       {
           return _info;
       }

       public ImmutableMap<String, String> getTagMap()
       {
           return _tagMap;
       }

       public PoolInfo getPoolInfo()
       {
           return new PoolInfo(_address, _info, _tagMap);
       }
   }
   private CostCalculationEngine _costCalculationEngine;

   private class CostCheck
       extends PoolCheckAdapter implements PoolCostCheckable
   {
       private static final long serialVersionUID = -77487683158664348L;

       private PoolCostInfo _info ;

       private CostCheck( String poolName , Entry e , long filesize ){
          super(poolName,filesize);
          _info     = e.getPoolCostInfo() ;

          CostCalculatable  cost = _costCalculationEngine.getCostCalculatable( _info ) ;

          cost.recalculate( filesize ) ;

          setSpaceCost( e._fakeSpace > -1.0 ?
                        e._fakeSpace :
                        cost.getSpaceCost() ) ;
          setPerformanceCost( e._fakeCpu > -1.0 ?
                              e._fakeCpu :
                              cost.getPerformanceCost() ) ;
          setTagMap( e.getTagMap() );
       }
   }

    public CostModuleV1()
    {
        _handlers.addMessageListener(this);
    }

    public void setCostCalculationEngine(CostCalculationEngine engine)
    {
        _costCalculationEngine = engine;
    }

    public synchronized void messageArrived(CellMessage envelope, PoolManagerPoolUpMessage msg)
    {
        if (! _update) {
            return;
        }

        CellAddressCore poolAddress = envelope.getSourceAddress();
        String poolName = msg.getPoolName();
        PoolV2Mode poolMode = msg.getPoolMode();
        PoolCostInfo newInfo = msg.getPoolCostInfo();
        Entry poolEntry = _hash.get(poolName);
        boolean isNewPool = poolEntry == null;

        /* Whether the pool mentioned in the message should be removed */
        boolean shouldRemovePool = poolMode.getMode() == PoolV2Mode.DISABLED ||
                poolMode.isDisabled(PoolV2Mode.DISABLED_STRICT) ||
                poolMode.isDisabled(PoolV2Mode.DISABLED_DEAD);

        if( isNewPool || shouldRemovePool) {
            _cachedPercentileCostCutIsValid = false;
        } else {
            PoolCostInfo currentInfo = poolEntry.getPoolCostInfo();
            considerInvalidatingCache(currentInfo, newInfo);
        }

        if (shouldRemovePool) {
            _hash.remove(poolName);
        } else if (newInfo != null) {
            _hash.put(poolName, new Entry(poolAddress, newInfo, msg.getTagMap()));
        }
    }

    private void considerInvalidatingCache(PoolCostInfo currentInfo, PoolCostInfo newInfo)
    {
        if( !_cachedPercentileCostCutIsValid) {
            return;
        }

        double currentCost = getPerformanceCost(currentInfo);
        double newCost = getPerformanceCost(newInfo);
        considerInvalidatingCache(currentCost, newCost);
    }

    private void considerInvalidatingCache(double currentCost, PoolCostInfo newInfo)
    {
        if( !_cachedPercentileCostCutIsValid) {
            return;
        }

        double newCost = getPerformanceCost(newInfo);
        considerInvalidatingCache(currentCost, newCost);
    }

    /* Check whether we should invalidate the cached.  We must do this when
     * a pool changes its relationship to the cost threshold:
     *       o  a pool with cost less than the cached value assumes a cost greater
     *                  than the cached value,
     *       o  a pool with cost greater than the cached value assumes a cost less
     *                  than the cached value.
     *       o  a pool with cost equal to the cached value assumes a cost less
     *                  than or greater than the cached value.
     */
    private void considerInvalidatingCache(double currentCost, double newCost)
    {
        if( Math.signum(currentCost-_cachedPercentileCostCut) !=
            Math.signum(newCost-_cachedPercentileCostCut)) {
            _cachedPercentileCostCutIsValid = false;
        }
    }

    private double getPerformanceCost(PoolCostInfo info)
    {
        CostCalculatable cost = _costCalculationEngine.getCostCalculatable(info);
        cost.recalculate(PERCENTILE_FILE_SIZE);
        return cost.getPerformanceCost();
    }

    public synchronized void messageToForward(PoolIoFileMessage msg)
    {
        String poolName = msg.getPoolName();
        Entry e = _hash.get(poolName);
        if (e == null) {
            return;
        }

        String requestedQueueName = msg.getIoQueueName();

        PoolCostInfo costInfo = e.getPoolCostInfo();
        double currentPerformanceCost = getPerformanceCost(costInfo);
        Map<String, NamedPoolQueueInfo> map =
            costInfo.getExtendedMoverHash();

        PoolCostInfo.PoolQueueInfo queue;
        PoolCostInfo.PoolSpaceInfo spaceInfo = costInfo.getSpaceInfo();

        if (map == null) {
            queue = costInfo.getMoverQueue();
        } else {
            requestedQueueName =
                (requestedQueueName == null ||
                 map.get(requestedQueueName) == null)
                ? costInfo.getDefaultQueueName()
                : requestedQueueName;
            queue = map.get(requestedQueueName);
        }

        int diff = 0;
        long pinned = 0;
        FileAttributes attributes = msg.getFileAttributes();
        if (msg.isReply() && msg.getReturnCode() != 0) {
            diff = -1;
            if (msg instanceof PoolAcceptFileMessage && attributes.isDefined(FileAttribute.SIZE)) {
                pinned = -msg.getFileAttributes().getSize();
            }
        } else if (!msg.isReply() && !_magic) {
            diff = 1;
            if (msg instanceof PoolAcceptFileMessage && attributes.isDefined(FileAttribute.SIZE)) {
                pinned = msg.getFileAttributes().getSize();
            }
        }

        queue.modifyQueue(diff);
        spaceInfo.modifyPinnedSpace(pinned);

        considerInvalidatingCache(currentPerformanceCost, costInfo);

        xsay("Mover"+(requestedQueueName==null?"":("("+requestedQueueName+")")) , poolName, diff, pinned, msg);
    }

    public synchronized void messageToForward(DoorTransferFinishedMessage msg)
    {
        String poolName = msg.getPoolName();
        Entry e = _hash.get(poolName);
        if (e == null) {
            return;
        }

        PoolCostInfo costInfo = e.getPoolCostInfo();
        double currentPerformanceCost = getPerformanceCost(costInfo);
        String requestedQueueName = msg.getIoQueueName();

        Map<String, NamedPoolQueueInfo> map =
            costInfo.getExtendedMoverHash();
        PoolCostInfo.PoolQueueInfo queue;

        if (map == null) {
            queue = costInfo.getMoverQueue();
        } else {
            requestedQueueName =
                (requestedQueueName == null) ||
                (map.get(requestedQueueName) == null)
                ? costInfo.getDefaultQueueName()
                : requestedQueueName;

            queue = map.get(requestedQueueName);
        }

        int diff = -1;
        long pinned = 0;

        queue.modifyQueue(diff);
        considerInvalidatingCache(currentPerformanceCost, costInfo);
        xsay("Mover"+(requestedQueueName==null?"":("("+requestedQueueName+")")), poolName, diff, pinned, msg);
    }

    public synchronized void messageToForward(PoolFetchFileMessage msg)
    {
         String poolName = msg.getPoolName();
         Entry e = _hash.get(poolName);
         if (e == null) {
             return;
         }

         PoolCostInfo costInfo = e.getPoolCostInfo();
         double currentPerformanceCost = getPerformanceCost(costInfo);
         PoolCostInfo.PoolQueueInfo queue = costInfo.getRestoreQueue();
         PoolCostInfo.PoolSpaceInfo spaceInfo = costInfo.getSpaceInfo();

         int diff;
         long pinned;
        if (msg.isReply()) {
            diff = -1;
            pinned = 0;
        } else {
            diff = 1;
            FileAttributes attributes = msg.getFileAttributes();
            if (attributes.isDefined(FileAttribute.SIZE)) {
                pinned = attributes.getSize();
            } else {
                pinned = 0;
            }
        }
        queue.modifyQueue(diff);
         spaceInfo.modifyPinnedSpace(pinned);
         considerInvalidatingCache(currentPerformanceCost, costInfo);
         xsay("Restore", poolName, diff, pinned, msg);
    }

    public synchronized void messageToForward(PoolMgrSelectPoolMsg msg)
    {
         if (!_magic) {
             return;
         }

         if (!msg.isReply()) {
             return;
         }
         String poolName = msg.getPoolName();
         Entry e = _hash.get(poolName);
         if (e == null) {
             return;
         }

         String requestedQueueName = msg.getIoQueueName();

         PoolCostInfo costInfo = e.getPoolCostInfo();
         double currentPerformanceCost = getPerformanceCost(costInfo);
         Map<String, NamedPoolQueueInfo> map =
             costInfo.getExtendedMoverHash();
         PoolCostInfo.PoolQueueInfo queue;
         PoolCostInfo.PoolSpaceInfo spaceInfo = costInfo.getSpaceInfo();

         if (map == null) {
            queue = costInfo.getMoverQueue();
         } else {
            requestedQueueName =
                (requestedQueueName == null) ||
                (map.get(requestedQueueName) == null)
                ? costInfo.getDefaultQueueName()
                : requestedQueueName;
            queue = map.get(requestedQueueName);
         }

         int diff = 1;
         long pinned =
             (msg instanceof PoolMgrSelectWritePoolMsg) ? msg.getFileSize() : 0;
         queue.modifyQueue(diff);
         spaceInfo.modifyPinnedSpace(pinned);
         considerInvalidatingCache(currentPerformanceCost, costInfo);
         xsay("Mover (magic)"+(requestedQueueName==null?"":("("+requestedQueueName+")")), poolName, diff, pinned, msg);
    }

    public synchronized void messageToForward(Pool2PoolTransferMsg msg)
    {
        _log.debug( "Pool2PoolTransferMsg : reply="+msg.isReply());

        String sourceName = msg.getSourcePoolName();
        Entry source = _hash.get(sourceName);
        if (source == null) {
            return;
        }

        PoolCostInfo sourceCostInfo = source.getPoolCostInfo();
        double currentSourcePerformanceCost = getPerformanceCost(sourceCostInfo);

        PoolCostInfo.PoolQueueInfo sourceQueue = sourceCostInfo.getP2pQueue();

        String destinationName = msg.getDestinationPoolName();
        Entry destination = _hash.get(destinationName);
        if (destination == null) {
            return;
        }

        PoolCostInfo destinationCostInfo = destination.getPoolCostInfo();
        double currentDestinationPerformanceCost =
            getPerformanceCost(destinationCostInfo);

        PoolCostInfo.PoolQueueInfo destinationQueue =
            destinationCostInfo.getP2pClientQueue();
        PoolCostInfo.PoolSpaceInfo destinationSpaceInfo =
            destinationCostInfo.getSpaceInfo();

        int diff = msg.isReply() ? -1 : 1;
        long pinned = msg.getFileAttributes().getSize();

        sourceQueue.modifyQueue(diff);
        destinationQueue.modifyQueue(diff);
        destinationSpaceInfo.modifyPinnedSpace(pinned);

        considerInvalidatingCache(currentSourcePerformanceCost, sourceCostInfo);
        considerInvalidatingCache(currentDestinationPerformanceCost,
                destinationCostInfo);

        xsay("P2P client (magic)", destinationName, diff, pinned, msg);
        xsay("P2P server (magic)", sourceName, diff, 0, msg);
    }

    /**
     * Defined by CostModule interface. Used by PoolManager to inject
     * the replies PoolManager sends to doors.
     */
    @Override
    public void messageArrived(CellMessage cellMessage)
    {
        _handlers.call(cellMessage);
    }

    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.append( "Submodule : CostModule (cm) : ").println(getClass().getName());
        pw.println("Version : $Revision$");
        pw.append(" Debug   : ").println(_debug?"on":"off");
        pw.append(" Update  : ").println(_update?"on":"off");
        pw.append(" Active  : ").println(_isActive?"yes":"no");
        pw.append(" Magic   : ").println(_magic?"yes":"no");
    }

    @Override
    public void beforeSetup()
    {
    }

    @Override
    public void afterSetup()
    {
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        pw.append( "#\n# Submodule CostModule (cm) : ")
            .println(this.getClass().getName());
        pw.println("# $Revision$ \n#\n") ;
        pw.println("cm set debug "+(_debug?"on":"off"));
        pw.println("cm set update "+(_update?"on":"off"));
        pw.println("cm set magic "+(_magic?"on":"off"));
    }

    private void xsay(String queue, String pool, int diff, long pinned, Object obj)
    {
        if (_debug) {
            _log.debug("CostModuleV1 : "+queue+" queue of "+pool+" modified by "+diff+"/" + pinned + " due to "+obj.getClass().getName());
        }
    }

   @Override
   public synchronized PoolCostCheckable getPoolCost( String poolName , long filesize ){
      Entry cost = _hash.get(poolName);

      if( ( cost == null ) ||( !cost.isValid() && _update  ) ) {
          return null;
      }

      return  new CostCheck( poolName , cost , filesize ) ;

   }

   @Override
   public synchronized double getPoolsPercentilePerformanceCost(double fraction) {

       if( fraction <= 0 || fraction >= 1) {
           throw new IllegalArgumentException("supplied fraction (" + Double.toString( fraction) +") not between 0 and 1");
       }

       if( !_cachedPercentileCostCutIsValid || _cachedPercentileFraction != fraction) {
           _cachedPercentileCostCut = calculatePercentileCostCut(fraction);
           _cachedPercentileFraction = fraction;
           _cachedPercentileCostCutIsValid = true;
       }

       return _cachedPercentileCostCut;
   }

   private double calculatePercentileCostCut(double fraction)
   {
       if( _hash.isEmpty()) {
           _log.debug( "no pools available");
           return 0;
       }

       _log.debug( "{} pools available", _hash.size());

       double poolCosts[] = new double[_hash.size()];

       int idx=0;
       for( Entry poolInfo : _hash.values()) {
           poolCosts[idx] = getPerformanceCost(poolInfo.getPoolCostInfo());
           idx++;
       }

       Arrays.sort(poolCosts);

       return poolCosts [ (int) Math.floor(fraction * _hash.size())];
   }




   @Override
   public boolean isActive(){ return _isActive ; }

    public static final String hh_cm_info = "";
    public String ac_cm_info(Args args)
    {
        StringWriter s = new StringWriter();
        getInfo(new PrintWriter(s));
        return s.toString();
    }

   public static final String hh_cm_set_debug = "on|off" ;
   public String ac_cm_set_debug_$_1( Args args ){
     if( args.argv(0).equals("on") ){ _debug = true ; }
     else if( args.argv(0).equals("off") ){ _debug = false ; }
     else {
         throw new IllegalArgumentException("on|off");
     }
     return "";
   }
   public static final String hh_cm_set_active = "on|off" ;
   public String ac_cm_set_active_$_1( Args args ){
     if( args.argv(0).equals("on") ){ _isActive = true ; }
     else if( args.argv(0).equals("off") ){ _isActive = false ; }
     else {
         throw new IllegalArgumentException("on|off");
     }
     return "";
   }
   public static final String hh_cm_set_update = "on|off" ;
   public String ac_cm_set_update_$_1( Args args ){
     if( args.argv(0).equals("on") ){ _update = true ; }
     else if( args.argv(0).equals("off") ){ _update = false ; }
     else {
         throw new IllegalArgumentException("on|off");
     }
     return "";
   }
   public static final String hh_cm_set_magic = "on|off" ;
   public String ac_cm_set_magic_$_1( Args args ){
     if( args.argv(0).equals("on") ){ _magic = true ; }
     else if( args.argv(0).equals("off") ){ _magic = false ; }
     else {
         throw new IllegalArgumentException("on|off");
     }
     return "";
   }
   public static final String hh_cm_fake = "<poolName> [off] | [-space=<spaceCost>|off] [-cpu=<cpuCost>|off]" ;
   public synchronized String ac_cm_fake_$_1_2( Args args ){
      String poolName = args.argv(0) ;
      Entry e = _hash.get(poolName);
      if( e == null ) {
          throw new
                  IllegalArgumentException("Pool not found : " + poolName);
      }

      if( args.argc() > 1 ){
        if( args.argv(1).equals("off") ){
           e._fakeCpu   = -1.0 ;
           e._fakeSpace = -1.0 ;
        }else{
           throw new
           IllegalArgumentException("Unknown argument : "+args.argv(1));
        }
        return "Faked Costs switched off for "+poolName ;
      }
      String val = args.getOpt("cpu") ;
      if( val != null ) {
          e._fakeCpu = Double.parseDouble(val);
      }
      val = args.getOpt("space") ;
      if( val != null ) {
          e._fakeSpace = Double.parseDouble(val);
      }

      return poolName+" -space="+e._fakeSpace+" -cpu="+e._fakeCpu ;
   }
   public static final String hh_xcm_ls = "<poolName> [<filesize>] [-l]" ;
   public synchronized Object ac_xcm_ls_$_0_2( Args args )
   {


      if( args.argc()==0 ){   // added by nicolo : binary full cm ls list

	   CostModulePoolInfoTable reply = new CostModulePoolInfoTable();

	   /* This cycle browse an HashMap of Entry
	    * and put the PoolCostInfo object in the
	    * InfoPoolTable to return.
	    */
	   for (Entry e : _hash.values() ){
  		   reply.addPoolCostInfo(e.getPoolCostInfo().getPoolName(), e.getPoolCostInfo());
	   }
	   return reply;

      }

      String poolName = args.argv(0) ;
      long filesize   = Long.parseLong( args.argc() < 2 ? "0" : args.argv(2) ) ;
      boolean pci     = args.hasOption("l") ;
      Object [] reply;

      if( pci ){

         Entry e = _hash.get(poolName) ;
         reply = new Object[3] ;
         reply[0] = poolName ;
         reply[1] = e == null ? null : e.getPoolCostInfo() ;
         reply[2] = e == null ? null : System.currentTimeMillis() - e.timestamp;

      }else{

         PoolCostCheckable pcc = getPoolCost( poolName , filesize ) ;

         reply = new Object[4] ;

         reply[0] = poolName ;
         reply[1] = filesize;
         reply[2] = pcc == null ? null : pcc.getSpaceCost();
         reply[3] = pcc == null ? null : pcc.getPerformanceCost();

      }

      return reply ;
   }
   public static final String hh_cm_ls = " -d  | -t | -r [-size=<filesize>] <pattern> # list all pools" ;
   public synchronized String ac_cm_ls_$_0_1( Args args )
   {
      StringBuilder   sb = new StringBuilder() ;
      boolean useTime   = args.hasOption("t") ;
      boolean useDetail = args.hasOption("d") ;
      boolean useReal   = args.hasOption("r") ;
      String  sizeStr   = args.getOpt("size") ;
      long    filesize  = Long.parseLong( sizeStr == null ? "0" : sizeStr ) ;
      Pattern pattern   = args.argc() == 0 ? null : Pattern.compile(args.argv(0)) ;


      for(  Entry e : _hash.values() ){

         String poolName = e.getPoolCostInfo().getPoolName() ;
         if( ( pattern != null ) && ( ! pattern.matcher(poolName).matches() ) ) {
             continue;
         }
         sb.append(e.getPoolCostInfo().toString()).append("\n") ;
         if( useReal ){
             PoolCostCheckable pcc = getPoolCost(poolName,filesize) ;
             if( pcc == null ) {
                 sb.append("NONE\n");
             } else {
                 sb.append(getPoolCost(poolName, filesize).toString()).
                         append("\n");
             }
         }
         if( useDetail ) {
             sb.append(new CostCheck(poolName, e, filesize).toString()).
                     append("\n");
         }
         if( useTime ) {
             sb.append(poolName).
                     append("=").
                     append(System.currentTimeMillis() - e.timestamp).
                     append("\n");
         }

      }

      return sb.toString();
   }

    @Override
    public synchronized Collection<PoolCostInfo> getPoolCostInfos()
    {
        Collection<PoolCostInfo> costInfos = new ArrayList<>();
        for (Entry entry: _hash.values()) {
            if (entry.isValid() || !_update) {
                costInfos.add(entry.getPoolCostInfo());
            }
        }
        return costInfos;
    }

    @Override
    public synchronized PoolCostInfo getPoolCostInfo(String poolName)
    {
        Entry entry = _hash.get(poolName);
        if (entry != null && (entry.isValid() || !_update)) {
            return entry.getPoolCostInfo();
        }
        return null;
    }

    @Override
    public synchronized
        List<PoolInfo> getPoolInfo(Iterable<String> pools)
    {
        List<PoolInfo> infos = new ArrayList<>();
        for (String pool: pools) {
            Entry entry = _hash.get(pool);
            if (entry != null && (entry.isValid() || !_update)) {
                infos.add(entry.getPoolInfo());
            }
        }
        return infos;
    }

    @Override
    public synchronized
        Map<String,PoolInfo> getPoolInfoAsMap(Iterable<String> pools)
    {
        Map<String,PoolInfo> map = new HashMap<>();
        for (String pool: pools) {
            Entry entry = _hash.get(pool);
            if (entry != null && (entry.isValid() || !_update)) {
                map.put(pool, entry.getPoolInfo());
            }
        }
        return map;
    }

    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException
    {
        in.defaultReadObject();
        _handlers = new CellMessageDispatcher("messageToForward");
        _handlers.addMessageListener(this);
    }
}
