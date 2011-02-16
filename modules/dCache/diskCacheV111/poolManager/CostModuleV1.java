// $Id: CostModuleV1.java,v 1.21 2007-08-01 20:19:23 tigran Exp $

package diskCacheV111.poolManager ;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellMessageDispatcher;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.CellInfoProvider;
import org.dcache.cells.CellSetupProvider;

import diskCacheV111.pools.CostCalculatable;
import diskCacheV111.pools.CostCalculationEngine;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.pools.PoolCostInfo.NamedPoolQueueInfo;
import diskCacheV111.vehicles.CostModulePoolInfoTable;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolCostCheckable;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellInfo;
import dmg.util.Args;

public class CostModuleV1
    implements CostModule,
               CellCommandListener,
               CellMessageReceiver,
               CellInfoProvider,
               CellSetupProvider
{
    /** The file size used when calculating performance cost ranked percentile  */
    public static final long PERCENTILE_FILE_SIZE = 104857600;
    private final static Logger _log = LoggerFactory.getLogger(CostModuleV1.class);

    private final Map<String, Entry> _hash = new HashMap<String, Entry>() ;
    private boolean _isActive = true ;
    private boolean _update = true ;
    private boolean _magic = true ;
    private boolean _debug = false ;
    private boolean _cachedPercentileCostIsValid = false;
    private double _cachedPercentileCost;
    private double _cachedPercentileFraction;
    private final CellMessageDispatcher _handlers =
        new CellMessageDispatcher("messageToForward");

    /**
     * Information about some specific pool.
     */
   private static class Entry {
       private long timestamp ;
       private PoolCostInfo _info ;
       private double _fakeCpu   = -1.0 ;
       private double _fakeSpace = -1.0 ;
       private Map<String, String> _tagMap = null;
       private Entry( PoolCostInfo info ){
          setPoolCostInfo(info) ;
       }
       private void setPoolCostInfo( PoolCostInfo info ){
          timestamp = System.currentTimeMillis();
          _info = info ;
       }
       private PoolCostInfo getPoolCostInfo(){
           return _info ;
        }
       private boolean isValid(){
          return ( System.currentTimeMillis() - timestamp ) < 5*60*1000L ;
       }
       private void setTagMap(Map<String, String> tagMap) {
           _tagMap = tagMap;
       }
       private Map<String, String> getTagMap() {
           return _tagMap;
       }
   }
   private CostCalculationEngine _costCalculationEngine = null ;
   private class CostCheck extends PoolCheckAdapter implements PoolCostCheckable  {

       private PoolCostInfo _info ;

       private static final long serialVersionUID = -77487683158664348L;

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

    public synchronized void messageArrived(PoolManagerPoolUpMessage msg)
    {
        if (! _update)
            return;

        String poolName = msg.getPoolName();
        PoolV2Mode poolMode = msg.getPoolMode();
        PoolCostInfo newInfo = msg.getPoolCostInfo();

        /* Whether the pool mentioned in the message should be removed */
        boolean removePool = poolMode.getMode() == PoolV2Mode.DISABLED ||
                poolMode.isDisabled(PoolV2Mode.DISABLED_STRICT) ||
                poolMode.isDisabled(PoolV2Mode.DISABLED_DEAD);


        /* Check whether we should invalidate the cached entry.  We should
         * only need to do this if:
         *   * a new pool is discovered,
         *   * an existing pool is removed,
         *   * either:
         *       o  a pool with cost less than the cached value assumes a cost greater
         *                  than the cached value,
         *       o  a pool with the cached value takes on a different value,
         *       o  a pool with cost greater than the cached value assumes a cost less
         *                  than the cached value.
         */
        if( _cachedPercentileCostIsValid) {
            Entry poolEntry = _hash.get( poolName);
            if( poolEntry != null) {
                if( removePool) {
                    _cachedPercentileCostIsValid = false;
                } else {
                    PoolCostInfo oldInfo = poolEntry.getPoolCostInfo();
                    double oldCost = _costCalculationEngine.getCostCalculatable( oldInfo).getPerformanceCost();
                    double newCost = _costCalculationEngine.getCostCalculatable( newInfo).getPerformanceCost();

                    if( Math.signum( oldCost-_cachedPercentileCost) != Math.signum( newCost-_cachedPercentileCost))
                        _cachedPercentileCostIsValid = false;
                }
            } else {
                if( !removePool)
                    _cachedPercentileCostIsValid = false;
            }
        }

        if ( !removePool) {
            if( newInfo != null) {
                Entry e = _hash.get(poolName);

                if (e == null) {
                    e = new Entry( newInfo);
                    _hash.put(poolName, e);
                } else {
                    e.setPoolCostInfo( newInfo);
                }
                e.setTagMap(msg.getTagMap());
            }
        } else {
            _hash.remove(poolName);
        }
    }

    public synchronized void messageToForward(PoolIoFileMessage msg)
    {
        String poolName = msg.getPoolName();
        Entry e = _hash.get(poolName);
        if (e == null)
            return;

        String requestedQueueName = msg.getIoQueueName();

        PoolCostInfo costInfo = e.getPoolCostInfo();
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
        if (msg.isReply() && msg.getReturnCode() != 0) {
            diff = -1;
            if (msg instanceof PoolAcceptFileMessage) {
                pinned = -msg.getStorageInfo().getFileSize();
            }
        } else if (!msg.isReply() && !_magic) {
            diff = 1;
            if (msg instanceof PoolAcceptFileMessage) {
                pinned = msg.getStorageInfo().getFileSize();
            }
        }

        queue.modifyQueue(diff);
        spaceInfo.modifyPinnedSpace(pinned);

        xsay("Mover"+(requestedQueueName==null?"":("("+requestedQueueName+")")) , poolName, diff, pinned, msg);
    }

    public synchronized void messageToForward(DoorTransferFinishedMessage msg)
    {
        String poolName = msg.getPoolName();
        Entry e = _hash.get(poolName);
        if (e == null)
            return;

        PoolCostInfo costInfo = e.getPoolCostInfo();
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

        xsay("Mover"+(requestedQueueName==null?"":("("+requestedQueueName+")")), poolName, diff, pinned, msg);
    }

    public synchronized void messageToForward(PoolFetchFileMessage msg)
    {
         String poolName = msg.getPoolName();
         Entry e = _hash.get(poolName);
         if (e == null)
             return;

         PoolCostInfo costInfo = e.getPoolCostInfo();
         PoolCostInfo.PoolQueueInfo queue = costInfo.getRestoreQueue();
         PoolCostInfo.PoolSpaceInfo spaceInfo = costInfo.getSpaceInfo();

         int diff;
         long pinned;
         if (!msg.isReply()) {
             diff = 1;
             pinned = msg.getStorageInfo().getFileSize();
         } else {
             diff = -1;
             pinned = 0;
         }
         queue.modifyQueue(diff);
         spaceInfo.modifyPinnedSpace(pinned);

         xsay("Restore", poolName, diff, pinned, msg);
    }

    public synchronized void messageToForward(PoolMgrSelectPoolMsg msg)
    {
         if (!_magic)
             return;

         if (!msg.isReply())
             return;
         String poolName = msg.getPoolName();
         Entry e = _hash.get(poolName);
         if (e == null)
             return;

         String requestedQueueName = msg.getIoQueueName();

         PoolCostInfo costInfo = e.getPoolCostInfo();
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

         xsay("Mover (magic)"+(requestedQueueName==null?"":("("+requestedQueueName+")")), poolName, diff, pinned, msg);
    }

    public synchronized void messageToForward(Pool2PoolTransferMsg msg)
    {
        _log.debug( "Pool2PoolTransferMsg : reply="+msg.isReply());

        String sourceName = msg.getSourcePoolName();
        Entry source = _hash.get(sourceName);
        if (source == null)
            return;

        PoolCostInfo.PoolQueueInfo sourceQueue =
            source.getPoolCostInfo().getP2pQueue();

        String destinationName = msg.getDestinationPoolName();
        Entry destination = _hash.get(destinationName);
        if (destination == null)
            return;

        PoolCostInfo.PoolQueueInfo destinationQueue =
            destination.getPoolCostInfo().getP2pClientQueue();
        PoolCostInfo.PoolSpaceInfo destinationSpaceInfo =
            destination.getPoolCostInfo().getSpaceInfo();

        int diff = msg.isReply() ? -1 : 1;
        long pinned = msg.getStorageInfo().getFileSize();

        sourceQueue.modifyQueue(diff);
        destinationQueue.modifyQueue(diff);
        destinationSpaceInfo.modifyPinnedSpace(pinned);

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

      if( ( cost == null ) ||( !cost.isValid() && _update  ) )
    	  return null ;

      return  new CostCheck( poolName , cost , filesize ) ;

   }

   @Override
   public synchronized double getPoolsPercentilePerformanceCost( double fraction) {

       if( fraction <= 0 || fraction >= 1)
           throw new IllegalArgumentException( "supplied fraction (" + Double.toString( fraction) +") not between 0 and 1");

       if( !_cachedPercentileCostIsValid || _cachedPercentileFraction != fraction) {

           _log.debug( "Rebuilding percentileCost cache");

           if( _hash.size() > 0) {
               if( _log.isDebugEnabled())
                   _log.debug( "  "+Integer.toString( _hash.size())+" pools available");

               double poolCosts[] = new double[ _hash.size()];

               int idx=0;
               for( Entry poolInfo : _hash.values()) {
                   CostCalculatable  cost = _costCalculationEngine.getCostCalculatable( poolInfo.getPoolCostInfo());
                   cost.recalculate( PERCENTILE_FILE_SIZE);
                   poolCosts[idx++] = cost.getPerformanceCost();
               }

               Arrays.sort(  poolCosts);

               _cachedPercentileCost = poolCosts [ (int) Math.floor( fraction * _hash.size())];

           } else {
               _log.debug( "  no pools available");
               _cachedPercentileCost = 0;
           }

           _cachedPercentileCostIsValid = true;
           _cachedPercentileFraction = fraction;
       }

       return _cachedPercentileCost;
   }




   @Override
   public boolean isActive(){ return _isActive ; }

    public String hh_cm_info = "";
    public String ac_cm_info(Args args)
    {
        StringWriter s = new StringWriter();
        getInfo(new PrintWriter(s));
        return s.toString();
    }

   public String hh_cm_set_debug = "on|off" ;
   public String ac_cm_set_debug_$_1( Args args ){
     if( args.argv(0).equals("on") ){ _debug = true ; }
     else if( args.argv(0).equals("off") ){ _debug = false ; }
     else throw new IllegalArgumentException("on|off") ;
     return "";
   }
   public String hh_cm_set_active = "on|off" ;
   public String ac_cm_set_active_$_1( Args args ){
     if( args.argv(0).equals("on") ){ _isActive = true ; }
     else if( args.argv(0).equals("off") ){ _isActive = false ; }
     else throw new IllegalArgumentException("on|off") ;
     return "";
   }
   public String hh_cm_set_update = "on|off" ;
   public String ac_cm_set_update_$_1( Args args ){
     if( args.argv(0).equals("on") ){ _update = true ; }
     else if( args.argv(0).equals("off") ){ _update = false ; }
     else throw new IllegalArgumentException("on|off") ;
     return "";
   }
   public String hh_cm_set_magic = "on|off" ;
   public String ac_cm_set_magic_$_1( Args args ){
     if( args.argv(0).equals("on") ){ _magic = true ; }
     else if( args.argv(0).equals("off") ){ _magic = false ; }
     else throw new IllegalArgumentException("on|off") ;
     return "";
   }
   public String hh_cm_fake = "<poolName> [off] | [-space=<spaceCost>|off] [-cpu=<cpuCost>|off]" ;
   public String ac_cm_fake_$_1_2( Args args ){
      String poolName = args.argv(0) ;
      Entry e = _hash.get(poolName);
      if( e == null )
         throw new
         IllegalArgumentException("Pool not found : "+poolName);

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
      if( val != null )e._fakeCpu = Double.parseDouble(val) ;
      val = args.getOpt("space") ;
      if( val != null )e._fakeSpace = Double.parseDouble(val);

      return poolName+" -space="+e._fakeSpace+" -cpu="+e._fakeCpu ;
   }
   public String hh_xcm_ls = "<poolName> [<filesize>] [-l]" ;
   public Object ac_xcm_ls_$_0_2( Args args )throws Exception {


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
      boolean pci     = args.getOpt("l") != null ;
      Object [] reply;

      if( pci ){

         Entry e = _hash.get(poolName) ;
         reply = new Object[3] ;
         reply[0] = poolName ;
         reply[1] = e == null ? null : e.getPoolCostInfo() ;
         reply[2] = e == null ? null : Long.valueOf( System.currentTimeMillis() - e.timestamp ) ;

      }else{

         PoolCostCheckable pcc = getPoolCost( poolName , filesize ) ;

         reply = new Object[4] ;

         reply[0] = poolName ;
         reply[1] = Long.valueOf( filesize ) ;
         reply[2] = pcc == null ? null : new Double( pcc.getSpaceCost() ) ;
         reply[3] = pcc == null ? null : new Double( pcc.getPerformanceCost() ) ;

      }

      return reply ;
   }
   public String hh_cm_ls = " -d  | -t | -r [-size=<filesize>] <pattern> # list all pools" ;
   public String ac_cm_ls_$_0_1( Args args )throws Exception {
      StringBuilder   sb = new StringBuilder() ;
      boolean useTime   = args.getOpt("t") != null ;
      boolean useDetail = args.getOpt("d") != null ;
      boolean useReal   = args.getOpt("r") != null ;
      String  sizeStr   = args.getOpt("size") ;
      long    filesize  = Long.parseLong( sizeStr == null ? "0" : sizeStr ) ;
      Pattern pattern   = args.argc() == 0 ? null : Pattern.compile(args.argv(0)) ;


      for(  Entry e : _hash.values() ){

         String poolName = e.getPoolCostInfo().getPoolName() ;
         if( ( pattern != null ) && ( ! pattern.matcher(poolName).matches() ) )continue ;
         sb.append(e.getPoolCostInfo().toString()).append("\n") ;
         if( useReal ){
             PoolCostCheckable pcc = getPoolCost(poolName,filesize) ;
             if( pcc == null )
                sb.append("NONE\n") ;
             else
                sb.append( getPoolCost(poolName,filesize).toString() ).
                append("\n");
         }
         if( useDetail )
             sb.append(new CostCheck(poolName,e,filesize).toString()).
             append("\n");
         if( useTime )
             sb.append(poolName).
                append("=").
                append(System.currentTimeMillis()-e.timestamp).
                append("\n");

      }

      return sb.toString();
   }


   @Override
   public synchronized PoolCostInfo getPoolCostInfo(String poolName) {

	   PoolCostInfo poolCostInfo = null;

	   Entry poolEntry = _hash.get(poolName);

	   if( poolEntry != null ) {
		   poolCostInfo = poolEntry.getPoolCostInfo();
	   }

	   return poolCostInfo;
   }
}
