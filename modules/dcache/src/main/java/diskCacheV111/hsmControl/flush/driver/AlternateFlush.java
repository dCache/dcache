//   $Id: AlternateFlush.java,v 1.4 2006-12-15 15:38:07 tigran Exp $
package diskCacheV111.hsmControl.flush.driver ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import diskCacheV111.hsmControl.flush.HsmFlushControlCore;
import diskCacheV111.hsmControl.flush.HsmFlushSchedulable;
import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.StorageClassFlushInfo;

import dmg.cells.nucleus.CellAdapter;
import dmg.util.CommandInterpreter;

import org.dcache.util.Args;

/**
 * @author Patrick Fuhrmann patrick.fuhrmann@desy.de
 * @version 0.0, Dec 03, 2005
 *
 */
 public class AlternateFlush implements HsmFlushSchedulable {

     private final static Logger _log =
         LoggerFactory.getLogger(AlternateFlush.class);

     private HsmFlushControlCore _core;
     private CommandInterpreter  _interpreter;

     private String _mode = "auto" ;
     private double _percentageToFlush = 0.5 ;
     private int    _countToFlush      = 5 ;
     private int    _flushAtOnce;
     /**
       * Our Pool class. Contains things we need to remember.
       * Is stored with setDriverHandle to avoid our own
       * bookkeeping.
       */
     private class Pool implements HsmFlushControlCore.DriverHandle {

        private String  name;
        private int     flushCounter;
        private boolean modeReady;
        private long    totalSpace;
        private long    preciousSpace;
        private int     preciousFileCount;
        private HsmFlushControlCore.Pool pool;

        private Pool( String name , HsmFlushControlCore.Pool pool ){
            this.name = name ;
            this.pool = pool ;
            update() ;
        }
        public void update(){

            PoolCellInfo cellInfo = pool.getCellInfo() ;
            if( cellInfo == null ) {
                return;
            }
            PoolCostInfo costInfo = cellInfo.getPoolCostInfo() ;
            PoolCostInfo.PoolSpaceInfo spaceInfo = costInfo.getSpaceInfo() ;

            totalSpace        = spaceInfo.getTotalSpace() ;
            preciousSpace     = spaceInfo.getPreciousSpace() ;
            preciousFileCount = countTotalPending() ;
        }
        private void flush(){
           flushCounter += flushPool( pool );
        }
        private boolean isFlushing(){
//           return countStorageClassesFlushing(pool) > 0 ;
             return flushCounter > 0 ;
        }
        private int countTotalPending(){
            return countTotalPendingPool( pool ) ;

        }
        public String toString(){ return name ; }
     }
     private class StackEntry {
         private long waitingSince = System.currentTimeMillis();
         private StackEntry( int state ){ this.state = state ; }
         private int state;
     }
     private class EngineStack {
         private Stack<StackEntry> _stack = new Stack<>() ;
         public void push( StackEntry entry ){
             _stack.push(entry) ;
         }
         public StackEntry pop(){
             return _stack.pop();
         }
         public boolean isEmpty(){ return _stack.empty() ; }
         public int getCurrentState(){
            return  _stack.empty() ? -1 : (_stack.peek()).state ;
         }
     }
     private EngineStack _engineStack = new EngineStack() ;

     private int _status;
     private final static int QUERY_ALL_POOLS_IO_MODE = 1 ;

     public AlternateFlush( CellAdapter cell , HsmFlushControlCore core ){
         _log.info("AlternateFlush started");
         _core        = core ;
         _interpreter = new CommandInterpreter( this ) ;
     }
     //--------------------------------------------------------------------------------------
     //--------------------------------------------------------------------------------------
     //
     //   call backs from the flush manager.
     //
     @Override
     public void init(){
         if(_evt) {
             _log.info("EVENT : Initiating ...");
         }

         Args args = _core.getDriverArgs() ;
         //
         // printout what we got from our master
         //
         for( int i = 0 ; i < args.argc() ; i++ ){
             _log.info("    args "+i+" : "+args.argv(i)) ;
         }
         for( int i = 0 ; i < args.optc() ; i++ ){
             _log.info("    opts "+args.optv(i)+"="+args.getOpt(args.optv(i))) ;
         }
         for (Object pool : _core.getConfiguredPools()) {
             _log.info("    configured pool : " + pool.toString());
         }
         //
         //  reset the pool modes of all pools we are responsible for.
         //  As a side effect we get the actual pool modes.
         //
         for (Object o : _core.getConfiguredPools()) {
             HsmFlushControlCore.Pool pool = (HsmFlushControlCore.Pool) o;
             pool.setDriverHandle(new Pool(pool.getName(), pool));
             pool.setReadOnly(false);
             _log.info("init : setting readonly=false : " + pool.getName());
         }

     }
     @Override
     public void propertiesUpdated( Map<String,Object> properties ){

        if(_evt) {
            _log.info("EVENT : propertiesUpdated : " + properties);
        }

        Set<String> keys = new HashSet<>( properties.keySet() ) ;
        //
        // for all properties we support, try to change the values
        // accordingly.
        //
         for (String key : keys) {
             switch (key) {
             case "mode": {
                 //
                 //    mode is ok, so try to change it.
                 //
                 Object obj = properties.get(key);
                 if (obj != null) {
                     String mode = obj.toString();
                     if (mode.equals("auto")) {
                         _mode = "auto";
                     } else if (mode.equals("manual")) {
                         _mode = "manual";
                     }
                     // else{
                     //    just don't do anything if the value is invalid
                     //    the requestor will get the unmodified retrun.
                     // }
                 }
                 break;
             }
             case "flush.count": {
                 Object obj = properties.get(key);
                 if (obj != null) {
                     try {
                         int count = Integer.parseInt(obj.toString());
                         if (count < 1) {
                             throw new
                                     IllegalArgumentException("Value for " + key + " not supported " + obj);
                         }
                         _countToFlush = count;
                     } catch (Exception ee) {
                         _log.warn("Exception while seting " + key + " " + ee);
                     }
                 }
                 break;
             }
             case "flush.atonce": {
                 Object obj = properties.get(key);
                 if (obj != null) {
                     try {
                         int count = Integer.parseInt(obj.toString());
                         if (count < 1) {
                             throw new
                                     IllegalArgumentException("Value for " + key + " not supported " + obj);
                         }
                         _flushAtOnce = count;
                     } catch (Exception ee) {
                         _log.warn("Exception while seting " + key + " " + ee);
                     }
                 }
                 break;
             }
             case "flush.percentage": {
                 Object obj = properties.get(key);
                 if (obj != null) {
                     try {
                         double percent = Double.parseDouble(obj.toString());
                         if (percent < 0.0) {
                             throw new
                                     IllegalArgumentException("Value for " + key + " not supported " + obj);
                         }

                         _percentageToFlush = percent;
                     } catch (Exception ee) {
                         _log.warn("Exception while seting " + key + " " + ee);
                     }
                 }
                 break;
             }
             default:
                 //
                 // remove the key to inform the requestor that we don't
                 // support this property.
                 //
                 properties.remove(key);
                 break;
             }
         }
        //
        // do as it would have been a query
        //
        properties.put( "mode"       , _mode ) ;
        properties.put( "flush.count"      , ""+_countToFlush ) ;
        properties.put( "flush.percentage" , ""+_percentageToFlush ) ;
        properties.put( "flush.atonce"     , ""+_flushAtOnce ) ;
        //
     }
     @Override
     public void poolIoModeUpdated( String poolName ,  HsmFlushControlCore.Pool pool ){

         if(_evt) {
             _log.info("EVENT : poolIoModeUpdated : " + pool);
         }

         Pool ip = getInternalPool( pool ) ;
         ip.modeReady = true ;
         ip.update() ;

     }
     @Override
     public void flushingDone( String poolName , String storageClassName , HsmFlushControlCore.FlushInfo flushInfo  ){

         if(_evt) {
             _log.info("EVENT : flushingDone : pool =" + poolName + ";class=" + storageClassName /* + "flushInfo="+flushInfo */);
         }

         HsmFlushControlCore.Pool pool = _core.getPoolByName( poolName ) ;
         if( pool == null ){
            _log.warn("flushingDone for a non configured pool : "+poolName);
            return ;
         }

         Pool ip = getInternalPool( pool ) ;
         ip.update() ;

         ip.flushCounter -- ;

         if( ip.flushCounter <= 0 ){
             ip.flushCounter = 0 ;
             _log.info("flushingDone : pool finished all flushing : "+poolName+" ; setting back to readWrite mode");
             pool.setReadOnly(false);
         }
     /*
        if( ! ip.isFlushing() ){
             _log.info("flushingDone : pool finished all flushing : "+poolName+" ; setting back to readWrite mode");
             pool.setReadOnly(false);
        }
      */
     }

     @Override
     public void reset(){
         if(_evt) {
             _log.info("EVENT : reset");
         }
     }
     @Override
     public void timer(){
         if(_evt) {
             _log.info("EVENT : timer");
         }
         //
         //
         // check for the next pool to flush.
         //
         Collection<String> set = new HashSet<>() ;
         for( HsmFlushControlCore.Pool pool = nextToFlush() ; pool != null ; pool = nextToFlush() ){

            String poolName = pool.getName() ;

            if( set.contains(poolName) ) {
                break;
            }
            set.add( poolName ) ;

            _log.info("timer : Good candidate to flush : "+poolName);

            Pool ip = getInternalPool( pool ) ;
            if( ! ip.modeReady ) {
                continue;
            }

            pool.setReadOnly(true);
            ip.flush( ) ;

         }

     }
     @Override
     public void poolFlushInfoUpdated( String poolName , HsmFlushControlCore.Pool pool ){

         if(_evt) {
             _log.info("EVENT : poolFlushInfoUpdated : " + pool.getName());
         }
         if( ! pool.isActive() ){
             _log.info( "poolFlushInfoUpdated : Pool : "+poolName+" inactive");
             return ;
         }
         //
         // make sure we store the incoming stuff in our internal structure.
         //
         getInternalPool( pool ).update() ;

     }
     /**
       *  Executes the external command with CommandInterpreter (using our ac_xx) commands.
       */
     @Override
     public void command( Args args  ){
         if(_evt) {
             _log.info("EVENT : command : " + args);
         }
         if (args.argc() == 0) {
             return;
         }
         try{
             Object reply = _interpreter.command( args ) ;
             if( reply == null ) {
                 throw new
                         Exception("Null pointer from command call");
             }
             _log.info("Command returns : "+reply.toString() );
         }catch(Exception ee ){
             _log.warn("Command returns an exception ("+ee.getClass().getName()+") : " + ee.toString());
         }
     }
     @Override
     public void prepareUnload(){
         if(_evt) {
             _log.info("EVENT : Preparing unload (ignoring)");
         }
     }
     @Override
     public void configuredPoolAdded( String poolName ){

         if(_evt) {
             _log.info("EVENT : Configured pool added : " + poolName);
         }

         HsmFlushControlCore.Pool pool = _core.getPoolByName( poolName ) ;
         if( pool == null ){
            _log.warn("Pool not found in _core database : "+poolName);
            return ;
         }

         Pool ip = getInternalPool( pool ) ;

         pool.setReadOnly(false);

     }
     @Override
     public void poolSetupUpdated(){
         if(_evt) {
             _log.info("EVENT : Pool Setup updated (ignoring)");
         }
     }
     @Override
     public void configuredPoolRemoved( String poolName ){
         if(_evt) {
             _log.info("EVENT : Configured pool removed : " + poolName + "  (ignoring)");
         }
     }
     //-------------------------------------------------------------------------------------------
     //
     //              C O M M A N D S
     //
     public static final String hh_dummy = "# dummy call" ;
     public String ac_dummy_$_1_99( Args args ){
        return args.toString();
     }
     public String ac_list_pools( Args args ){
       return null;
     }
     //-------------------------------------------------------------------------------------------
     //
     //               C O N V E N I E N E N T     F U N C T I O N S
     //
     private Pool getInternalPool( HsmFlushControlCore.Pool pool ){

            Pool ip = (Pool)pool.getDriverHandle() ;
            if( ip == null ){
               _log.warn("getInternalPool : Unconfigured pool arrived "+pool.getName()+"; configuring");
               pool.setDriverHandle( ip = new Pool( pool.getName() , pool ) ) ;
            }
            return ip ;

     }
     /**
       *
       * Convenient method to flush all pending storage classes of the specified pool.
       *
       * @param ip Internal pool representation.
       */
     private int flushPool( HsmFlushControlCore.Pool pool ){
         int flushing = 0 ;
         for (Object o : pool.getFlushInfos()) {

             HsmFlushControlCore.FlushInfo info = (HsmFlushControlCore.FlushInfo) o;
             StorageClassFlushInfo flush = info.getStorageClassFlushInfo();

             long size = flush.getTotalPendingFileSize();

             _log.info("flushPool : class = " + info
                     .getName() + " size = " + size + " flushing = " + info
                     .isFlushing());
             //
             // is precious size > 0 and are we not yet flushing ?
             //
             try {
                 if ((size > 0L) && !info.isFlushing()) {
                     _log.info("flushPool : !!! flushing " + pool
                             .getName() + " " + info.getName());
                     info.flush(_flushAtOnce);
                     flushing++;
                 }
             } catch (Exception ee) {
                 _log.warn("flushPool : Problem flushing " + pool
                         .getName() + " " + info.getName() + " " + ee);
             }

         }
         return flushing ;
     }
     /**
       * Counts the number pending requests per pool.
       *
       * @return Number of flush requests per pool.
       */
     private int countTotalActivePool( HsmFlushControlCore.Pool pool ){
         int total = 0 ;
         for (Object o : pool.getFlushInfos()) {

             HsmFlushControlCore.FlushInfo info = (HsmFlushControlCore.FlushInfo) o;
             StorageClassFlushInfo flush = info.getStorageClassFlushInfo();

             total += flush.getActiveCount();
         }
         return total ;
     }
     /**
       * Counts the number pending requests per pool.
       *
       * @return Number of flush requests per pool.
       */
     private int countTotalPendingPool( HsmFlushControlCore.Pool pool ){
         int total = 0 ;
         for (Object o : pool.getFlushInfos()) {

             HsmFlushControlCore.FlushInfo info = (HsmFlushControlCore.FlushInfo) o;
             StorageClassFlushInfo flush = info.getStorageClassFlushInfo();

             total += flush.getRequestCount();
         }
         return total ;
     }
     private boolean _ntf = true ;
     private boolean _evt = true ;
     /**
       * Central place to decide whether or not we want to flush
       * a pool. If 'null' is returned there is no pool ready yet.
       *
       *  @return Next pool to flush or 'null' if no pool is ready yet.
       */
     private HsmFlushControlCore.Pool nextToFlush(){

         List<HsmFlushControlCore.Pool> pools = _core.getConfiguredPools() ;
         //
         // Get all pools which are currently not flushing.
         //
         List<Pool> list =  new ArrayList<>();

         for (HsmFlushControlCore.Pool pool : pools) {
             if (_ntf) {
                 _log.info("nextToFlush : checking pool " + pool);
             }

             if (!pool.isActive()) {
                 continue;
             }

             Pool ip = (Pool) pool.getDriverHandle();

             if (ip.isFlushing()) {
                 if (_ntf) {
                     _log.info("nextToFlush : is already flushing " + pool
                             .getName());
                 }
             } else {
                 list.add(ip);
             }
         }
         //
         // make sure we have at least one pool to write on
         //
         if( list.size() < 2 ){
             if(_ntf) {
                 _log.info("nextToFlush : currently not enough pools to write on (" + list
                         .size() + ")");
             }
             return null ;
         }
         if(_ntf) {
             _log.info("nextToFlush : possible candidates : " + list);
         }
         /**
           *  Get pool with highest pending file count and pool with highest
           *  precious/total space ratio.
           */
         Pool   poolWithHighestCounter = null , poolWithHighestPercentage = null ;
         int    highestCounter    = -1 ;
         double highestPercentage = -1.0 ;
         for (Pool ip : list) {
             if (ip.preciousFileCount > highestCounter) {
                 poolWithHighestCounter = ip;
                 highestCounter = ip.preciousFileCount;
             }

             double percentage = ((double) ip.preciousSpace) / ((double) ip.totalSpace);
             if (percentage > highestPercentage) {
                 poolWithHighestPercentage = ip;
                 highestPercentage = percentage;
             }
         }
         if(_ntf) {
             _log.info("nextToFlush : highest percentage found for : " + poolWithHighestPercentage
                     .pool.getName() + " (" + highestPercentage + ")");
         }
         if(_ntf) {
             _log.info("nextToFlush : highest counter    found for : " + poolWithHighestCounter
                     .pool.getName() + " (" + highestCounter + ")");
         }
         if( highestPercentage > _percentageToFlush  ) {
             return poolWithHighestPercentage.pool;
         }
         if( highestCounter > _countToFlush ) {
             return poolWithHighestCounter.pool;
         }
         return  null ;
     }
     /**
       * Determines how many storage classes are in the process of being flushed on this pool.
       *
       * @return Number of storage class in progress of been flushed on this pool.
       */
     private int countStorageClassesFlushing( HsmFlushControlCore.Pool pool ){

         int flushing = 0 ;
         for (Object o : pool.getFlushInfos()) {

             if (((HsmFlushControlCore.FlushInfo) o).isFlushing()) {
                 flushing++;
             }

         }
         return flushing ;
     }
}
