//   $Id: HandlerExample.java,v 1.2 2006-04-03 05:51:56 patrick Exp $
package diskCacheV111.hsmControl.flush.driver ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
public class HandlerExample implements HsmFlushSchedulable {

    private final static Logger _log =
        LoggerFactory.getLogger(HandlerExample.class);

     private HsmFlushControlCore _core;
     private CommandInterpreter  _interpreter;
     private Map<String, Pool> _poolHash = new HashMap<>() ;
     private boolean             _doNothing;
     private Map<String, Object> _properties = new HashMap<>();

     private class Pool {
        private String _name;
        private Pool( String name ){  _name = name  ;}
        private long _lastUpdated;
        private int  _count;
        private void updated(){
           _lastUpdated = System.currentTimeMillis() ;
           _count ++ ;
        }
     }

     public HandlerExample( CellAdapter cell , HsmFlushControlCore core ){
         _log.info("HandlerExample started");
         _core = core ;
         _interpreter = new CommandInterpreter( this ) ;
     }
     @Override
     public void init(){
         _log.info("init called");
         Args args = _core.getDriverArgs() ;
         for( int i = 0 ; i < args.argc() ; i++ ){
             _log.info("    args "+i+" : "+args.argv(i)) ;
         }
         for( int i = 0 ; i < args.optc() ; i++ ){
             _log.info("    opts "+args.optv(i)+"="+args.getOpt(args.optv(i))) ;
         }
         _doNothing = args.hasOption("do-nothing") ;
         _properties.put( "mode" , _doNothing ? "manual" : "auto" ) ;

         for (Object o : _core.getConfiguredPoolNames()) {
             String poolName = o.toString();
             _log.info("    configured pool : " + poolName + _core
                     .getPoolByName(poolName).toString());
             _poolHash.put(poolName, new Pool(poolName));
         }
     }
     @Override
     public void prepareUnload(){
         _log.info("preparing unload");
     }
     @Override
     public void configuredPoolAdded( String poolName ){
         _log.info("configured pool added : "+poolName);

     }
     @Override
     public void configuredPoolRemoved( String poolName ){
         _log.info("configured pool removed : "+poolName);
         _poolHash.remove( poolName ) ;
     }
     @Override
     public void flushingDone( String poolName , String storageClassName , HsmFlushControlCore.FlushInfo flushInfo  ){

         _log.info("flushingDone : pool ="+poolName+";class="+storageClassName /* + "flushInfo="+flushInfo */ );

     }
     @Override
     public void command( Args args  ){
         _log.info("command : "+args);
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
     public String ac_set_mode_$_1( Args args ){
        String com = args.argv(0) ;
        if( com.equals("auto") ){
           _doNothing = false ;
        }else if( com.equals("manual") ){
           _doNothing = true ;
        }
        return "" ;
     }
     public String ac_set_property_$_1_2( Args args ){
        String com = args.argv(0) ;
        if( args.argc() == 1 ){
            _properties.remove( com ) ;
        }else{
            _properties.put( com , args.argv(1) ) ;
        }
        return "" ;
     }
     @Override
     public void poolSetupUpdated(){
         _log.info("pool Setup updated");
     }
     @Override
     public void poolIoModeUpdated( String poolName ,  HsmFlushControlCore.Pool pool ){
         _log.info("pool io mode updated : "+pool);
     }
     @Override
     public void reset(){
         _log.info("EVENT : reset");
     }
     @Override
     public void timer(){
         _log.info( "Timer at : "+System.currentTimeMillis());
     }
     @Override
     public void propertiesUpdated( Map<String,Object> properties )
     {
        Set<String> keys = new HashSet<>( properties.keySet() ) ;
        //
        // for all properties we support, try to change the values
        // accordingly.
        //
         for (String key : keys) {
             String ourPropertyValue = (String) _properties.get(key);

             if (ourPropertyValue == null) {
                 //
                 // we don't support this property, so remove
                 // it from the list.
                 //
                 properties.remove(key);
                 continue;
             }

             if (key.equals("mode")) {
                 Object obj = properties.get(key);
                 if (obj != null) {
                     String mode = obj.toString();
                     if (mode.equals("auto")) {
                         _doNothing = false;
                         _properties.put(key, "auto");
                     } else if (mode.equals("manual")) {
                         _doNothing = true;
                         _properties.put(key, "manual");
                     }
                     //
                     // if it is neither 'manual' nor 'auto' we
                     // just don't change the current value. When
                     // replying, client will notice.
                     //
                 }
             } else {
                 _properties.put(key, properties.get(key));
             }
         }
        //
        // do as it would have been a query
        //
        properties.clear() ;
        properties.putAll( _properties ) ;
        //
     }
     @Override
     public void poolFlushInfoUpdated( String poolName , HsmFlushControlCore.Pool pool ){

         if( _doNothing ) {
             return;
         }

         if( ! pool.isActive() ){
             _log.info( "poolFlushInfoUpdated : Pool : "+poolName+" inactive");
             return ;
         }
         PoolCellInfo cellInfo = pool.getCellInfo() ;
         PoolCostInfo costInfo = cellInfo.getPoolCostInfo() ;
         PoolCostInfo.PoolSpaceInfo spaceInfo = costInfo.getSpaceInfo() ;
         PoolCostInfo.PoolQueueInfo queueInfo = costInfo.getStoreQueue() ;

         long total    = spaceInfo.getTotalSpace() ;
         long precious = spaceInfo.getPreciousSpace() ;

         _log.info( "poolFlushInfoUpdated : Pool : "+poolName+";total="+total+";precious="+precious);
         //
         // loop over all storage classes of this pool and flush
         // those with have some files pending and which are not yet
         // in 'flush' status.
         //
         for (Object o : pool.getStorageClassNames()) {

             String storageClass = o.toString();

             HsmFlushControlCore.FlushInfo info = pool
                     .getFlushInfoByStorageClass(storageClass);
             StorageClassFlushInfo flush = info.getStorageClassFlushInfo();

             long size = flush.getTotalPendingFileSize();

             _log.info("poolFlushInfoUpdated :       class = " + storageClass + " size = " + size + " flushing = " + info
                     .isFlushing());
             //
             // is precious size > 0 and are we not yet flushing ?
             //
             try {
                 if ((size > 0L) && !info.isFlushing()) {
                     _log.info("poolFlushInfoUpdated :       flushing " + poolName + " " + storageClass);
                     info.flush(0);
                 }
             } catch (Exception ee) {
                 _log.warn("poolFlushInfoUpdated : Problem flushing " + poolName + " " + storageClass + " " + ee);
             }

         }
     }
}
