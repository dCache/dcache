//   $Id: HandlerExample.java,v 1.2 2006-04-03 05:51:56 patrick Exp $
package diskCacheV111.hsmControl.flush.driver ;
import  diskCacheV111.hsmControl.flush.* ;
import  diskCacheV111.pools.* ;
import  dmg.cells.nucleus.CellAdapter ;
import  dmg.util.* ;
import  java.util.*;

/**
 * @author Patrick Fuhrmann patrick.fuhrmann@desy.de
 * @version 0.0, Dec 03, 2005
 *
 */
 public class HandlerExample implements HsmFlushSchedulable {

     private HsmFlushControlCore _core        = null;
     private CommandInterpreter  _interpreter = null ;
     private HashMap             _poolHash    = new HashMap() ;
     private boolean             _doNothing   = false ;
     private Map                 _properties  = new HashMap() ;

     private class Pool {
        private String _name = null ;
        private Pool( String name ){  _name = name  ;}
        private long _lastUpdated = 0L ;
        private int  _count = 0 ;
        private void updated(){
           _lastUpdated = System.currentTimeMillis() ;
           _count ++ ;
        }
     }

     public HandlerExample( CellAdapter cell , HsmFlushControlCore core ){
         core.say("HandlerExample started");
         _core = core ;
         _interpreter = new CommandInterpreter( this ) ;
     }
     public void init(){
         say("init called");
         Args args = _core.getDriverArgs() ;
         for( int i = 0 ; i < args.argc() ; i++ ){
             say("    args "+i+" : "+args.argv(i)) ;
         }
         for( int i = 0 ; i < args.optc() ; i++ ){
             say("    opts "+args.optv(i)+"="+args.getOpt(args.optv(i))) ;
         }
         _doNothing = args.getOpt("do-nothing") != null ;
         _properties.put( "mode" , _doNothing ? "manual" : "auto" ) ;

         for( Iterator i = _core.getConfiguredPoolNames().iterator() ; i.hasNext() ; ){
             String poolName = i.next().toString() ;
             say("    configured pool : "+poolName+_core.getPoolByName(poolName).toString() ) ;
             _poolHash.put( poolName , new Pool(poolName) ) ;
         }
     }
     public void prepareUnload(){
         say("preparing unload");
     }
     public void configuredPoolAdded( String poolName ){
         say("configured pool added : "+poolName);

     }
     public void configuredPoolRemoved( String poolName ){
         say("configured pool removed : "+poolName);
         _poolHash.remove( poolName ) ;
     }
     public void flushingDone( String poolName , String storageClassName , HsmFlushControlCore.FlushInfo flushInfo  ){

         say("flushingDone : pool ="+poolName+";class="+storageClassName /* + "flushInfo="+flushInfo */ );

     }
     public void command( Args args  ){
         say("command : "+args);
         try{

             Object reply = _interpreter.command( args ) ;
             if( reply == null )
               throw new
               Exception("Null pointer from command call");

             say("Command returns : "+reply.toString() );

         }catch(Exception ee ){
             esay("Command returns an exception ("+ee.getClass().getName()+") : " + ee.toString());
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
     public void poolSetupUpdated(){
         say("pool Setup updated");
     }
     public void poolIoModeUpdated( String poolName ,  HsmFlushControlCore.Pool pool ){
         say("pool io mode updated : "+pool);
     }
     public void reset(){
         say("EVENT : reset");
     }
     public void timer(){
         say( "Timer at : "+System.currentTimeMillis());
     }
     public void propertiesUpdated( Map properties ){

        Set keys = new HashSet( properties.keySet() ) ;
        //
        // for all properties we support, try to change the values
        // accordingly.
        //
        for( Iterator i = keys.iterator() ; i.hasNext() ; ){

            String key = (String)i.next() ;

            String ourPropertyValue = (String)_properties.get( key ) ;

            if( ourPropertyValue == null ){
               //
               // we don't support this property, so remove
               // it from the list.
               //
               properties.remove( key ) ;
               continue ;
            }

            if( key.equals("mode") ){
                Object obj = properties.get( key ) ;
                if( obj != null ){
                   String mode = obj.toString() ;
                   if( mode.equals( "auto" ) ){
                       _doNothing = false ;
                       _properties.put( key , "auto" ) ;
                   }else if( mode.equals( "manual" ) ){
                       _doNothing = true ;
                       _properties.put( key , "manual" ) ;
                   }
                   //
                   // if it is neither 'manual' nor 'auto' we
                   // just don't change the current value. When
                   // replying, client will notice.
                   //
                }
            }else{
                _properties.put( key , properties.get( key ) ) ;
            }
        }
        //
        // do as it would have been a query
        //
        properties.clear() ;
        properties.putAll( _properties ) ;
        //
     }
     public void poolFlushInfoUpdated( String poolName , HsmFlushControlCore.Pool pool ){

         if( _doNothing )return ;

         if( ! pool.isActive() ){
             say( "poolFlushInfoUpdated : Pool : "+poolName+" inactive");
             return ;
         }
         PoolCellInfo cellInfo = pool.getCellInfo() ;
         PoolCostInfo costInfo = cellInfo.getPoolCostInfo() ;
         PoolCostInfo.PoolSpaceInfo spaceInfo = costInfo.getSpaceInfo() ;
         PoolCostInfo.PoolQueueInfo queueInfo = costInfo.getStoreQueue() ;

         long total    = spaceInfo.getTotalSpace() ;
         long precious = spaceInfo.getPreciousSpace() ;

         say( "poolFlushInfoUpdated : Pool : "+poolName+";total="+total+";precious="+precious);
         //
         // loop over all storage classes of this pool and flush
         // those with have some files pending and which are not yet
         // in 'flush' status.
         //
         for( Iterator i = pool.getStorageClassNames().iterator() ; i.hasNext() ; ){

             String storageClass = i.next().toString() ;

             HsmFlushControlCore.FlushInfo info  = pool.getFlushInfoByStorageClass(storageClass) ;
             StorageClassFlushInfo         flush = info.getStorageClassFlushInfo();

             long size   = flush.getTotalPendingFileSize() ;

             say("poolFlushInfoUpdated :       class = "+storageClass+" size = "+size+" flushing = "+info.isFlushing() ) ;
             //
             // is precious size > 0 and are we not yet flushing ?
             //
             try{
                if( ( size > 0L ) && ! info.isFlushing() ){
                   say("poolFlushInfoUpdated :       flushing "+poolName+" "+storageClass  );
                   info.flush(0);
                }
             }catch(Exception ee ){
                esay("poolFlushInfoUpdated : Problem flushing "+poolName+" "+storageClass+" "+ee);
             }

         }
     }
     /*
      * convinient routines
      */
     public void say( String message ){ _core.say(message) ; }
     public void esay( String message ){ _core.esay(message) ; }
}
