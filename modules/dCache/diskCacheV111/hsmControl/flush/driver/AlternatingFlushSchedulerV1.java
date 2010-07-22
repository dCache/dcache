//   $Id: AlternatingFlushSchedulerV1.java,v 1.2 2006-07-31 16:35:52 patrick Exp $
package diskCacheV111.hsmControl.flush.driver ;
import  diskCacheV111.hsmControl.flush.* ;
import  diskCacheV111.pools.* ;
import  dmg.cells.nucleus.CellAdapter ;
import  dmg.util.Args ;
import  dmg.util.CommandInterpreter ;
import  java.util.*;
import  java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Patrick Fuhrmann patrick.fuhrmann@desy.de
 * @version 0.0, Dec 03, 2005
 *
 */
 public class AlternatingFlushSchedulerV1 implements HsmFlushSchedulable {

     private final static Logger _log =
         LoggerFactory.getLogger(AlternatingFlushSchedulerV1.class);

     private HsmFlushControlCore _core            = null;
     private CommandInterpreter  _interpreter     = null ;
     private AltParameter        _parameter       = new AltParameter() ;
     private Map                 _hostMap         = new HashMap() ;
     private boolean             _suspendFlushing = false ;
     /**
       * Our Pool class. Contains things we need to remember.
       * Is stored with setDriverHandle to avoid our own
       * bookkeeping.
       */

     private static final int STATE_IDLE                    = 0 ;
     private static final int STATE_WAITING_FOR_MODE_CHANGE = 1 ;
     private static final int STATE_WAITING_FOR_IO_DONE     = 2 ;
     private static final int STATE_WAITING_FOR_FLUSH_READY = 4 ;
     /**
       *
       *-----------------------------------------------------------------------------------
       *
       *            OUR INTERNAL POOL REPRESENTATION
       *
       */
     private class Pool implements HsmFlushControlCore.DriverHandle {

        private String  _name          = null ;
        private int     _flushCounter  = 0 ;
        private long    _totalSpace    = 0 ;
        private long    _preciousSpace = 0L ;
        private PoolSet _hostPoolMap   = null ;
        private boolean _expectedReadOnly  = false ;
        private int     _preciousFileCount = 0 ;
        private String  _hostTag           = null ;
        private boolean _previousWasRdOnly = false ;
        private long    _oldestTimestamp   = 0L ;

        private HsmFlushControlCore.Pool _pool = null ;

        private Pool( String name , HsmFlushControlCore.Pool pool ){

            _name = name ;

            update( pool ) ;

        //    _pool.queryMode() ;

            if( _pool.isActive() )linkUs() ;
        }
        private PoolSet getParentPoolSet(){
           return _hostPoolMap ;
        }
        private void refreshLocalVariables(){

            PoolCellInfo cellInfo = _pool.getCellInfo() ;
            if( cellInfo == null )return ;
            PoolCostInfo costInfo = cellInfo.getPoolCostInfo() ;
            PoolCostInfo.PoolSpaceInfo spaceInfo = costInfo.getSpaceInfo() ;

            _totalSpace        = spaceInfo.getTotalSpace() ;
            _preciousSpace     = spaceInfo.getPreciousSpace() ;
            _preciousFileCount = countTotalPending() ;
            _oldestTimestamp   = getOldestFileTimestamp() ;

        }
        private void update(HsmFlushControlCore.Pool pool){

            _pool = pool ;
            if( ! _pool.isActive() )return ;
            //
            refreshLocalVariables() ;
            //
            if( _hostPoolMap == null )linkUs() ;

        }
        private void linkUs(){

            PoolCellInfo cellInfo = _pool.getCellInfo() ;

            Map    tagMap  = cellInfo.getTagMap() ;
            _hostTag = (String)tagMap.get("hostname");

            _hostTag = _hostTag == null ? ( "x-"+_name ) : _hostTag ;

            _hostPoolMap = (PoolSet)_hostMap.get(_hostTag) ;
            if( _hostPoolMap == null )_hostPoolMap = new PoolSet( _hostTag ) ;
            _hostMap.put( _hostTag , _hostPoolMap ) ;
            _hostPoolMap.put( _name , this ) ;
        }
        private void setReadOnly( boolean readOnly ){
             _previousWasRdOnly = _pool.isReadOnly() ;
            _expectedReadOnly   = readOnly ;
            _pool.setReadOnly(readOnly);
        }
        private boolean isReady(){
           return ( _pool != null ) && _pool.isActive()  && _pool.isPoolIoModeKnown() ;
        }
        private void flush(){
           _flushCounter += flushPool( _pool );
        }
        private boolean isFlushing(){
//           return countStorageClassesFlushing(pool) > 0 ;
             return _flushCounter > 0 ;
        }
        private int countTotalPending(){
            return countTotalPendingPool( _pool ) ;

        }
        private long getOldestFileTimestamp(){
            return getOldestFileTimestampPool( _pool ) ;

        }
        public String toString(){
           if( isReady() ){
              return _name + " precious file count : "+_preciousFileCount+" core : "+ _pool ;
           }else{
              return _name+" Not Ready" ;
           }
        }
        ///////////////////////////////////////////////////////////////////////
        //
        //     state engine for a pool.
        //
        private void timer() {
        }
        public void poolIoModeUpdated( boolean newIsReadOnly ){
        }
        public void flushingDone( HsmFlushControlCore.FlushInfo flushInfo  ){

           _flushCounter -- ;

        }
        public void poolFlushInfoUpdated( HsmFlushControlCore.Pool pool ){

           if( ! _pool.isActive() )return ;
           //
           refreshLocalVariables() ;

        }
     }
     /**
       *
       *-----------------------------------------------------------------------------------
       *
       *            OUR INTERNAL HOST REPRESENTATION
       *
       */
     private static final int PS_IDLE  =  0 ;
     private static final int PS_WAITING_FOR_STATE_CHANGE  =  1 ;
     private static final int PS_WAITING_FOR_FLUSH_DONE    =  2 ;
     private class PoolSet {

         private Map     _poolMap          = new HashMap() ;
         private String  _name             = null ;
         private int     _progressState    = PS_IDLE  ;
         private long    _progressStarted  = 0L ;
         private boolean _expectedReadOnly = false ;

         private PoolSet( String name ){
           _name = name ;
         }
         public void put( String name , Pool pool ){
            _poolMap.put( name , pool ) ;
         }
         public Pool get( String name ){
            return (Pool)_poolMap.get(name);
         }
         public Set keySet(){ return _poolMap.keySet() ; }
         public Collection values(){ return _poolMap.values() ; }
         public Set entrySet(){ return _poolMap.entrySet() ; }

         public void poolIoModeUpdated( Pool ip , boolean newIsReadOnly ){

            if(_parameter._p_poolset)_log.info("PROGRESS-H("+_name+"/"+ip._name+") new pool i/o mode "+newIsReadOnly);
            //
            // new pool mode arrived, in wrong state
            //
            if( _progressState != PS_WAITING_FOR_STATE_CHANGE ){
              _log.warn("PROGRESS-H("+_name+"/"+ip._name+
                   ") Warning : new pool i/o mode arrived unexpectedly in state "+_progressState);
              return ;
            }
            //
            // wrong pool mode arrived
            //
            if( ip._expectedReadOnly != newIsReadOnly ){
              _log.warn("PROGRESS-H("+_name+"/"+ip._name+") Warning : got I/O mode rdonly="+newIsReadOnly+" expected rdOnly="+_expectedReadOnly);
              return ;
            }
            //
            // Do have all pools if this poolSet the correct mode ?
            //
            int total  = 0 ;
            int modeOk = 0 ;
            for( Iterator nn = _poolMap.values().iterator() ; nn.hasNext() ; ){
               Pool ipool = (Pool)nn.next() ;
               if( ! ipool._pool.isActive() )continue ;
               total++ ;
               if( ! ( ipool._pool.isReadOnly() ^ ipool._expectedReadOnly ) )modeOk++ ;
            }
            if(_parameter._p_poolset)_log.info("PROGRESS-H("+_name+"/"+ip._name+") new pool i/o mode total="+total+";rdOnly="+modeOk);
            //
            // not yet
            //
            if( modeOk < total )return ;
            //
            //
            if( _expectedReadOnly ){
               //
               // all pools are readyOnly; flush them all.
               //
               if(_parameter._p_poolset)_log.info("PROGRESS-H("+_name+"/"+ip._name+") new pool i/o mode done; Flushing all");
               int flushed = 0 ;
               for( Iterator nn = _poolMap.values().iterator() ; nn.hasNext() ; ){
                  Pool ipool = (Pool)nn.next() ;
                  ipool._flushCounter = 0 ;
                  ipool.flush() ;
                  if( ipool._flushCounter > 0 )flushed++ ;
               }
               if( flushed > 0 ){
                  if(_parameter._p_poolset)_log.info("PROGRESS-H("+_name+"/"+ip._name+") new pool i/o mode done; flushed "+flushed+" pools");
                  newState( PS_WAITING_FOR_FLUSH_DONE ) ;
               }else{
                  if(_parameter._p_poolset)_log.info("PROGRESS-H("+_name+"/"+ip._name+") new pool i/o mode done; nothing to flush; switching back to read/write");
                  switchToNewPoolMode(false);
               }
            }else{
               if(_parameter._p_poolset)_log.info("PROGRESS-H("+_name+"/"+ip._name+") new pool i/o mode done; Switching back to IDLE");

               newState( PS_IDLE ) ;
            }
         }
         public boolean inProgress(){ return _progressState != 0 ; }
         public void flushingDone( Pool ip  , String storageClassName , HsmFlushControlCore.FlushInfo flushInfo  ){
            if(_parameter._p_poolset)_log.info("PROGRESS-H("+_name+"/"+ip._name+") flushingDone");
            //
            // check if all are done
            //
            int total = 0 ;
            int flushDone = 0 ;
            for( Iterator nn = _poolMap.values().iterator() ; nn.hasNext() ; ){
               Pool ipool = (Pool)nn.next() ;
               if( ! ipool._pool.isActive() )continue ;
               total++ ;
               if( ipool._flushCounter == 0 )flushDone++ ;
            }
            if(_parameter._p_poolset)_log.info("PROGRESS-H("+_name+"/"+ip._name+") flushing done : total="+total+";flushDone="+flushDone);
            if( flushDone < total )return ;
            switchToNewPoolMode(false);
         }
         private void flushAll(){
            if(_parameter._p_poolset)_log.info("PROGRESS-H("+_name+") flush all; switching to readOnly");
            switchToNewPoolMode(true);
         }
         private void newState( int state ){
            _progressState    = state ;
            _progressStarted  = System.currentTimeMillis() ;
         }
         private void switchToNewPoolMode( boolean readOnly ){
            newState( PS_WAITING_FOR_STATE_CHANGE ) ;
            _expectedReadOnly = readOnly ;
            for( Iterator nn = _poolMap.values().iterator() ; nn.hasNext() ; ){

               Pool ip = (Pool)nn.next() ;

               if( readOnly ){
                  //
                  // store current mode; if this was already rdOnly we shouldn't
                  // reset it after we are done.
                  //
                  //
                  ip.setReadOnly(readOnly);
                  //
               }else{ // read write or whatever it was before.
                  //
                  ip.setReadOnly( ip._previousWasRdOnly );
               }
            }

         }
     }
  /*
   *
   *-----------------------------------------------------------------------------------
   */

     private Pool getInternalPool( HsmFlushControlCore.Pool pool ){

         Pool ip = (Pool)pool.getDriverHandle() ;
         if( ip == null ){
            _log.warn("getInternalPool : Yet unconfigured pool arrived "+pool.getName()+"; configuring");
            pool.setDriverHandle( ip = new Pool( pool.getName() , pool ) ) ;
         }
         return ip ;

     }


     public AlternatingFlushSchedulerV1( CellAdapter cell , HsmFlushControlCore core ){
         _log.info("AlternateFlush started");
         _core        = core ;
         _interpreter = new CommandInterpreter( this ) ;
     }
     //--------------------------------------------------------------------------------------
     //
     //   call backs from the flush manager.
     //
     public void init(){
         if(_parameter._p_events)_log.info("EVENT : Initiating ...");

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
         for( Iterator i = _core.getConfiguredPools().iterator() ; i.hasNext() ; ){
             _log.info("    configured pool : "+(i.next()).toString() ) ;
         }
         //
         //  Walk through the already known pools, create our internal presentation
         //  and send the 'queryPoolIoMode'.
         //
         for( Iterator i = _core.getConfiguredPools().iterator() ; i.hasNext() ; ){

             HsmFlushControlCore.Pool pool = (HsmFlushControlCore.Pool)i.next();
             Pool ip = getInternalPool( pool ) ;
             _log.info("init : "+pool.getName() +" "+ip) ;
         }
         String tmp = args.getOpt("driver-config-file") ;
         if( tmp != null )_parameter = new AltParameter( new File(tmp) ) ;

     }
     //--------------------------------------------------------------------------------------
     //
     //    properties got updated.
     //
     public void propertiesUpdated( Map properties ){
         if(_parameter._p_events)_log.info("EVENT : propertiesUpdated : "+properties);
        _parameter.propertiesUpdated( properties ) ;
     }
     //--------------------------------------------------------------------------------------
     //
     //    pool I/O mode got updated.
     //
     public void poolIoModeUpdated( String poolName ,  HsmFlushControlCore.Pool pool ){

         if(_parameter._p_events)_log.info("EVENT : poolIoModeUpdated : "+pool);

         if( ! pool.isActive() ){
            _log.warn("poolIoModeUpdated : Pool "+poolName+" not yet active (ignoring)");
            return ;
         }

         Pool ip = getInternalPool( pool ) ;

         ip.poolIoModeUpdated( pool.isReadOnly() ) ;

         PoolSet poolSet = ip.getParentPoolSet() ;

         poolSet.poolIoModeUpdated( ip ,  pool.isReadOnly() ) ;

     }
     public void flushingDone( String poolName , String storageClassName , HsmFlushControlCore.FlushInfo flushInfo  ){

         if(_parameter._p_events)_log.info("EVENT : flushingDone : pool ="+poolName+";class="+storageClassName /* + "flushInfo="+flushInfo */ );

         HsmFlushControlCore.Pool pool = _core.getPoolByName( poolName ) ;
         if( pool == null ){
            _log.warn("flushingDone : for a non configured pool : "+poolName);
            return ;
         }
         if( ! pool.isActive() ){
            _log.warn("flushingDone : Pool "+poolName+" not yet active (ignoring)");
            return ;
         }

         Pool ip = getInternalPool( pool ) ;

         ip.flushingDone( flushInfo ) ;

         if( ip._flushCounter == 0 ){
            PoolSet poolSet = ip.getParentPoolSet() ;

            poolSet.flushingDone( ip , storageClassName , flushInfo ) ;
         }
     }
     public void poolFlushInfoUpdated( String poolName , HsmFlushControlCore.Pool pool ){

         if(_parameter._p_events)_log.info("EVENT : poolFlushInfoUpdated : "+pool.getName());
         //
         if( ! pool.isActive() ){
            _log.warn("poolFlushInfoUpdated : Pool "+poolName+" not yet active (ignoring)");
            return ;
         }
         Pool ip = getInternalPool( pool ) ;
         ip.poolFlushInfoUpdated(pool);


     }
     public void reset(){
         if(_parameter._p_events)_log.info("EVENT : reset");
     }
     public void timer(){
         if(_parameter._p_events)_log.info("EVENT : timer");
           //
           // check if the property file has been changed.
           //
           _parameter.updateIfNeeded() ;
           //
           // run the pool timer
           //
          for( Iterator hosts = _hostMap.entrySet().iterator() ; hosts.hasNext() ; ){
             Map.Entry hostEntry = (Map.Entry)hosts.next() ;
             PoolSet   hostMap   = (PoolSet)hostEntry.getValue() ;
             for( Iterator pools = hostMap.entrySet().iterator() ; pools.hasNext() ; ){
                Map.Entry e = (Map.Entry)pools.next() ;
                ((Pool)e.getValue()).timer();
             }
          }
          //
          // do the rule processing
          //
          if( ! _suspendFlushing )processPoolsetRules();

     }
     /**
       *  Executes the external command with CommandInterpreter (using our ac_xx) commands.
       */
     public void command( Args args  ){
         if(_parameter._p_events)_log.info("EVENT : command : "+args);
         try{
             Object reply = _interpreter.command( args ) ;
             if( reply == null )
               throw new
               Exception("Null pointer from command call");
             _log.info("Command returns : "+reply.toString() );
         }catch(Exception ee ){
             _log.warn("Command returns an exception ("+ee.getClass().getName()+") : " + ee.toString());
         }
     }
     public void prepareUnload(){
         if(_parameter._p_events)_log.info("EVENT : Preparing unload (ignoring)");
     }
     public void configuredPoolAdded( String poolName ){

         if(_parameter._p_events)_log.info("EVENT : Configured pool added : "+poolName);

         HsmFlushControlCore.Pool pool = _core.getPoolByName( poolName ) ;
         if( pool == null ){
            _log.warn("Pool not found in _core database : "+poolName);
            return ;
         }
         if( ! pool.isActive() ){
            _log.warn("Pool "+poolName+" not yet active (ignoring)");
            return ;
         }
         Pool ip = getInternalPool( pool ) ;

     }
     public void poolSetupUpdated(){
         if(_parameter._p_events)_log.info("EVENT : Pool Setup updated (ignoring)");
     }
     public void configuredPoolRemoved( String poolName ){
         if(_parameter._p_events)_log.info("EVENT : Configured pool removed : "+poolName+ "  (ignoring)");
     }
     //-----------------------------------------------------------------------------------------
     //
     // RULE ENGINE
     //
     private void processPoolsetRules(){

         if(_parameter._p_rules)_log.info("RULES : Processing PoolSet Rules...");
         int totalAvailable = 0 ;
         int totalReadOnly  = 0 ;
         int totalFlushing  = 0 ;
         int inProgress     = 0 ;

         //
         // get some statistics on the known pools.
         // (available,flushing, rdOnly). Add them to the
         // candidate list if 'isCandidate' returns true.
         //
         ArrayList candidates = new ArrayList() ;
         HashSet rdOnlyHash   = new HashSet() ;

         for( Iterator i = _core.getConfiguredPools().iterator() ; i.hasNext() ; ){
             HsmFlushControlCore.Pool pool = (HsmFlushControlCore.Pool)i.next();
             Pool ip = (Pool)pool.getDriverHandle() ;
             if( ( ip == null ) || ( ! ip.isReady() ) )continue ;

             totalAvailable ++ ;

             if( ip.isFlushing() )totalFlushing++ ;

             if( pool.isReadOnly() ){ totalReadOnly++ ; rdOnlyHash.add(pool.getName()) ; }

             if( ip.getParentPoolSet().inProgress() ){ inProgress ++ ; continue ; }

             if( isCandidate( ip ) )candidates.add( ip ) ;

         }
         if(_parameter._p_rules)_log.info("RULES : statistics : "+
             "total="+totalAvailable+
             ";readOnly="+totalReadOnly+
             ";flushing="+totalFlushing+
             ";progress="+inProgress+
             ";candidates="+candidates.size() ) ;

         if( candidates.size() == 0 ){
            if(_parameter._p_rules)_log.info("RULES : no candidates found");
            return ;
         }
         //if( totalReadOnly > (int)( (double)totalAvailable * _parameter._percentageToFlush ) ){
         //   if(_parameter._p_rules)_log.info("RULES : too many pools ReadOnly ("+totalReadOnly+" out of "+totalAvailable+")") ;
         //   return ;
         //}
         // DEBUG
         if(_parameter._p_rules){
           for( Iterator x = candidates.iterator() ; x.hasNext() ; ){
              Pool pp = (Pool)x.next() ;
              _log.info("RULES : weight of "+pp._name+" "+getPoolMetric(pp));
           }
         }
         Collections.sort( candidates , _poolComparator );
         //
         Pool ip = (Pool)candidates.get(0);
         if(_parameter._p_rules)_log.info("RULES : weight of top "+ip._name+" "+getPoolMetric(ip));
         //
         // step through all candidates and check if they would overbook the max number
         // of rdOnly pools.
         //
         PoolSet best = null ;
         for( Iterator pools = candidates.iterator() ; pools.hasNext() ; ){

             Pool    pp = (Pool)pools.next() ;
             PoolSet ps = pp.getParentPoolSet() ;

             if(_parameter._p_rules)_log.info("RULES : checking "+pp._name+"/"+ps._name+" "+getPoolMetric(pp));

             HashSet potentialRdOnlyPools = new HashSet( rdOnlyHash ) ;
             for( Iterator ppp = ps.keySet().iterator() ;  ppp.hasNext() ; ){
                 potentialRdOnlyPools.add( ppp.next().toString() ) ;
             }
             if(_parameter._p_rules)_log.info("RULES : potential rdOnlyPools : "+potentialRdOnlyPools);
             int total = potentialRdOnlyPools.size() ;

             if( total  > (int)( (double)totalAvailable * _parameter._percentageToFlush ) ){
                if(_parameter._p_rules)_log.info("RULES : "+ps._name+" would be too many pools ReadOnly ("+total+" out of "+totalAvailable+")") ;
                continue ;
             }

             best = ps ;
             break ;
         }
         if( best == null){
            if(_parameter._p_rules)_log.info("RULES : no candidates found after all");
            return ;
         }

         if(_parameter._p_rules)_log.info("RULES : flushing poolset : "+best._name+" with pools "+best.keySet() ) ;

         best.flushAll() ;

     }
     private static final long MEGA_BYTES = 1024L * 1024L ;
     private PoolComparator _poolComparator = new PoolComparator() ;
     public class PoolComparator implements Comparator<Pool> {
        public int compare( Pool a , Pool b ){
           double da = getPoolMetric( a ) ;
           double db = getPoolMetric( b ) ;
           return db == da ? 0 : da > db ? -1 : 1 ;
        }
      public int hashCode() {
          return 1;
      }
        public boolean equals( Object comp ){ return true ; }
     }
     private double getPoolMetric( Pool ip ){
       return
          (
         (double)ip._preciousSpace     / (double) ( _parameter._maxPreciousStored * MEGA_BYTES ) +
         (double)ip._preciousFileCount / (double) ( _parameter._maxFilesStored                 ) +
        ((double)( System.currentTimeMillis() - ip._oldestTimestamp ) /  (double)(_parameter._maxTimeStored   * 60000L) )          )
             /  3.0 ;
     }
     private boolean isCandidate( Pool ip ){
        ip.refreshLocalVariables();
        return
             ( ip._preciousSpace > ( _parameter._maxPreciousStored * MEGA_BYTES ) ) ||
             ( ip._preciousFileCount > _parameter._maxFilesStored               )   ||
             ( ip._oldestTimestamp + ( _parameter._maxTimeStored   * 60000L ) < System.currentTimeMillis() )
        ;

     }
     //-------------------------------------------------------------------------------------------
     //
     //              C O M M A N D S
     //
     public String hh_dummy = "# dummy call" ;
     public String ac_dummy_$_1_99( Args args ){
        return args.toString();
     }
     public String hh_suspend = "# suspend next flush sequence, not preemptive";
     public String ac_suspend( Args args ){
        _suspendFlushing = true ;
        return "" ;
     }
     public String hh_resume = "# resume flush sequence";
     public String ac_resume( Args args ){
        _suspendFlushing = false ;
        return "" ;
     }
     public String hh_list_pools = "[-p [-l]] [-c]" ;
     public String ac_list_pools( Args args ){
       boolean parentView     = args.getOpt("p") != null ;
       boolean configuredView = args.getOpt("c") != null ;
       boolean extended       = args.getOpt("e") != null ;
       if( parentView ){
          for( Iterator hosts = _hostMap.entrySet().iterator() ; hosts.hasNext() ; ){
             Map.Entry hostEntry = (Map.Entry)hosts.next() ;
             String hostName = (String)hostEntry.getKey() ;
             Map    hostMap  = (Map)hostEntry.getValue() ;
             _log.info(" >>"+hostName+"<<");
             for( Iterator pools = hostMap.entrySet().iterator() ; pools.hasNext() ; ){
                Map.Entry e = (Map.Entry)pools.next() ;
                String name = (String)e.getKey() ;
                _log.info("     "+name+ ( extended ? e.getValue().toString() : "" ) );
             }
          }
       }else if( configuredView ){
         for( Iterator i = _core.getConfiguredPools().iterator() ; i.hasNext() ; ){
             HsmFlushControlCore.Pool pool = (HsmFlushControlCore.Pool)i.next();
             _log.info(""+pool);

         }

       }
       return "";
     }
     public String ac_flush_poolset = "<poolSetName>" ;
     public String ac_flush_poolset_$_1(Args args ){
        String poolSetName = args.argv(0) ;
        PoolSet poolSet = (PoolSet)_hostMap.get(poolSetName);
        if( poolSet == null )
          throw new
          IllegalArgumentException( "PoolSet not found : "+poolSetName);

        if( poolSet.inProgress() )
          throw new
          IllegalArgumentException( "PoolSet in progress : "+poolSetName);


        poolSet.flushAll();
        return "" ;
     }
     //-------------------------------------------------------------------------------------------
     //
     //               C O N V E N I E N E N T     F U N C T I O N S
     //
     /**
       *
       * Convenient method to flush all pending storage classes of the specified pool.
       *
       * @param ip Internal pool representation.
       */
     private int flushPool( HsmFlushControlCore.Pool pool ){
         int flushing = 0 ;
         for( Iterator i = pool.getFlushInfos().iterator() ; i.hasNext() ; ){

             HsmFlushControlCore.FlushInfo info = (HsmFlushControlCore.FlushInfo)i.next() ;
             StorageClassFlushInfo         flush = info.getStorageClassFlushInfo();

             long size   = flush.getTotalPendingFileSize() ;

             _log.info("flushPool : class = "+info.getName()+" size = "+size+" flushing = "+info.isFlushing() ) ;
             //
             // is precious size > 0 and are we not yet flushing ?
             //
             try{
                if( ( size > 0L ) && ! info.isFlushing() ){
                   _log.info("flushPool : !!! flushing "+_parameter._flushAtOnce+" of "+pool.getName()+" "+info.getName()  );
                   info.flush(_parameter._flushAtOnce);
                   flushing++ ;
                }
             }catch(Exception ee ){
                _log.warn("flushPool : Problem flushing "+pool.getName()+" "+info.getName()+" "+ee);
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
         for( Iterator i = pool.getFlushInfos().iterator() ; i.hasNext() ; ){

             HsmFlushControlCore.FlushInfo info = (HsmFlushControlCore.FlushInfo)i.next() ;
             StorageClassFlushInfo         flush = info.getStorageClassFlushInfo();

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
         for( Iterator i = pool.getFlushInfos().iterator() ; i.hasNext() ; ){

             HsmFlushControlCore.FlushInfo info = (HsmFlushControlCore.FlushInfo)i.next() ;
             StorageClassFlushInfo         flush = info.getStorageClassFlushInfo();

             total += flush.getRequestCount();
         }
         return total ;
     }
     /**
       * Gets the oldest file of this pool.
       *
       * @return Timestamp of the oldest file of this pool.
       */
     private long getOldestFileTimestampPool( HsmFlushControlCore.Pool pool ){
         long oldest = System.currentTimeMillis() ;
         for( Iterator i = pool.getFlushInfos().iterator() ; i.hasNext() ; ){

             HsmFlushControlCore.FlushInfo info = (HsmFlushControlCore.FlushInfo)i.next() ;
             StorageClassFlushInfo         flush = info.getStorageClassFlushInfo();

             oldest = Math.min( oldest , flush.getOldestFileTimestamp() ) ;
         }
         return oldest ;
     }
     /**
       * Determines how many storage classes are in the process of being flushed on this pool.
       *
       * @return Number of storage class in progress of been flushed on this pool.
       */
     private int countStorageClassesFlushing( HsmFlushControlCore.Pool pool ){

         int flushing = 0 ;
         for( Iterator i = pool.getFlushInfos().iterator() ; i.hasNext() ; ){

             if( ((HsmFlushControlCore.FlushInfo)i.next()).isFlushing() )flushing++ ;

         }
         return flushing ;
     }

     ////////////////////////////////////////////////////////////////////////////
     //
     //             Parameter handling
     //
     private static String PARAMETER_MAX_FILE         = "max.files" ;
     private static String PARAMETER_MAX_MINUTES      = "max.minutes" ;
     private static String PARAMETER_MAX_MEGABYTES    = "max.megabytes" ;
     private static String PARAMETER_TIMER            = "timer" ;
     private static String PARAMETER_MODE             = "mode" ;
     private static String PARAMETER_FLUSH_PERCENTAGE = "max.rdonly.fraction" ;
     private static String PARAMETER_FLUSH_ATONCE     = "flush.atonce" ;
     private static String PARAMETER_RULE_TYPE        = "rules.type" ;

     private static String PARAMETER_PRINT_EVENTS           = "print.events" ;
     private static String PARAMETER_PRINT_RULES            = "print.rules" ;
     private static String PARAMETER_PRINT_POOL_PROGRESS    = "print.pool.progress" ;
     private static String PARAMETER_PRINT_POOLSET_PROGRESS = "print.poolset.progress" ;
     //
     //
     private class AltParameter extends Parameter {

        private long   _maxFilesStored    = 500L ;
        private long   _maxPreciousStored = ( 500L * 1024L ) ;
        private long   _maxTimeStored     = ( 2L * 60L ) ;
        private long   _timer             = ( 60L ) ;
        private double _percentageToFlush = 0.5 ;
        private int    _countToFlush      = 5 ;
        private int    _flushAtOnce       = 0 ;
        private boolean _p_events         = false ;
        private boolean _p_rules          = false ;
        private boolean _p_poolset        = false ;
//        private boolean _p_pool           = false ;
//        private String _mode              = "auto" ;
//        private String _ruleType          = "poolset" ;

        public AltParameter( ){ }

        public AltParameter( File configFile ){ super( configFile ) ; }

        public void propertiesUpdated( Map properties ){

           Set keys = new HashSet( properties.keySet() ) ;
           //
           // for all properties we support, try to change the values
           // accordingly.
           //
           for( Iterator i = keys.iterator() ; i.hasNext() ; ){
              String key = (String)i.next() ;
              try{
                 if( key.equals( PARAMETER_MAX_FILE ) ){
                    _maxFilesStored = handleLong( properties , PARAMETER_MAX_FILE , 0 , 999999999 ) ;
                 }else if( key.equals( PARAMETER_MAX_MEGABYTES ) ){
                    _maxPreciousStored = handleLong( properties , PARAMETER_MAX_MEGABYTES , 0 , 999999999 ) ;
                 }else if( key.equals( PARAMETER_MAX_MINUTES ) ){
                    _maxTimeStored = handleLong( properties , PARAMETER_MAX_MINUTES , 0 , 999999999 ) ;
                 }else if( key.equals( PARAMETER_TIMER ) ){
                    _timer = handleLong( properties , PARAMETER_TIMER , 0 , 999999999 ) ;
                 }else if( key.equals( PARAMETER_FLUSH_ATONCE ) ){
                    _flushAtOnce = handleInt( properties , PARAMETER_FLUSH_ATONCE , 0 , 999999999 ) ;
                 }else if( key.equals( PARAMETER_PRINT_EVENTS ) ){
                     _p_events  = handleBoolean( properties , PARAMETER_PRINT_EVENTS  ) ;
                 }else if( key.equals( PARAMETER_PRINT_RULES ) ){
                     _p_rules   = handleBoolean( properties , PARAMETER_PRINT_RULES  ) ;
                 }else if( key.equals( PARAMETER_PRINT_POOLSET_PROGRESS ) ){
                     _p_poolset = handleBoolean( properties , PARAMETER_PRINT_POOLSET_PROGRESS  ) ;
                 }else if( key.equals( PARAMETER_FLUSH_PERCENTAGE ) ){
                     _percentageToFlush = handleDouble( properties , PARAMETER_FLUSH_PERCENTAGE , 0.0 , 1.0 ) ;
//                 }else if( key.equals( PARAMETER_PRINT_POOL_PROGRESS ) ){
//                     _p_pool    = handleBoolean( properties , PARAMETER_PRINT_POOL_PROGRESS  ) ;
//                 }else if( key.equals( PARAMETER_MODE ) ){
//                     _mode = handleString( properties , PARAMETER_MODE , new String [] { "auto" , "manual" } ) ;
//                 }else if( key.equals( PARAMETER_RULE_TYPE ) ){
//                     _mode = handleString( properties , PARAMETER_RULE_TYPE , new String [] { "pool" , "poolset" } ) ;
                 }else{
                    //
                    // remove the key to inform the requestor that we don't
                    // support this property.
                    //
                    properties.remove( key ) ;
                 }
              }catch(Exception ee ){
                 _log.warn("Exception while seting "+key+" "+ee);
              }
           }
           //
           // do as it would have been a query
           //
           properties.put( PARAMETER_MAX_FILE         , ""+_maxFilesStored ) ;
           properties.put( PARAMETER_MAX_MEGABYTES    , ""+_maxPreciousStored ) ;
           properties.put( PARAMETER_MAX_MINUTES      , ""+_maxTimeStored ) ;
           properties.put( PARAMETER_TIMER            , ""+_timer ) ;
           properties.put( PARAMETER_FLUSH_PERCENTAGE , ""+_percentageToFlush ) ;
           properties.put( PARAMETER_FLUSH_ATONCE     , ""+_flushAtOnce ) ;
           properties.put( PARAMETER_PRINT_EVENTS     , ""+ _p_events) ;
           properties.put( PARAMETER_PRINT_RULES      , ""+ _p_rules) ;
           properties.put( PARAMETER_PRINT_POOLSET_PROGRESS , ""+ _p_poolset) ;
//           properties.put( PARAMETER_MODE             , _mode ) ;
//           properties.put( PARAMETER_RULE_TYPE        , _ruleType ) ;
//           properties.put( PARAMETER_PRINT_POOL_PROGRESS    , ""+ _p_pool) ;

       }

     }
     private class Parameter {

        private long   _configLastModified = 0L ;
        private File   _configFile         = null ;

        private Parameter(){}
        private Parameter( File configFile ){
           _configFile = configFile ;
        }
        protected void updateIfNeeded(){
           if( _configFile == null )return ;
           try{
               Map map = updateConfigIfNewer( _configFile ) ;
               if( map == null )return ;
               propertiesUpdated( map ) ;
           }catch(Exception ee ){
              return ;
           }
        }
        private Map updateConfigIfNewer( File configFile ) throws IOException {

           if( ! configFile.exists() )return null ;
           long lastModified = configFile.lastModified() ;
           if( lastModified <= _configLastModified )return null ;
           _configLastModified = lastModified ;
           return readConfigFile( configFile ) ;
        }
        private Map readConfigFile( File configFile ) throws IOException {
            BufferedReader br = new BufferedReader( new FileReader( configFile ) ) ;
            HashMap map = new HashMap() ;
            try{
                String line = null ;
                while( ( line = br.readLine() ) != null ){
                   line = line.trim() ;
                   if( line.equals("") || line.startsWith("#") )continue ;
                   int pos = line.indexOf('=') ;
                   if( ( pos <= 0 ) || ( pos == ( line.length() - 1 ) ) )continue ;
                   String key = line.substring(0,pos).trim() ;
                   if( key.length()  == 0 )continue ;
                   String value = line.substring(pos+1).trim() ;
                   if( value.length() == 0 )continue ;
                   map.put( key , value ) ;
                }
            }catch(EOFException eof){
            }finally{
               try{ br.close() ; }catch(IOException ee ){}
            }
            String [] x  = null ;
            x = new String [] { "x" , "y" } ;
            return map ;
        }
        protected String handleString( Map properties , String key , String [] options ){
           Object obj = properties.get( key ) ;
           if( obj == null )throw new IllegalArgumentException("No Value for "+key) ;
           String x = obj.toString() ;
           if( options == null )return x ;
           for( int i = 0 ; i < options.length ; i++ ){
              if( options[i].equals(x) )return x ;
           }
           throw new
           IllegalArgumentException("Value for "+key+" is not legal" );

        }
        protected int handleInt( Map properties , String key , int minValue , int maxValue ){
           Object obj = properties.get( key ) ;
           if( obj == null )throw new IllegalArgumentException("No Value for "+key) ;
           int count = Integer.parseInt( obj.toString() ) ;
           if( ( count < minValue ) || ( count > maxValue ) )
             throw new
             IllegalArgumentException("Value for "+key+" not in range "+minValue+" < n < "+maxValue );
           return count ;
        }
        protected double handleDouble( Map properties , String key , double minValue , double maxValue ){
           Object obj = properties.get( key ) ;
           if( obj == null )throw new IllegalArgumentException("No Value for "+key) ;
           double count = Double.parseDouble( obj.toString() ) ;
           if( ( count < minValue ) || ( count > maxValue ) )
             throw new
             IllegalArgumentException("Value for "+key+" not in range "+minValue+" < n < "+maxValue );
           return count ;
        }
        protected long handleLong( Map properties , String key , long minValue , long maxValue ){
           Object obj = properties.get( key ) ;
           if( obj == null )throw new IllegalArgumentException("No Value for "+key) ;
           long count = Integer.parseInt( obj.toString() ) ;
           if( ( count < minValue ) || ( count > maxValue ) )
             throw new
             IllegalArgumentException("Value for "+key+" not in range "+minValue+" < n < "+maxValue );
           return count ;
        }
        protected boolean handleBoolean( Map properties , String key ){
           Object obj = properties.get( key ) ;
           if( obj == null )throw new IllegalArgumentException("No Value for "+key) ;
           if( obj.toString().equals("true") ){
             return true ;
           }else if( obj.toString().equals("false") ){
             return false ;
           }else{
             throw new
             IllegalArgumentException("Value for "+key+" must be boolean (true/false)" );
           }
        }
    }
}
