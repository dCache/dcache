// $Id: HsmFlushControlManager.java,v 1.6 2006-07-31 16:35:50 patrick Exp $

package diskCacheV111.hsmControl.flush ;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.StorageClassFlushInfo;
import diskCacheV111.vehicles.PoolFlushControlInfoMessage;
import diskCacheV111.vehicles.PoolFlushDoFlushMessage;
import diskCacheV111.vehicles.PoolFlushGainControlMessage;
import diskCacheV111.vehicles.PoolManagerPoolModeMessage;
import diskCacheV111.vehicles.PoolStatusChangedMessage;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.util.Args;

import static java.util.concurrent.TimeUnit.SECONDS;

public class HsmFlushControlManager  extends CellAdapter {

    private static final Logger _log =
        LoggerFactory.getLogger(HsmFlushControlManager.class);

    private CellNucleus _nucleus;
    private Args        _args;
    private int         _requests;
    private int         _failed;
    private File        _database;
    private String      _status     = "init" ;
    private boolean     _logEnabled = true ;
    private final PoolCollector   _poolCollector;
    private Set<String> _poolGroupList           = new HashSet<>() ;
    private long            _getPoolCollectionTicker = 2L * 60L * 1000L ;
    private long            _timerInterval           =      30L * 1000L ;
    private FlushController _flushController         = new FlushController() ;
    private EventDispatcher _eventDispatcher         = new EventDispatcher() ;
    private SimpleDateFormat formatter   = new SimpleDateFormat ("MM.dd hh:mm:ss");
    private QueueWatch      _queueWatch;

    private Map<String,Object>    _properties        = new HashMap<>() ;
    private final Object _propertyLock      = new Object() ;
    private long   _propertiesUpdated;
    private final CellStub _poolManager;

    /**
      *   Usage : ... [options] <pgroup0> [<pgroup1>[...]]
      *   Options :
      *              -scheduler=<driver>
      *              -poolCollectionUpdate=<timeInMinutes>
      *              -gainControlUpdate=<timeInMinutes>
      *              -timer=<timeInSeconds>
      *
      *                 Options are forwarded to the driver as well.
      */
    public HsmFlushControlManager( String name , String  args ) throws Exception {
       super( name , args , false ) ;
       _nucleus = getNucleus() ;
       _args    = getArgs() ;
       _poolManager = new CellStub(this, new CellPath("PoolManager"), 30, SECONDS);
       try{
          if( _args.argc() < 1 ) {
              throw new
                      IllegalArgumentException("Usage : ... <pgroup0> [<pgroup1>[...]]");
          }

          for( int i = 0 , n = _args.argc() ; i <  n ; i++ ){
              _poolGroupList.add(_args.argv(0));
              _args.shift();
           }
           String tmp = _args.getOpt("scheduler") ;
           if( ( tmp != null ) && ( ! tmp.equals("") ) ){
               _eventDispatcher.loadHandler( tmp , false  , _args ) ;
           }

           _queueWatch = new QueueWatch() ;

           tmp = _args.getOpt("poolCollectionUpdate") ;
           if( ( tmp != null ) && ( ! tmp.equals("") ) ){
               try{
                  _getPoolCollectionTicker = Long.parseLong(tmp) * 60L * 1000L ;
               }catch(Exception ee ){
                  _log.warn("Illegal value for poolCollectionUpdate : "+tmp+" (chosing "+_getPoolCollectionTicker+" millis)");
               }
           }
           tmp = _args.getOpt("gainControlUpdate") ;
           if( ( tmp != null ) && ( ! tmp.equals("") ) ){
              long gainControlTicker = 0L ;
               try{
                  _queueWatch.setGainControlTicker( gainControlTicker = Long.parseLong(tmp) * 60L * 1000L ) ;
               }catch(Exception ee ){
                  _log.warn("Illegal value for gainControlUpdate : "+tmp+" (chosing "+gainControlTicker+" millis)");
               }
           }
           tmp = _args.getOpt("timer") ;
           if( ( tmp != null ) && ( ! tmp.equals("") ) ){
               try{
                  _timerInterval = Long.parseLong(tmp) * 1000L ;
               }catch(Exception ee ){
                  _log.warn("Illegal value for timer : "+tmp+" (chosing "+_timerInterval+" millis)");
               }
           }

       }catch(Exception e){
          start() ;
          kill() ;
          throw e ;
       }
       useInterpreter( true );
       _nucleus.newThread( _queueWatch , "queueWatch").start() ;

       start();

       _poolCollector = new PoolCollector() ;

    }

    private class QueueWatch implements Runnable {
       private long _gainControlTicker   = 1L * 60L * 1000L ;
       private long next_getPoolCollection ;
       private long next_sendGainControl   ;
       private long next_timerEvent ;

       private synchronized void setGainControlTicker( long gainControl ){
          _gainControlTicker = gainControl ;
          triggerGainControl() ;
       }
       @Override
       public void run(){
          _log.info("QueueWatch started" ) ;

          try{
             _log.info("QueueWatch : waiting awhile before starting");
             Thread.currentThread().sleep(10000);
          }catch(InterruptedException ie ){
             _log.warn("QueueWatch: interrupted during initial wait. Stopping");
             return ;
          }
          _status = "Running Collector" ;
          _poolCollector.runCollector( _poolGroupList ) ;

          long now = System.currentTimeMillis() ;

          next_getPoolCollection = now + 0 ;
          next_sendGainControl   = now + _gainControlTicker ;
          next_timerEvent        = now + _timerInterval ;

          while( ! Thread.currentThread().interrupted() ){
             synchronized( this ){
                _status = "Sleeping" ;
                try{
                   wait(10000);
                }catch(InterruptedException ie ){
                   break ;
                }

                now = System.currentTimeMillis() ;

                if( now > next_getPoolCollection ){
                   _status = "Running Collector" ;
                   _poolCollector.runCollector( _poolGroupList ) ;
                   next_getPoolCollection = now + _getPoolCollectionTicker ;
                }
                if( now > next_sendGainControl ){
                   _status = "Sending Flush Controls" ;
                   _flushController.sendFlushControlMessages(  2L * _gainControlTicker ) ;
                   next_sendGainControl = now + _gainControlTicker ;
                }
                if( now > next_timerEvent ){
                   _eventDispatcher.timer();
                   next_timerEvent = now + _timerInterval ;
                }
             }
          }
          _log.info( "QueueWatch stopped" ) ;
       }
       public synchronized void triggerGainControl(){
          next_sendGainControl = System.currentTimeMillis() - 10 ;
          notifyAll();
       }
       public synchronized void triggerGetPoolCollection(){
          next_getPoolCollection = System.currentTimeMillis() - 10 ;
          notifyAll();
       }
    }
    private class FlushController {
       //
       // sends 'gain control' messages to all clients we want
       // to control.
       //
       private List<String> _poolList;
       private boolean _control  = true ;

       private synchronized void updatePoolList( Collection<String> c ){
           _poolList = new ArrayList<>(c);
       }
       private synchronized void setControl( boolean control ){
          _control = control ;
       }
       private void sendFlushControlMessages( long gainControlInterval ){
          List<String> poolList;
          synchronized( this ){ poolList = _poolList ; }
          if( poolList == null ) {
              return;
          }
           for (String poolName : poolList) {
               PoolFlushGainControlMessage msg =
                       new PoolFlushGainControlMessage(
                               poolName,
                               _control ? gainControlInterval : 0L);

               try {
                   if (_logEnabled) {
                       _log.info("sendFlushControlMessage : sending PoolFlushGainControlMessage to " + poolName);
                   }
                   sendMessage(new CellMessage(new CellPath(poolName), msg));
               } catch (NoRouteToCellException ee) {
                   _log.warn("sendFlushControlMessage : couldn't send _poolGroupHash to " + poolName);
               }
           }
       }
    }
    public class HFCFlushInfo implements HsmFlushControlCore.FlushInfo {
       private StorageClassFlushInfo  _flushInfo, _previousFlushInfo;
       private boolean _flushingRequested;
       private boolean _flushingPending;
       private int     _flushingError;
       private Object  _flushingErrorObj;
       private long    _flushingId;
       private HFCPool _pool;
       private String  _name;

       public HFCFlushInfo( HFCPool pool , StorageClassFlushInfo flush ){
          _flushInfo = flush ;
          _pool      = pool ;
          _name      = _flushInfo.getStorageClass()+"@"+_flushInfo.getHsm() ;
       }
       @Override
       public synchronized String getName(){ return _name ; }
       public HsmFlushControlCore.FlushInfoDetails getDetails(){
           HsmFlushControllerFlushInfoDetails details = new HsmFlushControllerFlushInfoDetails() ;
           details._name = _name ;
           details._isFlushing = isFlushing() ;
           details._flushInfo  = _flushInfo ;
           return details ;
       }
       private synchronized void updateFlushInfo( StorageClassFlushInfo flush ){
           _previousFlushInfo = _flushInfo ;
           _flushInfo = flush ;
       }
       @Override
       public synchronized boolean isFlushing(){ return _flushingRequested || _flushingPending ; }
       @Override
       public synchronized void flush( int count ) throws Exception {

           flushStorageClass( _pool._poolName , _name , count ) ;
           _flushingRequested = true ;
           _flushingPending   = false ;
           _flushingError     = 0 ;
       }
       public synchronized void setFlushingAck( long id  ){
           _flushingPending   = true ;
           _flushingId        = id ;
       }
       public synchronized void setFlushingFailed( int errorCode , Object errorObject  ){
           _flushingPending   = false ;
           _flushingRequested = false ;
           _flushingId        = 0L ;
           _flushingError     = errorCode ;
           _flushingErrorObj  = errorObject ;
       }
       public synchronized void setFlushingDone(){
           setFlushingFailed(0,null);
       }

       @Override
       public synchronized StorageClassFlushInfo getStorageClassFlushInfo(){ return _flushInfo ; }
    }
    private class HFCPool implements HsmFlushControlCore.Pool  {

        private String       _poolName;
        private HashMap<String, HFCFlushInfo> flushInfos    = new HashMap<>() ;
        private PoolCellInfo cellInfo;
        private int          mode;
        private boolean      isActive;
        private long         lastUpdated;
        private int          answerCount;
        private HsmFlushControlCore.DriverHandle _driverHandle;

        private HFCPool( String poolName ){ _poolName = poolName ; }
        @Override
        public void setDriverHandle( HsmFlushControlCore.DriverHandle handle ){
            _driverHandle = handle ;
        }
        @Override
        public String getName(){
           return _poolName ;
        }
        public HsmFlushControlCore.PoolDetails getDetails(){
            HsmFlushControllerPoolDetails details = new HsmFlushControllerPoolDetails() ;
            details._name       = _poolName ;
            details._isActive   = isActive ;
            details._isReadOnly = isReadOnly() ;
            details._cellInfo   = cellInfo ;
            details._flushInfos = new ArrayList<>() ;
            for (HFCFlushInfo info : flushInfos.values()) {
                details._flushInfos.add(info.getDetails());
            }
            return details ;
        }
        @Override
        public HsmFlushControlCore.DriverHandle getDriverHandle(){
           return _driverHandle ;
        }
        @Override
        public boolean isActive(){ return isActive ; }
        @Override
        public void setReadOnly( boolean rdOnly ){
           setPoolReadOnly( _poolName , rdOnly ) ;
        }
        @Override
        public void queryMode(){
           queryPoolMode(_poolName ) ;
        }
        @Override
        public boolean isReadOnly(){
            return ( mode & PoolManagerPoolModeMessage.WRITE ) == 0  ;
        }
        @Override
        public boolean isPoolIoModeKnown(){
            return mode != 0 ;
        }
        @Override
        public Set<String> getStorageClassNames(){
           return new TreeSet<>( flushInfos.keySet() ) ;
        }
        @Override
        public List<HsmFlushControlCore.FlushInfoDetails> getFlushInfos(){
           return new ArrayList<HsmFlushControlCore.FlushInfoDetails>( flushInfos.values() ) ;
        }
        @Override
        public PoolCellInfo getCellInfo(){ return cellInfo ; }

        @Override
        public HsmFlushControlCore.FlushInfo getFlushInfoByStorageClass( String storageClass ){
           return flushInfos.get( storageClass );
        }
        public String getPoolModeString(){
           if( mode == 0 ) {
               return "UU";
           }
           String res;
           res =  ( mode & PoolManagerPoolModeMessage.READ  ) == 0  ? "-" : "R" ;
           res += ( mode & PoolManagerPoolModeMessage.WRITE ) == 0  ? "-" : "W" ;
           return res ;
        }
        public String toString(){
           StringBuilder sb = new StringBuilder() ;
           sb.append(_poolName).append(";IOMode=").append(getPoolModeString()) ;
           sb.append(";A=").append(isActive).
              append(";LU=").
              append( lastUpdated == 0 ?
                      "Never" :
                      ( "" + (  (System.currentTimeMillis()-lastUpdated ) / 1000L ) )  ) ;
           return sb.toString();
        }
    }
    private void flushStorageClass( String poolName , String storageClass , int count ) throws Exception {

           PoolFlushDoFlushMessage msg =
               new PoolFlushDoFlushMessage( poolName , storageClass ) ;
           msg.setMaxFlushCount( count ) ;

           sendMessage( new CellMessage( new CellPath(poolName) , msg ) ) ;


    }
    private void setPoolReadOnly( String poolName , boolean rdOnly ){
       try{
           PoolManagerPoolModeMessage msg =
              new PoolManagerPoolModeMessage(
                     poolName,
                     PoolManagerPoolModeMessage.READ |
                     ( rdOnly ? 0 : PoolManagerPoolModeMessage.WRITE ) ) ;


           sendMessage( new CellMessage( new CellPath("PoolManager") , msg ) ) ;

       }catch(Exception ee ){
           _log.warn("setPoolReadOnly : couldn't sent message to PoolManager"+ee);
       }
    }
    private void queryPoolMode( String poolName ){
       try{
           PoolManagerPoolModeMessage msg =
              new PoolManagerPoolModeMessage(
                     poolName,
                     PoolManagerPoolModeMessage.UNDEFINED  ) ;


           sendMessage( new CellMessage( new CellPath("PoolManager") , msg ) ) ;

       }catch(Exception ee ){
           _log.warn("queryPoolMode : couldn't sent message to PoolManager"+ee);
       }
    }
    private class PoolCollector implements FutureCallback<Object[]>
    {
       private boolean _active;
       private int     _waitingFor;
       private HashMap<String, Object[]> _poolGroupHash      = new HashMap<>() ;
       private HashMap<String, HFCPool> _configuredPoolList = new HashMap<>() ;
       private boolean _poolSetupReady;

       private PoolCollector(){

       }
       private void queryAllPoolModes(){
          List<HFCPool> list ;
          synchronized(this){
             list = new ArrayList<>( _configuredPoolList.values() ) ;
          }
           for (HFCPool pool : list) {
               pool.queryMode();
           }
       }
       private synchronized boolean isPoolSetupReady(){ return _poolSetupReady ; }
       private boolean isPoolConfigDone(){
           int pools = 0 ;
           int c     = 0 ;
           for( Iterator<HFCPool> i = _configuredPoolList.values().iterator() ; i.hasNext() ; pools++ ){
               HFCPool pool = i.next();
               if( pool.answerCount > 1 ) {
                   return true;
               }
               if( pool.answerCount > 0 ) {
                   c++;
               }
           }
           return c == pools  ;
       }
       private synchronized Map<String, Object[]> getPoolGroupHash(){
          return new HashMap<>( _poolGroupHash ) ;
       }
       private synchronized Set<String> getConfiguredPoolNames(){
           return new HashSet<>( _configuredPoolList.keySet() ) ;
       }
       private synchronized List<HsmFlushControlCore.Pool> getConfiguredPools(){
           return new ArrayList<HsmFlushControlCore.Pool>( _configuredPoolList.values() ) ;
       }
       private synchronized void runCollector( Collection<String> list ){
           if( _active ){
              _log.warn("runCollector : Still running") ;
              return ;
           }
           _active = true ;
           CellPath path = new CellPath( "PoolManager" ) ;
           for (Object o : list) {
               String command = "psux ls pgroup " + o;
               if (_logEnabled) {
                   _log.info("runCollector sending : " + command + " to " + path);
               }
               Futures.addCallback(_poolManager.send(command, Object[].class), this);
               _waitingFor++;
           }
       }
       private synchronized void answer(){
          _waitingFor-- ;
          if( _waitingFor == 0 ){
             if(_logEnabled) {
                 _log.info("PoolCollector : we are done : " + _poolGroupHash);
             }
             _active = false ;
             HashMap<String, HFCPool> map = new HashMap<>() ;
             if(_logEnabled) {
                 _log.info("Creating ping set");
             }
              for (Object o : _poolGroupHash.values()) {
                  Object[] c = (Object[]) o;
                  for (Object aC : c) {
                      String pool = (String) aC;
                      if (_logEnabled) {
                          _log.info("Adding pool : " + pool);
                      }
                      HFCPool p = _configuredPoolList.get(pool);
                      map.put(pool, p == null ? new HFCPool(pool) : p);
                  }
              }
             //
             // get the diff
             //
              for (Object o : map.keySet()) {
                  String poolName = o.toString();
                  if (_configuredPoolList.get(poolName) == null) {
                      _eventDispatcher.configuredPoolAdded(poolName);
                  }
              }
              for (Object o : _configuredPoolList.keySet()) {
                  String poolName = o.toString();
                  if (map.get(poolName) == null) {
                      _eventDispatcher.configuredPoolRemoved(poolName);
                  }
              }

             _flushController.updatePoolList( map.keySet() ) ;
             _configuredPoolList = map ;

             _poolSetupReady = true ;

             _eventDispatcher.poolSetupReady() ;

//             _queueWatch.triggerGainControl() ;
          }
       }
       public synchronized HFCPool getPoolByName( String poolName ){
          return _configuredPoolList.get(poolName);
       }

        @Override
        public void onSuccess(Object[] reply)
        {
            if(_logEnabled) {
                _log.info("answer Arrived: {}", Arrays.toString(reply));
            }
            if (reply.length >= 3 && reply[0] instanceof String && reply[1] instanceof Object []) {
                String poolGroupName = (String) reply[0] ;
                _poolGroupHash.put( poolGroupName , (Object[]) reply[1]);
                if(_logEnabled) {
                    _log.info("PoolCollector : " + ((Object[]) reply[1]).length + " pools arrived for " + poolGroupName);
                }
            }else{
                _log.warn("PoolCollector : invalid reply arrived");
            }
            answer();
        }

        @Override
        public void onFailure(Throwable t)
        {
            _log.warn("PoolCollector : onFailure : {}", t.toString());
            answer() ;
        }
    }

    public static final String hh_pgroup_add = "<pGroup0> [<pgroup1> [...]]" ;
    public String ac_pgroup_add_$_1_99( Args args ){
        for( int i = 0 ; i < args.argc() ; i++ ){
            _poolGroupList.add(args.argv(i)) ;
        }
        return "" ;
    }
    public static final String hh_pgroup_remove = "<pGroup0> [<pgroup1> [...]]" ;
    public String ac_pgroup_remove_$_1_99( Args args ){
        for( int i = 0 ; i < args.argc() ; i++ ){
            _poolGroupList.remove(args.argv(i)) ;
        }
        return "" ;
    }
    public static final String hh_pgroup_ls = "" ;
    public String ac_pgroup_ls( Args args ){
       StringBuilder sb = new StringBuilder() ;
        for (Object poolGroup : _poolGroupList) {
            sb.append(poolGroup.toString()).append("\n");
        }
       return sb.toString() ;
    }
    public static final String hh_set_control = "on|off [-interval=<seconds>]" ;
    public String ac_set_control_$_1( Args args ){
       String mode     = args.argv(0) ;
       String iString  = args.getOpt("interval") ;
       long   interval = 0L ;
       if( iString != null ){
          interval = Long.parseLong( iString ) * 1000L ;
          if( interval < 30000L ) {
              throw new
                      IllegalArgumentException("interval must be greater than 30");
          }
       }

        switch (mode) {
        case "on":
            _flushController.setControl(true);
            break;
        case "off":
            _flushController.setControl(false);
            break;
        default:
            throw new
                    IllegalArgumentException("set control on|off");
        }
       if( interval > 0L ){
          _queueWatch.setGainControlTicker( interval ) ;
       }
       return "" ;
    }
    public static final String hh_set_pool = "<poolName> rdonly|rw" ;
    public String ac_set_pool_$_2( Args args )
    {
        String poolName = args.argv(0) ;
        String mode     = args.argv(1) ;

        HFCPool pool = _poolCollector.getPoolByName( poolName ) ;
        if( pool == null ) {
            throw new
                    NoSuchElementException("Pool not found : " + poolName);
        }


        switch (mode) {
        case "rdonly":
            pool.setReadOnly(true);
            break;
        case "rw":
            pool.setReadOnly(false);
            break;
        default:
            throw new
                    IllegalArgumentException("Illegal mode : rdonly|rw");
        }

        return "Pool "+poolName+" set to "+mode ;

    }
    public static final String hh_query_pool_mode = "<poolName>" ;
    public String ac_query_pool_mode_$_1( Args args )
    {
        String poolName = args.argv(0) ;

        HFCPool pool = _poolCollector.getPoolByName( poolName ) ;
        if( pool == null ) {
            throw new
                    NoSuchElementException("Pool not found : " + poolName);
        }


        pool.queryMode();

        return "Pool mode query sent to Pool "+poolName ;

    }
    public static final String hh_flush_pool = "<poolName> <storageClass> [-count=<count>]" ;
    public String ac_flush_pool_$_2( Args args ) throws Exception {
        String poolName = args.argv(0) ;
        String storageClass = args.argv(1) ;
        String countString  = args.getOpt("count");

        int count = ( countString == null ) || countString.equals("") ? 0 : Integer.parseInt(countString) ;

        HFCPool pool = _poolCollector.getPoolByName( poolName ) ;
        if( pool == null ) {
            throw new
                    NoSuchElementException("Pool not found : " + poolName);
        }

        HsmFlushControlCore.FlushInfo info = pool.getFlushInfoByStorageClass( storageClass ) ;
        if( info == null ) {
            throw new
                    NoSuchElementException("StorageClass not found : " + storageClass);
        }

        info.flush( count ) ;

        return "Flush initiated for ("+poolName+","+storageClass+")" ;

    }
    public static final String hh_ls_pool = "[<poolName>] -l " ;
    public Object ac_ls_pool_$_0_1( Args args ){
        String poolName = args.argc() == 0 ? null : args.argv(0) ;
        boolean detail  = args.hasOption("l") ;
        boolean binary  = args.hasOption("binary") ;

        StringBuffer sb = new StringBuffer() ;
        if( poolName == null ){
            if( binary ){
               Collection<HsmFlushControlCore.PoolDetails> list = new ArrayList<>() ;
                for (Object pool : _eventDispatcher.getConfiguredPools()) {
                    list.add(((HFCPool) pool).getDetails());
                }
               return list ;
            }else{
               Set<String> set = new TreeSet<>( _poolCollector.getConfiguredPoolNames() ) ;
                for (Object configuredPoolName : set) {
                    String name = configuredPoolName.toString();
                    if (!detail) {
                        sb.append(name).append("\n");
                    } else {
                        HFCPool pool = _poolCollector.getPoolByName(name);
                        printPoolDetails2(pool, sb);
                    }
                }
            }
        }else{
            HFCPool pool = _poolCollector.getPoolByName( poolName ) ;
            if( pool == null ) {
                throw new
                        NoSuchElementException("Pool not found : " + poolName);
            }

            if( binary ){
                return pool.getDetails() ;
                /*
                HsmFlushControlCore.PoolDetails details = pool.getDetails() ;
                sb.append("PoolDetails\n").
                   append("   Name ").append(details.getName()).append("\n").
                   append("   CellInfo ").append(details.getCellInfo()).append("\n").
                   append("   isActive ").append(details.isActive()).append("\n") ;
                for( Iterator i = details.getFlushInfos().iterator() ; i.hasNext() ; ){
                   HsmFlushControlCore.FlushInfoDetails flush = (HsmFlushControlCore.FlushInfoDetails)i.next() ;
                   sb.append("   StorageClass ").append(flush.getName()).append("\n").
                      append("     isFlushing   ").append(flush.isFlushing()).append("\n").
                      append("     FlushInfo    ").append(flush.getStorageClassFlushInfo()).append("\n");
                }
                */
            }else{
               printPoolDetails2( pool , sb ) ;
            }
        }
        return sb.toString() ;
    }
    private void printPoolDetails2( HFCPool pool , StringBuffer sb ){

       sb.append( pool._poolName ) ;


       sb.append("\n").append(pool.toString()).append(" ");

       if( pool.cellInfo == null ) {
           return;
       }

       PoolCellInfo cellInfo = pool.cellInfo ;
       PoolCostInfo costInfo = cellInfo.getPoolCostInfo() ;
       PoolCostInfo.PoolSpaceInfo spaceInfo = costInfo.getSpaceInfo() ;
       PoolCostInfo.PoolQueueInfo queueInfo = costInfo.getStoreQueue() ;

       long total    = spaceInfo.getTotalSpace() ;
       long precious = spaceInfo.getPreciousSpace() ;


       sb.append("   ").append("Mode=").append(pool.getPoolModeString()).
                        append(";Total=").append(total).
                        append(";Precious=").append(precious).
                        append(";Frac=").append( (float)precious/(float)total ).
                        append(";Queue={").append(queueInfo.toString()).append("}\n");

        for (Object o : pool.flushInfos.values()) {

            HFCFlushInfo info = (HFCFlushInfo) o;
            StorageClassFlushInfo flush = info.getStorageClassFlushInfo();

            String storeName = flush.getStorageClass() + "@" + flush.getHsm();

            long size = flush.getTotalPendingFileSize();
            int count = flush.getRequestCount();
            int active = flush.getActiveCount();

            sb.append("   ").append(storeName).append("  ").
                    append(info._flushingRequested ? "R" : "-").
                    append(info._flushingPending ? "P" : "-").
                    append(info._flushingError != 0 ? "E" : "-").
                    append("(").append(info._flushingId).append(")").
                    append(";Size=").append(size).
                    append(";Count=").append(count).
                    append(";Active=").append(active).append("\n");

        }

    }
    private void printFlushInfo( StorageClassFlushInfo flush , StringBuffer sb ){
        sb.append("count=").append(flush.getRequestCount()).
           append(";bytes=").append(flush.getTotalPendingFileSize());
    }
    /*
    public static final String hh_infos_ls = "" ;
    public String ac_infos_ls( Args args ){
        HashMap map = new HashMap( _infoHash ) ;
        StringBuffer sb = new StringBuffer() ;
        for( Iterator it = map.entrySet().iterator() ; it.hasNext() ; ){
            Map.Entry entry = (Map.Entry)it.next() ;
            String poolName = entry.getKey().toString() ;
            PoolFlushGainControlMessage msg = (PoolFlushGainControlMessage)entry.getValue() ;
            long holdTimer = msg.getHoldTimer() ;
            PoolCellInfo poolInfo = msg.getCellInfo() ;
            StorageClassFlushInfo [] flush = msg.getFlushInfos() ;
            sb.append( poolName ).append("\n");
            sb.append( "    ").append("Hold Timer : ").append(holdTimer).append("\n");
            sb.append( "    ").append("CellInfo   : ").append(poolInfo.toString()).append("\n");
            if( flush != null ){
               for( int j = 0 ; j < flush.length ; j++ ){
                   sb.append("    Flush(").append(j).append(") ").append(flush[j]).append("\n");
               }
            }
        }
        return sb.toString();
    }
    */
    public String toString(){
       return "Req="+_requests+";Err="+_failed+";" ;
    }
    @Override
    public void getInfo( PrintWriter pw ){
       pw.println("HsmFlushControlManager : [$Id: HsmFlushControlManager.java,v 1.6 2006-07-31 16:35:50 patrick Exp $]" ) ;
       pw.println("Status     : "+_status);
       pw.print("PoolGroups : ");
        for (Map.Entry<String,Object[]> entry : _poolCollector.getPoolGroupHash().entrySet()) {
            String groupName = entry.getKey();
            Object[] pools = entry.getValue();
            pw.println(groupName + "(" + pools.length + "),");
        }
       pw.println("");
       pw.println("Driver     : "+(_eventDispatcher._schedulerName==null?"NONE":_eventDispatcher._schedulerName));
       pw.println("Control    : "+( _flushController._control ? "on" : "off" ) ) ;
       pw.println("Update     : "+( _queueWatch._gainControlTicker/1000L )+ " seconds");
       long propertiesUpdated = _propertiesUpdated ;
       if( propertiesUpdated == 0L ){
          pw.println("Update     : "+( _queueWatch._gainControlTicker/1000L )+ " seconds");
          pw.println("Properties : Not queried yet");
       }else{
          Map<String,Object> properties;
          synchronized( _propertyLock ){
              properties = _properties ;
          }
          if( ( properties != null ) && ( properties.size() > 0 ) ){
             pw.println("Properties : (age "+( ( System.currentTimeMillis() - propertiesUpdated ) / 1000L )+" seconds)" );
              for (Map.Entry<String,Object> entry : properties.entrySet()) {
                  pw.println("  " + entry.getKey() + "=" + entry.getValue());
              }
          }else{
             pw.println("Properties : None");
          }
       }
    }
    @Override
    public CellInfo getCellInfo(){

        FlushControlCellInfo info = new FlushControlCellInfo(  super.getCellInfo() ) ;

        info.setParameter( _eventDispatcher._schedulerName==null?"NONE":_eventDispatcher._schedulerName ,
                           _queueWatch._gainControlTicker ,
                           _flushController._control ,
                           new ArrayList<>( _poolCollector.getPoolGroupHash().keySet() ) ,
                           _status ) ;

        synchronized( _propertyLock ){
            info.setDriverProperties( System.currentTimeMillis() - _propertiesUpdated , _properties ) ;
        }
        //_log.info("DRIVER PROPERTIES : "+info ) ;
        return info ;
    }
    private class EventDispatcher implements HsmFlushControlCore, Runnable  {
       private class Event {

           private static final int INIT                    = 0 ;
           private static final int UNLOAD                  = 1 ;
           private static final int FLUSHING_DONE           = 2 ;
           private static final int POOL_FLUSH_INFO_UPDATED = 3 ;
           private static final int CALL_DRIVER             = 4 ;
           private static final int POOL_SETUP_UPDATED      = 5 ;
           private static final int CONFIGURED_POOL_ADDED   = 6 ;
           private static final int CONFIGURED_POOL_REMOVED = 7 ;
           private static final int POOL_IO_MODE_UPDATED    = 8 ;
           private static final int TIMER                   = 9 ;
           private static final int PROPERTIES_UPDATED      = 10 ;
           private static final int RESET                   = 11 ;

           int type;
           Object [] args;
           long timestamp = System.currentTimeMillis() ;

           private Event( int type ){ this.type = type ; }
           private Event( int type , Object obj ){
              this.type = type ;
              args = new Object[1] ;
              args[0] = obj ;
           }
           public String toString(){
             StringBuilder sb = new StringBuilder() ;
             sb.append("Event type : ").append(type);
             if( args != null ){
               for( int i = 0 ; i < args.length;i++) {
                   sb.append(";arg(").append(i).append(")=")
                           .append(args[i].toString());
               }
             }
             return sb.toString();
           }
       }
       private class Pipe {
           private ArrayList<Event> _list = new ArrayList<>() ;

           public synchronized void push( Event obj ){
              _list.add( obj ) ;
              notifyAll() ;
           }
           public synchronized Event pop() throws InterruptedException {
              while( _list.isEmpty() ) {
                  wait();
              }
              return _list.remove(0);
           }
       }
       private Class<?>[] _argumentClasses = {
            CellAdapter.class ,
            HsmFlushControlCore.class
       } ;
       private Object[] _argumentObjects = {
            diskCacheV111.hsmControl.flush.HsmFlushControlManager.this ,
            this
       } ;
       private HsmFlushSchedulable _scheduler;
       private String  _schedulerName;
       private String  _printoutName;
       private Pipe    _pipe;
       private Thread  _worker;
       private boolean _initDone;
       private Args    _driverArgs    = new Args("") ;

       @Override
       public void run(){
          runIt( _pipe ) ;
       }
       public void runIt( Pipe pipe ) {

           HsmFlushSchedulable s;
           Event   event;
           boolean done     = false ;
           boolean initDone = false ;
           _log.info(_printoutName+": Worker event loop started for "+_schedulerName );

           while( ( ! Thread.interrupted() ) && ( ! done ) ){

               try{ event = pipe.pop() ; }catch(InterruptedException e ){ break ; }
               synchronized( this ){ s = _scheduler ; if( s == null ) {
                   break;
               }
               }

               try{
                   if( ( ! initDone ) &&
                         _poolCollector.isPoolSetupReady() &&
                         _poolCollector.isPoolConfigDone()                        ){
                       s.init() ;
                       initDone = true ;
                       _poolCollector.queryAllPoolModes() ;
                   }

                   if( ! initDone ) {
                       continue;
                   }

                   switch( event.type ){

                      case Event.FLUSHING_DONE :
                         HFCFlushInfo info = (HFCFlushInfo)event.args[0] ;
                         s.flushingDone( info._pool._poolName ,
                                         info._flushInfo.getStorageClass()+"@"+
                                         info._flushInfo.getHsm() ,
                                         info ) ;
                      break ;

                      case Event.POOL_FLUSH_INFO_UPDATED :
                      {
                         HFCPool pool  = (HFCPool)event.args[0] ;
                         s.poolFlushInfoUpdated( pool._poolName , pool) ;
                      }
                      break ;
                      case Event.POOL_IO_MODE_UPDATED :
                      {
                         HFCPool pool  = (HFCPool)event.args[0] ;
                         s.poolIoModeUpdated( pool._poolName , pool) ;
                      }
                      break ;

                      case Event.CONFIGURED_POOL_ADDED :
                      {
                         String poolName  = (String)event.args[0] ;
                         s.configuredPoolAdded( poolName ) ;
                      }
                      break ;

                      case Event.CONFIGURED_POOL_REMOVED :
                      {
                         String poolName  = (String)event.args[0] ;
                         s.configuredPoolRemoved( poolName ) ;
                      }
                      break ;

                      case Event.PROPERTIES_UPDATED :
                      {
                         Map<String,Object> properties  = (Map<String, Object>) event.args[0];
                         s.propertiesUpdated( properties ) ;
                         synchronized( _propertyLock ){
                            _propertiesUpdated = System.currentTimeMillis() ;
                            _properties        = properties ;
                         }
                      }
                      break ;

                      case Event.POOL_SETUP_UPDATED :
                         s.poolSetupUpdated() ;
                      break ;

                      case Event.CALL_DRIVER :
                         Args args  = (Args)event.args[0] ;
                         s.command( args ) ;
                      break ;

                      case Event.TIMER :
                         s.timer() ;
                      break ;

                      case Event.RESET :
                         s.reset() ;
                      break ;

                      case Event.UNLOAD :
                         done = true ;
                      break ;

                   }
               }catch(Throwable t ){
                   _log.warn(_printoutName+": Exception reported by "+event.type+" : "+t, t);
               }
           }
           _log.info(_printoutName+": Worker event loop stopped");
           _log.info(_printoutName+": Preparing unload");

           synchronized( this ){

               if( _scheduler == null ) {
                   return;
               }

               try{
                   _scheduler.prepareUnload() ;
               }catch(Throwable t ){
                   _log.warn(_printoutName+": Exception in prepareUnload "+t, t);
               }

               _scheduler = null ;
               _worker    = null ;
               _initDone  = false ;

           }
       }
       private synchronized void flushingDone( HFCFlushInfo info ){
          if( _pipe != null ) {
              _pipe.push(new Event(Event.FLUSHING_DONE, info));
          }
       }
       private synchronized void poolSetupReady(){
          if( _pipe != null ) {
              _pipe.push(new Event(Event.POOL_SETUP_UPDATED, null));
          }
       }
       /*
       private synchronized void poolSetupReady(){
           if( ( _scheduler != null ) && ( _pipe != null ) && ! _initDone )
                _pipe.push( new Event( Event.INIT ) ) ;
       }
       */
       private synchronized void poolFlushInfoUpdated( HFCPool pool ){
          if( _pipe != null ) {
              _pipe.push(new Event(Event.POOL_FLUSH_INFO_UPDATED, pool));
          }
       }
       private synchronized void configuredPoolAdded( String pool ){
          if( _pipe != null ) {
              _pipe.push(new Event(Event.CONFIGURED_POOL_ADDED, pool));
          }
       }
       private synchronized void configuredPoolRemoved( String pool ){
          if( _pipe != null ) {
              _pipe.push(new Event(Event.CONFIGURED_POOL_REMOVED, pool));
          }
       }
       private synchronized void callDriver( Args args ){
          if( _pipe != null ) {
              _pipe.push(new Event(Event.CALL_DRIVER, args));
          }
       }
       private synchronized void poolIoModeUpdated( HFCPool pool ){
          if( _pipe != null ) {
              _pipe.push(new Event(Event.POOL_IO_MODE_UPDATED, pool));
          }
       }
       private synchronized void propertiesUpdated( Map<String, String> properties ){
          if( _pipe != null ) {
              _pipe.push(new Event(Event.PROPERTIES_UPDATED, properties));
          }
       }
       private synchronized void timer(){
          if( _pipe != null ) {
              _pipe.push(new Event(Event.TIMER));
          }
       }
       private synchronized void reset(){
          if( _pipe != null ) {
              _pipe.push(new Event(Event.RESET));
          }
       }
       private synchronized void loadHandler( String handlerName , boolean doInit , Args args ) throws Exception {

          if( _scheduler != null ) {
              throw new
                      IllegalArgumentException("Handler already registered");
          }

          Class<? extends HsmFlushSchedulable> c   = Class.forName(handlerName).asSubclass(HsmFlushSchedulable.class);
          Constructor<? extends HsmFlushSchedulable> con = c.getConstructor(_argumentClasses);

          _driverArgs    = args ;
          _schedulerName = handlerName ;

          String [] tmp  = handlerName.split("\\.") ;
          _printoutName  = tmp[ tmp.length - 1 ] ;

          _scheduler = con.newInstance( _argumentObjects );
          _pipe      = new Pipe() ;
          _worker    = _nucleus.newThread( this , "driver" ) ;
          _worker.start() ;

          /*
          if( doInit ){
             _initDone = true ;
             _pipe.push( new Event( Event.INIT ) ) ;
          }
          */

       }
       private synchronized void unloadHandler(){
          if( _pipe == null ) {
              throw new
                      IllegalArgumentException("No handler active");
          }

          Pipe pipe = _pipe ;
          _pipe = null ;

          pipe.push( new Event( Event.UNLOAD ) ) ;


       }

       //
       // interface HsmFlushControlCore ...
       //
       @Override
       public Args getDriverArgs(){ return _driverArgs ; }
       @Override
       public Pool getPoolByName( String poolName ){
          return _poolCollector.getPoolByName( poolName) ;
       }
       @Override
       public  List<Pool> getConfiguredPools(){
          return _poolCollector.getConfiguredPools() ;
       }
       @Override
       public Set<String> getConfiguredPoolNames(){
          return _poolCollector.getConfiguredPoolNames() ;
       }
    }
    public static final String hh_driver_reset = " # resets driver " ;
    public String ac_driver_reset( Args args )
    {
         _eventDispatcher.callDriver( args ) ;
         return "Command sent to driver" ;
    }
    public static final String hh_driver_command = " commands send to driver ... " ;
    public String ac_driver_command_$_0_999( Args args )
    {
         _eventDispatcher.callDriver( args ) ;
         return "Command sent to driver" ;
    }
    public static final String hh_driver_properties = " OPTIONS : -<key>=<value> ..." ;
    public String ac_driver_properties( Args args )
    {
         Map<String, String> map = new HashMap<>() ;
         for( int i = 0 , n = args.optc() ; i < n ; i++ ){
            String key   = args.optv(i) ;
            String value = args.getOpt(key) ;
            map.put( key , value ) ;
         }
         _eventDispatcher.propertiesUpdated( map ) ;
         return "Properties sent to driver, check with 'info'" ;
    }
    public static final String hh_load_driver = "<driveClassName> [driver arguments and options]" ;
    public String ac_load_driver_$_999( Args args ) throws Exception {
        String driverClass = args.argv(0) ;
        args.shift();
        _eventDispatcher.loadHandler( driverClass , true , args ) ;
        return "Loaded : "+driverClass;
    }
    public static final String hh_unload_driver = "" ;
    public String ac_unload_driver( Args args )
    {
        _eventDispatcher.unloadHandler() ;
        return "Unload scheduled";
    }
    /////////////////////////////////////////////////////////////////////////////////////////
    //
    //      message arrived handler
    //
    private void poolFlushDoFlushMessageArrived( PoolFlushDoFlushMessage msg ){
        if(_logEnabled) {
            _log.info("poolFlushDoFlushMessageArrived : " + msg);
        }
        String poolName  = msg.getPoolName() ;
        synchronized( _poolCollector ){
            HFCPool pool = _poolCollector.getPoolByName( poolName) ;
            if( pool == null ){
               _log.warn("poolFlushDoFlushMessageArrived : message arrived for non configured pool : "+poolName);
               return ;
            }
            String storageClass = msg.getStorageClassName()+"@"+msg.getHsmName() ;
            HFCFlushInfo info = pool.flushInfos.get( storageClass );
            if( info == null ){
               _log.warn("poolFlushDoFlushMessageArrived : message arrived for non existing storage class : "+storageClass);
               //
               // the real one doesn't exists anymore, so we simulate one.
               //
               info = new HFCFlushInfo( pool , new StorageClassFlushInfo( msg.getHsmName() , msg.getStorageClassName() ) ) ;
            }
            if( msg.getReturnCode() != 0 ){
               if(_logEnabled) {
                   _log.info("Flush failed (msg=" + (msg
                           .isFinished() ? "Finished" : "Ack") + ") : " + msg);
               }

               info.setFlushingFailed(msg.getReturnCode(),msg.getErrorObject());
               //
               // maybe we have to call flushingDone here as well.
               //
               return ;
            }
            if( msg.isFinished() ){
                if(_logEnabled) {
                    _log.info("Flush finished : " + msg);
                }

                updateFlushCellAndFlushInfos( msg , pool ) ;
                info.setFlushingDone() ;
                _eventDispatcher.flushingDone(info);
            }else{
                info.setFlushingAck(msg.getFlushId());
            }
        }
    }
    private void poolFlushGainControlMessageDidntArrive( String poolName ){
        if(_logEnabled) {
            _log.info("poolFlushGainControlMessageDidntArrive : " + poolName);
        }
        synchronized( _poolCollector ){
            HFCPool pool = _poolCollector.getPoolByName( poolName) ;
            if( pool == null ){
               _log.warn("PoolFlushGainControlMessage : message arrived for non configured pool : "+poolName);
               return ;
            }
            pool.isActive    = false ;
            pool.lastUpdated = System.currentTimeMillis();
            pool.answerCount ++ ;

            _eventDispatcher.poolFlushInfoUpdated( pool  ) ;
        }

    }
    private void updateFlushCellAndFlushInfos( PoolFlushControlInfoMessage msg , HFCPool pool ){

       pool.cellInfo = msg.getCellInfo() ;
       StorageClassFlushInfo [] flushInfos = msg.getFlushInfos() ;
       HashMap<String, HFCFlushInfo> map = new HashMap<>() ;
       if( flushInfos != null ){
           for (StorageClassFlushInfo flushInfo : flushInfos) {
               String storageClass = flushInfo.getStorageClass() + "@" + flushInfo
                       .getHsm();

               HFCFlushInfo info = pool.flushInfos
                       .get(storageClass);
               if (info == null) {
                   map.put(storageClass, new HFCFlushInfo(pool, flushInfo));
               } else {
                   info.updateFlushInfo(flushInfo);
                   map.put(storageClass, info);
               }
           }
       }
       pool.flushInfos  = map ;
       pool.isActive    = true ;
       pool.lastUpdated = System.currentTimeMillis();
       pool.answerCount ++ ;

    }
    private void poolFlushGainControlMessageArrived( PoolFlushGainControlMessage msg ){
        if(_logEnabled) {
            _log.info("PoolFlushGainControlMessage : " + msg);
        }
        String poolName  = msg.getPoolName() ;
        synchronized( _poolCollector ){
            HFCPool pool = _poolCollector.getPoolByName( poolName) ;
            if( pool == null ){
               _log.warn("PoolFlushGainControlMessage : message arrived for non configured pool : "+poolName);
               return ;
            }

            updateFlushCellAndFlushInfos( msg , pool ) ;

            _eventDispatcher.poolFlushInfoUpdated( pool ) ;
        }
    }
    private void poolModeInfoArrived( PoolManagerPoolModeMessage msg ){
        if(_logEnabled) {
            _log.info("PoolManagerPoolModeMessage : " + msg);
        }
        String poolName  = msg.getPoolName() ;
        synchronized( _poolCollector ){

            HFCPool pool = _poolCollector.getPoolByName( poolName) ;
            if( pool == null ){
               _log.warn("poolModeInfoArrived : message arrived for non configured pool : "+poolName);
               return ;
            }
            pool.mode = msg.getPoolMode() ;

            _eventDispatcher.poolIoModeUpdated( pool ) ;
        }
    }
    private void poolStatusChanged( PoolStatusChangedMessage msg ){
       String poolName = msg.getPoolName() ;
    }
    @Override
    public void messageArrived( CellMessage msg ){
       Object obj = msg.getMessageObject() ;
       _requests ++ ;
       if( obj instanceof PoolFlushGainControlMessage ){

          poolFlushGainControlMessageArrived( (PoolFlushGainControlMessage) obj ) ;

       }else if( obj instanceof PoolFlushDoFlushMessage ){

          poolFlushDoFlushMessageArrived( (PoolFlushDoFlushMessage) obj )  ;

       }else if( obj instanceof PoolManagerPoolModeMessage ){

          poolModeInfoArrived( (PoolManagerPoolModeMessage) obj )  ;

       }else if( obj instanceof PoolStatusChangedMessage ){

          poolStatusChanged( (PoolStatusChangedMessage) obj )  ;

       }else if( obj instanceof NoRouteToCellException ){

          NoRouteToCellException nrtc = (NoRouteToCellException)obj ;
          CellPath        path = nrtc.getDestinationPath() ;
          CellAddressCore core = path.getDestinationAddress() ;
          String          cellName = core.getCellName() ;
          _log.warn( "NoRouteToCell : "+ cellName + " ("+path+")");

          poolFlushGainControlMessageDidntArrive( cellName ) ;

       }else{
          _log.warn("Unknown message arrived ("+msg.getSourcePath()+") : "+
               msg.getMessageObject() ) ;
         _failed ++ ;
       }
    }
}
