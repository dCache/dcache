// $Id: HsmFlushController.java,v 1.6 2007-05-24 13:51:10 tigran Exp $


package org.dcache.pool.classic;

import diskCacheV111.pools.PoolCellInfo;
import diskCacheV111.pools.StorageClassFlushInfo;
import diskCacheV111.pools.StorageClassInfoFlushable;
import diskCacheV111.vehicles.*;
import org.dcache.cell.CellMessageReceiver;
import org.dcache.cell.CellCommandListener;
import org.dcache.cell.AbstractCellComponent;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;
import dmg.util.Formats;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.io.PrintWriter;

import org.apache.log4j.Logger;

public class HsmFlushController
    extends AbstractCellComponent
    implements Runnable,
               CellMessageReceiver,
               CellCommandListener
{
    private final static Logger _log =
        Logger.getLogger(HsmFlushController.class);

    private final Thread _worker  ;
    private int    _maxActive         = 1000 ;
    private int    _flushingInterval  = 60 ;
    private int    _retryDelayOnError = 60 ;
    //
    // make sure we don't flush if there is a flush controller
    //
    private long   _holdUntil         = System.currentTimeMillis() + 5L * 60L * 1000L ;
    //
    private final StorageClassContainer  _storageQueue ;
    private final HsmStorageHandler2     _storageHandler;

    public HsmFlushController(
              StorageClassContainer storageQueue,
              HsmStorageHandler2 storageHandler)
    {
        _worker         = new Thread(this, "flushing");
        _storageQueue   = storageQueue ;
        _storageHandler = storageHandler ;
    }

    private void setFlushInfos(PoolFlushControlInfoMessage flushInfo)
    {
       flushInfo.setCellInfo((PoolCellInfo)getCellInfo());
       List<StorageClassFlushInfo> list =
           new ArrayList<StorageClassFlushInfo>() ;

       for (StorageClassInfo info : _storageQueue.getStorageClassInfos()) {
           list.add(info.getFlushInfo());
       }
       flushInfo.setFlushInfos(list.toArray(new StorageClassFlushInfo[list.size()]));
    }

    public synchronized void  messageArrived( PoolFlushControlMessage flushControl , CellMessage message ){
       if( flushControl instanceof PoolFlushGainControlMessage ){

          PoolFlushGainControlMessage gain = (PoolFlushGainControlMessage)flushControl;
          long holdTimer = gain.getHoldTimer() ;

          if( holdTimer > 0 )synchronized( _parameterLock ){ _holdUntil = System.currentTimeMillis() + holdTimer; }

          if( flushControl.getReplyRequired() )setFlushInfos( gain ) ;

       }else if( flushControl instanceof PoolFlushDoFlushMessage ){
          new PrivateFlush( (PoolFlushDoFlushMessage)flushControl , message ) ;
          return ; /* reply from async run method */
       }else{
           flushControl.setFailed(354,"Message type not supported : "+flushControl.getClass().getName());
       }

       if (flushControl.getReplyRequired()) {
           message.revertDirection();
           try {
               sendMessage(message);
           } catch (NoRouteToCellException e) {
               esay("Problem replying : " + message + " " + e);
           }
       }
    }
    private class PrivateFlush implements Runnable, StorageClassInfoFlushable {
        private final PoolFlushDoFlushMessage _flush;
        private final CellMessage _message ;

        private PrivateFlush( PoolFlushDoFlushMessage flush , CellMessage message ){
           _flush = flush ;
           _message = message ;
           _message.revertDirection() ;

           new Thread(this, "Worker").start();
        }
        public void run(){

            String hsm          = _flush.getHsmName() ;
            String storageClass = _flush.getStorageClassName() ;
            String composed     = storageClass+"@"+hsm ;

            say("Starting flush for "+composed) ;
            try{
               long flushId = flushStorageClass( hsm , storageClass , _flush.getMaxFlushCount() , this ) ;
               _flush.setFlushId( flushId ) ;
               say("Finished flush for "+composed) ;
            }catch(Exception ee ){
               esay("Private flush failed for "+composed+" : "+ee);
               _flush.setFailed(576,ee);
            }
            if( _flush.getReplyRequired() ){
                try {
                   sendMessage(_message);
                } catch (NoRouteToCellException e) {
                   esay("Problem replying : " + _message + " " + e);
                }
            }
        }
        public void storageClassInfoFlushed( String hsm , String storageClass , long flushId , int requests , int failed ){
            say("Flush finished : "+hsm+"  "+storageClass+" , id="+flushId+";R="+requests+";f="+failed);

            if( _flush.getReplyRequired() ){
                 setFlushInfos( _flush ) ;
                _flush.setResult( requests , failed ) ;
                try {
                    sendMessage(_message);
                } catch (NoRouteToCellException e) {
                    esay("Problem replying : " + _message + " " + e);
                }
            }
        }
    }
    public long flushStorageClass( String hsm , String storageClass , int maxCount ){
        return flushStorageClass( hsm , storageClass , maxCount  , null ) ;
    }
    long flushStorageClass( String hsm , String storageClass , int maxCount , StorageClassInfoFlushable callback ){
        StorageClassInfo info  = _storageQueue.getStorageClassInfoByName( hsm , storageClass );
        say( "Flushing storageClass : "+info ) ;
        long id = info.submit( _storageHandler , maxCount , callback ) ;
        say( "Flushing storageClass : "+storageClass+" Done" ) ;
        return id ;
    }
    public void say(String message)
    {
        _log.info(message);
    }

    public void esay(String message)
    {
        _log.error(message);
    }

    public void esay(Throwable t)
    {
        _log.error(t);
    }

    public void start(){ _worker.start() ; }
    public String ac_flush_exception( Args args )throws Exception {
        Exception e = new Exception("Dummy Exception");

        e.fillInStackTrace() ;

        throw e ;
    }
    public String hh_flush_set_max_active = "<maxActiveFlush's>";
    public synchronized String ac_flush_set_max_active_$_1( Args args ){
        _maxActive = Integer.parseInt( args.argv(0) ) ;
        return "Max active flush = "+_maxActive ;
    }
    public String hh_flush_set_interval = "<flushing check interval/sec>" ;
    public String ac_flush_set_interval_$_1( Args args ){
        _flushingInterval = Integer.parseInt( args.argv(0) ) ;
        trigger() ;
        return "flushing interval set to "+_flushingInterval ;
    }
    public String hh_flush_set_retry_delay = "<errorRetryDelay>/sec" ;
    public String ac_flush_set_retry_delay_$_1( Args args ){
        _retryDelayOnError = Integer.parseInt( args.argv(0) ) ;
        return "Retry delay set to "+_retryDelayOnError+" sec";
    }
    public void printSetup( PrintWriter pw ){
        pw.println( "#\n# Flushing Thread setup\n#" ) ;
        pw.println( "flush set max active "+_maxActive ) ;
        pw.println( "flush set interval "+_flushingInterval ) ;
        pw.println( "flush set retry delay "+_retryDelayOnError ) ;
    }
    public void getInfo( PrintWriter pw ){
        pw.println( "Flushing Thread" ) ;
        pw.println( "   Flushing Interval /seconds   : "+_flushingInterval ) ;
        pw.println( "   Maximum classes flushing     : "+_maxActive ) ;
        pw.println( "   Minimum flush delay on error : "+_retryDelayOnError ) ;
        pw.println("  Remote controlled (hold until) : "+
            (  ( _holdUntil > System.currentTimeMillis() ) ? new Date(_holdUntil).toString(): "Locally Controlled" ) );
    }

    public Object ac_flush_ls( Args args ){
        long now = System.currentTimeMillis() ;
        if( args.getOpt("binary" ) == null ){
            StringBuffer sb = new StringBuffer() ;
            sb.append( Formats.field( "Class" , 20 , Formats.LEFT ) ) ;
            sb.append( Formats.field( "Active" , 8 , Formats.RIGHT ) ) ;
            sb.append( Formats.field( "Error"  , 8 , Formats.RIGHT ) ) ;
            sb.append( Formats.field( "Last/min" , 10 , Formats.RIGHT ) ) ;
            sb.append( Formats.field( "Requests" , 10 , Formats.RIGHT ) ) ;
            sb.append( Formats.field( "Failed"   , 10 , Formats.RIGHT ) ) ;
            sb.append("\n");
            for (StorageClassInfo info : _storageQueue.getStorageClassInfos()) {
                sb.append( Formats.field( info.getStorageClass()+"@"+info.getHsm() ,
                        20 , Formats.LEFT ) ) ;
                sb.append( Formats.field( ""+info.getActiveCount() , 8 , Formats.RIGHT ) ) ;
                sb.append( Formats.field( ""+info.getErrorCount()  , 8 , Formats.RIGHT ) ) ;
                long lastSubmit = info.getLastSubmitted() ;
                lastSubmit = lastSubmit == 0L ? 0L : (now - info.getLastSubmitted())/60000L ;
                sb.append( Formats.field( ""+lastSubmit , 10 , Formats.RIGHT ) ) ;
                sb.append( Formats.field( ""+info.getRequestCount() , 10 , Formats.RIGHT ) ) ;
                sb.append( Formats.field( ""+info.getFailedRequestCount() , 10 , Formats.RIGHT ) ) ;
                sb.append("\n");
            }
            return sb.toString();
        }else{ // is binary
            List list = new ArrayList();
            for (StorageClassInfo info : _storageQueue.getStorageClassInfos()) {
                Object [] o = new Object[7] ;
                o[0] = info.getHsm() ;
                o[1] = info.getStorageClass() ;
                o[2] = Long.valueOf( now - info.getLastSubmitted() ) ;
                o[3] = Long.valueOf( info.getRequestCount() ) ;
                o[4] = Long.valueOf( info.getFailedRequestCount() ) ;
                o[5] = Long.valueOf( info.getActiveCount() ) ;
                o[6] = Long.valueOf( info.getErrorCount() ) ;
                list.add(o);
            }

            return list.toArray() ;
        }
    }
    private Object  _parameterLock = new Object() ;
    public synchronized void run() {
        say("Flush Thread starting");
        long holdUntil = 0L;

        while (!Thread.interrupted()) {
            try {
                long now = System.currentTimeMillis() ;
                synchronized( _parameterLock ){ holdUntil = _holdUntil ;}
                if( _holdUntil < now ){
                    Iterator<StorageClassInfo> e = _storageQueue.getStorageClassInfos().iterator();

                    for( int active = 0 ; e.hasNext() && ( active < _maxActive ) ; ){

                        StorageClassInfo info     = e.next() ;
                        boolean          isActive = info.getActiveCount() > 0 ;
                        if( isActive ){

                            active ++ ;

                        }else if( info.isTriggered() &&
                                  ( ( now - info.getLastSubmitted() ) > (_retryDelayOnError*1000) ) ){

                            say( "Flushing : "+info ) ;
                            flushStorageClass( info.getHsm()  , info.getStorageClass() , 0 ) ;
                            active ++ ;
                        }
                    }
                }
            } catch (Exception me) {
                /* Catch all - we should not see any exceptions at this
                 * point so better dump the stack trace.
                 */
                esay(me);
            }
            try {
                wait(_flushingInterval * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        say("Flushing Thread finished");
    }
    public synchronized void trigger(){
        notifyAll() ;
    }
}
