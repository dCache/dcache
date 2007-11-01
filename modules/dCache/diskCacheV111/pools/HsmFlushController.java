// $Id: HsmFlushController.java,v 1.2 2006-04-03 05:36:39 patrick Exp $


package diskCacheV111.pools;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;
import  diskCacheV111.movers.* ;
import  diskCacheV111.repository.* ;
import  diskCacheV111.util.event.* ;

import  dmg.cells.nucleus.*;
import  dmg.util.*;
import  dmg.cells.services.* ;

import  java.util.*;
import  java.io.*;
import  java.net.*;
import  java.lang.reflect.* ;

public class HsmFlushController implements Runnable {

    private Thread _worker  ;
    private int    _maxActive         = 1000 ;
    private int    _flushingInterval  = 60 ;
    private int    _retryDelayOnError = 60 ;
    //
    // make sure we don't flush if there is a flush controller
    //
    private long   _holdUntil         = System.currentTimeMillis() + 5L * 60L * 1000L ;
    //
    private StorageClassContainer  _storageQueue   = null ;
    private CellAdapter            _cell           = null ;
    private HsmStorageHandler2     _storageHandler = null ;

    HsmFlushController( 
              CellAdapter cellAdapter , 
              StorageClassContainer  storageQueue ,
              HsmStorageHandler2 storageHandler  ){

        _worker         = cellAdapter.getNucleus().newThread( this , "flushing" ) ;
        _cell           = cellAdapter ;
        _storageQueue   = storageQueue ;
        _storageHandler = storageHandler ;
        say("HsmFlushController : $Id");

    }
    private void setFlushInfos( PoolFlushControlInfoMessage flushInfo ){
    
       flushInfo.setCellInfo( (PoolCellInfo)_cell.getCellInfo() ) ;
       ArrayList list = new ArrayList() ;

       for( Iterator i = _storageQueue.getStorageClassInfos() ; i.hasNext() ; ){ 
          list.add( ((StorageClassInfo)i.next()).getFlushInfo() ) ;
       }
       flushInfo.setFlushInfos( (StorageClassFlushInfo []) list.toArray( new StorageClassFlushInfo[0] ) ) ;
           
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
       
       if( flushControl.getReplyRequired() ){
           message.revertDirection() ;
           try{
              _cell.sendMessage(message);
           }catch(Exception e){
              esay("Problem replying : "+message+" "+e);
           }
       }
    }
    private class PrivateFlush implements Runnable, StorageClassInfoFlushable {
        private PoolFlushDoFlushMessage _flush = null ;
        private CellMessage _message = null ;
        private PrivateFlush( PoolFlushDoFlushMessage flush , CellMessage message ){
           _flush = flush ;
           _message = message ;
           _message.revertDirection() ;
           
           _cell.getNucleus().newThread( this , "Worker" ).start() ;
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
                try{
                   _cell.sendMessage(_message);
                }catch(Exception e){
                   esay("Problem replying : "+_message+" "+e);
                }
            }
        }
        public void storageClassInfoFlushed( String hsm , String storageClass , long flushId , int requests , int failed ){
            say("Flush finished : "+hsm+"  "+storageClass+" , id="+flushId+";R="+requests+";f="+failed);
            
            if( _flush.getReplyRequired() ){
                 setFlushInfos( _flush ) ;
                _flush.setResult( requests , failed ) ;
                try{
                   _cell.sendMessage(_message);
                }catch(Exception e){
                   esay("Problem replying : "+_message+" "+e);
                }
            }
        }  
    }
    long flushStorageClass( String hsm , String storageClass , int maxCount ){
        return flushStorageClass( hsm , storageClass , maxCount  , null ) ;
    }
    long flushStorageClass( String hsm , String storageClass , int maxCount , StorageClassInfoFlushable callback ){
        StorageClassInfo info  = _storageQueue.getStorageClassInfoByName( hsm , storageClass );
        say( "Flushing storageClass : "+info ) ;
        long id = info.submit( _storageHandler , maxCount , callback ) ;
        say( "Flushing storageClass : "+storageClass+" Done" ) ;
        return id ;
    }
    public void say( String message ){ _cell.say(message);}
    public void esay( String message ){ _cell.esay(message);}
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
    public String hh_flush_set_interval = "<flushing check inteval/sec>" ;
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
        Iterator i = _storageQueue.getStorageClassInfos() ;
        long now = System.currentTimeMillis() ;
        if( args.getOpt("binary" ) == null ){
            StringBuffer sb = new StringBuffer() ;
            sb.append( Formats.field( "Class" , 20 , Formats.LEFT ) ) ;
            sb.append( Formats.field( "Active" , 8 , Formats.RIGHT ) ) ;
            sb.append( Formats.field( "Error"  , 8 , Formats.RIGHT ) ) ;
            sb.append( Formats.field( "Last/min" , 10 , Formats.RIGHT ) ) ;
            sb.append( Formats.field( "Requests" , 10 , Formats.RIGHT ) ) ;
            sb.append( Formats.field( "Faied"    , 10 , Formats.RIGHT ) ) ;
            sb.append("\n");
            while( i.hasNext() ){
                StorageClassInfo info = (StorageClassInfo)i.next() ;
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
            ArrayList list = new ArrayList() ;
            while( i.hasNext() ){
                StorageClassInfo info = (StorageClassInfo)i.next() ;
                Object [] o = new Object[7] ; 
                o[0] = info.getHsm() ;
                o[1] = info.getStorageClass() ;
                o[2] = new Long( now - info.getLastSubmitted() ) ;
                o[3] = new Long( info.getRequestCount() ) ;
                o[4] = new Long( info.getFailedRequestCount() ) ;
                o[5] = new Long( info.getActiveCount() ) ;
                o[6] = new Long( info.getErrorCount() ) ;
                list.add(o);  
            }

            return list.toArray() ;
        }
    }
    private Object  _parameterLock = new Object() ;
    public synchronized void run() {
        say("Flush Thread starting");
        long holdUntil = 0L;
        while( ! Thread.currentThread().interrupted() ){
            long now = System.currentTimeMillis() ;
            synchronized( _parameterLock ){ holdUntil = _holdUntil ;}
            try{
                if( _holdUntil < now ){ 
                   Iterator e   = _storageQueue.getStorageClassInfos();

                   for( int active = 0 ; e.hasNext() && ( active < _maxActive ) ; ){

                       StorageClassInfo info     = (StorageClassInfo)e.next() ;
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
                try{
                    wait(_flushingInterval*1000);
                }catch (InterruptedException exc){
                    say( "Flushing Thread interrupted" ) ;
                    break ;
                }
            }catch( Exception me ){
                esay( "Flush thread : loop interrupted : "+me ) ;
            }
        }
        say( "Flushing Thread finished" ) ;
    }
    public synchronized void trigger(){
        notifyAll() ;
    }
}
