//
// $Id: IoHandler.java,v 1.7 2007-05-24 13:51:15 tigran Exp $
//
package diskCacheV111.doors.dCapV5 ;

import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;


import dmg.cells.nucleus.*;
import dmg.util.*;

import java.util.*;

/**
  * @author Patrick Fuhrmann
  * @version 0.1, Jan 18 2002
  *
  *
  *
  *  
  */

////////////////////////////////////////////////////////////////////
//
//      Io specific
//
public class IoHandler extends PnfsIdHandler {

  private static final int IDLE         = 0 ;
  private static final int STORAGE_INFO_AVAILABLE  = 1 ;
  private static final int WAITING_FOR_POOLMANAGER = 2 ;
  private static final int WAITING_FOR_POOL_OK     = 3 ;
  private static final int WAITING_FOR_POOL_DONE   = 4 ;

//       private StorageInfo _storageInfo = null ;
  private boolean     _retry       = true ;
  private String      _ioMode      = null ;
  private DCapProtocolInfo _protocolInfo = null ;
  private int         _retryCounter = -1 ;
  private String      _pool         = null ;

  private long  _pnfs_timeout        = 0 ;
  private long  _poolManager_timeout = 0 ;
  private long  _pool_ok_timeout     = 0 ;
  private long  _pool_done_timeout   = 0 ;
  private long  _retry_timer         = 0 ;

  private String _lastErrorMessage = null ;
  private int    _lastErrorValue   = 0 ;

  public IoHandler( SessionRoot sessionRoot , 
                    int sessionId , int commandId , VspArgs args )
         throws Exception {
         
      super( sessionRoot , sessionId , commandId , args ) ;

            _ioMode = _vargs.argv(1) ;      
      int   port    = Integer.parseInt( _vargs.argv(3) ) ;

      StringTokenizer st = new StringTokenizer( _vargs.argv(2) , "," ) ;
      String [] hosts    = new String[st.countTokens()]  ;
      for( int i = 0 ; i < hosts.length ; i++ )hosts[i] = st.nextToken() ;
      //
      //
      _protocolInfo = new DCapProtocolInfo( "DCap",3,0, hosts , port  ) ;
      _protocolInfo.setSessionId( _sessionId ) ;

      _retry             = getStringOption( "onerror" , "retry" ).equals("retry") ;
      _pnfs_timeout        = getLongOption( "pnfs-timeout"        , 10     ) * 1000L ;
      _poolManager_timeout = getLongOption( "poolManager-timeout" , 1*3600 ) * 1000L ;
      _pool_ok_timeout     = getLongOption( "pool-ok-timeout" ,     30     ) * 1000L ;
      _pool_done_timeout   = getLongOption( "pool-done-timeout" ,   5*3600 ) * 1000L ; 
      _retry_timer         = getLongOption( "retry-timer"       ,   5 * 60 ) * 1000L ;
      
           
  }
  public void go(){ 
     if( isLocked() ){
        say("IoHandler locked, initiating timer" ) ;
        _timer.addTimer(null,null,new LockObserver(),10000) ;
        return ;
     }
     getStorageInfo()  ;
  }
  public class LockObserver implements MessageTimerEvent {
  
     public void event( MessageEventTimer timer , 
                        Object eventObject ,
                        int    eventType             ){
         say("Timer triggered ..." ) ;
         go() ;
     }
  }
  public void getStorageInfo(){
      setState( IDLE , "<WaitingForStorageInfo>" ) ;
      send( new CellPath( "PnfsManager" ) , 
            _storageInfoRequest , 
            _pnfs_timeout ,
            new StorageInfoReceiver()  ) ;
      _retryCounter ++ ;
  }
  public String hh_info = "" ;
  public String ac_info( Args args )throws Exception {
     StringBuffer sb = new StringBuffer() ;
     sb.append(" Revision : $Revision: 1.7 $\n" ) ;
     sb.append(" PnfsId   : ").
        append( _pnfsId == null ? "<NotYetKnown>" : _pnfsId.toString()).
        append("\n") ;
     sb.append(" IoMode   : ").append(_ioMode).append("\n");
     sb.append(" Status   : ").append(getStatus()).append("\n");
     sb.append(" Retry    : ").append(_retryCounter).append("\n");
     if( _lastErrorMessage != null )
     sb.append(" Error    : <").append(_lastErrorValue)
                               .append("> ")
                               .append(_lastErrorMessage)
                               .append("\n") ;
     return sb.toString() ;
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //   Handles the Retries.
  //
  private class RetryHandler implements MessageTimerEvent {
     public void event( MessageEventTimer timer , 
                        Object eventObject ,
                        int    eventType             ){
         say("RetryHandler : running from 'go'" ) ;                  
         go() ;
     }  
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //   Handles the StorageInfo
  //
  private class StorageInfoReceiver implements MessageTimerEvent {
  
     public void event( MessageEventTimer timer , 
                        Object eventObject ,
                        int    eventType             ){
                        
        boolean retryRecommended = true ;
        String  errorMessage = null ;
        int     errorValue   = 0 ;
        say( "StorageInfoReceiver : got event "+eventType ) ;
        if( eventType == MessageEventTimer.MESSAGE_ARRIVED ){
           
           Object [] messageArray = (Object [])eventObject ;
           if( ( messageArray == null     ) ||
               ( messageArray.length != 1 ) ||
               ( ! ( messageArray[0] instanceof CellMessage ) )         ){
               
              esay( errorMessage = "PANIC : system error in StorageInfoReceiver: 1" ) ;
              errorValue = 666 ;  
              retryRecommended = false ;
              
              
           }else{
           
              Object answer = ((CellMessage)messageArray[0]).getMessageObject() ;
              if( answer instanceof NoRouteToCellException ){
                 esay( errorMessage = answer.toString() ) ;
                 errorValue = 34 ;  
                 retryRecommended = true ;              
              }else if( answer instanceof CacheException ){
                 CacheException ce = ((CacheException)answer) ;
                 errorMessage = ce.getMessage() ;
                 errorValue   = ce.getRc() ;
                 switch( ce.getRc() ){
                    case 4444 :
                      retryRecommended = true ;
                      break ;
                    default :
                      retryRecommended = false ;
                 }
              }else if( answer instanceof Exception ){
                 esay( errorMessage = answer.toString() ) ;
                 errorValue = 1 ;  
                 retryRecommended = false ;              
              }else if( answer instanceof PnfsGetStorageInfoMessage ){
                 try{
                    storageInfoArrived( (PnfsGetStorageInfoMessage)answer ) ;
                 }catch( CacheException ce ){
                    errorMessage = ce.getMessage() ;
                    errorValue   = ce.getRc() ;
                    switch( ce.getRc() ){
                       case 4444 :
                         retryRecommended = true ;
                         break ;
                       default :
                         retryRecommended = false ;
                    }
                 }catch( Exception e ){
                    esay( errorMessage = answer.toString() ) ;
                    errorValue = 1 ;  
                    retryRecommended = false ;              
                 }
              }else{
                 esay( errorMessage = "PANIC : system error in StorageInfoReceiver: 2" ) ;
                 errorValue = 667 ;  
                 retryRecommended = false ;
                 
              }
           }
                        
        }else if( eventType == MessageEventTimer.TIMEOUT_EXPIRED ){
           retryRecommended  = true ;
           errorMessage = "PnfsManager timed out" ;
           errorValue   = 1 ;
        }
        if( errorValue != 0 ){
            _lastErrorValue   = errorValue ;
            _lastErrorMessage = errorMessage ;
            if( _retry && retryRecommended ){
                _timer.addTimer( Integer.valueOf( _sessionId ) ,
                                 null ,
                                 new RetryHandler() ,
                                 _retry_timer ) ;
                setState( IDLE , "<RetryHandlerActivated>" ) ;
                esay("Retry timer restarted");
            }else{
                sendReply( "StorageInfoReceiver" , errorValue , errorMessage ) ;
                removeUs() ;
            }

        }
     }
     public void storageInfoArrived( PnfsGetStorageInfoMessage reply )
            throws CacheException{

         say( "pnfsGetStorageInfoArrived : "+reply ) ;
         int rc = reply.getReturnCode() ;
         if( rc != 0 )
             throw new
             CacheException( rc , reply.getErrorObject().toString() ) ;

         _storageInfo = reply.getStorageInfo() ;
         if( _pnfsId == null )_pnfsId = reply.getPnfsId() ;

         _info.setPnfsId( _pnfsId ) ;

         for( int i = 0 ; i < _vargs.optc() ; i++ ){
            String key   = _vargs.optv(i) ;
            String value = _vargs.getOpt(key) ;
            _storageInfo.setKey( key , value == null ? "" : value ) ;

         }
         PoolMgrGetPoolMsg getPoolMessage = null ;           
         FileMetaData                meta = reply.getMetaData() ;
         FileMetaData.Permissions   world = meta.getWorldPermissions() ;

         if( _isUrl && ! world.canRead() )
            throw new
            CacheException( 1 , "Permission denied (not world readable)" ) ;

         if( _storageInfo.isCreatedOnly()   ){
            //
            // the file is an  pnfsEntry only.
            // 'read only would be nonsense'
            //
            if( _ioMode.indexOf( 'w' ) < 0 )
               throw new
               CacheException( 2 , "File doesn't exist (can't be readOnly)" ) ;
            //
            // we need a write pool
            //
            _protocolInfo.setAllowWrite(true) ;
            //
            // try to get some space to store the file.
            //
            getPoolMessage = 
               new PoolMgrSelectWritePoolMsg(_pnfsId,_storageInfo,_protocolInfo,0) ;
         }else{
            //
            // sorry, we don't allow write (not yet)
            //
            if( _ioMode.indexOf( 'w' ) > -1 )
               throw new
               CacheException( 4 , "File is readOnly" ) ;

            //
            // we need to tell the mover as well.
            // The client may try to write without
            // specifying so.
            //
            _protocolInfo.setAllowWrite(false) ;
            //
            // try to get some space to store the file.
            //
            getPoolMessage = 
               new PoolMgrSelectReadPoolMsg(_pnfsId,_storageInfo,_protocolInfo,0) ;
         }

         if( _verbose )sendComment("opened");

         getPoolMessage.setId(_sessionId);

         setState( IDLE , "<WaitingForSPoolManager>" ) ;
         send( new CellPath( "PoolManager" ) , 
               getPoolMessage , 
               _poolManager_timeout ,
               new PoolSelectMessageReceiver()  ) ;
         _retryCounter ++ ;

         return ;          
     }
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //   Handles the StorageInfo
  //
  private class PoolSelectMessageReceiver implements MessageTimerEvent {
     public void event( MessageEventTimer timer , 
                        Object eventObject ,
                        int    eventType             ){
        boolean retryRecommended = true ;
        String  errorMessage = null ;
        int     errorValue   = 0 ;
        if( eventType == MessageEventTimer.MESSAGE_ARRIVED ){
           
           Object [] messageArray = (Object [])eventObject ;
           if( ( messageArray == null     ) ||
               ( messageArray.length != 1 ) ||
               ( ! ( messageArray[0] instanceof CellMessage ) )         ){
               
              esay( errorMessage = "PANIC : system error in StorageInfoReceiver: 1" ) ;
              errorValue = 666 ;  
              retryRecommended = false ;
              
              
           }else{
           
              Object answer = ((CellMessage)messageArray[0]).getMessageObject() ;
              if( answer instanceof NoRouteToCellException ){
                 esay( errorMessage = answer.toString() ) ;
                 errorValue = 666 ;  
                 retryRecommended = true ;              
              }else if( answer instanceof CacheException ){
                 CacheException ce = ((CacheException)answer) ;
                 errorMessage = ce.getMessage() ;
                 errorValue   = ce.getRc() ;
                 switch( ce.getRc() ){
                    case 4444 :
                      retryRecommended = true ;
                      break ;
                    default :
                      retryRecommended = false ;
                 }
              }else if( answer instanceof Exception ){
                 esay( errorMessage = answer.toString() ) ;
                 errorValue = 1 ;  
                 retryRecommended = false ;              
              }else if( answer instanceof PoolMgrSelectPoolMsg ){
                 try{
                    selectPoolArrived( (PoolMgrSelectPoolMsg) answer ) ;
                 }catch( CacheException ce ){
                    errorMessage = ce.getMessage() ;
                    errorValue   = ce.getRc() ;
                    switch( ce.getRc() ){
                       case 4444 :
                         retryRecommended = true ;
                         break ;
                       default :
                         retryRecommended = false ;
                    }
                 }catch( Exception e ){
                    esay( errorMessage = answer.toString() ) ;
                    errorValue = 1 ;  
                    retryRecommended = false ;              
                 }
              }else{
                 esay( errorMessage = "PANIC : system error in StorageInfoReceiver: 2" ) ;
                 errorValue = 667 ;  
                 retryRecommended = false ;
                 
              }
           }
                        
        }else if( eventType == MessageEventTimer.TIMEOUT_EXPIRED ){
           retryRecommended  = true ;
           errorMessage = "PoolManager timed out" ;
           errorValue   = 1 ;
        }

        if( errorValue != 0 ){
            _lastErrorValue   = errorValue ;
            _lastErrorMessage = errorMessage ;
            if( _retry && retryRecommended ){
                _timer.addTimer( Integer.valueOf( _sessionId ) ,
                                 null ,
                                 new RetryHandler() ,
                                 _retry_timer ) ;
                setState( IDLE , "<RetryHandlerActivated>" ) ;
                esay("Retry timer restarted");
            }else{
                sendReply( "StorageInfoReceiver" , errorValue , errorMessage ) ;
                removeUs() ;
            }

        }

     }
     public void selectPoolArrived( PoolMgrSelectPoolMsg reply )
            throws CacheException {

         say( "poolMgrGetPoolArrived : "+reply ) ;
         if( reply.getReturnCode() != 0 )
            throw new
            CacheException( reply.getReturnCode() , reply.getErrorObject().toString() ) ;

         if( ( ( _pool = reply.getPoolName() ) == null ) || ( _pool.equals("") ) )
            throw new
            CacheException( 33 , "No pools available" ) ;

         PoolIoFileMessage poolMessage  = null ;

         if( reply instanceof PoolMgrSelectReadPoolMsg ){
            poolMessage =
                  new PoolDeliverFileMessage(
                        _pool, 
                        _pnfsId ,
                        _protocolInfo ,
                        _storageInfo           ) ;
         }else if( reply instanceof PoolMgrSelectWritePoolMsg ){
            poolMessage =
                  new PoolAcceptFileMessage(
                        _pool, 
                        _pnfsId ,
                        _protocolInfo ,
                        _storageInfo           ) ;
         }else{
            throw new
            CacheException( 7 , "Illegal Message arrived : "+reply.getClass().getName() ) ;
         }

         poolMessage.setId( _sessionId ) ;  
         setState( IDLE , "<WaitingForPool...>" ) ;
         send( new CellPath( _pool ) , 
               poolMessage , 
               _pool_ok_timeout ,
               new PoolMessageHandler()  ) ;

         return ;
     }
     
  }
  ///////////////////////////////////////////////////////////////////////////
  //
  //   Handles message from the Pool (
  //
  private class PoolMessageHandler implements MessageTimerEvent {
  
     private PoolIoFileMessage           _poolIoFileMessage    = null ;
     private DoorTransferFinishedMessage _doorTransferFinished = null ;
     
     private boolean _retryRecommended = true ;
     private String  _errorMessage     = null ;
     private int     _errorValue       = 0 ;
     
     public void event( MessageEventTimer timer , 
                        Object eventObject ,
                        int    eventType             ){
        
        if( eventType == MessageEventTimer.MESSAGE_ARRIVED ){
           
           Object [] messageArray = (Object [])eventObject ;
           if( ( messageArray        == null )  || 
               ( messageArray.length == 0    )  || 
               ( messageArray.length >  2    )      ){
               
              esay( _errorMessage = "PANIC : system error in StorageInfoReceiver: 1" ) ;
              _errorValue = 221 ;  
              _retryRecommended = false ;
                            
           }else if( messageArray.length == 1 ){
           
              Object answer = ((CellMessage)messageArray[0]).getMessageObject() ;
              objectArrived( answer ) ;    
                     
           }else if( messageArray.length == 2 ){
              //
              // if we get two answers :
              // i)  NoRouteToCell : impossible
              // ii) one of them is a PoolIoFileMessage with rc == 0 ;
              //     (according to our understanding this must be the first,
              //      but who knows)
              //
              Object answer1 = ((CellMessage)messageArray[0]).getMessageObject() ;
              Object answer2 = ((CellMessage)messageArray[1]).getMessageObject() ;
              if( answer1 instanceof PoolIoFileMessage ){
                 _poolIoFileMessage =  (PoolIoFileMessage)answer1 ;
                 objectArrived( answer2 ) ;
              }else if( answer2 instanceof PoolIoFileMessage ){
                 _poolIoFileMessage =  (PoolIoFileMessage)answer2  ;
                 objectArrived( answer1 ) ;
              }else{
                 esay( _errorMessage = "PANIC : system error in StorageInfoReceiver: 3" ) ;
                 _errorValue = 223 ;  
                 _retryRecommended = false ;
              }
           }
        }else if( eventType == MessageEventTimer.TIMEOUT_EXPIRED ){
           _retryRecommended  = true ;
           _errorMessage = "PoolReply timed out" ;
           _errorValue   = 1 ;
        }
        if( _errorValue != 0 ){
            _lastErrorValue   = _errorValue ;
            _lastErrorMessage = _errorMessage ;
            if( _retry && _retryRecommended ){
                _timer.addTimer( Integer.valueOf( _sessionId ) ,
                                 null ,
                                 new RetryHandler() ,
                                 _retry_timer ) ;
                setState( IDLE , "<RetryHandlerActivated>" ) ;
                esay("Retry timer restarted");
            }else{
                sendReply( "StorageInfoReceiver" , _errorValue , _errorMessage ) ;
                removeUs() ;
            }

        }
        
     }
     private void objectArrived( Object answer ){
           
        if( answer instanceof NoRouteToCellException ){
        
           esay( _errorMessage = answer.toString() ) ;
           _errorValue = 666 ;  
           _retryRecommended = false ;
                         
        }else if( answer instanceof CacheException ){
        
           CacheException ce = ((CacheException)answer) ;
           _errorMessage     = ce.getMessage() ;
           _errorValue       = ce.getRc() ;
           
           switch( ce.getRc() ){
              case 4444 :
                _retryRecommended = true ;
                break ;
              default :
                _retryRecommended = false ;
           }
        }else if( answer instanceof Exception ){
        
           esay( _errorMessage = answer.toString() ) ;
           _errorValue = 1 ;  
           _retryRecommended = false ;   

        }else if( answer instanceof PoolIoFileMessage ){
        
           _poolIoFileMessage = (PoolIoFileMessage)answer ;
           say( "PoolIoFileMessage : rc = "+_poolIoFileMessage.getReturnCode() ) ;
           _errorMessage = _poolIoFileMessage.getErrorObject() != null ?
                           _poolIoFileMessage.getErrorObject().toString() : null ;
           _errorValue   = _poolIoFileMessage.getReturnCode() ;
           
           switch( _errorValue ){
              case 444 :
                 _retryRecommended = true ;
                 break ;
              case 0 :
                 if( _doorTransferFinished == null ){
                    _timer.reschedule( _pool_done_timeout ) ;
                 }else{
                    poolDone() ;
                 }
                 break ;
              default :
                 _retryRecommended = false ;
           }

        }else if( answer instanceof DoorTransferFinishedMessage ){
        
           _doorTransferFinished = (DoorTransferFinishedMessage)answer ;
           say( "DoorTransferFinishedMessage : rc = "+_doorTransferFinished.getReturnCode() ) ;

           _errorMessage = _doorTransferFinished.getErrorObject() != null ?
                           _doorTransferFinished.getErrorObject().toString() : null ;
           _errorValue   = _doorTransferFinished.getReturnCode() ;
           
           switch( _errorValue ){
              case 444 :
                 _retryRecommended = true ;
                 break ;
              case 0 :
                 if( _poolIoFileMessage == null ){
                    //
                    // this can't possibly happen, but we allow 10 seconds
                    // for the _poolIoFileMessage to arrive.
                    //
                    _timer.reschedule( 10000 ) ;
                 }else{
                    poolDone() ;
                 }
                 break ;
              default :
                 _retryRecommended = false ;
           }
              
        }else{
           esay( _errorMessage = "PANIC : system error in StorageInfoReceiver: 2" ) ;
           _errorValue = 222 ;  
           _retryRecommended = false ;

        }
     }
     private void poolDone(){
        setStatus("<done>") ;
        say( "Done") ;
        removeUs() ;
        sendReply( "poolDone" ,  0 , "" ) ;
     }                    
  }
  public String toString(){
     String pool = _pool == null ? "" : ( " ["+_pool+"] " ) ;
     if( _retryCounter != 0 ){
        return "io "+super.toString()+pool+" [retry="+_retryCounter+
                                ";v="+_lastErrorValue+
                                ";m="+_lastErrorMessage+"]" ;
     }else{
        return "io "+super.toString()+pool ;
     }
  }
}
