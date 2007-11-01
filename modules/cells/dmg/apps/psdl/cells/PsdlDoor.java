package dmg.apps.psdl.cells ;

import dmg.apps.psdl.vehicles.* ;

import  dmg.cells.nucleus.* ;
import  dmg.util.*;
import  java.util.* ;
import  java.io.* ;
import  java.net.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class PsdlDoor extends CellAdapter implements Runnable  {

   private CellNucleus _nucleus   = null ;
   private Thread      _worker    = null ;
   private Socket      _socket    = null ;
   private Object      _sendLock        = new Object() ;
   private Object      _lastRequestLock = new Object() ;
   private Hashtable   _requestHash     = new Hashtable() ;
   private Hashtable   _pendingHash     = new Hashtable() ;
   private CellPath    _pnfsMgr         = new CellPath( "PnfsMgr" ) ;
   private ObjectInputStream  _commandIn   = null ;
   private ObjectOutputStream _commandOut  = null ;
   private boolean            _commandOk   = false ;
   private Object             _commandLock = new Object() ;
   
   public PsdlDoor( String cellName , Socket socket )
          throws IOException {
   
      super( cellName , "" , false ) ;
      
      try{
         _socket     = socket ;
         _commandOut = new ObjectOutputStream( _socket.getOutputStream() ) ;
         _commandIn  = new ObjectInputStream( _socket.getInputStream() ) ;
         _commandOk  = true ;

         _nucleus = getNucleus() ;

         _worker  = new Thread( this ) ;
         _worker.start() ;

         useInterpreter( true ) ;
      }catch( IOException e ){
        start() ;
        kill() ;
        throw e ;
      }
      start() ;
      
   }
   public void run(){
      if( Thread.currentThread() == _worker ){
         boolean gracefulDeath = true ;

         while( true ){
            Object obj = null ;
            try{
               obj = _commandIn.readObject() ;
            }catch( EOFException e ){
               _nucleus.say( "Premature EOF from client" ) ;
               closeCommand() ;
               gracefulDeath = true ;
               break ;
            }catch( Exception e ){
               _nucleus.esay( "run : "+e ) ;
               closeCommand() ;
               gracefulDeath = false ;
               break ;
            }
            
            if( obj instanceof String ){
               _nucleus.say( "Exit requested by client" ) ;
               closeCommand() ;
               gracefulDeath = true ;
               break ;
            }else if( obj instanceof PsdlIoRequest ){
               requestArrived( (PsdlCoreRequest) obj ) ;
            }else{
               _nucleus.esay( "Unidentified Object arrived : "+obj.getClass() ) ;
            }
         
         }
         
         _doCleanUp( gracefulDeath ) ;
      
      }
   }
   private void closeCommand(){
      synchronized( _commandLock ){ 
         if( ! _commandOk )return ;
         _commandOk     = false ; 
         _nucleus.say( "Closing Socket" ) ;
         try{ _socket.close() ; }catch( Exception eee ){}
      }
   }
   private boolean writeObject( Object obj ){
      synchronized( _commandLock ){
         if( ! _commandOk )return false ;
         try{
            _commandOut.writeObject( obj  ) ;
         }catch( Exception ee ){
            esay( "writeObject failed : "+ee.toString() ) ;
            closeCommand() ;
         }
         return _commandOk ;
      }
   } 
   private void _doCleanUp( boolean ok ){
      say( "Starting cleanup phase" ) ;
      if( _requestHash.size() > 0 ){
         esay( "Cleanup found unack. requests ( waiting max. 10 seconds )" ) ;
         synchronized( _lastRequestLock ){
             long    startWaiting = System.currentTimeMillis() ;
             while( _requestHash.size() > 0 ){
               try{ _lastRequestLock.wait( 1000 ) ; }
               catch( InterruptedException ee ){}
               if( ( System.currentTimeMillis() - startWaiting ) > 10000 )break;
             }        
         }
         if( _requestHash.size() > 0 ){
            esay( "Last request didn't arrive ... dieing anyway" ) ;
         }else{
            esay( "Last request arrived ... we can die  now" ) ;
         }
         kill() ;
      }else{
         say( "Cleanup : all outstanding request satisfied" ) ;
         kill() ;
      }
   }
   private void requestArrived( PsdlCoreRequest req )  {
      say( "Request from client : "+req ) ;
      req.setRequestCommand( "init" ) ;
      try{

         synchronized( _sendLock ){
            CellMessage msg = new CellMessage( _pnfsMgr , req ) ;
                                   
            _nucleus.sendMessage( msg ) ;
            
            _pendingHash.put( msg.getUOID() , req ) ;
            _requestHash.put( req.getId()   , req ) ;
            
            _nucleus.say( "Request "+req.getId()+
                          " arrived and forwarded with UOID="+msg.getUOID() ) ;
         }
         
      }catch( Exception e ){

         if( e instanceof NoRouteToCellException ){
            req.setReturnValue( 11 , "No Route to "+_pnfsMgr ) ;
         }else{
            req.setReturnValue( 10 , e.toString() ) ;
         }
         _nucleus.esay( "requestArrived : sendMessage="+e.toString() ) ;
         writeObject( req ) ;
      }
   }  
   public String toString(){ 
       return " pending="+_requestHash.size()  ; 
   }
   
   public void getInfo( PrintWriter pw ){
     Enumeration e = _requestHash.elements() ;
     pw.println( "Outstanding Requests :" ) ;
     for( ; e.hasMoreElements() ; ){
         pw.println( e.nextElement().toString() ) ;
     }
   }
   public void   messageArrived( CellMessage msg ){
   
      Object obj = msg.getMessageObject() ;
      say( "Answer arrived : "+obj) ;
      PsdlCoreRequest req = null ;
      synchronized( _sendLock ){
         if( obj instanceof Exception ){
             Exception   e   = (Exception) obj ;
             req = (PsdlCoreRequest)_pendingHash.remove( msg.getLastUOID() ) ;
             if( req == null ){
                esay( "Exception with unknown UOID : "+msg.getLastUOID() ) ;
                return ;
             }
             _requestHash.remove( req.getId() ) ;
             if( e instanceof NoRouteToCellException ){
                req.setReturnValue( 11 , "No Route to "+_pnfsMgr  ) ;
             }else{
                req.setReturnValue( 10 , e.toString() ) ;
             }
             writeObject( req ) ;

         }else if( obj instanceof PsdlCoreRequest ){
             req = (PsdlCoreRequest) obj ;

             if( _requestHash.remove( req.getId() ) == null ){
                _nucleus.esay( "Didn't wait for : "+req ) ;
                return ;

             }
             writeObject( req ) ;
             synchronized( _lastRequestLock ){
                _lastRequestLock.notifyAll() ;
             }
         }
      }
     
     
   }

}
