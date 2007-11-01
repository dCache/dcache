package dmg.apps.psdl.cells ;

import  dmg.apps.psdl.vehicles.* ;
import  dmg.cells.nucleus.* ;
import  dmg.util.*;
import  java.util.* ;
import  java.io.* ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class         PoolManager 
          extends    CellAdapter 
          implements Runnable           {

   private CellNucleus _nucleus     = null ;
   private Hashtable   _poolHash    = new Hashtable() ;
   private String      _hsmMgrName  = "HsmMgr" ;
   
   public PoolManager( String cellName , String cellArgs ){
      super( cellName , cellArgs , false ) ;
      
      _nucleus  = getNucleus() ;      
                  
      start() ;
      
   }
   public String toString(){ 
       return "Pool Manager"; 
   }
   public void run(){
   }
   private void poolInfos( PrintWriter pw ){
      Enumeration e = _poolHash.elements() ;
      for( ; e.hasMoreElements() ; ){
      
          ((Pool)e.nextElement()).toWriter(pw);
      }
   }
   public void getInfo( PrintWriter pw ){
      super.getInfo( pw ) ;
      pw.println( " HsmManager    : "+_hsmMgrName ) ;
      poolInfos( pw ) ;
   }
   public void cleanUp(){
   }
   public void messageArrived( CellMessage msg ){
       Object obj = msg.getMessageObject() ;
       if( obj instanceof StateInfo ){
          //
          //  the alive messages from the related
          //  PoolObservers.
          //
          poolInfoArrived( msg , (StateInfo)obj ) ;
          //
       }else if( obj instanceof PsdlPutRequest ){
          //
          // the original put requests
          //
          putRequestArrived( msg , (PsdlPutRequest) obj ) ;
       }else if( obj instanceof RemoveRequest ){
          //
          // the remove requests are broadcasted to all
          // related PoolObservers.
          //
          broadcastRemove( msg  , (RemoveRequest)obj ) ;
          try{
             msg.revertDirection() ;
             sendMessage( msg ) ;
          }catch( Exception e ){
             esay( "PANIC : Couldn't revert remove message : "+e ) ;
          }
       }else if( obj instanceof NoRouteToCellException ){
          exceptionArrived( msg , (NoRouteToCellException) obj ) ;
       }else{
          esay( "Unidentified Object arrived : "+obj.getClass() ) ;
       }
     
   }
   public void messageToForward( CellMessage msg ){
       Object obj = msg.getMessageObject() ;
       if( obj instanceof PsdlPutRequest ){
           PsdlPutRequest req = (PsdlPutRequest)obj ;
           //
           // at this point we may receive
           //   - answer from HsmManager
           //        ok : go to next selected poolArea
           //        failed : rollback
           //   - answer from PoolObserver
           //        ok : simply forward
           //        failed : send to next PoolObserver
           //
           if( req.getRequestCommand().equals( "checkSpace" ) ){
              //
              // if hsmManager signals 'enough space' choose the
              // first selected pool, otherwise forward the 
              // failure to the door.
              //
              say( "Put request returned from HsmMgr : "+req.getReturnMessage() ) ;
              if( req.getReturnCode() == 0 ){
                 req.setRequestCommand("selectPools") ;
                 sendToNextPool( msg , (PsdlPutRequest)obj ) ;
              }else{
                 req.setRequestCommand("answer") ;
                 super.messageToForward( msg ) ;
              }
           }else{
              //
              // if everything is fine we simply forward the
              // answer, otherwise we try to select a different
              // pool.
              //
              req.setRequestCommand("answer") ;
              if( req.getReturnCode() == 0 ){
                 super.messageToForward( msg ) ;
              }else{
                 sendToNextPool( msg , (PsdlPutRequest)obj ) ;
              }
           }
       }
      
   }
   public void putRequestArrived( CellMessage msg , PsdlPutRequest req ){
       //
       //   mark the current path , so that we can restore it
       //   if the first selected pool doesn't match.
       //
       msg.markLocation() ;
       //
       //   get a list of matching pools and assign them to the request.
       //
       selectPool( msg , req ) ;
       //
       // send to the first choise
       //
       //       sendToNextPool( msg ,req ) ;
       //
       //  add the HsmManager name to the request and send it
       //
       req.setHsmManager( _hsmMgrName ) ;
       req.setRequestCommand( "checkSpace" ) ;
       sendHsmToManager( msg ,req ) ;
   }
   private void sendHsmToManager( CellMessage msg , PsdlHsmRequest req ){
      try{
          //
          // forward the message to the appropriate pool
          //
          String hsmManager = req.getHsmManager() ;
          say( "Sending put request to HsmManager "+hsmManager+" : "+req.getRequestCommand() ) ;
          msg.getDestinationPath().add( hsmManager ) ;
          msg.nextDestination() ;
          sendMessage( msg  ) ;
      }catch( Exception eee ){
          esay( "PANIC : can't send msg to "+msg.getDestinationPath() ) ;
      }
   }
   private void selectPool( CellMessage msg , PsdlPutRequest req ){
      //
      Enumeration e = _poolHash.elements() ;
      boolean found = false ;
      Pool     pool = null ;
      PoolInfo info = null ;
      String   hsmKey = req.getHsmProperties().getHsmKey() ;
      Vector   pools  = new Vector() ;
      //
      // try to find a matching pool
      //
      for( ; e.hasMoreElements() ; ){
         pool = (Pool)e.nextElement() ;
         info = pool.getInfo() ;
         //
         // is active
         //
         if( ! info.isActive() )continue ;
         //
         // correct hsmKey
         //   
         String [] keys = info.getHsmKeys() ;
         //
         // or does it match ( no keys means ok for all ) ;
         //
         int i = 0 ;
         for( ; ( i < keys.length ) && 
                ! Formats.match( keys[i] , hsmKey ) ; i++ ) ;

         if( ( keys.length != 0 ) &&
             ( i == keys.length )     )continue ;
          
         if( ( info.getOperations() & 0x1 ) == 0 )continue ;
         
         pools.addElement( pool ) ; 

      }
      if( pools.size() == 0 ){
         sendProblem( msg , 42 , "No pools matching selected attr." ) ;
         return ;
      }
      //
      // now sort the list of selected pools according to their
      // priorities.
      //
      // XXX bubblesort 
      Pool [] sorter = new Pool[pools.size()] ;
      pools.copyInto( sorter ) ;
      for( int i = 1 ; i < sorter.length ; i++ ){
         for( int j = 0 ; j < (sorter.length-i) ; j++ ){
            if( sorter[j].getInfo().getPriority() <=
                sorter[j+1].getInfo().getPriority()   )continue;
                
            Pool tmp = sorter[j] ;
            sorter[j] = sorter[j+1] ;
            sorter[j+1] = tmp ;
         }
      }
      String [] selectedPools = new String[sorter.length] ;
      for( int i = 0 ; i < selectedPools.length ; i++ )
         selectedPools[i] = sorter[i].getName() ;
      
      //
      //  assign the selected pools to the request block.
      //
      req.setSelectedPools( selectedPools ) ;
      
      StringBuffer sb = new StringBuffer() ;
      sb.append( "Selected Pools : " ) ;
      for( int i = 0 ; i < selectedPools.length ; i++ )
         sb.append( selectedPools[i] ).append( "," ) ;
      say( sb.toString() ) ;
      
   }
   private void sendToNextPool( CellMessage msg , PsdlPutRequest req ){
      msg.resetLocation() ;
      String poolName = req.nextSelectedPool() ;
      if( poolName == null ){
          sendProblem( msg , 44 , "No pools left" ) ;
          return ;
      }
      Pool pool = (Pool)_poolHash.get( poolName ) ;
      if( pool == null ){
         sendProblem( msg , 43 , "PANIC : selected pool not found"+poolName ) ;
         return ;
      }
      try{
          //
          // forward the message to the appropriate pool
          //
          say( "next Pool with correct attr. : "+pool ) ;
          msg.getDestinationPath().add( pool.getPath() ) ;
          msg.nextDestination() ;
          say( "Next pool selected : "+pool.getPath() ) ;
          sendMessage( msg  ) ;
      }catch( Exception eee ){
          esay( "PANIC : can't send msg to "+msg.getDestinationPath() ) ;
      }
   }
   private void sendProblem( CellMessage msg , int rc , String note ){
       PsdlCoreRequest req  = (PsdlCoreRequest)msg.getMessageObject() ;
       _nucleus.esay( "PROBLEM : ("+rc+") "+note+ " req : "+req  ) ;
       req.setReturnValue( rc , note ) ;
       msg.revertDirection() ;
       try{
          _nucleus.sendMessage( msg ) ;
       }catch( Exception e ){
          _nucleus.esay( "PANIC : Can't send answer to : "+
                          msg.getDestinationPath()+" : "+e ) ;
       }
   }
   private void exceptionArrived( CellMessage msg , 
                                  NoRouteToCellException exc ){
        
       esay( "PANIC : got exceptin for : "+msg.getDestinationPath()+" : "+exc);
       /*                          
       try{
          msg.revertDirection() ;
          sendMessage( msg ) ;
       }catch( Exception e ){
          esay( "PANIC : Can't inform client of NoRoute exception : "+
                msg.getDestinationPath() ) ;
       }
       */
   }
   private void broadcastRemove( CellMessage msg , RemoveRequest req ){
      Enumeration e = _poolHash.elements() ;
      for( ; e.hasMoreElements() ; ){
         Pool pool = (Pool)e.nextElement() ;
         CellMessage broadcast = 
             new CellMessage( pool.getPath() , req ) ;
         try{
            sendMessage( broadcast ) ;
         }catch( Exception iie ){
            esay( "PANIC : can't broadcast remove request to pool "+
                 pool.getName()+" exc: "+iie ) ;               
         }
      }
      return ;
   }
   public String ac_set_hsmmgr_$_1( Args args ){
       _hsmMgrName = args.argv(0) ;
       return "" ;
   }
   //
   //  pool info exchange
   //
   private class Pool {
       public PoolInfo _poolInfo ;
       public CellPath _path ;
       
       public Pool( PoolInfo info , CellPath path ){
          _poolInfo = info ;
          _path     = path ;
       }
       public PoolInfo getInfo(){ return _poolInfo ; }
       public CellPath getPath(){ return _path ; }
       public String   getName(){ return _poolInfo.getName() ; }
       public String   toString(){
          return _poolInfo.toString()+" : "+_path.toString() ;
       }
       public void toWriter( PrintWriter pw ){
          _poolInfo.toWriter( pw ) ;
          pw.println( "  Path        : "+_path) ;
       }
   
   }
   private void poolInfoArrived( CellMessage msg , StateInfo info ){
//      say( "Info arrived : "+info ) ;
      if( info.isUp() ){
         _poolHash.put( info.getName() , 
                        new Pool( (PoolInfo)info ,
                                  msg.getSourcePath() ) ) ;
      }else{
         say( "Got DOWN info for "+info ) ;
         _poolHash.remove( info.getName() ) ;
      }
   }

}
