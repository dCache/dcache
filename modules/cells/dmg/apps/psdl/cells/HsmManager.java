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
public class         HsmManager 
          extends    CellAdapter 
          implements Runnable           {

   private CellNucleus _nucleus     = null ;
   private String      _result      = null ;
   
   public HsmManager( String cellName , String cellArgs ){
      super( cellName , cellArgs , false ) ;
      
      _nucleus  = getNucleus() ;      
                  
      start() ;
      
   }
   public void run(){
   
   }
   public void cleanUp(){
   }
   public String ac_set_result_$_0_1( Args args ){
      if( args.argc() < 1 ){
          _result = null ;
          return "Result set to O.K." ;
      }else{
          _result = args.argv(0) ;
          return "Result set to : "+_result ;
      }
   }
   public void messageArrived( CellMessage msg ){
       Object obj = msg.getMessageObject() ;
       if( obj instanceof PsdlPutRequest ){
          //
          // the original put requests
          //
          putRequestArrived( msg , (PsdlPutRequest) obj ) ;
          //
       }else if( obj instanceof RemoveRequest ){
       }else if( obj instanceof NoRouteToCellException ){
       }else{
          esay( "Unidentified Object arrived : "+obj.getClass() ) ;
       }
     
   }
   public void putRequestArrived( CellMessage msg , PsdlPutRequest req ){
      if( req.getRequestCommand().equals("checkSpace") ){
          say( "Put request asks for spaceinfo : "+
               req.getHsmProperties().getHsmKey() ) ;
          if( _result != null ){
             req.setReturnValue( 44 , _result ) ;            
          }
          msg.revertDirection() ;
      }else if( req.getRequestCommand().equals("storeFile") ){
          say( "Put request request file storage to "+
               req.getHsmProperties().getHsmInfo() ) ;
          msg.nextDestination() ;
      }else{
          esay( "Unidentified put request arrived : "+req.getRequestCommand() ) ;
          return ;
      }
      try{
         sendMessage( msg ) ;
      }catch(Exception nrtc ){
         esay( "PANIC : can't send msg to "+msg.getDestinationPath() ) ;
      }     
   
   }
   public void messageToForward( CellMessage msg ){
       messageArrived( msg ) ;
      
   }
   
   
   
   
}
 
