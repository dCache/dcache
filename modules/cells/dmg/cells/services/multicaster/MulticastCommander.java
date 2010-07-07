package dmg.cells.services.multicaster ;

import dmg.cells.nucleus.* ;
import dmg.util.* ;

import java.util.* ;
import java.io.*;

public class MulticastCommander extends CellAdapter {
   private CellNucleus _nucleus = null ;
   private Args        _args    = null ;
   private CellPath    _path    = new CellPath("mc") ;
   public MulticastCommander( String name , String args )throws Exception {
       super( name , args , false ) ;
       _nucleus = getNucleus() ;
       _args    = getArgs() ;

       start() ;
   }
   public void messageToForward( CellMessage msg ){
        CellPath source = msg.getSourcePath() ;

        msg.nextDestination() ;

        try{
           sendMessage( msg ) ;
        }catch( NoRouteToCellException nrtc ){
           esay( "NoRouteToCell in messageToForward : "+nrtc ) ;
           esay( "Sending NoRouteToCellt to : "+source ) ;
           source.revert() ;
           try{
              sendMessage( new CellMessage( source , nrtc ) ) ;
           }catch(Exception ee ){
              esay( "can't return NoRouteToCell to : "+source ) ;
           }
        }catch( Exception eee ){
           esay( "Exception in messageToForward : "+eee ) ;
        }
   }
   public String hh_set_path = "<MulticastCell>" ;
   public String ac_set_path_$_1( Args args ){
       _path = new CellPath(args.argv(0)) ;
       return "" ;
   }
   public String hh_register = "<className> <instanceName>" ;
   public String ac_register_$_2( Args args )throws Exception {
      String className    = args.argv(0) ;
      String instanceName = args.argv(1) ;
      MulticastRegister register =
         new MulticastRegister(  className , instanceName  ) ;

      CellMessage thisMsg = getThisMessage() ;
      thisMsg.getDestinationPath().add( _path ) ;
      thisMsg.nextDestination() ;
      thisMsg.setMessageObject( register ) ;
      sendMessage( thisMsg ) ;
      return ""  ;
   }
   public String hh_send = "<className> <instanceName> <message>" ;
   public String ac_send_$_3( Args args )throws Exception {
      String className    = args.argv(0) ;
      String instanceName = args.argv(1) ;
      String info         = args.argv(2) ;
      MulticastMessage message =
         new MulticastMessage(  className , instanceName , info ) ;

      CellMessage thisMsg = getThisMessage() ;
      thisMsg.getDestinationPath().add( _path ) ;
      thisMsg.nextDestination() ;
      thisMsg.setMessageObject( message ) ;
      sendMessage( thisMsg ) ;
      return ""  ;
   }
   public String hh_close = "<className> <instanceName>" ;
   public String ac_close_$_2( Args args )throws Exception {
      String className    = args.argv(0) ;
      String instanceName = args.argv(1) ;
      MulticastClose close =
         new MulticastClose(  className , instanceName ) ;

      sendMessage( new CellMessage( _path , close )  ) ;
      return ""  ;
   }
   public String hh_unregister = "<className> <instanceName>" ;
   public String ac_unregister_$_2( Args args )throws Exception {
      String className    = args.argv(0) ;
      String instanceName = args.argv(1) ;
      MulticastUnregister unreg =
         new MulticastUnregister(  className , instanceName ) ;

      sendMessage( new CellMessage( _path , unreg )  ) ;
      return ""  ;
   }
   public String hh_open =
     "<className> <instanceName> [<detail>] [-overwrite]" ;
   public String ac_open_$_2_3( Args args )throws Exception {
      String className    = args.argv(0) ;
      String instanceName = args.argv(1) ;
      String detail       = args.argc() == 2 ? null : args.argv(2) ;
      boolean overwrite   = args.getOpt("overwrite") != null ;
      MulticastOpen open = new MulticastOpen(
             className , instanceName , detail ) ;
      open.setOverwrite(overwrite);

      CellMessage thisMsg = getThisMessage() ;
      thisMsg.getDestinationPath().add( _path ) ;
      thisMsg.nextDestination() ;
      thisMsg.setMessageObject( open ) ;
      CellMessage reply = sendAndWait( thisMsg  , 5000 ) ;
      if( reply == null ){
          return "Reply timed out" ;
      }else{
          return reply.getMessageObject().toString() ;
      }

   }
}
