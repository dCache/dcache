package dmg.cells.services.multicaster ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.Reply;

import org.dcache.util.Args;

public class MulticastCommander extends CellAdapter {

   private final static Logger _log =
       LoggerFactory.getLogger(MulticastCommander.class);

   private CellPath    _path    = new CellPath("mc") ;
   public MulticastCommander( String name , String args )
   {
       super(name, args);
       start() ;
   }

   @Override
   public void messageToForward( CellMessage msg ){
        msg.nextDestination() ;

        try{
           sendMessage( msg ) ;
        }catch(RuntimeException eee ){
           _log.warn( "Exception in messageToForward : "+eee ) ;
        }
   }
   public static final String hh_set_path = "<MulticastCell>" ;
   public String ac_set_path_$_1( Args args ){
       _path = new CellPath(args.argv(0)) ;
       return "" ;
   }

   public static final String hh_register = "<className> <instanceName>";
   public Reply ac_register_$_2(Args args)
   {
      String className = args.argv(0);
      String instanceName = args.argv(1);
      MulticastRegister register = new MulticastRegister(className, instanceName);
      return (endpoint, envelope) -> {
         envelope.getDestinationPath().add(_path);
         envelope.nextDestination();
         envelope.setMessageObject(register);
         endpoint.sendMessage(envelope);
      };
   }

   public static final String hh_send = "<className> <instanceName> <message>" ;
   public Reply ac_send_$_3( Args args )
   {
      String className = args.argv(0);
      String instanceName = args.argv(1);
      String info = args.argv(2);
      MulticastMessage message = new MulticastMessage(className, instanceName, info);
      return (endpoint, envelope) -> {
         envelope.getDestinationPath().add(_path);
         envelope.nextDestination();
         envelope.setMessageObject(message);
         endpoint.sendMessage(envelope);
      };
   }

   public static final String hh_close = "<className> <instanceName>" ;
   public String ac_close_$_2( Args args ){
      String className    = args.argv(0) ;
      String instanceName = args.argv(1) ;
      MulticastClose close =
         new MulticastClose(  className , instanceName ) ;

      sendMessage( new CellMessage( _path , close )  ) ;
      return ""  ;
   }
   public static final String hh_unregister = "<className> <instanceName>" ;
   public String ac_unregister_$_2( Args args ){
      String className    = args.argv(0) ;
      String instanceName = args.argv(1) ;
      MulticastUnregister unreg =
         new MulticastUnregister(  className , instanceName ) ;

      sendMessage( new CellMessage( _path , unreg )  ) ;
      return ""  ;
   }
   public static final String hh_open =
     "<className> <instanceName> [<detail>] [-overwrite]" ;
   public Reply ac_open_$_2_3(Args args)
   {
      String className = args.argv(0);
      String instanceName = args.argv(1);
      String detail = (args.argc() == 2) ? null : args.argv(2);
      boolean overwrite = args.hasOption("overwrite");
      MulticastOpen open = new MulticastOpen(className, instanceName, detail);
      open.setOverwrite(overwrite);

      return (endpoint, envelope) -> {
         envelope.getDestinationPath().add(_path);
         envelope.nextDestination();
         envelope.setMessageObject(open);
         endpoint.sendMessage(envelope);
      };
   }
}
