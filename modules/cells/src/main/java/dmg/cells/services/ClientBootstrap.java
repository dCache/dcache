package dmg.cells.services ;

import dmg.cells.nucleus.* ;
import dmg.cells.network.* ;
import dmg.util.* ;

import java.net.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class ClientBootstrap
       implements  Cell , CellEventListener , StateEngine {

   private final static Logger _log =
       LoggerFactory.getLogger(ClientBootstrap.class);

   private InetAddress _address ;
   private int         _port ;
   private Gate        _finalGate = new Gate( false ) ; // closed gate
   private Gate        _routeGate = new Gate( false ) ;
   private CellNucleus _nucleus  ;
   private boolean     _routeAdded = false ;
   private StateThread _engine ;
   private String  []  _commands ;

   public ClientBootstrap( String cellName , String arguments )
          throws Exception {

      Args args = new Args( arguments ) ;
      if( args.argc() < 2 ) {
          throw new IllegalArgumentException("USAGE : ... <host> <port>");
      }

      InetAddress address = InetAddress.getByName( args.argv(0) ) ;
      int         port    = new Integer( args.argv(1) ).intValue() ;

      _ClientBootstrap( cellName , address , port ) ;


   }
   public void _ClientBootstrap( String cellName , InetAddress address , int port )
          throws Exception {

       _address = address ;
       _port    = port ;

       _nucleus = new CellNucleus( this , cellName ) ;
       _nucleus.addCellEventListener( this ) ;

       _engine  = new StateThread( this ) ;
       _engine.start() ;
   }

   private final static int RST_CREATE_TUNNEL  =  1 ;
   private final static int RST_TUNNEL_READY   =  2 ;
   private final static int RST_TUNNEL_FAILED  =  3 ;
   private final static int RST_ROUTE_READY    =  4 ;
   private final static int RST_MSG_RECV       =  5 ;
   private final static int RST_MSG_SENT_FAILED   =  6 ;
   private final static int RST_WAITING        =  7 ;

   private final static String [] __rst_state_names = {
      "<init>"  , "<creatingTunnel>" , "<addRoute>" ,
      "<tunnelFailed>" , "<sendingRequest>" ,
      "<runningCommands>" , "<msgSentFailed>"  ,
      "<waiting>"

   };
   private String getRunState(){
     int state = _engine.getState() ;
     if( state < 0 ) {
         return "<finished>";
     }
     if( state >= __rst_state_names.length ) {
         return "<Panic>";
     }
     return __rst_state_names[state] ;
   }
   public int runState( int state ){
      _log.info( "runState : in state <"+getRunState()+">" ) ;
      switch( state ){
        case 0 :
        return RST_CREATE_TUNNEL ;

        case RST_CREATE_TUNNEL :
          new RetryTunnel( "tunnel*" , _address , _port ) ;
          return RST_TUNNEL_READY ;
        case RST_TUNNEL_FAILED :
        case RST_MSG_SENT_FAILED :
        return -1 ;

        case RST_TUNNEL_READY :
           _routeGate.check() ;
           //
           // now we should have at least one route
           //

        return RST_ROUTE_READY ;


        case RST_ROUTE_READY :
        {
            try {
                CellPath path = new CellPath("config");
                CellMessage msg = new CellMessage(
                        path,
                        "config " + _nucleus.getCellDomainName());

                msg = _nucleus.sendAndWait(msg, 20000L);
                //
                // retry after timeout ( in any case )
                //
                if (msg == null) {
                    _log.info("runState : sendAndWait timed out");
                    return RST_MSG_SENT_FAILED;
                }

                Object answer = msg.getMessageObject();
                if (answer == null) {
                    _log.info("runState : null object received");
                    return RST_MSG_SENT_FAILED;
                }

                if (!(answer instanceof String[])) {
                    _log.info("runState : answer is " + answer);
                    _engine.setState(RST_WAITING);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ee) {
                    }
                    ;

                    return RST_ROUTE_READY;
                }

                _commands = (String[]) answer;

                return RST_MSG_RECV;

            } catch (NoRouteToCellException sme) {
                return RST_MSG_SENT_FAILED;
            } catch (InterruptedException sme) {
                return RST_MSG_SENT_FAILED;
            }
        }

        case RST_MSG_RECV :
        {
           //
           // _commands should contain the informations
           // from the configuration server.
           //
           CellShell shell = new CellShell( _nucleus ) ;
           for( int i = 0  ; i < _commands.length ; i++ ){
              _log.info( "runState : command : "+_commands[i] ) ;
              try{
                String answer = shell.command( _commands[i] ) ;
                _log.info( "runState : answer  : "+answer ) ;
              }catch( Exception eee ){}
           }
           _log.info( "runState : Command execution finished" ) ;
           return -1 ;
        }



      }
      return 0 ;


   }
   //
   // interface from Cell
   //
   public String toString(){
      return  _nucleus.getCellDomainName()+
              " Run State : "+getRunState()  ;
   }
   public String getInfo(){
      StringBuffer sb = new StringBuffer() ;
      sb.append( " Run State : "+getRunState()+"\n" ) ;
      return  sb.toString() ;
   }
   public void   messageArrived( MessageEvent me ){
     if( me instanceof LastMessageEvent ){
        _finalGate.open() ;
     }else{
        CellMessage msg  = me.getMessage() ;
        if( msg.isFinalDestination() ){
           Object      obj  = msg.getMessageObject() ;
           _log.info( "Msg arrived (f) : "+msg ) ;
           if( obj instanceof String ){
              String command = (String)obj ;
              if( command.length() < 1 ) {
                  return;
              }

           }
        }
     }
   }
   public void   prepareRemoval( KillEvent ce ){
     _finalGate.check() ;
     // this will remove whatever was stored for us
   }
   public synchronized void  routeAdded( CellEvent ce ){
      _log.info( "routeAdded : Got routing info" );
      if( ! _routeAdded  ){
         CellRoute route  = (CellRoute)ce.getSource() ;
         if( route.getRouteType() != CellRoute.DOMAIN ) {
             return;
         }

         Args args = new Args( "-default *@"+route.getDomainName() ) ;
         CellRoute defRoute =
              new CellRoute( args ) ;
         _log.info( "routeAdded : adding default : "+defRoute ) ;
         _nucleus.routeAdd( defRoute ) ;

         _routeAdded = true ;
         _routeGate.open() ;

      }
   }
   public void   exceptionArrived( ExceptionEvent ce ){
//     _log.info( " exceptionArrived "+ce ) ;
   }
   //
   // interface from CellEventListener
   //
   public void  cellCreated( CellEvent  ce ){
//     _log.info( " cellCreated "+ce ) ;
   }
   public void  cellDied( CellEvent ce ){
//     _log.info( " cellDied "+ce ) ;
   }
   public void  cellExported( CellEvent ce ){
//     _log.info( " cellExported "+ce ) ;
   }
   public void  routeDeleted( CellEvent ce ){
//     _log.info( " routeDeleted "+ce ) ;
   }

}
