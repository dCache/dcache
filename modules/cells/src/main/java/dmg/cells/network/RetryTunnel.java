package dmg.cells.network ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellDomainInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.CellTunnel;
import dmg.cells.nucleus.CellTunnelInfo;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.KillEvent;
import dmg.cells.nucleus.LastMessageEvent;
import dmg.cells.nucleus.MessageEvent;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.RoutedMessageEvent;
import dmg.util.Gate;
import dmg.util.StateEngine;
import dmg.util.StateThread;

import org.dcache.util.Args;
import org.dcache.util.Version;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class RetryTunnel implements Cell,
                                    Runnable,
                                    CellTunnel,
                                    StateEngine  {

   private final static Logger _log =
       LoggerFactory.getLogger(RetryTunnel.class);

   private InetAddress  _address;
   private int          _port;
   private CellNucleus  _nucleus;
   private Thread       _receiverThread;
   private final Object       _receiverLock    = new Object();

   private BlockingQueue<CellMessage>         _messageArrivedQueue = new LinkedBlockingQueue<>() ;
   private Gate         _finalGate           = new Gate(false) ;
   private ObjectInputStream  _input;
   private ObjectOutputStream _output;
   private Socket          _socket ;
   private String          _mode              = "None" ;
   private CellRoute       _route;
   private CellDomainInfo  _remoteDomainInfo;
   private StateThread     _engine;
   private long            _connectionStarted;
   private int             _connectionRetries;
   //
   // some statistics
   //
   private int  _connectionRequests;
   private int  _messagesToTunnel;
   private int  _messagesToSystem;

   private final static int CST_CONNECTING    =  1 ;
   private final static int CST_CON_TIMEOUT   =  2 ;
   private final static int CST_CON_FAILED    =  3 ;
   private final static int CST_CONNECTED     =  4 ;
   private final static int CST_PROT_START    =  5 ;
   private final static int CST_PROT_TIMEOUT  =  6 ;
   private final static int CST_PROT_FAILED   =  7 ;
   private final static int CST_PROT_OK       =  8 ;
   private final static int SST_SEND_READY    =  9 ;
   private final static int SST_SENDING       = 10 ;
   private final static int SST_SEND_TIMEOUT  = 11 ;
   private final static int SST_SEND_FAILED   = 12 ;
   private final static int SST_RECV_FAILED   = 13 ;
   private final static int CST_SHUTDOWN      = 14 ;

   private final static String [] _cst_states = {

      "<init>"         , "<connecting>"  , "<con_timeout>" ,
      "<con_failed>"   , "<connected>"   , "<prot_start>" ,
      "<prot_timeout>" , "<prot_failed>" , "<prot_ok>" ,
      "<send_ready>"   , "<sending>"     , "<send_timeout>" ,
      "<send_failed>"  , "<recv_failed>"

   } ;
   private final Version version = Version.of(this);

   public RetryTunnel( String cellName , Socket socket ) {

      _mode     = "Accepted" ;
      _socket   = socket ;
      _nucleus  = new CellNucleus( this , cellName ) ;

      _engine   = new StateThread( this ) ;
      _engine.start() ;

   }
   public RetryTunnel( String cellName , String argString )
          throws UnknownHostException {

      Args args = new Args( argString ) ;
      if( args.argc() < 2 ) {
          throw new
                  IllegalArgumentException(
                  "Usage : RetryTunnel <host> <port>");
      }


      _RetryTunnel( cellName ,
                     args.argv(0) ,
              new Integer(args.argv(1))) ;
   }
   public RetryTunnel( String cellName , String host , int port )
          throws UnknownHostException {
      this( cellName , InetAddress.getByName( host ) , port ) ;
   }
   private void _RetryTunnel( String cellName , String host , int port )
          throws UnknownHostException {
      _mode    = "Connection" ;

      _address =  InetAddress.getByName( host );
      _port    = port ;

      _nucleus = new CellNucleus( this , cellName ) ;

      _engine   = new StateThread( this ) ;
      _engine.start() ;


   }
   public RetryTunnel( String cellName , InetAddress address , int port ) {
      _mode    = "Connection" ;

      _address = address ;
      _port    = port ;

      _nucleus = new CellNucleus( this , cellName ) ;

      _engine   = new StateThread( this ) ;
      _engine.start() ;
   }
   @Override
   public void run(){
      if( Thread.currentThread() == _receiverThread ){

        try{
           Object obj ;
           while( ( ( obj = _input.readObject() ) != null ) && ! Thread.interrupted() ){
              CellMessage msg = (CellMessage) obj ;
              _log.info( "receiverThread : Message from tunnel : "+msg ) ;
              try{
                  _nucleus.sendMessage(msg, true, true);
                  _messagesToSystem ++ ;
              }catch( NoRouteToCellException nrtce ){
                 _log.info( "receiverThread : Exception in sendMessage : "+nrtce ) ;
              }
           }
        }catch( Exception ioe ){
           _log.info( "receiverThread : Exception in readObject : "+ioe ) ;
           if( _mode.equals("Connection") ){
              _engine.setState( SST_RECV_FAILED ) ;
           }else{
              _log.info( "receiverThread : Initiating kill sequence... " ) ;
              _nucleus.kill() ;
           }
        }


      }

   }
   private String _printState(){
      int state = _engine.getState() ;
      if( ( state < 0 ) || ( state >= _cst_states.length ) ) {
          return "<Unknown>";
      }
      return _cst_states[state] ;
   }
   @Override
   public int runState( int state ){

     long now = new Date().getTime() ;

     _log.info( " runState : "+_printState() ) ;

     switch( state ){
       case 0 :
       if( _mode.equals("Connection") ) {
           return CST_CONNECTING;
       }
       if( _mode.equals("Accepted")   ) {
           return CST_CONNECTED;
       }
       return -1 ; // kind of panic

       case CST_CONNECTING :
         _engine.setState( CST_CONNECTING ,
                           20 ,  /* seconds timeout */
                           CST_CON_TIMEOUT     ) ;
         _connectionStarted = now ;
         try{
            _connectionRequests++ ;
            _socket  = new Socket( _address , _port ) ;
            _engine.setState( CST_CONNECTED ) ;
         }catch( Exception se ){
            _engine.setState( CST_CON_FAILED ) ;
         }
       break ;

       case CST_CON_FAILED :
          if( _mode.equals("Accepted")   ) {
              return CST_SHUTDOWN;
          }
       case CST_CON_TIMEOUT :
       {
         int diff = (int)( now - _connectionStarted ) ;
         diff     = 30 - diff ;
         if( diff > 0 ){
           try{ Thread.sleep(diff*1000);}
           catch( Exception se ){}
         }
         _connectionRetries ++ ;

         return CST_CONNECTING ;
       }


       case CST_CONNECTED :
       {
         _engine.setState( CST_PROT_START,
                           20 ,  /* seconds timeout */
                           CST_PROT_TIMEOUT     ) ;
         try{
            _makeStreams() ;
            _engine.setState( CST_PROT_OK) ;
         }catch( Exception mse ){
            _engine.setState( CST_PROT_FAILED ) ;

         }

       }
       break ;

       case CST_PROT_FAILED :
       case CST_PROT_TIMEOUT :
          try{ _socket.close() ; }catch( Exception ce ){}
       return CST_CON_FAILED ;

       case CST_PROT_OK :
         synchronized( _receiverLock ){
            if( _receiverThread != null ) {
                _receiverThread.interrupt();
            }
            _receiverThread = new Thread( this ) ;
            _receiverThread.start() ;
            _route = new CellRoute(
                         _remoteDomainInfo.getCellDomainName() ,
                         _nucleus.getCellName() ,
                         CellRoute.DOMAIN ) ;
            _nucleus.routeAdd( _route ) ;
            _log.info( " engine : Route added : "+_route );
         }
       return  SST_SEND_READY ;

       case SST_SEND_READY :
       {
          Object msg = _messageArrivedQueue.poll() ;
          _engine.setState( SST_SENDING , 10 , SST_SEND_TIMEOUT ) ;
          try{
             _output.writeObject( msg ) ;
             _output.flush();
             _output.reset() ;
             _messagesToTunnel++ ;
             _engine.setState( SST_SEND_READY ) ;
          }catch( Exception we ){
             _engine.setState( SST_SEND_FAILED ) ;
          }
       }
       break ;
       case SST_RECV_FAILED :
       case SST_SEND_FAILED :
       case SST_SEND_TIMEOUT :
          try{ _socket.close() ; }catch( IOException ce ){}
          removeRoute() ;
       return CST_CON_FAILED ;

       case CST_SHUTDOWN :
          _nucleus.kill() ;
       return -1 ;

     }
     return 0 ;

   }

   @Override
   public CellTunnelInfo getCellTunnelInfo(){
       return new CellTunnelInfo( _nucleus.getCellName() ,
               new CellDomainInfo(_nucleus.getCellDomainName()),
                                 _remoteDomainInfo ) ;

   }
   private void _makeStreams() throws Exception {
      _output  = new ObjectOutputStream( _socket.getOutputStream() ) ;
      if( _output == null ){
          throw new IOException( "OutputStream == null" ) ;
      }
      _input   = new ObjectInputStream(  _socket.getInputStream() ) ;
      if( _input == null ){
          _output.close() ;
          throw new IOException( "InputStream == null" ) ;
      }
       _output.writeObject(new CellDomainInfo(_nucleus.getCellDomainName())) ;
      Object obj = _input.readObject() ;
      if( obj == null ) {
          throw new IOException("Premature EOS encountered");
      }
      _remoteDomainInfo = (CellDomainInfo) obj ;
   }
   @Override
public String toString(){
      if( _remoteDomainInfo == null ) {
          return "M=" + _mode + ";S=" + _printState();
      } else {
          return "M=" + _mode +
                  ";S=" + _printState() +
                  ";P=" + _remoteDomainInfo.getCellDomainName();
      }
   }
   @Override
   public String getInfo(){
     StringBuilder sb = new StringBuilder() ;
     sb.append("Simple Tunnel : ").append(_nucleus.getCellName()).append("\n");
     sb.append("Mode          : ").append(_mode).append("\n");
     sb.append("Status        : ").append(_printState()).append("\n");
     sb.append("con. Requests : ").append(_connectionRequests).append("\n");
     sb.append("Msg Queued    : ").append(_messageArrivedQueue.size())
             .append("\n");
     sb.append("-> Tunnel     : ").append(_messagesToTunnel).append("\n");
     sb.append("-> Domain     : ").append(_messagesToSystem).append("\n");
     if( _remoteDomainInfo == null ) {
         sb.append("Peer          : N.N.\n");
     } else {
         sb.append("Peer          : ")
                 .append(_remoteDomainInfo.getCellDomainName()).append("\n");
     }

     return sb.toString() ;
   }
   @Override
   public void   messageArrived( MessageEvent me ){
//     _log.info( "message Arrived : "+me ) ;

     if( me instanceof RoutedMessageEvent ){
        CellMessage msg = me.getMessage() ;
        _log.info( "messageArrived : queuing "+msg ) ;
        try {
            _messageArrivedQueue.put( msg ) ;
        } catch (InterruptedException e) {
           // forced by Blocking Queue interface
        }
     }else if( me instanceof LastMessageEvent ){
        _log.info( "messageArrived : opening final gate" ) ;
        _finalGate.open() ;
     }else{
        _log.info( "messageArrived : dumping junk message "+me ) ;
     }

   }
   private void removeRoute(){
     synchronized( _receiverLock ){
         if( _route != null ){
            _log.info( "removeRoute : removing route" ) ;
            _nucleus.routeDelete( _route ) ;
            _route = null ;
         }
     }
   }
   @Override
   public synchronized void   prepareRemoval( KillEvent ce ){

     _log.info( "prepareRemoval : initiated "+ce ) ;
     _log.info( "prepareRemoval : waiting for final Gate to open" ) ;
     _finalGate.check() ;
     _log.info( "prepareRemoval : final gate passed -> closing" ) ;
     _engine.stop() ;
     synchronized( _receiverLock ){
         if( _receiverThread != null ) {
             _receiverThread.interrupt();
         }
         removeRoute() ;
     }
     try{
         _input.close();
         _output.close() ;
         _socket.close() ;
     }catch( Exception nsea ){
         _log.info( "prepareRemoval : Problem while closing : "+nsea ) ;
     }
   }
   @Override
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.info( "exceptionArrived : "+ce ) ;
   }

   @Override
   public CellVersion getCellVersion()
   {
       return new CellVersion(version);
   }
}
