package dmg.cells.network ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

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

import org.dcache.util.Args;
import org.dcache.util.Version;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class SimpleTunnel implements Cell, Runnable, CellTunnel {

   private final static Logger _log =
       LoggerFactory.getLogger(SimpleTunnel.class);

   private CellNucleus  _nucleus;
   private Thread       _senderThread;
   private Thread       _receiverThread;
   private Thread       _connectorThread;
   private ObjectInputStream  _input;
   private ObjectOutputStream _output;
   private Socket       _socket ;
   private String       _state           = "Not Initialized" ;
   private String       _mode            = "None" ;
   private CellRoute       _route;
   private boolean         _ready;
   private final Object          _readyLock    = new Object() ;
   private CellDomainInfo  _remoteDomainInfo;
   private final Version version = Version.of(this);

   public SimpleTunnel( String cellName , String argString )
          throws Exception {

      Args args = new Args( argString ) ;
      if( args.argc() < 2 ) {
          throw new IllegalArgumentException("Wrong Usage");
      }


      _SimpleTunnel( cellName ,
                     args.argv(0) ,
              new Integer(args.argv(1))) ;
   }
   public SimpleTunnel( String cellName , String host , int port )
          throws Exception {
      _SimpleTunnel( cellName , host , port ) ;
   }
   private void _SimpleTunnel( String cellName , String host , int port )
          throws Exception {

      InetAddress address = InetAddress.getByName( host ) ;

      _socket  = new Socket( address , port ) ;
      _mode    = "Connection" ;
      _nucleus = new CellNucleus( this , cellName ) ;

      _connectorThread = _nucleus.newThread( this , "Connector" ) ;
      _connectorThread.start() ;


   }
   public SimpleTunnel( String cellName , Socket socket ){

      _mode    = "Acception" ;
      _nucleus = new CellNucleus( this , cellName ) ;


      _socket  = socket ;

      _connectorThread = _nucleus.newThread( this , "Connector" ) ;
      _connectorThread.start() ;



   }
   @Override
   public CellTunnelInfo getCellTunnelInfo(){
       return new CellTunnelInfo( _nucleus.getCellName() ,
               new CellDomainInfo(_nucleus.getCellDomainName()),
                                 _remoteDomainInfo ) ;

   }
   private void _connector() throws Exception {
       _output.writeObject(new CellDomainInfo(_nucleus.getCellDomainName())) ;
       Object obj = _input.readObject() ;
       if( obj == null ) {
           throw new IOException("Premature EOS encountered");
       }
       _remoteDomainInfo = (CellDomainInfo) obj ;
   }
   private void _acceptor() throws Exception {
       _output.writeObject(new CellDomainInfo(_nucleus.getCellDomainName())) ;
       Object obj = _input.readObject() ;
       if( obj == null ) {
           throw new IOException("Premature EOS encountered");
       }
       _remoteDomainInfo = (CellDomainInfo) obj ;
    }
   private void _makeStreams() throws IOException {
      _output  = new ObjectOutputStream( _socket.getOutputStream() ) ;
      if( _output == null ){
          throw new IOException( "OutputStream == null" ) ;
      }
      _input   = new ObjectInputStream(  _socket.getInputStream() ) ;
      if( _input == null ){
          _output.close() ;
          throw new IOException( "InputStream == null" ) ;
      }
   }
   @Override
   public void run(){
      if( Thread.currentThread() == _connectorThread ){
        _state = "Initializing" ;
         try{
           _log.info( "Creating Streams in "+_mode+" Mode" ) ;
           _makeStreams()  ;
           _log.info( "Streams created" ) ;
           _log.info( "Running "+_mode+" Protocol" ) ;
           if( _mode.equals("Acception" )) {
               _acceptor();
           } else {
               _connector();
           }
           _log.info( "Protocol ready ("+_remoteDomainInfo+")" ) ;

         }catch( Exception nse ){
           _log.info( " Problem in Initial Protocol : "+nse ) ;
           try{_socket.close() ;}catch(Exception ee){}
             _nucleus.kill() ;
           return ;
         }
         _log.info( "Starting I/O threads " ) ;
         _receiverThread = _nucleus.newThread( this , "Receiver" ) ;
         _receiverThread.start() ;
         _senderThread = _nucleus.newThread( this , "Sender" ) ;
         _senderThread.start() ;

         _route = new CellRoute( _remoteDomainInfo.getCellDomainName() ,
                                 _nucleus.getCellName() ,
                                 CellRoute.DOMAIN ) ;
         _log.info( "Route added : "+_route );
         _nucleus.routeAdd( _route ) ;
        _state = "Active" ;

      }else if( Thread.currentThread() == _receiverThread ){
        try{
           Object obj ;
           while( ( obj = _input.readObject() ) != null ){
              CellMessage msg = (CellMessage) obj ;
              _log.info( " Message from tunnel : "+msg ) ;
              try{
                  _nucleus.sendMessage(msg, true, true);
              }catch( NoRouteToCellException nrtce ){
                 _log.info( "Exception while resending message : "+nrtce ) ;
              }
           }
        }catch( Exception ioe ){
           _log.info( "Exception while receiving message : "+ioe ) ;
           _nucleus.kill() ;
        }
      }else if( Thread.currentThread() == _senderThread ){
      }
   }
   public String toString(){
      if( _remoteDomainInfo == null ) {
          return "M=" + _mode + ";S=" + _state;
      } else {
          return "M=" + _mode + ";S=" + _state + ";P=" + _remoteDomainInfo
                  .getCellDomainName();
      }
   }
   @Override
   public String getInfo(){
     StringBuilder sb = new StringBuilder() ;
     sb.append("Simple Tunnel : ").append(_nucleus.getCellName()).append("\n");
     sb.append("Mode          : ").append(_mode).append("\n");
     sb.append("Status        : ").append(_state).append("\n");
     if( _remoteDomainInfo == null ) {
         sb.append("Peer          : N.N.\n");
     } else {
         sb.append("Peer          : ").append(_remoteDomainInfo
                 .getCellDomainName()).append("\n");
     }

     return sb.toString() ;
   }
   @Override
   public void   messageArrived( MessageEvent me ){
//     _log.info( "message Arrived : "+me ) ;
     if( me instanceof RoutedMessageEvent ){
       try{

          CellMessage msg = me.getMessage() ;
          _log.info( "Message tunneling : "+msg ) ;
          _output.writeObject( msg ) ;
          _output.flush();


       }catch( Exception ioe ){
          _log.info( "Exception while sending message : "+ioe ) ;
       }
     }else if( me instanceof LastMessageEvent ){
        _log.info( "Got last message ; releasing lock " ) ;
        synchronized( _readyLock ){
            _ready = true ;
            _readyLock.notifyAll();
        }
     }else{
     }

   }
   @Override
   public synchronized void   prepareRemoval( KillEvent ce ){
     _state = "Removing" ;
     _log.info( "PrepareRemoval initiated"+ce ) ;
     _log.info( "PrepareRemoval : removing route" ) ;
     if( _route != null ) {
         _nucleus.routeDelete(_route);
     }
     _route = null ;
     synchronized( _readyLock ){
        if( ! _ready ){
           _log.info( "PrepareRemoval : waiting for last message to be processed" ) ;
           try{ _readyLock.wait()  ; }catch(InterruptedException ie){}
        }
     }
     _log.info( "PrepareRemoval : closing streams" ) ;
     try{
           _input.close();
           _output.close() ;
           _socket.close() ;
     }catch( Exception nsea ){
           _log.info( " Problem in i/o : "+nsea ) ;
     }
     _state = "Dead" ;
   }
   @Override
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.info( " exceptionArrived "+ce ) ;
   }

   @Override
   public CellVersion getCellVersion()
   {
       return new CellVersion(version);
   }
}
