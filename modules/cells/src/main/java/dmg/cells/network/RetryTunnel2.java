package dmg.cells.network ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellDomainInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.CellTunnel;
import dmg.cells.nucleus.CellTunnelInfo;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.KillEvent;
import dmg.cells.nucleus.LastMessageEvent;
import dmg.cells.nucleus.MessageEvent;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.RoutedMessageEvent;
import dmg.util.Gate;
import dmg.util.StreamEngine;

import org.dcache.util.Args;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      RetryTunnel2
       extends    CellAdapter
       implements Cell,
                  Runnable,
                  CellTunnel   {

   private final static Logger _log =
       LoggerFactory.getLogger(RetryTunnel2.class);

   private String       _host;
   private Args         _args;
   private int          _port;
   private CellNucleus  _nucleus;
   private Thread       _connectionThread;
   private Thread       _ioThread;
   private final Object _routeLock        = new Object() ;
   private final Object _tunnelOkLock     = new Object() ;
   private boolean      _tunnelOk;
   private String          _mode              = "None" ;
   private String          _status            = "<init>" ;
   private CellRoute       _route;
   private CellDomainInfo  _remoteDomainInfo;
   private StreamEngine    _engine;
   private ObjectInputStream  _input;
   private ObjectOutputStream _output;
   private Gate               _finalGate = new Gate(false) ;
   //
   // some statistics
   //
   private int  _connectionRequests;
   private int  _messagesToTunnel;
   private int  _messagesToSystem;
   private int  _connectionRetries;

   public RetryTunnel2( String cellName , StreamEngine engine , Args args )
   {

     super( cellName , "System" , args , true ) ;

      _engine   = engine ;
      _mode     = "Accepted" ;
      _nucleus  = getNucleus() ;

      _ioThread = _nucleus.newThread( this , "IoThread" ) ;
      _ioThread.start() ;

      _log.info( "Constructor : acceptor started" ) ;

      _status = "<connected>" ;
   }
   public RetryTunnel2( String cellName , String argString )
   {

      super( cellName , "System" , argString , false ) ;
      _log.info( "CellName : "+cellName+ " ; args : "+argString ) ;

      _args    = getArgs() ;
      _nucleus = getNucleus() ;

      if( _args.argc() < 2 ){
          start() ;
          kill() ;

          throw new
          IllegalArgumentException(
            "Usage : ... <host> <port>" ) ;
      }

      _RetryTunnel2( cellName ,
                     _args.argv(0) ,
                     Integer.valueOf( _args.argv(1) ) ) ;

      start() ;
   }
   public RetryTunnel2( String cellName , String host , int port )
   {

      super( cellName , "System" ,  host+" "+port , true ) ;
      _args = getArgs() ;
      _RetryTunnel2( cellName , host , port ) ;
   }
   private void _RetryTunnel2( String cellName , String host , int port )
   {


      _mode    = "Connection" ;

      _host    = host ;
      _port    = port ;

      _connectionThread = _nucleus.newThread(this,"Connection") ;
      _connectionThread.start() ;


   }
   private void runConnection(){
      long    start = 0 ;
      Socket  socket;
      while( ! Thread.interrupted() ){
         _log.info( "Trying to connect "+_host+"("+_port+")" ) ;
         _status = "<connecting-"+_connectionRetries+">" ;
         try{
            _connectionRetries ++ ;
            start  = System.currentTimeMillis() ;
            socket = new Socket( _host , _port ) ;
            _makeStreams( socket.getInputStream() ,
                          socket.getOutputStream()   ) ;

            synchronized( _tunnelOkLock ){
                _tunnelOk = true ;
            }
            runIo() ;

         }catch(InterruptedIOException iioe ){
            _log.warn(iioe.toString(), iioe);
            break ;
         }catch(InterruptedException ie ){
            _log.warn(ie.toString(), ie);
            break ;
         }catch(Exception ee ){
            _log.warn(ee.toString());
            removeRoute() ;
            synchronized(_tunnelOkLock){
                _tunnelOk = false ;
            }
            try{_output.close();}catch(Exception ii){}
            try{_input.close();}catch(Exception ii){}
            long diff = 30000 - ( System.currentTimeMillis() - start ) ;
            diff = diff < 4000 ? 4000 : diff ;
            try{
               _log.info("runConnection : Going to sleep for "+(diff/1000)+" seconds" ) ;
               _status = "<waiting-"+_connectionRetries+">" ;
               Thread.sleep(diff) ;
            }catch(InterruptedException ieie){
               _log.warn( "runConnection : Sleep interrupted" ) ;
               break ;
            }
         }

      }
   }
   private void runIoThread(){
      try{
         _log.info("runIoThread : creating streams" ) ;
         _status = "<protocol>" ;
         _makeStreams( _engine.getInputStream() ,
                       _engine.getOutputStream()   ) ;
         _log.info("runIoThread : enabling tunnel" ) ;
         synchronized( _tunnelOkLock ){
              _tunnelOk = true ;
         }
         _log.info("runIoThread : starting IO" ) ;
         runIo() ;
         _status = "<io-fin>" ;
         _log.warn( "runIoThread : unknown state 2 " ) ;

      }catch(Exception ioe ){
         _log.warn( "runIoThread : "+ioe, ioe ) ;
         _status = "<io-shut>" ;
         _log.warn("Disabling tunnel" ) ;
         synchronized( _tunnelOkLock ){
              _tunnelOk = false ;
         }
         _log.warn( "Closing streams" ) ;
         try{_output.close();}catch(IOException ii){}
         try{_input.close();}catch(IOException ii){}
         _log.warn( "Killing myself" ) ;
         kill() ;
      }finally{
         _log.info( "runIoThread : finished" ) ;
      }

   }
   private void runIo() throws Exception {
      Object obj ;

      _status = "<io>" ;
      try{
         while( ( ! Thread.interrupted() ) &&
                ( ( obj = _input.readObject() ) != null  )    ){

            CellMessage msg = (CellMessage) obj ;

            _log.info( "receiverThread : Message from tunnel : "+msg ) ;

            try{
               sendMessage( msg ) ;
               _messagesToSystem ++ ;
            }catch( NoRouteToCellException nrtce ){
                _log.warn( nrtce.toString(), nrtce ) ;
            }
         }
      }finally{
         _status = "<io-shutdown>" ;
      }

   }
   @Override
   public void   messageArrived( MessageEvent me ){

     if( me instanceof RoutedMessageEvent ){
        synchronized( _tunnelOkLock ){
           CellMessage msg = me.getMessage() ;
           if( _tunnelOk ){
               _log.info( "messageArrived : "+msg ) ;
               try{
                   _output.writeObject( msg ) ;
                   _output.reset() ;
                   _messagesToTunnel ++ ;
               }catch(IOException ioe ){
                   _log.warn("messageArrived : "+ioe, ioe ) ;
                   _tunnelOk = false ;
                   try{_output.close();}catch(IOException ii){}
                   try{_input.close();}catch(IOException ii){}
               }
           }else{
               _log.warn( "Tunnel down : dumping : "+msg ) ;
           }
        }
     }else if( me instanceof LastMessageEvent ){
         _log.info( "messageArrived : opening final gate" ) ;
        _finalGate.open() ;
     }else{
        _log.warn( "messageArrived : dumping junk message "+me ) ;
     }

   }
   @Override
   public void run(){
      if( Thread.currentThread() == _connectionThread ){
         runConnection() ;
      }else if( Thread.currentThread() == _ioThread ){
         runIoThread() ;
      }
   }
   @Override
   public CellTunnelInfo getCellTunnelInfo(){
       return new CellTunnelInfo( _nucleus.getCellName() ,
               new CellDomainInfo(_nucleus.getCellDomainName()),
                                 _remoteDomainInfo ) ;

   }
   private void _makeStreams( InputStream in , OutputStream out )
           throws Exception {

      _output  = new ObjectOutputStream( out ) ;

      if( _output == null ) {
          throw new
                  IOException("OutputStream == null");
      }

      _input   = new ObjectInputStream( in ) ;
      if( _input == null ){
          try{ _output.close() ; }catch(IOException ie){}
          throw new
          IOException( "InputStream == null" ) ;
      }
      Object obj;
      try{
          _output.writeObject(new CellDomainInfo(_nucleus.getCellDomainName())) ;
         if( ( obj  = _input.readObject() ) == null ) {
             throw new
                     IOException("EOS encountered while reading DomainInfo");
         }

      }catch(IOException ieww ){
         try{ _output.close() ; _output = null ;}catch(IOException ie){}
         try{ _input.close() ; _input = null ;}catch(IOException ie){}
         throw ieww ;
      }
      _remoteDomainInfo = (CellDomainInfo) obj ;
      synchronized( _routeLock ){
         removeRoute() ;
         _route = new CellRoute(
                      _remoteDomainInfo.getCellDomainName() ,
                      _nucleus.getCellName() ,
                      CellRoute.DOMAIN ) ;
         _log.info( "addingRoute : "+_route) ;
         _nucleus.routeAdd( _route ) ;
      }
   }
   public String toString(){
      if( _tunnelOk ){
           return _status+"/"+_mode+" -> "+
                  (_remoteDomainInfo==null?"???":
                  _remoteDomainInfo.getCellDomainName()) ;
      }else{
           return _status+"/"+_mode ;
      }
   }
   @Override
   public void getInfo( PrintWriter pw ){
     pw.println( "Simple Tunnel : "+_nucleus.getCellName()) ;
     pw.println( "Mode          : "+_mode) ;
     pw.println( "Status        : "+_mode) ;
     pw.println( "con. Requests : "+_connectionRequests ) ;
     pw.println( "con. Retries  : "+_connectionRetries ) ;
     pw.println( "-> Tunnel     : "+_messagesToTunnel ) ;
     pw.println( "-> Domain     : "+_messagesToSystem ) ;
     if( _remoteDomainInfo == null ) {
         pw.println("Peer          : N.N.");
     } else {
         pw.println("Peer          : " +
                 _remoteDomainInfo.getCellDomainName());
     }

   }
   private void removeRoute(){
     synchronized( _routeLock ){
         if( _route != null ){
            _log.info( "removeRoute : removing route "+_route ) ;
            _nucleus.routeDelete( _route ) ;
            _route = null ;
         }
     }
   }
   @Override
   public synchronized void   prepareRemoval( KillEvent ce ){
     removeRoute() ;
     _log.info("Setting tunnel down" ) ;
     synchronized( _tunnelOkLock ){ _tunnelOk = false ; }
     try{_input.close();}catch(IOException ee){}
     try{_output.close();}catch(IOException ee){}
     _log.info( "Streams  closed" ) ;
     _finalGate.check() ;
     _log.info( "Gate Opened. Bye Bye" ) ;


   }
   @Override
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.warn( "exceptionArrived : "+ce ) ;
   }

}
