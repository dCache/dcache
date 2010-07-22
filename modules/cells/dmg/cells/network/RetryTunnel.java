package dmg.cells.network ;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;
import  java.util.Date ;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import  java.io.* ;
import  java.net.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

   private InetAddress  _address         = null ;
   private int          _port            = 0 ;
   private CellNucleus  _nucleus         = null ;
   private Thread       _receiverThread  = null ;
   private Object       _receiverLock    = new Object();

   private BlockingQueue<CellMessage>         _messageArrivedQueue = new LinkedBlockingQueue<CellMessage> () ;
   private Gate         _finalGate           = new Gate(false) ;
   private ObjectInputStream  _input     = null ;
   private ObjectOutputStream _output    = null ;
   private Socket          _socket ;
   private String          _mode              = "None" ;
   private CellRoute       _route             = null ;
   private CellDomainInfo  _remoteDomainInfo  = null ;
   private StateThread     _engine            = null ;
   private long            _connectionStarted = 0 ;
   private int             _connectionRetries = 0 ;
   //
   // some statistics
   //
   private int  _connectionRequests    = 0 ;
   private int  _messagesToTunnel    = 0 ;
   private int  _messagesToSystem    = 0 ;

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

   public RetryTunnel( String cellName , Socket socket )
          throws Exception {

      _mode     = "Accepted" ;
      _socket   = socket ;
      _nucleus  = new CellNucleus( this , cellName ) ;

      _engine   = new StateThread( this ) ;
      _engine.start() ;

   }
   public RetryTunnel( String cellName , String argString )
          throws Exception {

      Args args = new Args( argString ) ;
      if( args.argc() < 2 )
           throw new
           IllegalArgumentException(
             "Usage : RetryTunnel <host> <port>" ) ;


      _RetryTunnel( cellName ,
                     args.argv(0) ,
                     new Integer( args.argv(1) ).intValue() ) ;
   }
   public RetryTunnel( String cellName , String host , int port )
          throws Exception {
      _RetryTunnel( cellName , host , port ) ;
   }
   private void _RetryTunnel( String cellName , String host , int port )
          throws Exception {


      _mode    = "Connection" ;

      _address = InetAddress.getByName( host ) ;
      _port    = port ;

      _nucleus = new CellNucleus( this , cellName ) ;

      _engine   = new StateThread( this ) ;
      _engine.start() ;


   }
   public RetryTunnel( String cellName , InetAddress address , int port )
          throws Exception {


      _mode    = "Connection" ;

      _address = address ;
      _port    = port ;

      _nucleus = new CellNucleus( this , cellName ) ;

      _engine   = new StateThread( this ) ;
      _engine.start() ;


   }
   public void run(){
      if( Thread.currentThread() == _receiverThread ){

        try{
           Object obj ;
           while( ( ( obj = _input.readObject() ) != null ) && ! Thread.interrupted() ){
              CellMessage msg = (CellMessage) obj ;
              _log.info( "receiverThread : Message from tunnel : "+msg ) ;
              try{
                 _nucleus.sendMessage( msg ) ;
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
      if( ( state < 0 ) || ( state >= _cst_states.length ) )
         return "<Unknown>" ;
      return _cst_states[state] ;
   }
   public int runState( int state ){

     long now = new Date().getTime() ;

     _log.info( " runState : "+_printState() ) ;

     switch( state ){
       case 0 :
       if( _mode.equals("Connection") )return CST_CONNECTING ;
       if( _mode.equals("Accepted")   )return CST_CONNECTED ;
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
          if( _mode.equals("Accepted")   )return CST_SHUTDOWN ;
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
            if( _receiverThread != null )
               _receiverThread.interrupt() ;
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

   public CellTunnelInfo getCellTunnelInfo(){
      return new CellTunnelInfo( _nucleus.getCellName() ,
                                 _nucleus.getCellDomainInfo() ,
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
      _output.writeObject( _nucleus.getCellDomainInfo() ) ;
      Object obj = _input.readObject() ;
      if( obj == null )
         throw new IOException( "Premature EOS encountered" ) ;
      _remoteDomainInfo = (CellDomainInfo) obj ;
   }
   @Override
public String toString(){
      if( _remoteDomainInfo == null )
        return "M="+_mode+";S="+_printState() ;
      else
        return "M="+_mode+
               ";S="+_printState()+
               ";P="+_remoteDomainInfo.getCellDomainName() ;
   }
   public String getInfo(){
     StringBuffer sb = new StringBuffer() ;
     sb.append( "Simple Tunnel : "+_nucleus.getCellName()+"\n" ) ;
     sb.append( "Mode          : "+_mode+"\n" ) ;
     sb.append( "Status        : "+_printState()+"\n" ) ;
     sb.append( "con. Requests : "+_connectionRequests+"\n" ) ;
     sb.append( "Msg Queued    : "+_messageArrivedQueue.size()+"\n" ) ;
     sb.append( "-> Tunnel     : "+_messagesToTunnel+"\n" ) ;
     sb.append( "-> Domain     : "+_messagesToSystem+"\n" ) ;
     if( _remoteDomainInfo == null )
        sb.append( "Peer          : N.N.\n" ) ;
     else
        sb.append( "Peer          : "+
                   _remoteDomainInfo.getCellDomainName()+"\n" ) ;

     return sb.toString() ;
   }
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
   public synchronized void   prepareRemoval( KillEvent ce ){

     _log.info( "prepareRemoval : initiated "+ce ) ;
     _log.info( "prepareRemoval : waiting for final Gate to open" ) ;
     _finalGate.check() ;
     _log.info( "prepareRemoval : final gate passed -> closing" ) ;
     _engine.stop() ;
     synchronized( _receiverLock ){
         if( _receiverThread != null )_receiverThread.interrupt() ;
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
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.info( "exceptionArrived : "+ce ) ;
   }

}
