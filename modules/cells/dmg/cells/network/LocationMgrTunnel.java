package dmg.cells.network ;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.NotSerializableException;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellDomainInfo;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.CellTunnel;
import dmg.cells.nucleus.CellTunnelInfo;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.KillEvent;
import dmg.cells.nucleus.LastMessageEvent;
import dmg.cells.nucleus.MessageEvent;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.RoutedMessageEvent;
import dmg.cells.services.login.SshCAuth_Key;
import dmg.protocols.ssh.SshClientAuthentication;
import dmg.protocols.ssh.SshStreamEngine;
import dmg.util.Args;
import dmg.util.Gate;
import dmg.util.StreamEngine;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 5 Mar 2001
  */
public class      LocationMgrTunnel
       extends    CellAdapter
       implements Runnable,
                  CellTunnel   {

   private String       _host             = null ;
   private Args         _args             = null ;
   private int          _port             = 0 ;
   private String       _security         = null ;
   private final CellNucleus  _nucleus     ;
   private Thread       _connectionThread = null ;
   private Thread       _acceptThread     = null ;
   private String       _remoteDomain     = null ;
   private String       _locationManager  = null ;
   private final Object       _routeLock        = new Object() ;
   private final Object       _tunnelOkLock     = new Object() ;
   private boolean      _tunnelOk         = false ;
   private boolean      _overwriteLM      = false ;
   private int          _debug            = 0 ;
   private String          _mode              = "None" ;
   private String          _status            = "<init>" ;
   private CellRoute       _route             = null ;
   private CellDomainInfo  _remoteDomainInfo  = null ;
   private StreamEngine    _engine            = null ;
   private ObjectInputStream  _input         = null ;
   private ObjectOutputStream _output        = null ;
   private SocketChannel      _socketChannel = null ;
   private Socket             _socket        = null;
   private final Gate        _finalGate = new Gate(false) ;
   private SshClientAuthentication _clientAuth = null ;

   private final static Logger _logMessages = Logger.getLogger("logger.org.dcache.cells.messages");

   //
   // some statistics
   //
   private int  _connectionRequests  = 0 ;
   private int  _messagesToTunnel    = 0 ;
   private int  _messagesToSystem    = 0 ;
   private int  _connectionRetries   = 0 ;

   public LocationMgrTunnel( String cellName , StreamEngine engine , Args args )
          throws Exception {

     super( cellName , "System" , args , true ) ;

      _engine   = engine ;
      _mode     = "Accepted" ;
      _nucleus  = getNucleus() ;
      _args     = getArgs() ;

      _acceptThread = _nucleus.newThread( this , "AcceptIoThread" ) ;
      _acceptThread.start() ;

      say( "Constructor : acceptor started : "+args ) ;

      _status = "<connected>" ;

      setDebugValue() ;
   }

   public LocationMgrTunnel( String cellName , String argString )
          throws Exception {

      super( cellName , "System" , argString , false ) ;

      _args    = getArgs() ;
      _nucleus = getNucleus() ;
      _mode     = "Connected" ;
      try{
         if( _args.argc() < 2 )
             throw new
             IllegalArgumentException(
               "Usage : ... <remoteDomain> <locationManager> [-overwriteLM]" ) ;

         _remoteDomain    = _args.argv(0) ;
         _locationManager = _args.argv(1);

         _overwriteLM = _args.getOpt("overwriteLM") != null ;

          setDebugValue();

      }catch(Exception ee){
         start() ;
         kill() ;
         throw ee ;
      }

      _connectionThread = _nucleus.newThread( this , "ConnectionIoThread" ) ;
      _connectionThread.start() ;
      start() ;
   }
   private void setDebugValue(){
       Object context = _nucleus.getDomainContext("LocationMgrTunnel.debug") ;
       if( context != null ){
          String debugValue = context.toString() ;
          if( debugValue.equals("on") || debugValue.equals("true") || debugValue.equals("") ){
              _debug = 1 ;
          }else if( debugValue.equals("off") || debugValue.equals("false") ){
              _debug = 0 ;
          }else{
              try{
                 _debug = Integer.parseInt( debugValue ) ;
              }catch(Exception ee ){
                _debug = 1 ;
              }
          }
       }
   }
   public void findDomain() throws InterruptedException {

      if( _overwriteLM ){

         _host = _remoteDomain ;
         _port = Integer.parseInt( _locationManager ) ;

         return ;
      }
      String      query = "where is "+_remoteDomain ;
      CellPath    path  = new CellPath( _locationManager ) ;
      CellMessage msg   = new CellMessage( path , query ) ;
      CellMessage reply = null ;

      for( int i = 0 ; ! Thread.interrupted() ; i++ ){
        say("Sending ("+i+") '"+query+"'") ;

        try{
           if( ( reply = sendAndWait( msg , 5000 ) ) != null ){
              Object obj = reply.getMessageObject() ;
              if( ( obj != null ) && ( obj instanceof String ) ){
                  Args args = new Args( obj.toString() ) ;
                  if( ( args.argc() < 3 ) ||
                      ( ! args.argv(0).equals("location" ) ) ||
                      ( ! args.argv(1).equals(_remoteDomain) )  )
                    throw new
                    IllegalArgumentException("Invalid reply : "+obj) ;

                  String address = args.argv(2) ;
                  if( address.equals("none") )
                     throw new
                     Exception("Address not known to LocationManager yet");

                  int inx = address.indexOf(":") ;
                  if( ( inx <= 0 ) || ( inx == ( address.length() - 1 ) ) )
                     throw new
                     IllegalArgumentException("Invalid address type : "+address) ;

                  _host = address.substring(0,inx);
                  _port = Integer.parseInt(address.substring(inx+1)) ;

                  String security = args.getOpt("security") ;
                  if( security != null ){
                      Args x = new Args(security);
                      String prot = x.getOpt("prot") ;
                      if( prot != null ){
                         _security = prot ;
                      }else if( x.argc() > 0 ){
                         _security = x.argv(0) ;
                      }else{
                         throw new
                         IllegalArgumentException("Not a proper security context \""+security+"\"");
                      }

                  }
                  say("Got a proper address : host="+_host+";port="+_port+";security="+_security);
                  //
                  // jipi we are done.
                  //
                  return ;
              }
           }
           esay( "No (valid) reply from "+_locationManager ) ;
        }catch( InterruptedException ie ){
           esay( "'findDomain' interrupted");
           throw ie ;
        }catch(Exception ee ){
           esay( "Problem getting address of "+_remoteDomain+" : "+ee ) ;
        }
        try{
           Thread.sleep(10000) ;
        }catch(InterruptedException ie ){
           esay( "'findDomain' (sleep) interrupted");
           throw ie ;
        }
      }
   }

    private void connectionThread()
    {
        long start = 0;
        while (!Thread.interrupted()) {
            try {
                say("Trying to find address of <" + _remoteDomain + ">");
                _status = "<searching-"+_remoteDomain+">";

                findDomain();

                say("Trying to connect to <" + _host + ":" + _port + ">");
                _status = "<connecting-" + _host  +":"
                    + _port + "-" + _connectionRetries+">";
                _connectionRetries++;
                start = System.currentTimeMillis();
                InetAddress inetAddress = InetAddress.getByName(_host);

                String niochannel =
                    _args == null ? null : _args.getOpt("niochannel");
                say("niochannel : " + niochannel +  " " +_args);
                if ((niochannel != null)
                    && (niochannel.equals("") || niochannel.equals("true"))) {
                    SocketAddress address =
                        new InetSocketAddress(inetAddress, _port);
                    _socketChannel = SocketChannel.open(address);
                    _socket = _socketChannel.socket();
                    //               _socket.setTcpNoDelay(true);

                    makeObjectStreams(_socketChannel);
                    say("connectionThread running NIO");
                } else {
                    _socket = new Socket(inetAddress, _port);

                    if (_security == null) {
                        say("connectionThread non encrypted IO");
                        makeObjectStreams(_socket.getInputStream(),
                                          _socket.getOutputStream());
                    } else if (_security.equalsIgnoreCase("ssh")
                               || _security.equalsIgnoreCase("ssh1")) {
                        _clientAuth = new SshCAuth_Key(_nucleus, _args);

                        say("connectionThread encrypted IO");
                        SshStreamEngine engine =
                            new SshStreamEngine(_socket, _clientAuth);
                        makeObjectStreams(engine.getInputStream(),
                                          engine.getOutputStream());
                    } else {
                        throw new
                            Exception("Security mode not supported : " + _security);
                    }
                    say("connectionThread running regular IO (non-nio)");
                }
                _socket.setKeepAlive(true);

                runIo();
            } catch (InterruptedIOException e) {
                _status = "<interrupted>";
                esay("connectionThread interrupted (IO)");
                Thread.currentThread().interrupt();
            } catch (InterruptedException e) {
                _status = "<interrupted>";
                esay("connectionThread interrupted");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                esay("Problem in connecting [" + _status + "] : " + e);
                //            esay(ee) ;
                removeRoute();
                synchronized (_tunnelOkLock) {
                    _tunnelOk = false;
                }
                closeSocket();
                long diff = 30000 - (System.currentTimeMillis() - start);
                diff = diff < 4000 ? 4000 : diff;
                try {
                    say("connectionThread : Going to sleep for "
                        + (diff / 1000) + " seconds");
                    _status = "<waiting-" + _connectionRetries + ">";
                    Thread.sleep(diff);
                } catch (InterruptedException f) {
                    say("connectionThread : Sleep interrupted");
                    Thread.currentThread().interrupt();
                }
            }
        }
        say("connectionThread finished");
    }

    private void acceptThread()
    {
      try {
         _status = "<protocol>";

         _socket = _engine.getSocket();
         _socket.setKeepAlive(true);
         _socketChannel = _socket.getChannel();

         if (_socketChannel == null) {
            makeObjectStreams(_engine.getInputStream(),
                              _engine.getOutputStream());
            say("acceptThread : starting regular IO");
         } else {
            makeObjectStreams(_socketChannel);
            say("acceptThread : starting channel IO");
         }
         runIo();
      } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
      } catch (EOFException e) {
          /* Remote site closed stream. Let's shut down and assume
           * that the remote site will recreate the channel if needed.
           */
          say(e.getMessage());
      } catch (SocketException e) {
          /* Most likely this is because the channel was closed. In
           * any case, we assume that the remote end will recreate the
           * channel if needed.
           */
          say(e.getMessage());
      } catch (Throwable e) {
         esay(e);
      } finally {
         _status = "<io-shut>";
         say("acceptThread : finished");
      }
   }

   private void say(String str , CellMessage msg ){
     if( ( _debug > 1 ) && ( _nucleus.getPrintoutLevel() > 0 ) ){
        try{
           msg = new CellMessage(msg) ;
        }catch(Exception ee ){}
     }
     say( str + msg ) ;
   }

    private void runIo() throws IOException, ClassNotFoundException
    {
        boolean success = false;
        _status = "<io>";
        try {
            while (!Thread.currentThread().isInterrupted()) {
                CellMessage msg = (CellMessage)_input.readObject();
                if (_debug > 0)
                    say("Tunnel to Domain : ", msg);

                if (msg == null)
                    throw new EOFException("End of object stream detected");

                if (_logMessages.isDebugEnabled()) {
                    String messageObject =
                        msg.getMessageObject() == null
                        ? "NULL"
                        : msg.getMessageObject().getClass().getName();

                    _logMessages.debug("tunnelSendMessage src="
                                       + msg.getSourceAddress()
                                       + " dest=" + msg.getDestinationAddress()
                                       + " [" + messageObject + "] UOID="
                                       + msg.getUOID().toString());
                }

                try {
                    sendMessage(msg);
                    _messagesToSystem++;
                } catch (NoRouteToCellException e) {
                    esay(e);
                } catch (NotSerializableException e) {
                    /* Ouch, the object we just deserialized could not
                     * be serialized. This should not happen, so if it
                     * does this is clearly a bug.
                     */
                    throw new RuntimeException("Bug found", e);
                }
            }
            success = true;
        } catch (SocketException e) {
            if (Thread.currentThread().isInterrupted()) {
                success = true;
            } else {
                throw e;
            }
        } finally {
            _status = "<io-shutdown>";
            if (!success) {
                synchronized (_tunnelOkLock) {
                    _tunnelOk = false;
                    _tunnelOkLock.notifyAll();
                }
            }
        }
    }

   public void   messageArrived( MessageEvent me ){

     if( me instanceof RoutedMessageEvent ){
        synchronized( _tunnelOkLock ){
           CellMessage msg = me.getMessage() ;
           if( _debug > 0 )say("Domain to Tunnel : " , msg ) ;

           if( _logMessages.isDebugEnabled() ) {
           	String messageObject = msg.getMessageObject() == null? "NULL" : msg.getMessageObject().getClass().getName();
      	   _logMessages.debug("tunnelMessageArrived src=" + msg.getSourceAddress() +
      			   " dest=" + msg.getDestinationAddress() + " [" + messageObject + "] UOID=" + msg.getUOID().toString() );
           }
           if( _tunnelOk ){
               try{
                  _output.writeObject( msg ) ;
                  _output.flush();
                  _output.reset() ;
                  _messagesToTunnel ++ ;
               }catch(IOException ioe ){
                  esay("messageArrived : "+ioe ) ;
                  esay(ioe) ;
                  //
                  // currently this has no effect.
                  // we relay on the 'read' exception.
                  //
                  _tunnelOk = false ;
                  _tunnelOkLock.notifyAll() ;
                  try {
                      _socket.shutdownInput();
                  } catch (IOException e) {
                      /* This may happen if the socket is already
                       * closed, in which case we are happy.
                       */
                  }
               }
           }else{
               esay( "Tunnel down : dumping : "+msg ) ;
           }
        }
     }else if( me instanceof LastMessageEvent ){
         say( "messageArrived : opening final gate" ) ;
        _finalGate.open() ;
     }else{
        esay( "messageArrived : dumping junk message "+me ) ;
     }

   }

    public void run()
    {
        try {
            if (Thread.currentThread() == _connectionThread) {
                connectionThread();
            } else if (Thread.currentThread() == _acceptThread) {
                acceptThread();
            }
        } finally {
            closeSocket();
            kill();
        }
    }

   public CellTunnelInfo getCellTunnelInfo(){
      return new CellTunnelInfo( _nucleus.getCellName() ,
                                 _nucleus.getCellDomainInfo() ,
                                 _remoteDomainInfo ) ;

   }
   private static final int BUFFER_SIZE = 4*1024;
   private class ByteArrayInputStream extends InputStream {
       private ByteBuffer _buffer = ByteBuffer.allocateDirect(BUFFER_SIZE) ;
       private int _capacity = _buffer.capacity() ;
       private SocketChannel _channel = null ;
       public ByteArrayInputStream( SocketChannel channel ) throws IOException {
          _channel = channel ;
       }
       public int read() throws IOException {
          byte [] b = new byte[1] ;
          if( ( read(b) ) < 0 )return -1 ;
          return b[0] ;
       }
       public int read( byte [] b ) throws IOException {
           return read( b , 0 , b.length );
       }
       public int read2( byte [] b , int offset , int size ) throws IOException {
          int total = 0 ;
          while( size > 0 ){
             _buffer.clear() ;
             _buffer.limit( Math.min( size , _capacity ) ) ;
             int amount = _channel.read( _buffer ) ;
             if( amount <= 0 )return total == 0 ? -1 : total ;
             _buffer.rewind();
             _buffer.get( b , offset , amount ) ;
             size -= amount ;
             offset += amount ;
             total += amount ;
          }
          return total ;
       }
       public int read( byte [] b , int offset , int size ) throws IOException {
          _buffer.clear() ;
          _buffer.limit( Math.min( size , _capacity ) ) ;
          int amount = _channel.read( _buffer ) ;
          if( amount <= 0 )return -1 ;
          _buffer.rewind();
          _buffer.get( b , offset , amount ) ;
          return amount ;
       }
       public int available() throws IOException {
          return super.available();
       }
   }
   private class ByteArrayOutputStream extends OutputStream {
       private ByteBuffer _buffer = ByteBuffer.allocateDirect(BUFFER_SIZE) ;
       private int _capacity = _buffer.capacity() ;
       private SocketChannel _channel = null ;
       public ByteArrayOutputStream( SocketChannel channel ) throws IOException {
          _channel = channel ;
       }
       public void write( int b )throws IOException {
           byte [] x = new byte[1] ;
           x[0] = (byte) b ;
           write( x , 0 , 1 );
       }
       public synchronized void write( byte [] b , int offset , int size ) throws IOException {
           while( size > 0 ){
               _buffer.clear() ;
               int amount = Math.min( size , _capacity );
               _buffer.put( b , offset , amount ) ;
               offset += amount ;
               size   -= amount ;
               _buffer.limit(_buffer.position());
               _buffer.rewind();
               int written = _channel.write( _buffer ) ;
           }
       }
       public void write( byte [] b ) throws IOException {

           write( b, 0 , b.length ) ;

       }
       public void flush() throws IOException {
         super.flush();
       }
   }
   private void makeObjectStreams( SocketChannel channel )throws Exception {

      say( "Creating object (nio) streams" ) ;

      makeObjectStreams( new ByteArrayInputStream( channel ) ,
                         new ByteArrayOutputStream( channel ) );


      say( "Object streams created (from nio channel)" ) ;
   }
   private void makeObjectStreams( InputStream in , OutputStream out )
           throws Exception {

      say( "Creating object streams" ) ;
      _output  = new ObjectOutputStream( out ) ;
      if( _output == null )
          throw new
          IOException( "OutputStream == null" ) ;

      _input   = new ObjectInputStream( new BufferedInputStream( in ) ) ;
      if( _input == null )
          throw new
          IOException( "InputStream == null" ) ;

      say( "Object streams created (from regular streams)" ) ;

      negotiateDomains() ;

   }

   private void negotiateDomains() throws Exception {
      say( "Exchangeing DomainInfos" ) ;

      CellDomainInfo info = _nucleus.getCellDomainInfo() ;

      say( "Sending Domain info : "+info ) ;
      //
      // send our info's
      //
      _output.writeObject( _nucleus.getCellDomainInfo() ) ;
      _output.flush();
      //
      // read remote info's
      //
      Object obj = _input.readObject() ;
      if( obj == null )
         throw new
         IOException( "EOS encountered while reading DomainInfo" ) ;

      _remoteDomainInfo = (CellDomainInfo) obj ;
      say( "Received Domain info : "+_remoteDomainInfo ) ;
      //
      //  has to be done before adding the routed
      //
      say("acceptThread : enabling tunnel" ) ;
      synchronized( _tunnelOkLock ){
           _tunnelOk = true ;
      }
      //
      // install the remove DOMAIN route
      //
      removeRoute() ;
      addRoute( _remoteDomainInfo.getCellDomainName() ) ;

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
   public void getInfo( PrintWriter pw ){
     pw.println( "Location Mgr Tunnel : "+_nucleus.getCellName()) ;
     pw.println( "Mode          : "+_mode) ;
     pw.println( "Status        : "+_status) ;
     pw.println( "con. Requests : "+_connectionRequests ) ;
     pw.println( "con. Retries  : "+_connectionRetries ) ;
     pw.println( "-> Tunnel     : "+_messagesToTunnel ) ;
     pw.println( "-> Domain     : "+_messagesToSystem ) ;
     if( _remoteDomainInfo == null )
        pw.println( "Peer          : N.N." ) ;
     else
        pw.println( "Peer          : "+
                   _remoteDomainInfo.getCellDomainName() ) ;

     return ;
   }
   private synchronized void removeRoute(){
      if( _route != null ){
          say( "Removing Route : "+_route ) ;
          _nucleus.routeDelete( _route ) ;
          _route = null ;
      }
   }
   private synchronized void addRoute( String remoteDomainName ){
      _route = new CellRoute(
                   remoteDomainName ,
                   _nucleus.getCellName() ,
                   CellRoute.DOMAIN ) ;
      say( "Adding Route : "+_route) ;
      _nucleus.routeAdd( _route ) ;
   }

    private synchronized void closeSocket()
    {
        say("Closing socket");
        if (_socket != null) {
            /* Notice that closing the socket will automatically close
             * the input and output streams and any associated
             * channel.
             */
            try {
                _socket.close();
            } catch(IOException e) {
            }
            _socket = null;
        }
        say("Socket closed");
    }

   public synchronized void  prepareRemoval(KillEvent ce) {
       removeRoute();
       say("Setting tunnel down");
       synchronized (_tunnelOkLock) {
           _tunnelOk = false;
       }
       _finalGate.check();

       if (_acceptThread != null) {
           _acceptThread.interrupt();
       }
       if (_connectionThread != null) {
           _connectionThread.interrupt();
       }

       closeSocket();

       say( "Gate Opened. Bye Bye" );
   }

   public void   exceptionArrived( ExceptionEvent ce ){
     _nucleus.say( "exceptionArrived : "+ce ) ;
   }
}
