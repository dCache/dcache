package dmg.cells.network ;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;
import  java.util.Date ;
import  java.io.* ;
import  java.net.* ;
import  java.nio.channels.*;
import  java.nio.*;

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
   private CellNucleus  _nucleus          = null ;
   private Thread       _connectionThread = null ;
   private Thread       _acceptThread     = null ;
   private String       _remoteDomain     = null ;
   private String       _locationManager  = null ;
   private Object       _routeLock        = new Object() ;
   private Object       _tunnelOkLock     = new Object() ;
   private boolean      _tunnelOk         = false ;
   private int          _debug            = 0 ;
   private String          _mode              = "None" ;
   private String          _status            = "<init>" ;
   private CellRoute       _route             = null ;
   private CellDomainInfo  _remoteDomainInfo  = null ;
   private StreamEngine    _engine            = null ;
   private ObjectInputStream  _input         = null ;
   private ObjectOutputStream _output        = null ;
   private SocketChannel      _socketChannel = null ;
   private Gate               _finalGate = new Gate(false) ;
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
               "Usage : ... <remoteDomain> <locationManager>" ) ;
         
         _remoteDomain    = _args.argv(0) ;
         _locationManager = _args.argv(1);

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
                  int inx = address.indexOf(":") ;
                  if( ( inx <= 0 ) || ( inx == ( address.length() - 1 ) ) )
                     throw new
                     IllegalArgumentException("Invalid address type : "+address) ;
                  _host = address.substring(0,inx);
                  _port = Integer.parseInt(address.substring(inx+1)) ;
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
   
   private void connectionThread(){
      long    start = 0 ;
      Socket  socket = null ;
      while( ! Thread.interrupted() ){
      
         try{
            say( "Trying to find address of <"+_remoteDomain+">" ) ;
            _status = "<searching-"+_remoteDomain+">" ;

            findDomain() ;

            say( "Trying to connect to <"+_host+":"+_port+">" ) ;
            _status = "<connecting-"+_connectionRetries+">" ;
            _connectionRetries ++ ;
            start  = System.currentTimeMillis() ;
            InetAddress inetAddress = InetAddress.getByName( _host ) ;
            
            String niochannel = _args == null ? null : _args.getOpt("niochannel") ;
            say("niochannel : "+niochannel+ " " +_args);
            if(  ( niochannel != null ) && ( niochannel.equals("") || niochannel.equals("true") ) ){
               socket = SocketChannel.open( new InetSocketAddress( inetAddress , _port ) ).socket() ;
//               socket.setTcpNoDelay(true);
               _socketChannel = socket.getChannel() ;
               
               makeObjectStreams( _socketChannel ) ;
               say("connectionThread running NIO") ;
               
            }else{
               socket = new Socket( inetAddress , _port ) ;
               makeObjectStreams( 
                     socket.getInputStream() ,
                     socket.getOutputStream()   ) ;
                     
               say("connectionThread running regular IO") ;
            }
     
            runIo() ;
            
         }catch(InterruptedIOException iioe ){
            _status = "<interrupted>" ;
            esay("connectionThread interrupted (IO)") ;
            break ;
         }catch(InterruptedException ie ){
            _status = "<interrupted>" ;
            esay("connectionThread interrupted") ;
            break ;
         }catch(Throwable ee ){
            esay(""+ee) ;
            esay(ee) ;
            removeRoute() ;
            synchronized(_tunnelOkLock){
                _tunnelOk = false ;
            }
            closeStreams() ;
            long diff = 30000 - ( System.currentTimeMillis() - start ) ;
            diff = diff < 4000 ? 4000 : diff ;
            try{
               say("connectionThread : Going to sleep for "+(diff/1000)+" seconds" ) ;
               _status = "<waiting-"+_connectionRetries+">" ;
               Thread.sleep(diff) ;
            }catch(InterruptedException ieie){
               esay( "connectionThread : Sleep interrupted" ) ;
               break ;
            }
         }
         
      }
      closeStreams();
      say( "connectionThread finished");
   }
   
   private void acceptThread(){
      try{
         _status = "<protocol>" ;
         
         Socket socket = _engine.getSocket() ;
         _socketChannel = socket.getChannel() ;
         
         if( _socketChannel == null ){
            makeObjectStreams( 
                  _engine.getInputStream() , 
                  _engine.getOutputStream()   ) ;

            say("acceptThread : starting regular IO" ) ;
         }else{
            makeObjectStreams( _socketChannel ) ;

            say("acceptThread : starting channel IO" ) ;
         }
         runIo() ;
                  
      }catch(Throwable ioe ){
         esay( "acceptThread : "+ioe ) ;
         _status = "<io-shut>" ;
         if( !( ioe instanceof EOFException ) )esay(ioe) ;
         closeStreams() ;
         say( "acceptThread : Killing myself" ) ;
         kill() ; 
      }finally{
         say( "acceptThread : finished" ) ;
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
   private void runIo() throws Throwable {
      
      _status = "<io>" ;
      try{
         while( ! Thread.interrupted() ){

            CellMessage msg = (CellMessage) _input.readObject() ;
            if( _debug > 0 )say("Tunnel to Domain : " , msg ) ;
            if( msg == null )
               throw new 
               EOFException("End of object stream detected");
               
            try{  
               sendMessage( msg ) ;
               _messagesToSystem ++ ;              
            }catch( NoRouteToCellException nrtce ){
               esay( nrtce ) ;
            }
         }
         throw new
         InterruptedException("runIo has been interrupted") ;
         
      }catch(Throwable ee ){
         esay( "runIO : "+ee ) ;
         synchronized( _tunnelOkLock ){
            _tunnelOk = false ;
            _tunnelOkLock.notifyAll() ;
         }
         throw ee ;
      }finally{
         _status = "<io-shutdown>" ;
      }
   
   }
   public void   messageArrived( MessageEvent me ){
     
     if( me instanceof RoutedMessageEvent ){
        synchronized( _tunnelOkLock ){
           CellMessage msg = me.getMessage() ;
           if( _debug > 0 )say("Domain to Tunnel : " , msg ) ;
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
   public void run(){
      if( Thread.currentThread() == _connectionThread ){
         connectionThread() ;
      }else if( Thread.currentThread() == _acceptThread ){
         acceptThread() ;
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
   private synchronized void closeStreams(){
      say( "Closing streams" ) ;
      if( _input != null )try{_input.close();}catch(Exception ee){}
      _input = null ;
      if( _output != null )try{_output.close();}catch(Exception ee){}
      _output = null ;
      say( "Streams closed" ) ;
   }
   public synchronized void   prepareRemoval( KillEvent ce ){
     removeRoute() ;
     say("Setting tunnel down" ) ;
     synchronized( _tunnelOkLock ){ _tunnelOk = false ; }
     _finalGate.check() ;
     say( "Gate Opened. Bye Bye" ) ;
     
     
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _nucleus.say( "exceptionArrived : "+ce ) ;
   }
 
}
