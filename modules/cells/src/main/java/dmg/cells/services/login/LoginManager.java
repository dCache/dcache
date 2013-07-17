// $Id: LoginManager.java,v 1.46 2007-10-22 12:30:38 behrmann Exp $
//
package  dmg.cells.services.login ;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;
import dmg.util.KeepAliveListener;
import dmg.util.StreamEngine;
import dmg.util.UserValidatable;

import org.dcache.auth.Subjects;
import org.dcache.util.Version;

import static com.google.common.collect.Maps.newHashMap;

/**
 **
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
 */
public class       LoginManager
       extends     CellAdapter
       implements  UserValidatable {

  private final CellNucleus  _nucleus ;
  private final Args         _args ;
  private final Args         _childArgs ;
  private final ListenThread _listenThread ;
  private int          _connectionDeniedCounter;
  private String       _locationManager;
  private int          _loginCounter, _loginFailures;
  private boolean      _sending;
  private Class<?>     _loginClass        = Object.class ;
  private final CellVersion _version;
  private Constructor<?>_loginConstructor;
  private Constructor<?>_authConstructor;
  private int          _maxLogin          = -1 ;
  private final Map<String,Object>      _childHash   = new HashMap<>() ;
  /**
   * actually, _childCount have to be equal to _childHash.size(). But while
   * cells needs some time to die, _childHash contains cells which are in removing state,
   * while  _childCount shows active cells only.
   */
  private int             _childCount;
  private String          _authenticator;
  private KeepAliveThread _keepAlive;

  private LoginBrokerHandler _loginBrokerHandler;
  private static Logger _log = LoggerFactory.getLogger(LoginManager.class);
  private static Logger _logSocketIO = LoggerFactory.getLogger("logger.dev.org.dcache.io.socket");

  private Class<?> [][] _loginConSignature = {
    {  String.class ,
       StreamEngine.class } ,
    {  String.class  ,
       StreamEngine.class ,
       Args.class           }
  } ;

  private Class<?> [] _authConSignature = {
     CellNucleus.class , Args.class
  } ;
  private  Class<?> [] _loginPntSignature = { int.class     } ;

  private  int      _loginConType      = -1 ;
  private  String _protocol ;
  private  String _authClassName ;
  private  Class<?>  _authClass ;

  /**
  *<pre>
  *   usage   &lt;listenPort&gt; &lt;loginCellClass&gt;
  *           [-prot=ssh|telnet|raw]
  *                    default : telnet
  *           [-auth=&lt;authenticationClass&gt;]
  *                    default : ssh    : dmg.cells.services.login.SshSAuth_A
  *                              telnet : dmg.cells.services.login.TelnetSAuth_A
  *                              raw    : none
  *
  *         all residual arguments and all options are sent to
  *         the &lt;loginCellClass&gt; :
  *            &lt;init&gt;(String name , StreamEngine engine , Args args )
  *
  *         and to the Authentication module (class)
  *
  *            &lt;init&gt;(CellNucleus nucleus , Args args )
  *
  *         Both get their own copy.
  *</pre>
  */
  public LoginManager( String name , String argString ) throws Exception {

      super( name , argString , false ) ;

      _nucleus  = getNucleus() ;
      _args     = getArgs() ;
      try{
         Args args = _args ;
         if( args.argc() < 2 ) {
             throw new
                     IllegalArgumentException(
                     "USAGE : ... <listenPort> <loginCellClass>" +
                             " [-prot=ssh|telnet|raw] [-auth=<authCell>]" +
                             " [-maxLogin=<n>|-1]" +
                             " [-keepAlive=<seconds>]" +
                             " [-acceptErrorWait=<msecs>]" +
                             " [args givenToLoginClass]");
         }

          _childArgs = new Args(argString.replaceFirst("(^|\\s)-export($|\\s)", ""));

         _protocol = args.getOpt("prot") ;
         checkProtocol();
         _log.info( "Using Protocol : {}",_protocol ) ;

         int listenPort    = Integer.parseInt( args.argv(0) ) ;
         args.shift() ;
         _childArgs.shift();

         // which cell to start
         if( args.argc() > 0 ){
            _loginClass = Class.forName( args.argv(0) ) ;
            _log.info( "Using login class : {}", _loginClass.getName() ) ;
            args.shift() ;
            _childArgs.shift();
         }
         _version = new CellVersion(Version.of(_loginClass));
         // get the authentication
         _authenticator = args.getOption("authenticator", "pam") ;

         if( ( _authClassName = args.getOpt("auth") ) == null ){
             switch (_protocol) {
             case "ssh":
                 _authClass = SshSAuth_A.class;
                 break;
             case "raw":
                 _authClass = null;
                 break;
             case "telnet":
                 _authClass = TelnetSAuth_A.class;
                 break;
             }
            if( _authClass != null ) {
                _log.info("Using authentication Module : " + _authClass);
            }
         }else if( _authClassName.equals( "none" ) ){
//            _authClass = dmg.cells.services.login.NoneSAuth.class ;
         }else{
            _log.info( "Using authentication Module : "+_authClassName ) ;
            _authClass = Class.forName(_authClassName) ;
         }
         if( _authClass != null ){
            _authConstructor = _authClass.getConstructor( _authConSignature ) ;
            _log.info( "Using authentication Constructor : "+_authConstructor ) ;
         }else{
            _authConstructor = null ;
            _log.info( "No authentication used" ) ;
         }
         try{
            _loginConstructor = _loginClass.getConstructor( _loginConSignature[1] ) ;
            _loginConType     = 1 ;
         }catch( NoSuchMethodException nsme ){
            _loginConstructor = _loginClass.getConstructor( _loginConSignature[0] ) ;
            _loginConType     = 0 ;
         }
         _log.info( "Using constructor : "+_loginConstructor ) ;
         String maxLogin = args.getOpt("maxLogin") ;
         if( maxLogin != null ){
            try{
               _maxLogin = Integer.parseInt(maxLogin);
            }catch(NumberFormatException ee){/* bad values ignored */}
         }

         String loginBroker = _args.getOpt("loginBroker");
          if ( loginBroker != null) {
              _loginBrokerHandler = new LoginBrokerHandler(Splitter.on(",").omitEmptyStrings().split(loginBroker));
              addCommandListener(_loginBrokerHandler);

              if (_maxLogin < 0) {
                  _maxLogin = 100000;
              }
          }

         if( _maxLogin < 0 ){
            _log.info("MaxLogin feature disabled") ;
         }else{
            _nucleus.addCellEventListener( new LoginEventListener() ) ;
            _log.info("Maximum Logins set to :"+_maxLogin ) ;
         }

         // keep alive
         long   keepAlive      = args.getLongOption("keepAlive", 0L) ;
         _log.info("Keep Alive set to "+keepAlive+" seconds") ;
         keepAlive *= 1000L ;
         _keepAlive = new KeepAliveThread(keepAlive) ;

         // get the location manager
         _locationManager = args.getOpt("lm") ;

         _listenThread  = new ListenThread(listenPort) ;
         _log.info( "Listening on port "+_listenThread.getListenPort() ) ;


         _nucleus.newThread( _listenThread , "listen" ).start() ;
         _nucleus.newThread( new LocationThread() , "Location" ).start() ;
         _nucleus.newThread( _keepAlive , "KeepAlive" ).start() ;

      }catch( Exception e ){
         if(e instanceof RuntimeException) {
             _log.warn( "LoginManger >"+getCellName()+"< got exception : "+e, e);
         }
         start() ;
         kill() ;
         throw e ;
      }

      start();
  }

    private void checkProtocol() throws IllegalArgumentException {
        if (_protocol == null) {
            _protocol = "telnet";
        }
        if (!(_protocol.equals("ssh") ||
                _protocol.equals("telnet") ||
                _protocol.equals("raw"))) {
            throw new IllegalArgumentException("Protocol must be telnet or ssh or raw");
        }
    }

    @Override
    public CellVersion getCellVersion(){
         return _version;
    }

  public class LoginBrokerHandler implements Runnable {

     private static final long EAGER_UPDATE_TIME = 1000;

     private String[] _loginBrokers;
     private String _protocolFamily;
     private String _protocolVersion;
     private long   _brokerUpdateTime;
     private long   _currentBrokerUpdateTime = EAGER_UPDATE_TIME;
     private double _brokerUpdateOffset;
     private LoginBrokerInfo _info;
     private double _currentLoad;
     private final Thread _thread;

     private LoginBrokerHandler(Iterable<String> loginBrokers){

        _loginBrokers = Iterables.toArray(loginBrokers, String.class);
        _protocolFamily    = _args.getOption("protocolFamily", _protocol) ;
        _protocolVersion = _args.getOption("protocolVersion", "1.0") ;
        _brokerUpdateTime = TimeUnit.valueOf(_args.getOption("brokerUpdateTimeUnit")).toMillis(_args.getLongOption("brokerUpdateTime"));
        _brokerUpdateOffset =  _args.getDoubleOption("brokerUpdateOffset");

        _info = new LoginBrokerInfo(
                     _nucleus.getCellName() ,
                     _nucleus.getCellDomainName() ,
                     _protocolFamily ,
                     _protocolVersion ,
                     _loginClass.getName() ) ;

        _info.setUpdateTime( _brokerUpdateTime ) ;
        _thread = _nucleus.newThread(this, "loginBrokerHandler");
     }

     public void start() {
         _thread.start();
     }

      public void stop() {
          _thread.interrupt();
      }

     @Override
     public void run(){
        try{
          synchronized(this){
             while( ! Thread.interrupted() ){
                try{
                   runUpdate() ;
                }catch(Exception ie){
                   _log.warn("Login Broker Thread reports : "+ie);
                }
                wait(_currentBrokerUpdateTime);
             }
          }
        }catch( Exception io ){
          _log.info( "Login Broker Thread terminated due to "+io ) ;
        }
     }
     public static final String hh_get_children = "[-binary]" ;
     public Object ac_get_children( Args args ){
        boolean binary = args.hasOption("binary") ;
        synchronized( _childHash ){
           if( binary ){
              String [] list = new String[_childHash.size()] ;
              list = _childHash.keySet().toArray(list);
              return new LoginManagerChildrenInfo( getCellName() , getCellDomainName(), list ) ;
           }else{
              StringBuilder sb = new StringBuilder() ;
              for(String child : _childHash.keySet() ){
                 sb.append(child).append("\n");
              }
              return sb.toString();
           }
        }
     }
     public static final String hh_lb_set_update = "<updateTime/sec>" ;
     public String ac_lb_set_update_$_1( Args args ){
        long update = Long.parseLong( args.argv(0) )*1000 ;
        if( update < 2000 ) {
            throw new
                    IllegalArgumentException("Update time out of range");
        }

        synchronized(this){
           _brokerUpdateTime = update ;
           _info.setUpdateTime(update) ;
           notifyAll() ;
        }
        return "" ;
     }
     private synchronized void runUpdate(){

        InetAddress[] addresses = _listenThread.getInetAddress();

        if( (addresses == null) || ( addresses.length == 0 ) ) {
            return;
        }

        String[] hosts = new String[addresses.length];

        /**
         *  Add addresses ensuring preferred ordering: external addresses are before any
         *  internal interface addresses.
         */
        int nextExternalIfIndex = 0;
        int nextInternalIfIndex = addresses.length-1;

         for (InetAddress addr : addresses) {
             if (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress() &&
                     !addr.isSiteLocalAddress() && !addr.isMulticastAddress()) {
                 hosts[nextExternalIfIndex++] = addr.getHostName();
             } else {
                 hosts[nextInternalIfIndex--] = addr.getHostName();
             }
         }

        _info.setHosts(hosts);
        _info.setPort(_listenThread.getListenPort());
        _info.setLoad(_currentLoad);

        _currentBrokerUpdateTime = _brokerUpdateTime;
        for (String loginBroker: _loginBrokers) {
            try {
                sendMessage(new CellMessage(new CellPath(loginBroker),_info));
            } catch (NoRouteToCellException ee) {
                _log.info("Failed to register with LoginBroker: {}",
                        ee.getMessage());
                _currentBrokerUpdateTime = EAGER_UPDATE_TIME;
            }
        }
     }
     public void getInfo( PrintWriter pw ){
        if( _loginBrokers == null ){
           pw.println( "    Login Broker : DISABLED" ) ;
           return ;
        }
        pw.println( "    LoginBroker      : "+ _loginBrokers) ;
        pw.println( "    Protocol Family  : "+_protocolFamily ) ;
        pw.println( "    Protocol Version : "+_protocolVersion ) ;
        pw.println( "    Update Time      : "+(_brokerUpdateTime/1000)+" seconds" ) ;
        pw.println( "    Update Offset    : "+
                    ((int)(_brokerUpdateOffset*100.))+" %" ) ;

     }

     private void loadChanged( int children , int maxChildren ){

       synchronized( this ){
          _currentLoad = (double)children / (double) maxChildren ;
          if(  Math.abs( _info.getLoad() - _currentLoad ) > _brokerUpdateOffset ){
            notifyAll() ;
          }
       }
     }
  }
  private class LoginEventListener implements CellEventListener {
     @Override
     public void cellCreated( CellEvent ce ) { /* forced by interface */  }
     @Override
     public void cellDied( CellEvent ce ) {
        synchronized( _childHash ){
           String removedCell = ce.getSource().toString() ;
           if( ! removedCell.startsWith( getCellName() ) ) {
               return;
           }

       	/*
       	 *  while in some cases remove may be issued prior cell is inserted into _childHash
       	 *  following trick is used:
       	 *  if there is no mapping for this cell, we create a 'dead' mapping, which will
       	 *  allow following put to identify it as a 'dead' and remove it.
       	 *
       	 */

           Object newCell = _childHash.remove( removedCell ) ;
           if( newCell == null ) {
        	   // it's a dead cell, put it back
        	   _childHash.put(removedCell, new Object() );
        	   _log.warn("LoginEventListener : removing DEAD cell: "+removedCell);
           }
           _log.info("LoginEventListener : removing : "+removedCell);
           _childCount -- ;
           childrenCounterChanged() ;
        }
     }
     @Override
     public void cellExported( CellEvent ce ) { /* forced by interface */ }
     @Override
     public void routeAdded( CellEvent ce )   { /* forced by interface */ }
     @Override
     public void routeDeleted( CellEvent ce ) { /* forced by interface */ }
  }
  //
  // the 'send to location manager thread'
  //
  private class LocationThread implements Runnable {
     @Override
     public void run(){

        int listenPort = _listenThread.getListenPort() ;

        _log.info("Sending 'listeningOn "+getCellName()+" "+listenPort+"'") ;
        _sending = true ;
        String dest = _locationManager;
        if( dest == null ) {
            return;
        }
        CellPath path = new CellPath( dest ) ;
        CellMessage msg =
           new CellMessage(
                 path ,
                 "listening on "+getCellName()+" "+listenPort ) ;

        for( int i = 0 ; ! Thread.interrupted() ; i++ ){
          _log.info("Sending ("+i+") 'listening on "+getCellName()+" "+listenPort+"'") ;

          try{
             if( sendAndWait( msg , 5000 ) != null ){
                _log.info("Portnumber successfully sent to "+dest ) ;
                _sending = false ;
                break ;
             }
             _log.warn( "No reply from "+dest ) ;
          }catch( InterruptedException ie ){
             _log.warn( "'send portnumber thread' interrupted");
             break ;
          }catch(Exception ee ){
             _log.warn( "Problem sending portnumber "+ee ) ;
          }
          try{
             Thread.sleep(10000) ;
          }catch(InterruptedException ie ){
             _log.warn( "'send portnumber thread' (sleep) interrupted");
             break ;

          }
        }
     }
  }
  private class KeepAliveThread implements Runnable {
     private long   _keepAlive;
     private final Object _lock      = new Object() ;
     private KeepAliveThread( long keepAlive ){
        _keepAlive = keepAlive ;
     }
     @Override
     public void run(){
        synchronized( _lock ){
          _log.info("KeepAlive Thread started");
          while( ! Thread.interrupted() ){
             try{
                if( _keepAlive < 1 ){
                   _lock.wait() ;
                }else{
                   _lock.wait( _keepAlive ) ;
                }
             }catch(InterruptedException ie ){
                _log.info("KeepAlive thread done (interrupted)");
                break ;
             }

             if( _keepAlive > 0 ) {
                 try {
                     runKeepAlive();
                 } catch (Throwable t) {
                     _log.warn("runKeepAlive reported : " + t);
                 }
             }
          }

        }

     }
     private void setKeepAlive( long keepAlive ){
        synchronized( _lock ){
           _keepAlive = keepAlive ;
           _log.info("Keep Alive value changed to "+_keepAlive);
           _lock.notifyAll() ;
        }
     }
     private long getKeepAlive(){
        return _keepAlive ;
     }
  }
  public static final String hh_set_keepalive = "<keepAliveValue/seconds>";
  public String ac_set_keepalive_$_1( Args args ){
     long keepAlive = Long.parseLong( args.argv(0) ) ;
     _keepAlive.setKeepAlive( keepAlive * 1000L ) ;
     return "keepAlive value set to "+keepAlive+" seconds" ;
  }

  public void runKeepAlive(){
     List<Object> list;
     synchronized( _childHash ){
        list = new ArrayList<>( _childHash.values() ) ;
     }

     for( Object o : list ){

        if( ! ( o instanceof KeepAliveListener ) ) {
            continue;
        }
        try{
           ((KeepAliveListener)o).keepAlive() ;
        }catch(Throwable t ){
           _log.warn("Problem reported by : "+o+" : "+t);
        }
     }
  }

  // the cell implementation
  @Override
public String toString(){
     return
        "p="+(_listenThread==null?"???":(""+_listenThread.getListenPort()))+
        ";c="+_loginClass.getName() ;
  }
  @Override
public void getInfo( PrintWriter pw ){
    pw.println( "  -- Login Manager $Revision: 1.46 $") ;
    pw.println( "  Listen Port    : "+_listenThread.getListenPort() ) ;
    pw.println( "  Login Class    : "+_loginClass ) ;
    pw.println( "  Protocol       : "+_protocol ) ;
    pw.println( "  NioChannel     : "+( _listenThread._serverSocket.getChannel() != null ) ) ;
    pw.println( "  Auth Class     : "+_authClass ) ;
    pw.println( "  Logins created : "+_loginCounter ) ;
    pw.println( "  Logins failed  : "+_loginFailures ) ;
    pw.println( "  Logins denied  : "+_connectionDeniedCounter ) ;
    pw.println( "  KeepAlive      : "+(_keepAlive.getKeepAlive()/1000L) ) ;

    if( _maxLogin > -1 ) {
        pw.println("  Logins/max     : " + _childHash
                .size() + "(" + _childCount + ")/" + _maxLogin);
    }

    if( _locationManager != null ) {
        pw.println("  Location Mgr   : " + _locationManager +
                " (" + (_sending ? "Sending" : "Informed") + ")");
    }

    if( _loginBrokerHandler != null ){
       pw.println( "  LoginBroker Info :" ) ;
       _loginBrokerHandler.getInfo( pw ) ;
    }
  }
  public static final String hh_set_max_logins = "<maxNumberOfLogins>|-1" ;
  public String ac_set_max_logins_$_1( Args args )
  {
      int n = Integer.parseInt( args.argv(0) ) ;
      if( ( n > -1 ) && ( _maxLogin < 0 ) ) {
          throw new
                  IllegalArgumentException("Can't switch off maxLogin feature");
      }
      if( ( n < 0 ) && ( _maxLogin > -1 ) ) {
          throw new
                  IllegalArgumentException("Can't switch on maxLogin feature");
      }

      synchronized( _childHash ){
         _maxLogin = n ;
         childrenCounterChanged() ;
      }
      return "" ;
  }
  @Override
public void cleanUp(){
     _log.info( "cleanUp requested by nucleus, closing listen socket" ) ;
     if( _listenThread != null ) {
         _listenThread.shutdown();
     }
     _log.info( "Bye Bye" ) ;
  }

  private class ListenThread implements Runnable {
     private int          _listenPort;
     private ServerSocket _serverSocket;
     private boolean      _shutdown;
     private Thread       _this;
     private long         _acceptErrorTimeout;
     private boolean      _isDedicated;

     private ListenThread(int listenPort) throws Exception {
        _listenPort   = listenPort ;

        try{
           _acceptErrorTimeout = Long.parseLong(_args.getOpt("acceptErrorWait"));
        }catch(NumberFormatException ee ){ /* bad values ignored */}

         openPort() ;
     }

     private void startLoginBrokerUpdates() {
         if(_loginBrokerHandler != null) {
             _loginBrokerHandler.start();
         }
     }

     private void stopLoginBrokerUpdates() {
         if(_loginBrokerHandler != null) {
             _loginBrokerHandler.stop();
         }
     }

     private void openPort() throws Exception  {

        String ssf = _args.getOpt("socketfactory") ;
        String local   = _args.getOpt("listen");

        if( ssf == null ){
           SocketAddress socketAddress;

           if ( (local == null ) || local.equals("*") || local.equals("")  ) {
               socketAddress =  new InetSocketAddress( _listenPort ) ;
           }else{
               socketAddress = new InetSocketAddress( InetAddress.getByName(local) , _listenPort ) ;
               _isDedicated = true;
           }

           _serverSocket = ServerSocketChannel.open().socket();
           _serverSocket.bind( socketAddress );
           _listenPort   = _serverSocket.getLocalPort() ;

        }else{
           StringTokenizer st = new StringTokenizer(ssf,",");

           /*
            * socket factory initialization has following format:
            *   <classname>[<arg1>,...]
            */
           if( st.countTokens() < 2 ) {
               throw new
               IllegalArgumentException( "Invalid Arguments for 'socketfactory'");
           }

           String tunnelFactoryClass = st.nextToken();
           /*
            * the rest is passed to factory constructor as String[]
            */
           String[] farctoryArgs = new String[ st.countTokens()];
           for( int i = 0; st.hasMoreTokens() ; i++) {
               farctoryArgs[i] = st.nextToken();
           }


           Class<?> []  constructorArgClassA = { String[].class , Map.class } ;
           Class<?> []  constructorArgClassB = { String[].class } ;


           Class<?>     ssfClass = Class.forName(tunnelFactoryClass);
           Object [] args;

           Constructor<?> ssfConstructor;
           try{
              ssfConstructor = ssfClass.getConstructor(constructorArgClassA) ;
              args = new Object[2] ;
              args[0] = farctoryArgs;
              Map<String, Object> map = newHashMap(getDomainContext()) ;
              map.put( "UserValidatable" , LoginManager.this ) ;
              args[1] = map ;
           }catch( Exception ee ){
              ssfConstructor = ssfClass.getConstructor(constructorArgClassB) ;
              args = new Object[1] ;
              args[0] = farctoryArgs;
           }

           Object obj;
           try {
              obj = ssfConstructor.newInstance(args);
           } catch (InvocationTargetException e) {
               Throwable t = e.getCause();
               if(t instanceof Exception) {
                   throw (Exception)t;
               } else {
                   throw new Exception(t.getMessage(), t);
               }
           }

           Method meth = ssfClass.getMethod("createServerSocket", new Class[0]) ;
           _serverSocket = (ServerSocket)meth.invoke( obj ) ;

           if ( (local == null ) || local.equals("*") || local.equals("")  ) {
               _serverSocket.bind(new InetSocketAddress( _listenPort ) );
           }else{
               _serverSocket.bind(new InetSocketAddress(InetAddress.getByName(local), _listenPort ) );
               _isDedicated = true;
           }

           _log.info("ListenThread : got serverSocket class : "+_serverSocket.getClass().getName());
        }

        if( _logSocketIO.isDebugEnabled() ) {
            _logSocketIO.debug("Socket BIND local = " + _serverSocket.getInetAddress() + ":" + _serverSocket.getLocalPort() );
        }
        _log.info("Nio Socket Channel : "+(_serverSocket.getChannel()!=null));
     }
     public int getListenPort(){ return _listenPort ; }
     public InetAddress[] getInetAddress(){
         InetAddress[] addresses = null;
         if( _isDedicated ) {
             if( _serverSocket != null ) {
                 addresses = new InetAddress[1];
                 addresses[0] =  _serverSocket.getInetAddress() ;
             }
         }else{

//            put all local Ip addresses, except loopback
             try {
                 Enumeration<NetworkInterface> ifList = NetworkInterface.getNetworkInterfaces();

                 Vector<InetAddress> v = new Vector<>();
                 while( ifList.hasMoreElements() ) {

                     NetworkInterface ne = ifList.nextElement();

                     Enumeration<InetAddress> ipList = ne.getInetAddresses();
                     while( ipList.hasMoreElements() ) {
                        InetAddress ia = ipList.nextElement();
                        // Currently we do not handle ipv6
                        if( ! (ia instanceof Inet4Address) ) {
                            continue;
                        }
                        if( ! ia.isLoopbackAddress() ) {
                            v.add( ia ) ;
                        }
                     }
                 }
                 addresses = v.toArray( new InetAddress[ v.size() ] );
             }catch(SocketException se_ignored) {}
         }

         return addresses;
     }

     @Override
     public void run(){
         _this = Thread.currentThread() ;

         try {
             startLoginBrokerUpdates();
             while (true) {
                 Socket socket;
                 try {
                     socket = _serverSocket.accept();
                     socket.setKeepAlive(true);
                     socket.setTcpNoDelay(true);
                     if (_logSocketIO.isDebugEnabled()) {
                         _logSocketIO.debug("Socket OPEN (ACCEPT) remote = "
                                 + socket.getInetAddress() + ":" + socket.getPort()
                                 + " local = " + socket.getLocalAddress() + ":"
                                 + socket.getLocalPort());
                     }
                     _log.info("Nio Channel (accept) : " + (socket.getChannel() != null));

                     int currentChildHash;
                     synchronized (_childHash) {
                         currentChildHash = _childCount;
                     }
                     _log.info("New connection : " + currentChildHash);
                     if ((_maxLogin > -1) && (currentChildHash >= _maxLogin)) {
                         _connectionDeniedCounter++;
                         _log.warn("Connection denied " + currentChildHash + " > " + _maxLogin);
                         _logSocketIO.warn("number of allowed logins exceeded.");
                         ShutdownEngine engine = new ShutdownEngine(socket);
                         engine.start();
                         continue;
                     }
                     _log.info("Connection request from " + socket.getInetAddress());
                     synchronized (_childHash) {
                         _childCount++;
                     }
                     _nucleus.newThread(
                             new RunEngineThread(socket),
                             "ClientThread-" + socket.getInetAddress() + ":" + socket.getPort()).start();

                 } catch (InterruptedIOException ioe) {
                     _log.warn("Listen thread interrupted");
                     try {
                         _serverSocket.close();
                     } catch (IOException ee) {
                     }
                     break;
                 } catch (IOException ioe) {
                     if (_serverSocket.isClosed()) {
                         break;
                     }

                     _log.warn("Got an IO Exception ( closing server ) : " + ioe);
                     try {
                         _serverSocket.close();
                     } catch (IOException ee) {
                     }
                     if (_acceptErrorTimeout <= 0L) {
                         break;
                     }
                     _log.warn("Waiting " + _acceptErrorTimeout + " msecs");
                     try {
                         Thread.sleep(_acceptErrorTimeout);
                     } catch (InterruptedException ee) {
                         _log.warn("Recovery halt interrupted");
                         break;
                     }
                     _log.warn("Resuming listener");
                     try {

                         openPort();

                     } catch (Exception ee) {
                         _log.warn("openPort reported : " + ee);
                         _log.warn("Waiting " + _acceptErrorTimeout + " msecs");
                         try {
                             Thread.sleep(_acceptErrorTimeout);
                         } catch (InterruptedException eee) {
                             _log.warn("Recovery halt interrupted");
                             break;
                         }
                     }
                 }

             }
         }finally {
             stopLoginBrokerUpdates();
         }
         _log.info( "Listen thread finished");
     }


     /**
      * Class that closes the output half of a TCP socket,
      * drains any pending input and closes the input once drained.
      * After creation, the {@link #start} method must be called.  The activity
      * occurs on a separate thread, allowing the start method to be
      * non-blocking.
      */
     public class ShutdownEngine extends Thread {
         private final Socket _socket;

         public ShutdownEngine(Socket socket) {
           super("Shutdown");
           _socket = socket;
         }

         @Override
         public void run(){
           InputStream inputStream;
           OutputStream outputStream;
           try{
              inputStream  = _socket.getInputStream() ;
              outputStream = _socket.getOutputStream() ;
              outputStream.close() ;
              byte [] buffer = new byte[1024] ;
              /*
               * eat the outstanding date from socket and close it
               */
              while( inputStream.read(buffer,0,buffer.length) > 0 ) {
              }
              inputStream.close() ;
           }catch(IOException ee ){
              _log.warn("Shutdown : "+ee.getMessage() ) ;
           }finally{
        	   try {
               	if( _logSocketIO.isDebugEnabled() ) {
            		_logSocketIO.debug("Socket CLOSE (ACCEPT) remote = " + _socket.getInetAddress() + ":" + _socket.getPort() +
            					" local = " +_socket.getLocalAddress() + ":" + _socket.getLocalPort() );
            	}
				_socket.close() ;
			} catch (IOException e) {
				// ignore
			}
           }

           _log.info( "Shutdown : done");
         }
     }

     public synchronized void shutdown(){

        _log.info("Listen thread shutdown requested") ;
        //
        // it is still hard to stop an Pending I/O call.
        //
        if( _shutdown || ( _serverSocket == null ) ) {
            return;
        }
        _shutdown = true ;

        try{
            if (_logSocketIO.isDebugEnabled()) {
                _logSocketIO.debug("Socket SHUTDOWN local = " +
                        _serverSocket.getInetAddress() + ":" +
                        _serverSocket.getLocalPort());
            }
            _serverSocket.close() ; }
        catch(IOException ee){
            _log.warn( "ServerSocket close : "+ee  ) ;
        }

        if (_serverSocket.getChannel() == null) {
            _log.info("Using faked connect to shutdown listen port");
            try {
                new Socket("localhost", _listenPort).close();
            } catch (IOException e) {
                _log.debug("ServerSocket faked connect: {}", e.getMessage());
            }
        }

        _this.interrupt() ;

        _log.info("Shutdown sequence done");
     }
  }

  private class RunEngineThread implements Runnable {
     private Socket _socket;
     private RunEngineThread( Socket socket ){
        _socket = socket ;
     }
     @Override
     public void run(){
       Thread t = Thread.currentThread() ;
       try{
          _log.info( "acceptThread ("+t+"): creating protocol engine" ) ;

          StreamEngine engine;
          if (_authConstructor != null) {
               engine = StreamEngineFactory.newStreamEngine(_socket, _protocol,
                       _nucleus, getArgs());
          } else {
               engine = StreamEngineFactory.newStreamEngineWithoutAuth(_socket,
                       _protocol);
          }

          String userName = Subjects.getDisplayName(engine.getSubject());
          _log.info( "acceptThread ("+t+"): connection created for user "+userName ) ;
          Object [] args ;

          int p = userName.indexOf('@');

          if( p > -1 ) {
              userName = p == 0 ? "unknown" : userName.substring(0, p);
          }

          if( _loginConType == 0 ){
             args =  new Object[2] ;
             args[0] = getCellName()+"-"+userName+"*" ;
             args[1] = engine ;
          }else{
             args =  new Object[3] ;
             args[0] = getCellName()+"-"+userName+"*" ;
             args[1] = engine ;
             args[2] = new Args(_childArgs);
          }

          Object cell = _loginConstructor.newInstance( args ) ;
          if( _maxLogin > -1 ){
             try{
                Method m = cell.getClass().getMethod( "getCellName" , new Class[0] ) ;
                String cellName = (String)m.invoke( cell) ;
                _log.info("Invoked cell name : "+cellName ) ;
                synchronized( _childHash ){

                	/*
                     *  while cell may be already gone do following trick:
                     *  if put return an old cell, then it's a dead cell and we
                     *  have to remove it. Dead cell is inserted by cleanup procedure:
                     *  if a remove for non existing cells issued, then cells is dead, and
                     *  we put it into _childHash.
                     */

                   Object deadCell = _childHash.put(cellName,cell) ;
                      if(deadCell != null ) {
                         _childHash.remove(cellName);
                         _log.warn("Cell died, removing " + cellName) ;
                      }

                   childrenCounterChanged() ;
                }
             }catch( Exception ee ){
                 _log.warn("Can't determine child name " + ee, ee) ;
             }
          }
          _loginCounter ++ ;

       }catch( Exception e ){
          try{ _socket.close() ; }catch(IOException ee ){/* dead any way....*/}
          _log.warn( "Exception in secure protocol : {}", e.toString() ) ;
          _loginFailures ++ ;
          synchronized( _childHash ){ _childCount -- ; }
       }


     }
  }
  private void childrenCounterChanged(){
      int children = _childHash.size() ;
      _log.info( "New child count : "+children ) ;
      if( _loginBrokerHandler != null ) {
          _loginBrokerHandler.loadChanged(children, _maxLogin);
      }
  }
  @Override
  public boolean validateUser( String userName , String password ){
     String [] request = new String[5] ;

     request[0] = "request" ;
     request[1] = userName ;
     request[2] = "check-password" ;
     request[3] = userName ;
     request[4] = password ;

     try{
        CellMessage msg = new CellMessage( new CellPath(_authenticator) ,
                                           request ) ;

        msg = sendAndWait( msg , 10000 ) ;
        if( msg == null ){
                  _log.warn("Pam request timed out {}",Thread.currentThread().getStackTrace());
        return false ;
        }

        Object [] r = (Object [])msg.getMessageObject() ;

        return (Boolean) r[5];

      } catch (NoRouteToCellException ee) {
          _log.warn(ee.toString(), ee);
          return false;
      } catch (InterruptedException ee) {
          _log.warn(ee.toString(), ee);
          return false;
      }
  }
}
