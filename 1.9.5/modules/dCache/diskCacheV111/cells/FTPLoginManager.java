// $Id: FTPLoginManager.java,v 1.2 2006-09-05 14:10:57 tigran Exp $


package diskCacheV111.cells;

import java.lang.reflect.* ;
import java.net.* ;
import java.io.* ;
import java.util.*;

import dmg.cells.nucleus.*; 
import dmg.util.*;
import dmg.protocols.telnet.* ;

/**
 **
  *  
  *
  * @author Charles G Waldman
  * @version 0.0, Sep 15 1999
  * Modified from TelnetLoginManager.java
  * 
 */
public class      FTPLoginManager 
       extends    CellAdapter
       implements Cell, Runnable  {

  private String       _cellName ;
  private CellNucleus  _nucleus ;
  private int          _listenPort ;
  private ServerSocket _serverSocket ;
  private Thread       _listenThread ;
  private int          _connectionRequestCounter   = 0 ;
  private int          _connectionAcceptionCounter = 0 ;
  private Hashtable    _connectionThreads = new Hashtable() ;
  private Args         _args ;
  private String       _loginCellClass =  "dmg.cells.services.StreamLoginCell" ;
  private boolean      _opt_localhost , _opt_dummy ;
  private boolean      _opt_elch , _opt_anyuser , _opt_raw  ;


  private static final String __usage = 
     "<port> [loginCell] [-dummy] [-localhost] [-anyuser] [-elch]" ;
  /**
   */
  public FTPLoginManager( String name , String args ) throws Exception {
    super( name , args , false ) ;
    _nucleus       = getNucleus() ;
    _args          = getArgs() ;      
    _cellName      = name ;
    
    try{
      if( _args.argc() < 1 )
             throw new IllegalArgumentException( "USAGE : ... "+__usage ) ;
      
      _listenPort = new Integer( _args.argv(0) ).intValue() ;
      
      if( _args.argc() > 1 )
	_loginCellClass = _args.argv(1) ;
      
      _opt_dummy     = false ;
      _opt_localhost = false ;
      _opt_anyuser   = false ;
      _opt_elch      = true ;
      _opt_raw       = false ;
      for( int i = 0 ; i < _args.optc() ; i++ ){
	if( _args.optv(i).equals( "-dummy" ) )_opt_dummy = true ;
	else if( _args.optv(i).equals( "-localhost" ) )_opt_localhost = true ;
	else if( _args.optv(i).equals( "-elch" ) )_opt_elch = true ;          
	else if( _args.optv(i).equals( "-anyuser" ) )_opt_anyuser = true ;          
	else if( _args.optv(i).equals( "-raw" ) )_opt_raw = true ;          
      }
      _serverSocket  = new ServerSocket( _listenPort ) ;
    }catch( Exception e ){
      start() ;
      kill() ;
      if( e instanceof IllegalArgumentException )
	throw (IllegalArgumentException)e ;
      
      throw new IllegalArgumentException( e.toString() ) ;
    }

    
       
    _listenThread  = new Thread( this , "listenThread" ) ;       
    _listenThread.start() ;
    
    //       _nucleus.setPrintoutLevel( 0xf ) ;
       
    start() ;
  }
  public String prompt(){
    return "FTP";
  }
  
  public void cleanUp(){ 
    try{
      say( "Trying to close serverSocket" ) ;
      _serverSocket.close() ;
      say( "Trying serverSocket close" ) ;
    }catch( Exception ee ){}
  }
  private void acceptConnections(){
    //
    // wait for all the keys
    //
    while( true ){
      Socket      socket = null ;
      try{
	socket = _serverSocket.accept() ;
	_connectionRequestCounter ++ ;
	_nucleus.say( "Connection request from "+socket.getInetAddress() ) ;
	Thread t = new Thread( this ) ;
	_connectionThreads.put( t , socket ) ;
	t.start() ;
      }catch( IOException ioe ){
	_nucleus.esay( "Got an IO Exception ( closing server ) : "+ioe ) ;
	break ;
      }catch( Exception ee ){
	_nucleus.esay( "Got an Exception in getting keys ( closing connection ) : "+ee ) ;
	try{ socket.close() ; }catch( IOException ioex ){}
	continue ;
      }
    }
    
  }
  public void acceptConnection( Socket socket ){
    Thread t = Thread.currentThread() ;
    try{
      _nucleus.say( "acceptThread ("+t+"): creating protocol engine" ) ;
      StreamEngine engine   ;
      engine = _opt_raw ?
	(StreamEngine)new DummyStreamEngine( socket ) :
	(StreamEngine)new TelnetStreamEngine( socket, null) ;
      
      String       userName = engine.getUserName().getName() ; 
      _nucleus.say( "acceptThread ("+t+
		    "): connection created for user "+userName ) ;
      String cellName = "tn-"+userName+"*" ;
      
      String [] paraNames = new String[1] ;
      Object [] parameter = new Object[1] ;
      paraNames[0] = "dmg.util.StreamEngine" ;
      parameter[0] = engine ;
      createNewCell( _loginCellClass , cellName , paraNames , parameter ) ;
      
    }catch( Exception e ){
      _nucleus.esay( "Exception in TelnetStreamEngine : "+e ) ;
      if( e instanceof InvocationTargetException ){
	Exception ie = 
	  (Exception)((InvocationTargetException)e).getTargetException() ;
	_nucleus.esay( "TargetException in TelnetStreamEngine : "+ie ) ;
      }
      try{ socket.close(); }catch(Exception ee){}
    }
    
    
  }
  public void run(){
    Socket currentSocket = null ;
    
    if( Thread.currentThread() == _listenThread ){
      
      acceptConnections() ;
      
    }else if( ( currentSocket = (Socket)
		_connectionThreads.remove( Thread.currentThread() )
                ) != null ){
      
      acceptConnection( currentSocket ) ;      
      
    } 
    
  }
  public String toString(){
    return "P="+_listenPort+";C="+_loginCellClass; 
  }
  public void getInfo( PrintWriter pw){
    pw.println( " ListenPort     : "+_listenPort ) ;
    pw.println( " LoginCellClass : "+_loginCellClass ) ;
    return  ;
  }
  //
  //
  public boolean isHostOk( InetAddress host ){
    _nucleus.say( "Request for host "+host+" ("+host.getHostName()+")" ) ;
    if( _opt_dummy )return true ;
    if( _opt_localhost && (  host.getHostName().equals("localhost") ))return true ; 
    return false ;
  }
  public boolean isUserOk( InetAddress host , String user ){
    _nucleus.say( "Request for host "+host+" user "+user ) ;
    return _opt_anyuser ;
  }
  public boolean isPasswordOk( InetAddress host , String user , String passwd ){
    _nucleus.say( "Request for host "+host+" user "+user+" password "+passwd ) ;
    return passwd.equals("elch") ? true : false ;
  }
}

