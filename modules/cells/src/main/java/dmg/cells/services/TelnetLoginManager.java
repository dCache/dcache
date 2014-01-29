package  dmg.cells.services ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;

import dmg.cells.nucleus.Cell;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellNucleus;
import dmg.protocols.telnet.TelnetServerAuthentication;
import dmg.protocols.telnet.TelnetStreamEngine;
import dmg.util.DummyStreamEngine;
import dmg.util.StreamEngine;

import org.dcache.auth.Subjects;
import org.dcache.util.Args;

/**
 **
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
 */
public class      TelnetLoginManager
       extends    CellAdapter
       implements Cell, Runnable, TelnetServerAuthentication  {

  private final static Logger _log =
        LoggerFactory.getLogger(TelnetLoginManager.class);

  private String       _cellName ;
  private CellNucleus  _nucleus ;
  private int          _listenPort ;
  private ServerSocket _serverSocket ;
  private Thread       _listenThread ;
  private int          _connectionRequestCounter;
  private int          _connectionAcceptionCounter;
  private Hashtable<Thread, Socket>    _connectionThreads = new Hashtable<>() ;
  private Args         _args ;
  private String       _loginCellClass =  "dmg.cells.services.StreamLoginCell" ;
  private boolean      _opt_localhost , _opt_dummy ;
  private boolean      _opt_elch , _opt_anyuser , _opt_raw  ;


  private static final String __usage =
     "<port> {loginCell] [-dummy] [-localhost] [-anyuser] [-elch]" ;
  /**
  */
  public TelnetLoginManager( String name , String args )
  {
       super( name , args , false ) ;
       _nucleus       = getNucleus() ;
       _args          = getArgs() ;
       _cellName      = name ;

       try{
          if( _args.argc() < 1 ) {
              throw new IllegalArgumentException("USAGE : ... " + __usage);
          }

          _listenPort = new Integer(_args.argv(0));

          if( _args.argc() > 1 ) {
              _loginCellClass = _args.argv(1);
          }

          _opt_dummy     = false ;
          _opt_localhost = false ;
          _opt_anyuser   = false ;
          _opt_elch      = true ;
          _opt_raw       = false ;
          for( int i = 0 ; i < _args.optc() ; i++ ){
             if( _args.optv(i).equals( "-dummy" ) ) {
                 _opt_dummy = true;
             } else if( _args.optv(i).equals( "-localhost" ) ) {
                 _opt_localhost = true;
             } else if( _args.optv(i).equals( "-elch" ) ) {
                 _opt_elch = true;
             } else if( _args.optv(i).equals( "-anyuser" ) ) {
                 _opt_anyuser = true;
             } else if( _args.optv(i).equals( "-raw" ) ) {
                 _opt_raw = true;
             }
          }
          _serverSocket  = new ServerSocket( _listenPort ) ;
       }catch( Exception e ){
          start() ;
          kill() ;
          if( e instanceof IllegalArgumentException ) {
              throw (IllegalArgumentException) e;
          }

          throw new IllegalArgumentException( e.toString() ) ;
       }



       _listenThread  = new Thread( this , "listenThread" ) ;
       _listenThread.start() ;

//       _nucleus.setPrintoutLevel( 0xf ) ;

       start() ;
  }
  @Override
  public void cleanUp(){
      try {
          _log.info("Trying to close serverSocket");
          _serverSocket.close();
          _log.info("Trying serverSocket close returned");
      } catch (Exception ee) {
          _log.warn("ignoring exception on telnetoutputstream.write {}", ee.toString());
      }
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
               _log.info( "Connection request from "+socket.getInetAddress() ) ;
               Thread t = new Thread( this ) ;
               _connectionThreads.put( t , socket ) ;
               t.start() ;
            }catch( IOException ioe ){
               _log.warn( "Got an IO Exception ( closing server ) : "+ioe ) ;
               break ;
            }catch( Exception ee ){
               _log.warn( "Got an Exception in getting keys ( closing connection ) : "+ee ) ;
               if(socket != null) {
                   try {
                       socket.close();
                   } catch (IOException ioex) {
                   }
               }
            }
         }

  }
  public void acceptConnection( Socket socket ){
    Thread t = Thread.currentThread() ;
    try{
       _log.info( "acceptThread ("+t+"): creating protocol engine" ) ;
       StreamEngine engine   ;
       engine = _opt_raw ?
               new DummyStreamEngine( socket ) :
               new TelnetStreamEngine( socket , this );

       String name = Subjects.getDisplayName(engine.getSubject());
       _log.info( "acceptThread ("+t+
                     "): connection created for user "+name ) ;
       String cellName = "tn-"+name+"*" ;

       String [] paraNames = new String[1] ;
       Object [] parameter = new Object[1] ;
       paraNames[0] = "dmg.util.StreamEngine" ;
       parameter[0] = engine ;
       createNewCell( _loginCellClass , cellName , paraNames , parameter ) ;

    }catch( Exception e ){
       _log.warn( "Exception in TelnetStreamEngine : "+e ) ;
       if( e instanceof InvocationTargetException ){
          Exception ie =
             (Exception)((InvocationTargetException)e).getTargetException() ;
             _log.warn( "TargetException in TelnetStreamEngine : "+ie ) ;
       }
       try{ socket.close(); }catch(Exception ee){}
    }


  }
  @Override
  public void run(){
     Socket currentSocket;

     if( Thread.currentThread() == _listenThread ){

         acceptConnections() ;

      }else if( ( currentSocket = _connectionThreads.remove( Thread.currentThread() )
                ) != null ){

         acceptConnection( currentSocket ) ;

      }

  }
  public String toString(){
       return "P="+_listenPort+";C="+_loginCellClass;
  }
  @Override
  public void getInfo( PrintWriter pw){
    pw.println( " ListenPort     : "+_listenPort ) ;
    pw.println( " LoginCellClass : "+_loginCellClass ) ;
  }
  //
  // ssh server authetication
  //
   @Override
   public boolean isHostOk( InetAddress host ){
      _log.info( "Request for host "+host+" ("+host.getHostName()+")" ) ;
      if( _opt_dummy ) {
          return true;
      }
      if( _opt_localhost && (  host.getHostName().equals("localhost") )) {
          return true;
      }
      return false ;
   }
   @Override
   public boolean isUserOk( InetAddress host , String user ){
      _log.info( "Request for host "+host+" user "+user ) ;
      return _opt_anyuser ;
   }
   @Override
   public boolean isPasswordOk( InetAddress host , String user , String passwd ){
      _log.info( "Request for host "+host+" user "+user+" password "+passwd ) ;
      return passwd.equals("elch") ? true : false ;
   }
}

