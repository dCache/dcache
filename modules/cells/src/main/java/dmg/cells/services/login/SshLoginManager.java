package  dmg.cells.services.login ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.StreamLoginCell;
import dmg.protocols.ssh.SshRsaKey;
import dmg.protocols.ssh.SshRsaKeyContainer;
import dmg.protocols.ssh.SshServerAuthentication;
import dmg.protocols.ssh.SshSharedKey;
import dmg.protocols.ssh.SshStreamEngine;
import dmg.util.StreamEngine;

import org.dcache.util.Args;

/**
 **
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
 */
public class       SshLoginManager
       extends     CellAdapter
       implements  Runnable, SshServerAuthentication  {

  private final static Logger _log =
      LoggerFactory.getLogger(SshLoginManager.class);

  private String       _cellName ;
  private CellNucleus  _nucleus ;
  private int          _listenPort ;
  private ServerSocket _serverSocket;
  private Thread       _listenThread ;
  private int          _connectionRequestCounter;
  private int          _connectionAcceptionCounter;
  private Hashtable<Thread, Socket>    _connectionThreads = new Hashtable<>() ;

  private  SshRsaKey          _hostKey  , _serverKey ;
  private  SshRsaKeyContainer _userKeys , _hostKeys ;
  private  int                _keyUpdateInterval = 30 ;

  private  int  _loginCounter, _loginFailures;

  private  Class<? extends StreamLoginCell> _loginClass        = StreamLoginCell.class ;
  private  Constructor<? extends StreamLoginCell> _loginConstructor;
  private  Class<?>[]           _loginConSignature0 = { String.class ,
                                                      StreamEngine.class } ;
  private  Class<?>[]           _loginConSignature1 = { String.class ,
                                                      StreamEngine.class ,
                                                      Args.class } ;
  private  Class<?>[]           _loginPntSignature = { int.class     } ;
  private  int                  _loginConType      = -1 ;
  /**
  */
  public SshLoginManager( String name , String argString ) throws Exception {

      super( name , argString , false ) ;

      _cellName      = name ;
      try{
         Args args = getArgs() ;
         if( args.argc() < 1 ) {
             throw new
                     IllegalArgumentException("USAGE : ... <listenPort> [<loginClass> [...]]");
         }

         _listenPort    = Integer.parseInt( args.argv(0) );
         args.shift() ;
         if( args.argc() > 0 ){
            _loginClass       = Class.forName( args.argv(0) ).asSubclass(StreamLoginCell.class);
            _log.info( "Using login class : "+_loginClass.getName() ) ;
            args.shift() ;
         }
         try{
            _loginConstructor = _loginClass.getConstructor( _loginConSignature1 ) ;
            _loginConType     = 1 ;
         }catch( NoSuchMethodException nsme ){
            _loginConstructor = _loginClass.getConstructor( _loginConSignature0 ) ;
            _loginConType     = 0 ;
         }
         _log.info( "Using constructor : "+_loginConstructor ) ;
         _serverSocket  = new ServerSocket( _listenPort ) ;

         _nucleus       = getNucleus() ;

         _listenThread  = new Thread( this , "listenThread" ) ;

         _listenThread.start() ;
      }catch( Exception e ){
         _log.warn( "SshLoginManger >"+getCellName()+"< got exception : "+e ) ;
         start() ;
         kill() ;
         throw e ;
      }

      start() ;

  }
  //
  // the cell implemetation
  //
   public String toString(){ return "p="+_listenPort+";c="+_loginClass.getName() ; }
   @Override
   public void getInfo( PrintWriter pw ){
     pw.println( "  -- Ssh Login Manager" ) ;
     pw.println( "  Listen Port    : "+_listenPort ) ;
     pw.println( "  Login Class    : "+_loginClass ) ;
     pw.println( "  Logins created : "+_loginCounter ) ;
     pw.println( "  Logins failed  : "+_loginFailures ) ;
   }

  @Override
  public void cleanUp(){
     _log.info( "cleanUp requested by nucleus, closing listen socket" ) ;
     if( _serverSocket != null ) {
         try {
             _serverSocket.close();
         } catch (IOException eee) {
         }
     }
     _log.info( "Bye Bye" ) ;
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
       SshStreamEngine engine = new SshStreamEngine( socket , this ) ;

       String userPrincipal = engine.getName();
       _log.info( "acceptThread ("+t+"): connection created for user "+userPrincipal ) ;
       Object [] args ;
       if( _loginConType == 0 ){
          args =  new Object[2] ;
          args[0] = getCellName()+"-"+userPrincipal+"*" ;
          args[1] = engine ;
       }else{
          args =  new Object[3] ;
          args[0] = getCellName()+"-"+userPrincipal+"*" ;
          args[1] = engine ;
          args[2] = new Args(getArgs());
       }
       try{
            Object cell = _loginConstructor.newInstance( args ) ;
            _loginCounter ++ ;
       }catch( Exception ie ){
          _log.warn( "Can't create new instance of "+_loginClass.getName()+" "+ie ) ;
          engine.close() ;
          _loginFailures ++ ;
       }

    }catch( Exception e ){
       _log.warn( "Exception in secure protocol : "+e ) ;
       _loginFailures ++ ;
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
  //
  // ssh server authentication
  //
  private SshRsaKey getIdentity( String keyName ){

     Map<String,Object> sshContext =
         (Map<String,Object>) _nucleus.getDomainContext().get("Ssh");

     if( sshContext == null ){
        _log.warn( "Auth ("+keyName+") : Ssh Context unavailable" ) ;
        return null ;
     }

     SshRsaKey   key =  (SshRsaKey)sshContext.get( keyName ) ;

     _log.info( "Auth : Request for "+keyName+(key==null?" Failed":" o.k.") ) ;
     return key ;
  }
  @Override
  public SshRsaKey  getHostRsaKey(){ return getIdentity("hostIdentity" ) ; }
  @Override
  public SshRsaKey  getServerRsaKey(){return getIdentity("serverIdentity" ) ; }
  @Override
  public SshSharedKey  getSharedKey( InetAddress host , String keyName ){
     _log.info( "Auth : Request for Shared Key denied" ) ;
     return null ;
  }

  @Override
  public boolean   authUser( InetAddress addr, String user ){
     _log.info( "Auth : User Request for user "+user+" host "+addr+" denied" ) ;
     return false ;
  }
  @Override
  public boolean   authRhosts( InetAddress addr, String user ){
     _log.info( "Auth : Rhost Request for user "+user+" host "+addr+" denied" ) ;
     return false ;
  }

  @Override
  public boolean   authPassword(  InetAddress addr, String user, String password ) {
     _log.info( "Auth : Password Request for user "+user+" host "+addr ) ;
     Map<String,Object> sshContext =
         (Map<String,Object>) _nucleus.getDomainContext().get("Ssh");
     if( sshContext == null ){
        _log.warn( "Auth authPassword : Ssh Context unavailable for request from User "+
                       user+" Host "+addr ) ;
        return false ;
     }
     Object userObject = sshContext.get( "userPasswords" ) ;
     if( userObject == null ){
        _log.warn( "Auth authPassword : userPasswords not available" ) ;
        return false ;
     }
     if( userObject instanceof Hashtable ){
        Hashtable<String,String> passwords = (Hashtable<String,String>) userObject ;
        String realPassword = passwords.get( user ) ;
        if( realPassword != null ){
           if( password.equals( realPassword ) ){
              return true ;
           }else{
              _log.warn( "Auth authPassword : user "+user+" password mismatch " ) ;
              return false ;
           }
        }
        _log.warn( "Auth authPassword : user "+user+" not found " ) ;
        return false ;
     }else if( userObject instanceof String ){
        CellPath path = new CellPath( (String) userObject ) ;
        _log.info( "Auth passwd : using : "+path ) ;
        Object [] request = new Object[5] ;
        request[0] = "request" ;
        request[1] = "unknown" ;
        request[2] = "check-password" ;
        request[3] = user ;
        request[4] = password ;
        CellMessage msg = new CellMessage( path , request ) ;
        try{
            msg = getNucleus().sendAndWait(msg, (long) 4000);
            if( msg == null ){
               _log.warn( "request for user >"+user+"< timed out" ) ;
               return false ;
            }
        }catch(NoRouteToCellException | InterruptedException e ){
            _log.warn( "Problem for user >"+user+"< : "+e ) ;
            return false ;
        } catch (ExecutionException e) {
            _log.warn( "Problem for user >"+user+"< : "+e.getCause() ) ;
            return false ;
        }
         Object obj;
        if( ( obj = msg.getMessageObject() ) == null ){
           _log.warn( "Request response is null" ) ;
           return false ;
        }
        if( ! ( obj instanceof Object [] ) ){
           _log.warn( "Response not Object[] : "+obj.getClass() ) ;
           return false ;
        }else{
           request = (Object[])obj ;
           if( request.length < 6 ){
              _log.warn( "Response length < 6") ;
              return false ;
           }
           if( ( ! ( request[0] instanceof String )        ) ||
               ( ! request[0].equals("response") ) ||
               ( ! ( request[5] instanceof Boolean )       )    ){
               _log.warn( "Not a response" ) ;
               return false ;
           }
           _log.info( "Response for >"+user+"< : "+request[5] ) ;
           return (Boolean) request[5];
        }

     }

     return false ;
  }
  private SshRsaKey getPublicKey( String domain , SshRsaKey modulusKey ,
                                  InetAddress addr, String user){
     Map<String,Object> sshContext =
         (Map<String,Object>)_nucleus.getDomainContext().get("Ssh");
     _log.info( "Serching Key in "+domain ) ;
     _log.info( ""+modulusKey ) ;
     if( sshContext == null ){
        _log.warn( "Auth ("+domain+") : Ssh Context unavailable for request from User "+
                       user+" Host "+addr ) ;
        return null ;
     }
     SshRsaKeyContainer container = (SshRsaKeyContainer)sshContext.get( domain ) ;
     if( container == null ){
        _log.warn( "Auth ("+domain+") : Ssh "+domain+" unavailable for request from User "+
                       user+" Host "+addr ) ;
        return null ;
     }else{
//       Enumeration e = container.elements() ;
//       for( ; e.hasMoreElements() ; ){
//           SshRsaKey key = (SshRsaKey)e.nextElement() ;
//           _log.info( key.toString() ) ;
//       }
     }
     SshRsaKey key = container.findByModulus( modulusKey ) ;
     if( key == null ){
        _log.warn( "Auth ("+domain+") : Ssh key not found from User "+
                       user+" Host "+addr ) ;
        return null ;
     }

     return key ;

  }
  @Override
  public SshRsaKey authRsa( InetAddress addr, String user , SshRsaKey userKey ){
     SshRsaKey key = getPublicKey( "knownUsers" , userKey , addr , user  ) ;
     String    domain = "knownUsers" ;
     if( key == null ) {
         return null;
     }
     String keyUser = key.getComment() ;
     StringTokenizer st = new StringTokenizer( keyUser , "@" ) ;
     keyUser = st.nextToken() ;
     if( keyUser.equals(user) ){
        _log.info( "Auth ("+domain+
                      ") : Ssh key ("+key.getComment()+
                      ") found for user "+user+
                      " Host "+addr ) ;
        return key ;
     }else{
        _log.info( "Auth ("+domain+
                      ") : Ssh key mismatch "+keyUser+" <> "+user ) ;
        return null ;
     }
  }
  @Override
  public SshRsaKey authRhostsRsa( InetAddress addr, String user ,
                                  String reqUser , SshRsaKey hostKey ){
     _log.info( "Auth (authRhostsRsa) : host="+addr+
                   " user="+user+" reqUser="+reqUser ) ;
     if( ! user.equals( reqUser ) ){
        _log.info( "Auth : user mismatch , proxy user not allowed" ) ;
        return null ;
     }
     return getPublicKey( "knownHosts"  , hostKey , addr , user ) ;
  }
}

