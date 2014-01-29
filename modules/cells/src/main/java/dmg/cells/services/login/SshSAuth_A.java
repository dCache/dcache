package  dmg.cells.services.login ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Hashtable;
import java.util.Map;
import java.util.StringTokenizer;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.protocols.ssh.SshRsaKey;
import dmg.protocols.ssh.SshRsaKeyContainer;
import dmg.protocols.ssh.SshServerAuthentication;
import dmg.protocols.ssh.SshSharedKey;

import org.dcache.util.Args;

/**
 **
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
 */
public class       SshSAuth_A
       implements  SshServerAuthentication  {

  private final static Logger _log =
      LoggerFactory.getLogger(SshSAuth_A.class);

  private  SshRsaKey          _hostKey  , _serverKey ;
  private  SshRsaKeyContainer _userKeys , _hostKeys ;
  private  CellNucleus        _nucleus ;
  private  Args               _args ;
  /**
  */
  public SshSAuth_A( CellNucleus nucleus , Args args ){
      _nucleus = nucleus ;
      _args    = args ;
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
  public SshRsaKey  getHostRsaKey(){
      return getIdentity("hostIdentity" ) ;
  }
  @Override
  public SshRsaKey  getServerRsaKey(){
      return getIdentity("serverIdentity" ) ;
  }
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
  public boolean authPassword(  InetAddress addr,
                                String user,
                                String password             ){

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
            msg = _nucleus.sendAndWait( msg , 4000 ) ;
            if( msg == null ){
               _log.warn( "request for user >"+user+"< timed out" ) ;
               return false ;
            }
        }catch(Exception e ){
            _log.warn( "Problem for user >"+user+"< : "+e ) ;
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
         (Map<String,Object>) _nucleus.getDomainContext().get("Ssh");
     _log.info( "Serching Key in "+domain ) ;
     _log.info( ""+modulusKey ) ;
     if( sshContext == null ){
        _log.warn( "Auth ("+domain+
              ") : Ssh Context unavailable for request from User "+user+
              " Host "+addr ) ;
        return null ;
     }
     SshRsaKeyContainer container =
                  (SshRsaKeyContainer)sshContext.get( domain ) ;
     if( container == null ){
        _log.warn( "Auth ("+domain+") : Ssh "+domain+
              " unavailable for request from User "+user+
              " Host "+addr ) ;
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
  public SshRsaKey authRsa( InetAddress addr,
                            String user ,
                            SshRsaKey userKey         ){

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

