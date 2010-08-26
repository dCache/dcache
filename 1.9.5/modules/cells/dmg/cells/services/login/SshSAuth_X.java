package  dmg.cells.services.login ;

import java.lang.reflect.* ;
import java.net.* ;
import java.io.* ;
import java.util.*;
import dmg.cells.nucleus.*;
import dmg.util.*;
import dmg.protocols.ssh.* ;

/**
 **
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
 */
public class       SshSAuth_X
       implements  SshServerAuthentication  {


  private  SshRsaKey          _hostKey  , _serverKey ;
  private  SshRsaKeyContainer _userKeys , _hostKeys ;
  private  CellNucleus        _nucleus ;
  private  Args               _args ;
  /**
  */
  public SshSAuth_X( CellNucleus nucleus , Args args ){
      _nucleus = nucleus ;
      _args    = args ;
  }
  public void say( String str ){ _nucleus.say( str ) ; }
  public void esay( String str ){ _nucleus.esay( str ) ; }
  //
  // ssh server authentication
  //
  private SshRsaKey getIdentity( String keyName ){

      Map<String,Object> sshContext =
          (Map<String,Object>) _nucleus.getDomainContext().get("Ssh");

     if( sshContext == null ){
        esay( "Auth ("+keyName+") : Ssh Context unavailable" ) ;
        return null ;
     }

     SshRsaKey   key =  (SshRsaKey)sshContext.get( keyName ) ;

     say( "Auth : Request for "+keyName+(key==null?" Failed":" o.k.") ) ;
     return key ;
  }
  public SshRsaKey  getHostRsaKey(){
      return getIdentity("hostIdentity" ) ;
  }
  public SshRsaKey  getServerRsaKey(){
      return getIdentity("serverIdentity" ) ;
  }
  public SshSharedKey  getSharedKey( InetAddress host , String keyName ){
     say( "Auth : Request for Shared Key denied" ) ;
     return null ;
  }

  public boolean   authUser( InetAddress addr, String user ){
     say( "Auth : User Request for user "+user+" host "+addr+" denied" ) ;
     return true ;
  }
  public boolean   authRhosts( InetAddress addr, String user ){
     say( "Auth : Rhost Request for user "+user+" host "+addr+" denied" ) ;
     return true ;
  }

  public boolean authPassword(  InetAddress addr,
                                String user,
                                String password             ){

     say( "Auth : Password Request for user "+user+" host "+addr ) ;
     return true ;
  }
  private SshRsaKey getPublicKey( String domain , SshRsaKey modulusKey ,
                                  InetAddress addr, String user){
      Map<String,Object> sshContext =
          (Map<String,Object>) _nucleus.getDomainContext().get("Ssh");
     say( "Serching Key in "+domain ) ;
     say( ""+modulusKey ) ;
     if( sshContext == null ){
        esay( "Auth ("+domain+
              ") : Ssh Context unavailable for request from User "+user+
              " Host "+addr ) ;
        return null ;
     }
     SshRsaKeyContainer container =
                  (SshRsaKeyContainer)sshContext.get( domain ) ;
     if( container == null ){
        esay( "Auth ("+domain+") : Ssh "+domain+
              " unavailable for request from User "+user+
              " Host "+addr ) ;
        return null ;
     }else{
//       Enumeration e = container.elements() ;
//       for( ; e.hasMoreElements() ; ){
//           SshRsaKey key = (SshRsaKey)e.nextElement() ;
//           say( key.toString() ) ;
//       }
     }
     SshRsaKey key = container.findByModulus( modulusKey ) ;
     if( key == null ){
        esay( "Auth ("+domain+") : Ssh key not found from User "+
                       user+" Host "+addr ) ;
        return null ;
     }

     return key ;

  }
  public SshRsaKey authRsa( InetAddress addr,
                            String user ,
                            SshRsaKey userKey         ){

     SshRsaKey key = getPublicKey( "knownUsers" , userKey , addr , user  ) ;
     String    domain = "knownUsers" ;
     if( key == null )return null ;
     String keyUser = key.getComment() ;
     StringTokenizer st = new StringTokenizer( keyUser , "@" ) ;
     keyUser = st.nextToken() ;
     if( keyUser.equals(user) ){
        say( "Auth ("+domain+
                      ") : Ssh key ("+key.getComment()+
                      ") found for user "+user+
                      " Host "+addr ) ;
        return key ;
     }else{
        say( "Auth ("+domain+
                      ") : Ssh key mismatch "+keyUser+" <> "+user ) ;
        return null ;
     }
  }
  public SshRsaKey authRhostsRsa( InetAddress addr, String user ,
                                  String reqUser , SshRsaKey hostKey ){
     say( "Auth (authRhostsRsa) : host="+addr+
                   " user="+user+" reqUser="+reqUser ) ;
     if( ! user.equals( reqUser ) ){
        say( "Auth : user mismatch , proxy user not allowed" ) ;
        return null ;
     }
     return getPublicKey( "knownHosts"  , hostKey , addr , user ) ;
  }
}

