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
public class       SshSAuth_A
       implements  SshServerAuthentication  {


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
     return false ;
  }
  public boolean   authRhosts( InetAddress addr, String user ){
     say( "Auth : Rhost Request for user "+user+" host "+addr+" denied" ) ;
     return false ;
  }

  public boolean authPassword(  InetAddress addr,
                                String user,
                                String password             ){

     say( "Auth : Password Request for user "+user+" host "+addr ) ;
     Map<String,Object> sshContext =
         (Map<String,Object>) _nucleus.getDomainContext().get("Ssh");
     if( sshContext == null ){
        esay( "Auth authPassword : Ssh Context unavailable for request from User "+
                       user+" Host "+addr ) ;
        return false ;
     }
     Object userObject = sshContext.get( "userPasswords" ) ;
     if( userObject == null ){
        esay( "Auth authPassword : userPasswords not available" ) ;
        return false ;
     }
     if( userObject instanceof Hashtable ){
        Hashtable passwords = (Hashtable)userObject ;
        String realPassword = (String)passwords.get( user ) ;
        if( realPassword != null ){
           if( password.equals( realPassword ) ){
              return true ;
           }else{
              esay( "Auth authPassword : user "+user+" password mismatch " ) ;
              return false ;
           }
        }
        esay( "Auth authPassword : user "+user+" not found " ) ;
        return false ;
     }else if( userObject instanceof String ){
        CellPath path = new CellPath( (String) userObject ) ;
        say( "Auth passwd : using : "+path ) ;
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
               esay( "request for user >"+user+"< timed out" ) ;
               return false ;
            }
        }catch(Exception e ){
            esay( "Problem for user >"+user+"< : "+e ) ;
            return false ;
        }
        Object obj = null ;
        if( ( obj = msg.getMessageObject() ) == null ){
           esay( "Request response is null" ) ;
           return false ;
        }
        if( ! ( obj instanceof Object [] ) ){
           esay( "Response not Object[] : "+obj.getClass() ) ;
           return false ;
        }else{
           request = (Object[])obj ;
           if( request.length < 6 ){
              esay( "Response length < 6") ;
              return false ;
           }
           if( ( ! ( request[0] instanceof String )        ) ||
               ( ! ((String)request[0]).equals("response") ) ||
               ( ! ( request[5] instanceof Boolean )       )    ){
               esay( "Not a response" ) ;
               return false ;
           }
           say( "Response for >"+user+"< : "+request[5] ) ;
           return ((Boolean)request[5]).booleanValue() ;
        }

     }

     return false ;
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

