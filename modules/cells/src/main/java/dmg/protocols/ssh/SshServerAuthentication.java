package dmg.protocols.ssh ;

import java.net.InetAddress;

public interface SshServerAuthentication {

   SshRsaKey  getHostRsaKey() ;
   SshRsaKey  getServerRsaKey() ;

   boolean   authUser(      InetAddress addr, String user ) ;
   boolean   authPassword(  InetAddress addr, String user, String password ) ;
   boolean   authRhosts(    InetAddress addr, String user ) ;
   SshRsaKey authRsa(       InetAddress addr, String user , SshRsaKey userKey ) ;
   SshRsaKey authRhostsRsa( InetAddress addr, String user ,
                            String reqUser  , SshRsaKey hostKey ) ;

   //
   // ssh protocol extension to support shared keys as well
   //
   SshSharedKey  getSharedKey( InetAddress host , String keyName ) ;
}
