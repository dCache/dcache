package dmg.protocols.ssh ;

import java.net.InetAddress;

public interface SshClientAuthentication {

   boolean  isHostKey( InetAddress host , SshRsaKey key ) ;

   String          getUser() ;
   SshAuthMethod   getAuthMethod() ;

   //
   // ssh protocol extension to support shared keys as well
   //
   SshSharedKey  getSharedKey( InetAddress host ) ;


}
