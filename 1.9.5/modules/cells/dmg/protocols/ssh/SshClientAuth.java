package dmg.protocols.ssh ;

import java.net.InetAddress ;

public interface SshClientAuth {

   String getUser() ;
   String getPassword() ;
   
   SshRsaKey getRsaIdentity() ;
   //
   //  to verify the hostKey we got from the server
   //  
   SshRsaKey  getRsaKeyOf( byte [] modulusBytes ) ;

   void       failed( String reason ) ;
   void       ready() ;
}
 
