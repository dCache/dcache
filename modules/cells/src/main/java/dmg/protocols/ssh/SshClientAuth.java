package dmg.protocols.ssh ;

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
 
