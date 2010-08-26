package dmg.protocols.ssh ;


public class SshAuthRhostsRsa extends SshAuthMethod {

  public SshAuthRhostsRsa( String user , SshRsaKey key ){
     super( SshAuthMethod.AUTH_RHOSTS_RSA , user , key ) ;
  }

} 
