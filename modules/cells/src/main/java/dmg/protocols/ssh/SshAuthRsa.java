package dmg.protocols.ssh ;


public class SshAuthRsa extends SshAuthMethod {

  public SshAuthRsa( SshRsaKey key ){
     super( SshAuthMethod.AUTH_RSA , key ) ;
  }

}
