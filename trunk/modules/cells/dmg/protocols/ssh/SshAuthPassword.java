package dmg.protocols.ssh ;


public class SshAuthPassword extends SshAuthMethod {

  public SshAuthPassword( String password ){
    super( SshAuthMethod.AUTH_PASSWORD , password ) ;
  }

} 
