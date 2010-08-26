package dmg.protocols.ssh ;


public class SshAuthRhosts extends SshAuthMethod {

  public SshAuthRhosts( String user ){
    super( SshAuthMethod.AUTH_RHOSTS , user ) ;
  }

}
