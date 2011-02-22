package diskCacheV111.vehicles.transferManager;

/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

public class RemoteGsiftpDelegateUserCredentialsMessage extends TransferManagerMessage
{
  private String host;
  private int port;
  private Long requestCredentialId;

  public RemoteGsiftpDelegateUserCredentialsMessage(String host,int port,Long requestCredentialId)
  {
      this.host = host;
      this.port = port;
      this.requestCredentialId =  requestCredentialId;
  }


  public String getHost() {
      return host;
  }

  public int getPort() {
      return port;
  }

  public Long getRequestCredentialId(){
      return requestCredentialId;
  }


}



