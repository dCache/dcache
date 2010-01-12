package diskCacheV111.vehicles.transferManager;
import java.net.InetAddress;
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
  
  public RemoteGsiftpDelegateUserCredentialsMessage(long id,long callerUniqueId,String host,int port,Long requestCredentialId)
  {
      super(id,callerUniqueId);
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



