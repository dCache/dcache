package diskCacheV111.vehicles;

import java.net.InetSocketAddress;

/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */
public class RemoteHttpDataTransferProtocolInfo implements IpProtocolInfo
{
  private String name  = "Unkown" ;
  private int    minor;
  private int    major;
  private InetSocketAddress addr;
  private int bufferSize;
  private String sourceHttpUrl;
  private long   transferTime;
  private long   bytesTransferred;
  private int    sessionId;

  private static final long serialVersionUID = 4482469147378465931L;

  public RemoteHttpDataTransferProtocolInfo(String protocol, int major, int minor, InetSocketAddress addr, int buf_size, String sourceHttpUrl)
  {
    this.name  = protocol ;
    this.minor = minor ;
    this.major = major ;
    this.addr = addr ;
    this.sourceHttpUrl = sourceHttpUrl;
    this.bufferSize =buf_size;
  }

  public String getSourceHttpUrl()
  {
      return sourceHttpUrl;
  }
  public int getBufferSize()
  {
      return bufferSize;
  }
   //
  //  the ProtocolInfo interface
  //
  @Override
  public String getProtocol()
  {
      return name ;
  }

  @Override
  public int    getMinorVersion()
  {
    return minor ;
  }

  @Override
  public int    getMajorVersion()
  {
    return major ;
  }

  @Override
  public String getVersionString()
  {
    return name+"-"+major+"."+minor ;
  }

  //
  // and the private stuff
  //

  public String toString()
  {
    StringBuilder sb = new StringBuilder() ;
    sb.append(getVersionString()) ;
    sb.append(addr.getAddress().getHostAddress());
    sb.append(':').append(addr.getPort()) ;

    return sb.toString() ;
  }

    @Override
    public InetSocketAddress getSocketAddress() {
        return addr;
    }
}



