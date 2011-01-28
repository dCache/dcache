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
  private int    minor = 0 ;
  private int    major = 0 ;
  private String [] hosts  = null ;
  private int bufferSize = 0;
  private String sourceHttpUrl;
  private int    port  = 0 ;
  private long   transferTime     = 0 ;
  private long   bytesTransferred = 0 ;
  private int    sessionId        = 0 ;

  private static final long serialVersionUID = 4482469147378465931L;

  public RemoteHttpDataTransferProtocolInfo(String protocol, int major, int minor, String[] hosts, int port, int buf_size, String sourceHttpUrl)
  {
    this.name  = protocol ;
    this.minor = minor ;
    this.major = major ;
    this.hosts = new String[1] ;
    this.hosts = hosts ;
    this.port  = port ;
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
  public String getProtocol()
  {
      return name ;
  }

  public int    getMinorVersion()
  {
    return minor ;
  }

  public int    getMajorVersion()
  {
    return major ;
  }

  public String getVersionString()
  {
    return name+"-"+major+"."+minor ;
  }

  //
  // and the private stuff
  //
  public int    getPort()
  {
      return port ;
  }
  public String [] getHosts()
  {
      return hosts ;
  }


  public String toString()
  {
    StringBuffer sb = new StringBuffer() ;
    sb.append(getVersionString()) ;
    for(int i = 0 ; i < hosts.length ; i++ )
    {
      sb.append(',').append(hosts[i]) ;
    }
    sb.append(':').append(port) ;

    return sb.toString() ;
  }

    @Override
    public InetSocketAddress getSocketAddress() {
        // enforced by interface
        return null;
    }
}



