package diskCacheV111.vehicles.transferManager;

import java.io.Serializable;
import java.net.InetSocketAddress;

import org.ietf.jgss.GSSCredential;

import diskCacheV111.vehicles.IpProtocolInfo;

import static com.google.common.base.Preconditions.checkArgument;

public class RemoteGsiftpTransferProtocolInfo implements IpProtocolInfo
{
    private static final long serialVersionUID = 7046410066693122355L;

    private final String name;
    private final int minor;
    private final int major;
    private final String [] hosts;
    private final String gsiftpUrl;
    private final int port;
    private long transferTime = 0;
    private long bytesTransferred = 0;
    private final String gsiftpTranferManagerName;
    private final String gsiftpTranferManagerDomain;
    private boolean emode = true;
    private int streams_num = 5;
    private int bufferSize = 0;
    private int tcpBufferSize = 0;
    @Deprecated // for compatibility with pools before 1.9.14
    private final Long requestCredentialId;
    private final String user;
    private final GSSCredential credential;

    public RemoteGsiftpTransferProtocolInfo(String protocol,
                                            int major,
                                            int minor,
                                            String[] hosts,
                                            int port,
                                            String gsiftpUrl,
                                            String gsiftpTranferManagerName,
                                            String gsiftpTranferManagerDomain,
                                            int bufferSize,
                                            int tcpBufferSize,
                                            @Deprecated
                                            Long requestCredentialId,
                                            GSSCredential credential)
    {
        this(protocol,
             major,
             minor,
             hosts,
             port,
             gsiftpUrl,
             gsiftpTranferManagerName,
             gsiftpTranferManagerDomain,
             bufferSize,
             tcpBufferSize,
             requestCredentialId,
             credential,
             null);
    }

    public RemoteGsiftpTransferProtocolInfo(String protocol,
                                            int major,
                                            int minor,
                                            String[] hosts,
                                            int port,
                                            String gsiftpUrl,
                                            String gsiftpTranferManagerName,
                                            String gsiftpTranferManagerDomain,
                                            int bufferSize,
                                            int tcpBufferSize,
                                            @Deprecated
                                            Long requestCredentialId,
                                            GSSCredential credential,
                                            String user)
    {
        checkArgument(credential instanceof Serializable,
                      "Credential must be Serializable");

        this.name = protocol;
        this.minor = minor;
        this.major = major;
        this.hosts = hosts;
        this.port = port;
        this.gsiftpUrl = gsiftpUrl;
        this.gsiftpTranferManagerName = gsiftpTranferManagerName;
        this.gsiftpTranferManagerDomain = gsiftpTranferManagerDomain;
        this.bufferSize = bufferSize;
        this.tcpBufferSize = tcpBufferSize;
        this.requestCredentialId = requestCredentialId;
        this.credential = credential;
        this.user = user;
    }

  public String getGsiftpUrl()
  {
      return gsiftpUrl;
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

  public boolean isFileCheckRequired() { return true; }

  /** Getter for property gsiftpTranferManagerName.
   * @return Value of property gsiftpTranferManagerName.
   */
  public String getGsiftpTranferManagerName() {
      return gsiftpTranferManagerName;
  }

  /** Getter for property gsiftpTranferManagerDomain.
   * @return Value of property gsiftpTranferManagerDomain.
   */
  public String getGsiftpTranferManagerDomain() {
      return gsiftpTranferManagerDomain;
  }

  /** Getter for property emode.
   * @return Value of property emode.
   */
  public boolean isEmode() {
      return emode;
  }

  /** Setter for property emode.
   * @param emode New value of property emode.
   */
  public void setEmode(boolean emode) {
      this.emode = emode;
  }

  /** Getter for property streams_num.
   * @return Value of property streams_num.
   */
  public int getNumberOfStreams() {
      return streams_num;
  }

  /** Setter for property streams_num.
   * @param streams_num New value of property streams_num.
   */
  public void setNumberOfStreams(int streams_num) {
      this.streams_num = streams_num;
  }

  /** Getter for property tcpBufferSize.
   * @return Value of property tcpBufferSize.
   */
  public int getTcpBufferSize() {
      return tcpBufferSize;
  }

  /** Setter for property tcpBufferSize.
   * @param tcpBufferSize New value of property tcpBufferSize.
   */
  public void setTcpBufferSize(int tcpBufferSize) {
      this.tcpBufferSize = tcpBufferSize;
  }

    @Deprecated
    public Long getRequestCredentialId() {
        return requestCredentialId;
    }

    public String getUser() {
        return user;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        // enforced by interface
        return null;
    }

    public GSSCredential getCredential()
    {
        return credential;
    }
}



