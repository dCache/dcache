package diskCacheV111.vehicles.transferManager;
import diskCacheV111.vehicles.IpProtocolInfo;

/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

public class RemoteGsiftpTransferProtocolInfo implements IpProtocolInfo
{
  private String name  = "Unkown" ;
  private int    minor = 0 ;
  private int    major = 0 ;
  private String [] hosts  = null ;
  private String gsiftpUrl;
  private int    port  = 0 ;
  private long   transferTime     = 0 ;
  private long   bytesTransferred = 0 ;
  private int    sessionId        = 0 ;
  private String gsiftpTranferManagerName;
  private String gsiftpTranferManagerDomain;
  private long id;
  private long sourceId;
  private boolean emode = true;
  private int streams_num = 5;
  private int bufferSize = 0;
  private int tcpBufferSize = 0;
  private Long requestCredentialId;
  private String user=null;

  private static final long serialVersionUID = 7046410066693122355L;

  public RemoteGsiftpTransferProtocolInfo(String protocol,
    int major,
    int minor,
    String[] hosts,
    int port,
    String gsiftpUrl,
    String gsiftpTranferManagerName,
    String gsiftpTranferManagerDomain,
    long id,
    long sourceId,
    int bufferSize,
    int tcpBufferSize,
    Long requestCredentialId,
    String user
    )
  {
    this(protocol,
    major,
    minor,
    hosts,
    port,
    gsiftpUrl,
    gsiftpTranferManagerName,
    gsiftpTranferManagerDomain,
    id,
    sourceId,
    bufferSize,
    tcpBufferSize,
    requestCredentialId);
    this.user=user;
  }

  public RemoteGsiftpTransferProtocolInfo(String protocol,
    int major,
    int minor,
    String[] hosts,
    int port,
    String gsiftpUrl,
    String gsiftpTranferManagerName,
    String gsiftpTranferManagerDomain,
    long id,
    long sourceId,
    int bufferSize,
    int tcpBufferSize,
    Long requestCredentialId
    )
  {
    this.name  = protocol ;
    this.minor = minor ;
    this.major = major ;
    this.hosts = new String[1] ;
    this.hosts = hosts ;
    this.port  = port ;
    this.gsiftpUrl = gsiftpUrl;
    this.gsiftpTranferManagerName = gsiftpTranferManagerName;
    this.gsiftpTranferManagerDomain = gsiftpTranferManagerDomain;
    this.id = id;
    this.sourceId = sourceId;
    this.bufferSize =bufferSize;
    this.tcpBufferSize = tcpBufferSize;
    this.requestCredentialId = requestCredentialId;
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
  public java.lang.String getGsiftpTranferManagerName() {
      return gsiftpTranferManagerName;
  }

  /** Getter for property gsiftpTranferManagerDomain.
   * @return Value of property gsiftpTranferManagerDomain.
   */
  public java.lang.String getGsiftpTranferManagerDomain() {
      return gsiftpTranferManagerDomain;
  }

  /** Getter for property id.
   * @return Value of property id.
   */
  public long getId() {
      return id;
  }

  /** Getter for property sourceId.
   * @return Value of property sourceId.
   */
  public long getSourceId() {
      return sourceId;
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
  public int getStreams_num() {
      return streams_num;
  }

  /** Setter for property streams_num.
   * @param streams_num New value of property streams_num.
   */
  public void setStreams_num(int streams_num) {
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

    public Long getRequestCredentialId() {
        return requestCredentialId;
    }

    public String getUser() {
        return user;
    }
}



