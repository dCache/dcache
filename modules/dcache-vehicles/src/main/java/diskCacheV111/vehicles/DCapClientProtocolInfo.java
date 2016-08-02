package diskCacheV111.vehicles;

import java.net.InetSocketAddress;

/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

public class DCapClientProtocolInfo implements IpProtocolInfo
{
  private String name  = "Unkown" ;
  private final int    minor;
  private final int    major;
  private final InetSocketAddress addr;
  private String gsiftpUrl;
  private long   transferTime;
  private long   bytesTransferred;
  private int    sessionId;
  private final String initiatorCellName;
  private final String initiatorCellDomain;
  private final long id;
  private final int bufferSize;
  private int tcpBufferSize;

  private static final long serialVersionUID = -8861384829188018580L;

  public DCapClientProtocolInfo(String protocol,
    int major,
    int minor,
    InetSocketAddress addr,
    String initiatorCellName,
    String initiatorCellDomain,
    long id,
    int bufferSize,
    int tcpBufferSize)
  {
    this.name  = protocol ;
    this.minor = minor ;
    this.major = major ;
    this.addr = addr ;
    this.initiatorCellName = initiatorCellName;
    this.initiatorCellDomain = initiatorCellDomain;
    this.id = id;
    this.bufferSize =bufferSize;
    this.tcpBufferSize = tcpBufferSize;

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
    return name + '-' + major + '.' + minor ;
  }

  //
  // and the private stuff
  //

  public String toString()
  {
    String sb = getVersionString() +
                addr.getAddress().getHostAddress() +
                ':' + addr.getPort();

    return sb;
  }

  /** Getter for property gsiftpTranferManagerName.
   * @return Value of property gsiftpTranferManagerName.
   */
  public String getInitiatorCellName() {
      return initiatorCellName;
  }

  /** Getter for property gsiftpTranferManagerDomain.
   * @return Value of property gsiftpTranferManagerDomain.
   */
  public String getInitiatorCellDomain() {
      return initiatorCellDomain;
  }

  /** Getter for property id.
   * @return Value of property id.
   */
  public long getId() {
      return id;
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

  public boolean isFileCheckRequired() {
      return true;
  }

    @Override
    public InetSocketAddress getSocketAddress() {
        return addr;
    }
}



