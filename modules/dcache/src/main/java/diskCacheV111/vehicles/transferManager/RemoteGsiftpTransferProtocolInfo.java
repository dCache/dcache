package diskCacheV111.vehicles.transferManager;

import org.globus.gsi.X509Credential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import diskCacheV111.vehicles.IpProtocolInfo;

import static com.google.common.base.Preconditions.checkArgument;

public class RemoteGsiftpTransferProtocolInfo implements IpProtocolInfo
{
    private static final long serialVersionUID = 7046410066693122355L;

    private final String name;
    private final int minor;
    private final int major;
    private InetSocketAddress addr;
    private final String gsiftpUrl;
    private long transferTime;
    private long bytesTransferred;
    private final String gsiftpTranferManagerName;
    private final String gsiftpTranferManagerDomain;
    private boolean emode = true;
    private int streams_num = 5;
    private int bufferSize;
    private int tcpBufferSize;
    private final String user;

    private PrivateKey key;
    private X509Certificate[] certChain;

    public RemoteGsiftpTransferProtocolInfo(String protocol,
                                            int major,
                                            int minor,
                                            InetSocketAddress addr,
                                            String gsiftpUrl,
                                            String gsiftpTranferManagerName,
                                            String gsiftpTranferManagerDomain,
                                            int bufferSize,
                                            int tcpBufferSize,
                                            GlobusGSSCredentialImpl credential)
            throws GSSException
    {
        this(protocol,
             major,
             minor,
             addr,
             gsiftpUrl,
             gsiftpTranferManagerName,
             gsiftpTranferManagerDomain,
             bufferSize,
             tcpBufferSize,
             credential,
             null);
    }

    public RemoteGsiftpTransferProtocolInfo(String protocol,
                                            int major,
                                            int minor,
                                            InetSocketAddress addr,
                                            String gsiftpUrl,
                                            String gsiftpTranferManagerName,
                                            String gsiftpTranferManagerDomain,
                                            int bufferSize,
                                            int tcpBufferSize,
                                            GlobusGSSCredentialImpl credential,
                                            String user) throws GSSException
    {
        checkArgument(credential instanceof Serializable,
                      "Credential must be Serializable");

        this.name = protocol;
        this.minor = minor;
        this.major = major;
        this.addr = addr;
        this.gsiftpUrl = gsiftpUrl;
        this.gsiftpTranferManagerName = gsiftpTranferManagerName;
        this.gsiftpTranferManagerDomain = gsiftpTranferManagerDomain;
        this.bufferSize = bufferSize;
        this.tcpBufferSize = tcpBufferSize;
        this.user = user;
        this.key = credential.getPrivateKey();
        this.certChain = credential.getCertificateChain();
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
    return name+"-"+major+"."+minor ;
  }


  public String toString()
  {
    StringBuilder sb = new StringBuilder() ;
    sb.append(getVersionString()).append(" ");
    sb.append(addr.getAddress().getHostAddress());
    sb.append(':').append(addr.getPort()) ;

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

    public String getUser() {
        return user;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return addr;
    }

    public GlobusGSSCredentialImpl getCredential() throws GSSException
    {
        return new GlobusGSSCredentialImpl(new X509Credential(key, certChain),
                GSSCredential.INITIATE_ONLY);
    }
}
