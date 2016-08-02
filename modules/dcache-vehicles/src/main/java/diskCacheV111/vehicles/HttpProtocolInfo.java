package diskCacheV111.vehicles;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

public class HttpProtocolInfo implements IpProtocolInfo
{
    /**
     * This enum propagates the user-agent's choice whether to download to
     * the pool so it can send the correct content-disposition header.  This
     * is crazy, but necessary until HTML supports some mechanism to do this
     * purely in the browser.  The 'download' attribute for the 'a' tag seems
     * promising; but, until IE supports it, we can't relying on it.
     */
    public enum Disposition {
        // hint to user-agent to show content in browser
        INLINE,

        // hint to user-agent to download content
        ATTACHMENT
    }

  private String _name  = "Unkown" ;
  private final int    _minor;
  private final int    _major;
  private final InetSocketAddress _clientSocketAddress;
  private long   _transferTime;
  private long   _bytesTransferred;

  /* TODO: Change this to long (but remember backwards compatibility!) */
  private int    _sessionId;

  private boolean _writeAllowed;
  private final String httpDoorCellName;
  private final String httpDoorDomainName;
  private final String path;
  private final URI _location;

  private final Disposition _disposition;

  private static final long serialVersionUID = 8002182588464502270L;

  public HttpProtocolInfo( String protocol, int major , int minor ,
                           InetSocketAddress clientSocketAddress,
                           String httpDoorCellName ,
                           String httpDoorDomainName,
                           String path,
                           URI location)
  {
      this(protocol, major, minor, clientSocketAddress, httpDoorCellName,
              httpDoorDomainName, path, location, null);
  }

  public HttpProtocolInfo( String protocol, int major , int minor ,
                           InetSocketAddress clientSocketAddress,
                           String httpDoorCellName ,
                           String httpDoorDomainName,
                           String path,
                           URI location,
                           Disposition disposition)
  {
    _name  = protocol ;
    _minor = minor ;
    _major = major ;
    _clientSocketAddress = clientSocketAddress;
    this.httpDoorCellName = httpDoorCellName;
    this.httpDoorDomainName = httpDoorDomainName;
    this.path = path;
    _location = location;
    _disposition = disposition;
  }

  public String getHttpDoorCellName()
  {
    return httpDoorCellName;
  }
  public String getHttpDoorDomainName()
  {
    return httpDoorDomainName;
  }
  public String getPath()
  {
    return path;
  }
  public int getSessionId()
  {
    return _sessionId ;
  }
  public void setSessionId( int sessionId )
  {
    _sessionId = sessionId ;
  }
  //
  //  the ProtocolInfo interface
  //
  @Override
  public String getProtocol(){ return _name ; }
  @Override
  public int    getMinorVersion()
  {
    return _minor ;
  }

  @Override
  public int    getMajorVersion()
  {
    return _major ;
  }

  @Override
  public String getVersionString()
  {
    return _name + '-' + _major + '.' + _minor ;
  }

  //
  // and the private stuff
  //
  public void   setBytesTransferred( long bytesTransferred )
  {
    _bytesTransferred = bytesTransferred ;
  }

  public void   setTransferTime( long transferTime )
  {
    _transferTime = transferTime ;
  }

  public long getTransferTime()
  {
    return _transferTime ;
  }

  public long getBytesTransferred()
  {
    return _bytesTransferred ;
  }

  public String toString()
  {
      String sb = getVersionString() +
                  ':' + _clientSocketAddress.getAddress().getHostAddress() +
                  ':' + _clientSocketAddress.getPort() +
                  ':' + httpDoorCellName +
                  ':' + httpDoorDomainName +
                  ':' + path;

      return sb;
  }
  //
  // io mode
  //
  public boolean isWriteAllowed()
  {
    return _writeAllowed ;
  }

  public void    setAllowWrite( boolean allow )
  {
    _writeAllowed = allow ;
  }

    @Override
    public InetSocketAddress getSocketAddress() {
        return _clientSocketAddress;
    }

    /**
     * Returns the location of the file. The location is defined
     * as for the HTTP location header for a 201 response, or for
     * the content-location header for other replies. It points
     * to the original URI as seen at the HTTP door.
     */
    public URI getLocation() {
        return _location;
    }

    /**
     * The hint how dCache should supply the User-Agent describing how the
     * content should be used.
     */
    public Disposition getDisposition() {
        return _disposition != null ? _disposition : Disposition.ATTACHMENT;
    }
}



