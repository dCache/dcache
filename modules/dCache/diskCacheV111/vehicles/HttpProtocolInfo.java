package diskCacheV111.vehicles;
/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */

public class HttpProtocolInfo implements IpProtocolInfo
{
  private String _name  = "Unkown" ;
  private int    _minor = 0 ;
  private int    _major = 0 ;
  private String [] _hosts  = null ;
  private int    _port  = 0 ;
  private long   _transferTime     = 0 ;
  private long   _bytesTransferred = 0 ;
  private int    _sessionId        = 0 ;
  private boolean _writeAllowed    = false ;
  private String httpDoorCellName;
  private String httpDoorDomainName;
  private String path;
  
  private static final long serialVersionUID = 8002182588464502270L;
  
  public HttpProtocolInfo( String protocol, int major , int minor ,
                           String host    , int port  , 
                           String httpDoorCellName ,
                           String httpDoorDomainName,
                           String path )
  {
    _name  = protocol ;
    _minor = minor ;
    _major = major ; 
    _hosts = new String[1] ;
    _hosts[0]  = host ;
    _port  = port ;
    this.httpDoorCellName = httpDoorCellName;
    this.httpDoorDomainName = httpDoorDomainName;
    this.path = path;
  }
  public HttpProtocolInfo( String protocol, int major , int minor ,
                           String [] hosts, int port ,
                           String httpDoorCellName ,
                           String httpDoorDomainName,
                           String path )
  {
    _name  = protocol ;
    _minor = minor ;
    _major = major ; 
    _hosts = new String[hosts.length] ;
    System.arraycopy( hosts , 0 , _hosts , 0 , hosts.length ) ;
    _port  = port ;
    this.httpDoorCellName = httpDoorCellName;
    this.httpDoorDomainName = httpDoorDomainName;
    this.path = path;
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
  public String getProtocol(){ return _name ; }
  public int    getMinorVersion()
  { 
    return _minor ; 
  }
  
  public int    getMajorVersion()
  { 
    return _major ; 
  }
  
  public String getVersionString()
  {
    return _name+"-"+_major+"."+_minor ;
  }
  
  //
  // and the private stuff
  //
  public int    getPort(){ return _port ; }
  public String [] getHosts(){ return _hosts ; }
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
    StringBuffer sb = new StringBuffer() ;
    sb.append(getVersionString()) ;
    for(int i = 0 ; i < _hosts.length ; i++ )
    {
      sb.append(',').append(_hosts[i]) ;
    }
    sb.append(':').append(_port) ;
    sb.append(':').append(httpDoorCellName);
    sb.append(':').append(httpDoorDomainName);
    sb.append(':').append(path);
         
    return sb.toString() ; 
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
  public boolean isFileCheckRequired() {
    return true;
  }
   
}



