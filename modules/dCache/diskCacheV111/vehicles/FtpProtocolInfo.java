package diskCacheV111.vehicles ;

public class FtpProtocolInfo implements IpProtocolInfo {
   private String _name  = "Unkown" ;
   private int    _minor = 0 ;
   private int    _major = 0 ;
   private String _host  = null ;
   private int    _port  = 0 ;
   private long   _transferTime     = 0 ;
   private long   _bytesTransferred = 0 ;
   
   private static final long serialVersionUID = -7505989248613293135L;
   
   public FtpProtocolInfo( String protocol, int major , int minor ,
                           String host    , int port          ){
      _name  = protocol ;
      _minor = minor ;
      _major = major ; 
      _host  = host ;
      _port  = port ;
   }
   //
   //  the ProtocolInfo interface
   //
   public String getProtocol(){ return _name ; }
   public int    getMinorVersion(){ return _minor ; }
   public int    getMajorVersion(){ return _major ; }
   public String getVersionString(){
       return _name+"-"+_major+"."+_minor ;
   }
   //
   // and the private stuff
   //
   public int    getPort(){ return _port ; }
   public String getHost(){ return _host ; }
   public String [] getHosts(){ 
       String [] result = new String[1] ;
       result[0] = _host ;
       return result ;
   }
   public void   setBytesTransferred( long bytesTransferred ){
      _bytesTransferred = bytesTransferred ;
   }
   public void   setTransferTime( long transferTime ){
      _transferTime = transferTime ;
   }
   public long getTransferTime(){ return _transferTime ; }
   public long getBytesTransferred(){ return _bytesTransferred ; }
   //
   public String toString(){  return getVersionString() ; }
   //
   public boolean isFileCheckRequired() {
       return true;
   }
   
}
