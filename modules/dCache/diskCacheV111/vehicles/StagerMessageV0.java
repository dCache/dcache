package diskCacheV111.vehicles;
import  diskCacheV111.util.* ;
import java.net.InetSocketAddress;

public class StagerMessageV0  extends StagerMessage {
   private String _storageClass = null ;
   private String _bfid = null ;
   private String _hsm  = null ;
   private String _host = null ;
   private String _protocol = null ;
   private int    _minor    = 0 ;
   private int    _major    = 0 ;
   
   private static final long serialVersionUID = 4469066464567546218L;
   
   public class _StorageInfo extends GenericStorageInfo {
       
       private static final long serialVersionUID = -4539657116285745024L;
       
      private _StorageInfo(){}
      public String getStorageClass(){ return _storageClass ; }
      public String getBitfileId(){ return _bfid ; }
      public String getHsm(){ return _hsm ; }
      public long   getFileSize(){ return 0 ; }
      public void   setFileSize( long fileSize ){}
      public boolean isCreatedOnly(){ return false ; }
      public boolean isStored(){ return true ; }
      public String  getKey( String key ){ return "" ; }
      public void    setKey( String key , String value ){}
      public String  toString(){
       return "Hsm="+_hsm+";sc="+_storageClass+";bf="+_bfid ;
      }
      public void setBitfileId(String bitfileId ){ _bfid = bitfileId ; }

      
   }
   public class _ProtocolInfo implements IpProtocolInfo {
       
       private static final long serialVersionUID = 1752886092046157556L;
       
      private _ProtocolInfo(){}
      public String [] getHosts(){
         String [] hosts = new String[1] ;
         hosts[0] = _host ; 
         return hosts ;
      }
      public int       getPort(){ return 0 ; }
      public String getProtocol(){ return _protocol ; }
      public int    getMinorVersion(){return _minor ; }
      public int    getMajorVersion(){ return _major ; }
      public String getVersionString(){ 
        return _protocol+"-"+_major+"."+_minor ; 
      }
      public String toString(){
        return getVersionString()+";L="+_host ;
      }
      public boolean isFileCheckRequired() { return true; }
       @Override
       public InetSocketAddress getSocketAddress() {
           // enforced by interface
           return null;
       }
   }
   public StagerMessageV0( PnfsId pnfsId ){
      super( pnfsId ) ;
      setStorageInfo( new _StorageInfo() ) ;
      setProtocolInfo( new _ProtocolInfo() ) ;
   }
   public void setStorage( String hsm , String storageClass , String bitfileId ){
      _hsm          = hsm ;
      _bfid         = bitfileId ;
      _storageClass = storageClass ;
   }
   public void setProtocol( String protocol , 
                            int majorVersion , int minorVersion ,
                            String location ){
      _protocol = protocol ;
      _major    = majorVersion ;
      _minor    = minorVersion ;
      _host     = location ;
   }
}
