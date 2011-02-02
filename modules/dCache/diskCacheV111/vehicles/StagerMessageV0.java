package diskCacheV111.vehicles;
import  diskCacheV111.util.* ;
import java.net.InetSocketAddress;
import org.dcache.vehicles.FileAttributes;

public class StagerMessageV0  extends StagerMessage {
   private String _storageClass = null ;
   private String _bfid = null ;
   private String _hsm  = null ;
   private String _host = null ;
   private String _protocol = null ;
   private int    _minor    = 0 ;
   private int    _major    = 0 ;

   private static final long serialVersionUID = 4469066464567546218L;
   public StagerMessageV0(FileAttributes fileAttributes){
      super(fileAttributes) ;
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
