package diskCacheV111.vehicles;
import  java.util.Map ;
import  java.util.HashMap ;

public class FtpSimpleStorageInfo implements StorageInfo {
    private String  _storageGroup = null ;
    private String  _path         = null ;
    private boolean _put          = false ;
    private long    _fileSize     = 0 ;
    
    private static final long serialVersionUID = 8020376279024967270L;
    
    public FtpSimpleStorageInfo( String storageGroup ,
                                 String path             ){
                                 
        _storageGroup = storageGroup ; 
        _path         = path ;                            
        _put          = true ;                     
    }
    public FtpSimpleStorageInfo( String path ){
                                 
        _path         = path ;       
        _put          = false ;                     
    }
    public String getCacheClass(){ return null ; }
    public String getHsm(){ return null ;}
    public String getStorageClass(){ return _storageGroup ; }
    public long   getFileSize(){ return _fileSize ; }
    public void   setFileSize( long fileSize ){ _fileSize = fileSize ; }
    public String getStorageGroup(){ return _storageGroup ; }
    public String getPath(){ return _path ; }
    public boolean isPut(){ return _put ; }
    public String toString(){ 
       return "SI(class="+getStorageClass()+")" ;
    }
    //
    //
    public boolean isStored(){ return false ; }
    public boolean isCreatedOnly(){ return true ; }
    public String  getKey(String key ){ return null ; }
    public void    setKey(String key , String value ){ }
    public Map     getMap(){ return new HashMap() ; }
    public String  getBitfileId(){ return "<Unknown>"; }
    public void setBitfileId( String bfid ){}

} 
