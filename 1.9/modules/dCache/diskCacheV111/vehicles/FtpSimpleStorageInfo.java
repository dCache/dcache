package diskCacheV111.vehicles;

public class FtpSimpleStorageInfo extends GenericStorageInfo {

    private String  _path         = null ;
    private boolean _put          = false ;
    
    private static final long serialVersionUID = 8020376279024967270L;
    
    public FtpSimpleStorageInfo( String storageGroup ,
                                 String path             ){
                                 
        super(null,storageGroup); 
        _path         = path ;                            
        _put          = true ;                     
    }
    public FtpSimpleStorageInfo( String path ){
                                 
        _path         = path ;       
        _put          = false ;                     
    }
    public String getPath(){ return _path ; }
    public boolean isPut(){ return _put ; }
    public String toString(){ 
       return "SI(class="+getStorageClass()+")" ;
    }
    
} 
