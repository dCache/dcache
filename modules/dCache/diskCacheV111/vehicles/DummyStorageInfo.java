package diskCacheV111.vehicles;
import  java.util.HashMap ;
import  java.util.Map ;
public class DummyStorageInfo implements StorageInfo {
    private long    _filesize = 0L ;
    private HashMap _hash = null ;
    
    private static final long serialVersionUID = 6967102248906563009L;
    
    public String getStorageClass(){
       return "<dummy>" ;
    }
    public void setBitfileId( String bfid ){}
    public String getBitfileId(){
       return "<dummy>" ;
    }
    public String getCacheClass(){
       return "<dummy>" ;
    }
    public String getHsm(){
       return "<dummy>" ;
    }
    public long   getFileSize(){
       return _filesize ;
    }
    /**
      *  Set size of BitFile
      */
    public void   setFileSize( long fileSize ){
       _filesize = fileSize ;
    }
    public boolean isCreatedOnly(){ return false ; }
    public boolean isStored(){ return true ;}
    public String  getKey( String key ){
       return _hash == null ? null : (String)_hash.get(key) ;
    }
    public synchronized void    setKey( String key , String value ){
       if( _hash == null )_hash = new HashMap() ;
       _hash.put( key , value ) ;
    }
    public synchronized Map getMap(){ 
      return _hash == null ? new HashMap() : new HashMap( _hash ) ; 
    }
} 
 
