package diskCacheV111.vehicles ;


import java.util.* ;

public class GenericStorageInfo implements StorageInfo, java.io.Serializable {

   static final long serialVersionUID = 2089636591513548893L;

   private boolean   _isNew      = true ;
   private boolean   _isStored   = false ;
   private HashMap   _keyHash    = null ;
   private String    _hsm        = null ;
   private String    _cacheClass = null ;
   private long      _fileSize   = 0 ;
   private String    _storageClass   = null ;
   private String    _bitfileId  = null ;
   
   public GenericStorageInfo(){}
   public GenericStorageInfo( String hsm , String storageClass ){
   
      _storageClass = storageClass ;
      _hsm          = hsm ;
   }
   public void setStorageClass( String storageClass ){
      _storageClass = storageClass ;
   }
   public String getStorageClass(){ return _storageClass ; }
   
   public String getBitfileId(){ return _bitfileId == null ? "<Unknown>" : _bitfileId ; }
   public void setBitfileId( String bitfileId ){ _bitfileId = bitfileId ; }
   
   public void addKeys( Hashtable keys ){ _keyHash = new HashMap() ; _keyHash.putAll(keys) ; }
   public void addKeys( HashMap keys ){ _keyHash = new HashMap() ; _keyHash.putAll(keys) ; }
   public Map  getMap(){ return  _keyHash == null ? new HashMap() : new HashMap( _keyHash ) ; }
   public String getKey( String key ){ 
      Object x = null ;
      return _keyHash == null ? null  :
             ( x = _keyHash.get( key ) ) == null ? null : 
             x.toString()   ; 
   }
   public void setKey( String key , String value ){
      if( _keyHash == null )_keyHash = new HashMap() ;
      if( value == null )_keyHash.remove(key) ;
      else _keyHash.put( key , value ) ;
      return ;
   }
   public long   getFileSize(){ return _fileSize ; } 
   public void   setFileSize( long size ){ _fileSize = size ; } 
   
   public boolean isStored(){ return _isStored ; }
   public void    setIsStored( boolean isStored ){ _isStored = isStored ; }
   
   
   public void    setIsNew(boolean isNew ){ _isNew = isNew ; }
   public boolean isCreatedOnly(){  return _isNew ; }
   public void    setIsCreatedOnly( boolean isCreated  ){ _isNew = isCreated ; }
   
   public String getHsm(){ return _hsm ; }
   public void   setHsm( String hsm ){ _hsm = hsm ; }

   public String getCacheClass(){ return _cacheClass ; }
   public void   setCacheClass( String cacheClass ){ _cacheClass = cacheClass ; }
   
   public String toString(){
      String sc = getStorageClass() ;
      String cc = getCacheClass() ;
      String hsm = getHsm() ;
      StringBuffer sb = new StringBuffer() ;
      sb.append("size=").append(getFileSize()).
         append(";new=").append(isCreatedOnly()).
         append(";stored=").append(isStored()).
         append(";sClass=").append(sc==null?"-":sc).
         append(";cClass=").append(cc==null?"-":cc).
         append(";hsm=").append(hsm==null?"-":hsm).append(";") ;
      if( _keyHash != null ){
         Iterator e = _keyHash.entrySet().iterator() ;
         while( e.hasNext() ){
            Map.Entry entry = (Map.Entry)e.next() ;
            String key = entry.getKey().toString() ;
            String value = entry.getValue().toString() ;
            sb.append(key).append("=").append(value).append(";") ;
         }
      
      }
      return sb.toString() ;
   }


}
