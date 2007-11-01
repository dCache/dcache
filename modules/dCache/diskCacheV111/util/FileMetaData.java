// $Id: FileMetaData.java,v 1.9 2006-03-28 08:52:32 tigran Exp $
package diskCacheV111.util ;
import java.io.* ;
import java.util.* ;
import java.text.* ;
public class FileMetaData implements Serializable {
   private int  _uid = -1 ;
   private int  _gid = -1 ;
   private long _size = 0L ;
   private long _created      = 0L , 
                _lastAccessed = 0L , 
                _lastModified = 0L;
   private boolean _isRegular     = true ;
   private boolean _isDirectory   = false ;
   private boolean _isLink        = false ;
   private static final SimpleDateFormat __formatter =
       new SimpleDateFormat("MM.dd-hh:mm:ss") ;
       
   public class Permissions  implements Serializable {
       private boolean _canRead = false ;
       private boolean _canWrite = false ;
       private boolean _canExecute = false ;
       
       private static final long serialVersionUID = -1340210599513069884L;
       
       private Permissions(){ this(0) ; }
       private Permissions( int perm ){
           set( perm ) ;
       }
       public void set( int perm ){
          _canRead    = ( perm & 0x4 ) > 0 ;
          _canWrite   = ( perm & 0x2 ) > 0 ;
          _canExecute = ( perm & 0x1 ) > 0 ;
       }
       public boolean canRead(){ return _canRead ; }
       public boolean canWrite(){ return _canWrite ; }
       public boolean canExecute(){ return _canExecute ; }
       public boolean canLookup(){ return canExecute() ; }
       public String toString(){
         return (_canRead?"r":"-")+
                (_canWrite?"w":"-")+
                (_canExecute?"x":"-") ;
       }
   }
   private Permissions _user = new Permissions() ;
   private Permissions _group = new Permissions() ;
   private Permissions _world = new Permissions() ;
   
   private static final long serialVersionUID = -6379734483795645452L;
   
   public FileMetaData(){}
   public FileMetaData( int uid , int gid , int permissions ){
      this( false , uid , gid , permissions ) ;
   }
   public FileMetaData( boolean isDirectory , int uid , int gid , int permission ){
      _uid = uid ;
      _gid = gid ;
      _isDirectory = isDirectory ;
      _user  = new Permissions( ( permission >> 6 ) & 0x7 ) ;
      _group = new Permissions( ( permission >> 3 ) & 0x7 ) ;
      _world = new Permissions( permission & 0x7 ) ;
   }
   public void setFileType( boolean isRegular , 
                            boolean isDirectory , 
                            boolean isLink        ){
      _isRegular   = isRegular ;
      _isDirectory = isDirectory ;
      _isLink      = isLink ;
   }
   public void setSize( long size ){ _size = size ; }
   public void setTimes( long accessed , long modified , long created ){
      _created      = created * 1000L ;
      _lastModified = modified * 1000L ;
      _lastAccessed = accessed * 1000L ;
   }
   public long getFileSize(){ return _size ; }
   public long getCreationTime(){ return _created ; }
   public long getLastModifiedTime(){ return _lastModified ; }
   public long getLastAccessedTime(){ return _lastAccessed ; }
   public boolean isDirectory(){ return _isDirectory ; }
   public boolean isSymbolicLink(){ return _isLink ; }
   public boolean isRegularFile(){ return _isRegular ; }
   public Permissions getUserPermissions(){ return _user ; }
   public Permissions getGroupPermissions(){ return _group ; }
   public Permissions getWorldPermissions(){ return _world ; }
   public int getUid(){ return _uid ; }
   public int getGid(){ return _gid ; }
   
   public void setUid(int newUid) { _uid = newUid; }
   public void setGid(int newGid) { _gid = newGid; }
   
   public String getPermissionString(){
      return (_isDirectory?"d":_isLink?"l":_isRegular?"-":"x")+
             _user+_group+_world ;
   }
   public String toString(){
      return "["+(_isDirectory?"d":_isLink?"l":_isRegular?"-":"x")+
             _user+_group+_world+";"+_uid+";"+_gid+"]"+
             "[c="+__formatter.format(new Date(_created))+
             ";m="+__formatter.format(new Date(_lastModified))+
             ";a="+__formatter.format(new Date(_lastAccessed))+"]" ;
   }
}
