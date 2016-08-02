package diskCacheV111.vehicles ;


import java.io.FileInputStream;
import java.io.ObjectInputStream;

/**
  * Implementation of the StorageInfo for Enstore. Adds 'fileFamily',
  * 'storageGroup', 'bfid, 'volume, and 'location' to the general
  * attributes.
  */
public class EnstoreStorageInfo extends GenericStorageInfo {
   private final String _family;
   private final String _group;
   private String _volume   = "<unknown>" ;
   private String _location = "<unknown>" ;
   private String _path;

   private static final long serialVersionUID = 8640934581729492133L;

   public EnstoreStorageInfo( String storageGroup , String fileFamily ){
      setHsm("enstore");
      _family = fileFamily ;
      _group  = storageGroup ;
      setIsNew( true ) ;

   }
   public EnstoreStorageInfo( String storageGroup ,
                              String fileFamily ,
                              String bfid ){
      setHsm("enstore");
      _family = fileFamily ;
      _group  = storageGroup ;
      setBitfileId(bfid) ;
      setIsNew( false ) ;

   }
   public void setPath( String path){ _path = path ; }
   public String getPath(){ return _path ; }
   @Override
   public String getStorageClass() {
      return (_group==null?"None":_group) + '.' +
             (_family==null?"None":_family) ;
   }
   public String toString(){
      return
              super.toString() +
              ";path=" + (_path==null?"<Unknown>":_path) +
              ";group=" + (_group==null?"<Unknown>":_group) +
              ";family=" + (_family==null?"<Unknown>":_family) +
              ";bfid=" + getBitfileId() +
              ";volume=" + _volume +
              ";location=" + _location + ';';
   }
   public String getStorageGroup(){ return _group ; }
   public String getFileFamily(){ return _family ; }
   public void setVolume( String volume ){ _volume = volume ; }
   public void setLocation(String location ){ _location = location ; }

   public String getVolume(){ return _volume ; }
   public String getLocation(){ return _location ; }

   public static void main (String[] args) throws Exception {
       ObjectInputStream io = new ObjectInputStream (
          new FileInputStream(args[0]));
       Object o = io.readObject();
       if(!(o instanceof EnstoreStorageInfo)) {
           System.err.println(" Object read is not a FileInputStream!!!");
           System.exit(1);
       }
       EnstoreStorageInfo se = (EnstoreStorageInfo) o;
       System.out.println(se);
   }
}

