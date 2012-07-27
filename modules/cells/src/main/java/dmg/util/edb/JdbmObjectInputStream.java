package dmg.util.edb ;

import java.io.* ;

public class      JdbmObjectInputStream 
       extends    DataInputStream 
       implements ObjectInput      {
   
   public JdbmObjectInputStream( DataInputStream in ){
      super( in ) ;
   }
   public void readLongArray( long [] array )throws IOException {
       for( int i = 0 ; i < array.length ; i++ ) {
           array[i] = readLong();
       }
   
   }
   @Override
   public Object readObject() throws IOException, ClassNotFoundException {
      short code = readShort() ;
      JdbmSerializable obj;
      switch( code ){
         case JdbmSerializable.BASIC :
             obj = new JdbmBasic() ;
             ((JdbmSerializable)obj).readObject( this ) ;
         break ;
         case JdbmSerializable.FILE_HEADER :
             obj = new JdbmFileHeader() ;
             ((JdbmSerializable)obj).readObject( this ) ;
         break ;
         case JdbmSerializable.BUCKET_ELEMENT :
             obj = new JdbmBucketElement() ;
             ((JdbmSerializable)obj).readObject( this ) ;
         break ;
         case JdbmSerializable.BUCKET :
             obj = new JdbmBucket() ;
             ((JdbmSerializable)obj).readObject( this ) ;
         break ;
         default :
            throw new
            ClassNotFoundException( "Class not found : "+code ) ;
      }
      return obj ;
   }
}
