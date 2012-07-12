package dmg.util.edb ;

import java.io.* ;

public class      JdbmObjectOutputStream 
       extends    DataOutputStream 
       implements ObjectOutput{
   public JdbmObjectOutputStream( DataOutputStream out ){
      super( out ) ;
   }
   @Override
   public void writeObject( Object obj ) throws IOException {
   
      if( obj instanceof JdbmBasic ){
          writeShort( JdbmSerializable.BASIC ) ;
          ((JdbmSerializable)obj).writeObject( this ) ;
      }else if( obj instanceof long [] ){
          long [] x = (long[])obj ;
          for( int i = 0 ; i < x.length ; i++ ) {
              writeLong(x[i]);
          }
      }else if( obj instanceof JdbmFileHeader ){
          writeShort( JdbmSerializable.FILE_HEADER ) ;
          ((JdbmSerializable)obj).writeObject( this ) ;
      }else{
         throw new
         IllegalArgumentException("PANIC : Unknown object" ) ;
      }
   }
}
