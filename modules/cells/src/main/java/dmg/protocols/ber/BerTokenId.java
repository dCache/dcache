package dmg.protocols.ber ;
import java.util.* ;

public class BerTokenId extends BerObject {
   private int _id;
   public BerTokenId(int id){                    
       super( BerObject.UNIVERSAL , true , id  ) ;
       _id = id ;
   }
   @Override
   public String getTypeString(){
       return super.getTypeString()+" TokenId" ;
   }
   @Override
   public byte [] getEncodedData(){
       byte [] x = new byte[2] ;
       x[0] = (byte)_id ;
       x[1] = (byte)0 ;
       return x ;
   }
}
