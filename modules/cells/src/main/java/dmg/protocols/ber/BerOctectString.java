package dmg.protocols.ber ;
import java.util.* ;

public class BerOctectString extends BerObject {

   public BerOctectString( byte [] data , int off , int size ){                    
       super( BerObject.UNIVERSAL , true , 4 , data , off , size ) ;
   }
   @Override
   public String getTypeString(){
       return super.getTypeString()+" OctectString" ;
   }
}
