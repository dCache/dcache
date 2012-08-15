package dmg.protocols.ber ;
import java.util.* ;

public class BerOctectString extends BerObject {

   private static final long serialVersionUID = 4480144778623214231L;

   public BerOctectString( byte [] data , int off , int size ){
       super( BerObject.UNIVERSAL , true , 4 , data , off , size ) ;
   }
   @Override
   public String getTypeString(){
       return super.getTypeString()+" OctectString" ;
   }
}
