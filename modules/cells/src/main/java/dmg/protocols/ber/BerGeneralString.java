package dmg.protocols.ber ;

import java.util.* ;

public class BerGeneralString extends BerObject {

   private String _string;
   
   public BerGeneralString( String string ){
                        
       super( BerObject.UNIVERSAL , true , 27 ) ;
       _string = string ;   
   }
   public BerGeneralString( byte [] data , int off , int size ){
                        
       super( BerObject.UNIVERSAL , true , 27 ) ;
       
       StringBuilder sb = new StringBuilder();
       for( int i = 0 ; i < size ; i++ ) {
           sb.append((char) data[off + i]);
       }
          
       _string = sb.toString() ;
   }
   public String toString(){ return _string ; }
   @Override
   public String getTypeString(){
   
      return super.getTypeString()+" GeneralString" ;
   }
   @Override
   public byte [] getEncodedData(){
      return getEncodedData( _string.getBytes() ) ;
   }
   public static void main( String [] args ){
       BerGeneralString ber = new BerGeneralString(args[0]);
       ber.printNice() ;
       byte [] r = ber.getEncodedData() ;
       BerObject.displayHex(r) ;
       System.exit(0);
   }
}
