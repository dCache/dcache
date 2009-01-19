package dmg.protocols.ber ;

import java.util.* ;

public class BerSequence extends BerContainer {

   public BerSequence(){
      super( BerObject.UNIVERSAL , 16 ) ;
   }
   public static void main( String [] args ){
      BerSequence ber = new BerSequence() ;
      
      ber.addObject( new BerInteger(455) ) ;
      ber.addObject( new BerGeneralString( "hallo otto" ) ) ;
      
      ber.printNice() ;
      
      byte [] data = ber.getEncodedData() ;
      
      displayHex( data ) ;
      
      System.exit(0);
   }
}
