package dmg.protocols.ber ;

public class BerContext extends BerContainer {

   private static final long serialVersionUID = 4282986337370344213L;

   public BerContext( int tag , BerObject obj ){
      super( BerObject.CONTEXT , tag ) ;
      addObject( obj ) ;
   }
   public static void main( String [] args ){
      BerSequence ber = new BerSequence() ;
      
      ber.addObject( new BerContext( 22 , new BerInteger(455) ) ) ;
      ber.addObject( new BerContext( 50 , new BerGeneralString( "hallo otto" ) ) ) ;
      ber.addObject( new BerGeneralString( "hallo otto" ) ) ;
      
      ber.printNice() ;
      
      byte [] data = ber.getEncodedData() ;
      
      displayHex( data ) ;
      
      System.exit(0);
   }
}
