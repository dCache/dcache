package dmg.protocols.ber ;

public class BerApplication extends BerContainer {

   private static final long serialVersionUID = 4829211618889508429L;

   public BerApplication( int tag ){
      super( BerObject.APPLICATION , tag ) ;
   }
   public static void main( String [] args ){
      BerApplication ber = new BerApplication(44) ;
      
      ber.addObject( new BerContext( 22 , new BerInteger(455) ) ) ;
      ber.addObject( new BerContext( 50 , new BerGeneralString( "hallo otto" ) ) ) ;
      ber.addObject( new BerGeneralString( "hallo otto" ) ) ;
      
      ber.printNice() ;
      
      byte [] data = ber.getEncodedData() ;
      
      displayHex( data ) ;
      
      System.exit(0);
   }
}
