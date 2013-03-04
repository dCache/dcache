package dmg.security.cipher.rsa ;

import java.math.BigInteger;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class RsaTest {

  private static final int DEFAULT_BITS  = 1024 ;

  public static void main( String [] args ){
      if( args.length < 2 ){
        System.out.println( " USAGE : <bits> <certainty>" ) ;
        System.exit(4);
      }
      String eString = args[0] ;
      String nString = args[1] ;

      BigInteger n = new BigInteger( nString ) ;
      System.out.println( " BitLength : "+n.bitLength()+
                          " ; BitCount : " + n.bitCount() ) ;
  }


}
