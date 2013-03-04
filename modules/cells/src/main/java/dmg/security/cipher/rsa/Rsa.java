package dmg.security.cipher.rsa ;

import java.math.BigInteger;
import java.util.Date;
import java.util.Random;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class Rsa {

  private static final int DEFAULT_BITS  = 1024 ;

  public static void main( String [] args ){
      if( args.length < 2 ){
        System.out.println( " USAGE : <bits> <certainty>" ) ;
        System.exit(4);
      }
      Random     random = new Random( new Date().getTime() ) ;
      BigInteger bigOne = BigInteger.valueOf(1L) ;
      int bits  = new Integer(args[0]);
      int cert  = new Integer(args[1]);
      int bitsp = ( bits + 1 ) / 2;
      int bitsq = bits - bitsp ;

      BigInteger e = BigInteger.valueOf( 67L ) ;
      BigInteger q , p , x , y , n ,  d , dmp1 , dmq1 , iqmp ;

      while( true ){
          p = new BigInteger( bitsp , cert , random );
          Rsa.say( " p  : "+p.toString(16) ) ;
          x = p.subtract( bigOne ) ;
          Rsa.say( " x  : "+x.toString(16) ) ;
          y = x.gcd( e ) ;
          Rsa.say( " y  : "+y.toString(16) ) ;
          if( y.compareTo( bigOne ) == 0 ) {
              break;
          }
          System.out.println( " Nooo : "+p.toString(16) ) ;
      }
      while( true ){
          q = new BigInteger( bitsq , cert , random );
          Rsa.say( " q  : "+q.toString(16) ) ;
          x = q.subtract( bigOne ) ;
          Rsa.say( " x  : "+x.toString(16) ) ;
          y = x.gcd( e ) ;
          Rsa.say( " y  : "+y.toString(16) ) ;
          if( ( y.compareTo( bigOne ) == 0 ) &&
              ( p.compareTo( q )      != 0 )     ) {
              break;
          }
          System.out.println( " Nooo : "+q.toString(16) ) ;
      }
      if( p.compareTo( q ) < 0 ){
        x = p ;
        p = q ;
        q = x ;
      }
      n = p.multiply( q ) ;
      x = p.subtract( bigOne ) ;
      y = q.subtract( bigOne ) ;
      d = x.multiply( y ) ;
      d = e.modInverse( d ) ;
      dmp1 = d.mod( x ) ;
      dmq1 = d.mod( y ) ;
      iqmp = q.modInverse( p ) ;
      System.out.println( " p    = "+p.toString(16) ) ;
      System.out.println( " q    = "+q.toString(16) ) ;
      System.out.println( " e    = "+e.toString(16) ) ;
      System.out.println( " n    = "+n.toString(16) ) ;
      System.out.println( " d    = "+d.toString(16) ) ;
      System.out.println( " dmp1 = "+dmp1.toString(16) ) ;
      System.out.println( " dmq1 = "+dmq1.toString(16) ) ;
      System.out.println( " iqmp = "+iqmp.toString(16) ) ;
  }
  public static void say( String s ){
    System.out.println( s );
  }
}
