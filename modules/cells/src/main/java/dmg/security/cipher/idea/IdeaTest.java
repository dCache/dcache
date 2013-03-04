 package dmg.security.cipher.idea ;

 import java.util.Date;
 import java.util.Random;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
 public class IdeaTest {


     public static void main( String [] args ){

          if( args.length < 1 ) {
              System.exit(1);
          }

          String plain = args[0] ;

          Random  r   = new Random( new Date().getTime() ) ;
          byte [] key = new byte[16] ;
          r.nextBytes( key ) ;
          Idea   idea = new Idea( key ) ;

          // idea is our cipher machine ;

          long start = new Date().getTime() ;
          byte [] cipher     = idea.encryptECB( plain.getBytes() ) ;
          byte [] plainAgain = idea.decryptECB( cipher )  ;
          long stop = new Date().getTime() ;

          String plainString = new String( plainAgain ) ;

          System.out.println( " CFB64 You sayed : "+plainString ) ;
          System.out.println( " Time : "+(stop-start) ) ;

     }


 }
