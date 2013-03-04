package dmg.security.cipher ;

import java.util.Date;
import java.util.Random;

import dmg.security.cipher.blowfish.Jblowfish;
import dmg.security.cipher.des.Jdes;
import dmg.security.cipher.idea.Jidea;

public class StreamTest {
  private static byte [] __key =
    { 10 , 33 , -10 , -49 , 112 , -8 , 7 , -109 ,
      55 , -9 , -101 , -4 , -112 , -46 , 94 , -124    } ;

  private static String __usage = "USAGE : StreamTest idea|des|blowfish ecb|cfb|cbc [blocks]" ;
  public static void main( String [] args ){
     BlockCipher cipher = null  ;

     if( args.length < 3 ){
        System.err.println( __usage ) ;
        System.exit(3) ;
     }
     String cipherType = args[0] ;
     String cipherMode = args[1] ;
     int    blocks     = new Integer(args[2]);
     Random r          = new Random( new Date().getTime() ) ;
//     byte [] key       = __key ;
     byte [] key = new byte[16] ;
     r.nextBytes( key ) ;
      switch (cipherType) {
      case "idea":
          cipher = new Jidea(key);
          break;
      case "des":
          cipher = new Jdes(key);
          break;
      case "blowfish":
          cipher = new Jblowfish(key);
          break;
      default:
          System.err.println(__usage);
          System.exit(4);
      }
     int     block  = cipher.getBlockLength() / 8 ;
     byte [] vector = new byte[block] ;

     StreamFromBlockCipher encrypt =
        new StreamFromBlockCipher( cipher , vector ) ;
     StreamFromBlockCipher decrypt =
        new StreamFromBlockCipher( cipher , vector ) ;

     byte [] in  = new byte[block*blocks] ;
     byte [] out = new byte[block*blocks] ;
     byte [] chk = new byte[block*blocks] ;

     r.nextBytes( in ) ;

     long start = 0 , en = 0 , de = 0 ;

      switch (cipherMode) {
      case "ecb":
          start = new Date().getTime();
          for (int i = 0; i < blocks; i++) {
              encrypt.encryptECB(in, i * block, out, i * block);
          }

          en = new Date().getTime();
          for (int i = 0; i < blocks; i++) {
              decrypt.decryptECB(out, i * block, chk, i * block);
          }

          de = new Date().getTime();
          break;
      case "cfb":
          start = new Date().getTime();
          encrypt.encryptCFB(in, 0, out, 0, block * blocks);
          en = new Date().getTime();
          decrypt.decryptCFB(out, 0, chk, 0, block * blocks);
          de = new Date().getTime();
          break;
      case "cbc":
          start = new Date().getTime();
          encrypt.encryptCBC(in, 0, out, 0, block * blocks);
          en = new Date().getTime();
          decrypt.decryptCBC(out, 0, chk, 0, block * blocks);
          de = new Date().getTime();
          break;
      default:
          System.err.println(__usage);
          System.exit(5);
      }
     say( " Cipher Type       : "+cipherType ) ;
     say( " Cipher Mode       : "+cipherMode ) ;
     say( " Cipher Block size : "+block ) ;
     say( " Encryption Key    : "+byteToHexString( key ) ) ;
     say( " Encryption Time   : "+(en-start) ) ;
     say( " Decryption Time   : "+(de-en) ) ;
     if( blocks < 5 ){
        say( " Original Data     : "+byteToHexString( in ) ) ;
        say( " Encrypted Data    : "+byteToHexString( out ) ) ;
        say( " Decrypted Data    : "+byteToHexString( chk ) ) ;
     }
     int i;
     for( i = 0 ; i < in.length ; i++ ) {
         if (in[i] != chk[i]) {
             break;
         }
     }
     if( i < in.length ) {
         System.exit(3);
     }
     System.exit(0);

  }
  static public String byteToHexString( byte b ) {
       String s = Integer.toHexString( ( b < 0 ) ? ( 256 + (int)b ) : (int)b  ) ;
       if( s.length() == 1 ) {
           return "0" + s;
       } else {
           return s;
       }
  }
  static public String byteToHexString( byte [] bytes ) {

	  StringBuilder sb = new StringBuilder(bytes.length +1);

      for (byte aByte : bytes) {
          sb.append(byteToHexString(aByte)).append(" ");
      }
       return sb.toString() ;
  }
  private static void say( String str ){ System.out.println( str ) ; }

}
