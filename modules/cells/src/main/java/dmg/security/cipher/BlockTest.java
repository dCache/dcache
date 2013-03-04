package dmg.security.cipher ;

import dmg.security.cipher.blowfish.Jblowfish;

public class BlockTest {


  private static byte [] __key =
    { 10 , 33 , -10 , -49 , 112 , -8 , 7 , -109 ,
      55 , -9 , -101 , -4 , -112 , -46 , 94 , -124    } ;


  private static byte [] __in =
     { 77 ,6 , 120 , 113 , 3 , 5 , 90 , 55 ,
       39 , 54 , 62 , 101 , 49 , 32 , 93 , 44 ,
       77 ,6 , 120 , 113 , 3 , 5 , 90 , 55 ,
       39 , 54 , 62 , 101 , 49 , 32 , 93 , 44 } ;

  private static byte [] __dataIn = { -1 , 2 , -2 , 4 , -3 , 6 , -4 , 8  } ;


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
  public static byte parseByte(  String str ){
     int i = Integer.parseInt( str , 16 ) ;
     return (byte) i ;
  }
  public static void main2( String [] args ){

      for (String arg : args) {
          byte b = BlockTest.parseByte(arg);
          System.out.println(arg + " : " + b + " " + byteToHexString(b));
      }

     BlockCipher cipher = new Jblowfish( __key ) ;

     byte [] outBlock = new byte[__dataIn.length] ;
     byte [] revBlock = new byte[__dataIn.length] ;

     cipher.encrypt( __dataIn  , 0 , outBlock , 0 ) ;
     cipher.decrypt( outBlock  , 0 , revBlock , 0 ) ;

     say( " Data : "+byteToHexString( __dataIn ) ) ;
     say( " En   : "+byteToHexString( outBlock ) ) ;
     say( " De   : "+byteToHexString( revBlock ) ) ;
  }
  public static void main( String [] args ){

     if( args.length < 34 ){
       System.err.println( " ... <cipher> e|d <key=xx xx xx ...> <values=vv vv vv>" ) ;
       System.exit(3);
     }
     byte [] vector = new byte[8] ;
     byte [] key    = new byte[32] ;

     String mode = args[1] ;

     for( int i = 0 ; i < key.length ; i++ ) {
         key[i] = parseByte(args[i + 2]);
     }


     byte [] in = new byte[args.length-2-key.length] ;
     for( int i = 0 ; i < in.length ; i++ ) {
         in[i] = parseByte(args[i + 2 + key.length]);
     }


     say( " Key               : "+byteToHexString(key) ) ;
     say( " Mode              : "+mode ) ;
     say( " Original Data     : "+byteToHexString( in ) ) ;

     BlockCipher cipher = new Jblowfish( key ) ;

     StreamFromBlockCipher encrypt =
        new StreamFromBlockCipher( cipher , vector , "cbc" ) ;

     byte [] out = new byte[in.length] ;

     if( mode.equals("e") ){
         encrypt.encryptCBC(   in  , 0 , out , 0 , in.length ) ;
         say( " Encrypted Data    : "+byteToHexString( out ) ) ;
     }else{
         encrypt.decryptCBC(   in  , 0 , out , 0 , in.length ) ;
         say( " Decrypted Data    : "+byteToHexString( out ) ) ;
     }

  }

}
