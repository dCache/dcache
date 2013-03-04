package dmg.protocols.kerberos ;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class Base64 {

   private static String __base64 =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/" ;
  // 012345678901234567890123456789012345678901234567890123456789012345
   public static String encode( byte [] data ){
      if( data == null ) {
          throw new
                  NullPointerException("data == null ");
      }

      StringBuilder out = new StringBuilder() ;
      int c = 0 ;
      int i;
      for( i = 0; i < data.length ; i++ ){
        int d = data[i] ;
        d = d < 0 ? ( d+256 ) : d ;
        c <<= 8 ;
        c += d ;
        if( ( i % 3 ) == 2 ){
           out.append(__base64.charAt((c&0x0fc0000) >> 18)) ;
           out.append(__base64.charAt((c&0x003f000) >> 12)) ;
           out.append(__base64.charAt((c&0x0000fc0) >> 6)) ;
           out.append(__base64.charAt((c&0x000003f) >> 0)) ;
           c = 0 ;
        }
      }
      int r = data.length % 3 ;
      if( r == 1 ){
         c <<= 16 ;
         out.append(__base64.charAt((c&0x0fc0000) >> 18)) ;
         out.append(__base64.charAt((c&0x003f000) >> 12)) ;
         out.append('=');
         out.append('=');
      }else if( r == 2 ){
         c <<= 8 ;
         out.append(__base64.charAt((c&0x0fc0000) >> 18)) ;
         out.append(__base64.charAt((c&0x003f000) >> 12)) ;
         out.append(__base64.charAt((c&0x0000fc0) >> 6)) ;
         out.append('=');
      }
      return out.toString() ;
   }
   public static byte [] decode( String str ){
      int n= str.length() ;
      if( n == 0 ) {
          return new byte[0];
      }
      if( ( n % 4 ) != 0 ) {
          throw new
                  IllegalArgumentException("Not a base64(not mod 4)");
      }

      int rn = n ;
      if( str.charAt(rn-1) == '=' ) {
          rn--;
      }
      if( str.charAt(rn-1) == '=' ) {
          rn--;
      }
      int diff = ( n - rn ) ;
      byte [] data = new byte[n/4*3-diff] ;

      int d       = 0 ;
      int dataPos = 0 ;
      int x;
      int c;
      for( int i = 0 ; i < rn ; i++ ){
         c = str.charAt(i) ;
         int p = __base64.indexOf(c) ;
         if( p < 0 ) {
             throw new
                     IllegalArgumentException("Not a base64 (wrong char set)");
         }
         d <<= 6 ;
         d += p ;
         if( ( i % 4 ) == 3 ){
             x = ( d >> 16 ) & 0xff ;
             data[dataPos++] = (byte)(x > 128 ? ( x - 256) : x );
             x = ( d >> 8 ) & 0xff ;
             data[dataPos++] = (byte)(x > 128 ? ( x - 256) : x );
             x = d & 0xff ;
             data[dataPos++] = (byte)(x > 128 ? ( x - 256) : x );
         }
      }
      if( diff == 2 ){
         d <<= 12 ;
         x = ( d >> 16 ) & 0xff ;
         data[dataPos++] = (byte)(x > 128 ? ( x - 256) : x );
      }else if( diff == 1 ){
         d <<= 6 ;
         x = ( d >> 16 ) & 0xff ;
         data[dataPos++] = (byte)(x > 128 ? ( x - 256) : x );
         x = ( d >> 8  ) & 0xff ;
         data[dataPos++] = (byte)(x > 128 ? ( x - 256) : x );
      }
      return data ;
   }
   private static String __rest = "!@#$%^&*()_+=-][]{};'\\\":|<>?,./ `~" ;
   private static void displayHexLine( byte [] data , int off , int size ,
                                       PrintWriter pw ){
      size = Math.min( size , 16 ) ;
      int pos = off ;
      int col;
      for( col = 0 ; col < size ; col ++ ){
        pw.print( byteToHex(data[pos+col]) ) ;
        pw.print(" " ) ;
        if( col == 7 ) {
            pw.print(" ");
        }
      }
      for( ; col < 16 ; col ++ ){
        pw.print("-- " ) ;
        if( col == 7 ) {
            pw.print(" ");
        }
      }
      pw.print( " *" ) ;
      for( col = 0 ; col < size ; col++ ){
         char c = (char)data[pos+col] ;
         if( Character.isLetterOrDigit(c) ||
             ( __rest.indexOf(c) > -1 )
            ){
            pw.print(c) ;
         }else{
            pw.print(".") ;
         }
      }
      for( ; col < 16 ; col++ ) {
          pw.print(" ");
      }
      pw.println("*");

   }
   public static void displayHex( byte [] data ){
      displayHex( data , new PrintWriter( new OutputStreamWriter( System.out ) ));
   }
   public static void displayHex( byte [] data , PrintWriter pw ){
      int l    = data.length ;
      int pos  = 0 ;
      int rest = data.length ;
      for( int row = 0 ; true ; row++ , pos += 16 ){
          displayHexLine( data , pos , Math.min( rest , 16 ) , pw ) ;
          rest -= 16 ;
          if( rest <= 0 ) {
              break;
          }
      }
      pw.flush() ;
   }
   public static String byteToHex( int d ){
      d &= 0xff ;
      d = d < 0 ? ( d+256 ) : d ;
      String x = ""+Integer.toHexString(d) ;
      return  x.length() == 1 ? "0"+x : x ;
   }
   public static void main( String [] args )throws Exception {
      if( args.length == 0 ){
         System.err.println( "Usage : ... <file> ..." ) ;
         System.exit(4);
      }
      File file = new File( args[0] ) ;
      long len = file.length() ;
      byte [] data = new byte[(int)len] ;

      DataInputStream in = new DataInputStream( new FileInputStream( file ) ) ;
      in.readFully(data) ;
      in.close() ;
      Base64.displayHex( data ) ;



      /*
      byte [] data = new byte[args.length] ;
      for( int i = 0 ; i < args.length ; i++ ){
         data[i] = (byte)Integer.parseInt(args[i]) ;
         System.out.print(byteToHex(data[i])) ;
      }

      System.out.println("");
      displayHex( data ) ;
      System.out.println("");
      String xx = Base64.encode( data ) ;
      System.out.println(xx) ;

      byte [] dd = Base64.decode( xx ) ;
      for( int i = 0 ; i < dd.length ; i++ ){
         int d = dd[i] ;
         System.out.print( ""+(d < 0 ? ( d+256 ) : d )+" " ) ;
      }
      System.out.println("");
      */
      System.exit(0);
   }
}
