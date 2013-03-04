package dmg.security.cipher ;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;

import dmg.security.cipher.rsa.RsaEncryptionKey;
/*
 *  ssh private key file (binary)
 *
 *   byte []    id  = "SSH PRIVATE KEY FILE FORMAT 1.1\n\0"
 *   byte        cypherType  ( 0 = SSH_CIPHER_NONE )
 *   int         reserved
 *   int         bits
 *   mp_int      n
 *   mp_int      e
 *   string      comment
 *   char [4]    a b a b
 *   mp_int      d
 *   mp_int      u
 *   mp_int      p
 *   mp_int      q
 *
 *       string :
 *                    int              length
 *                    byte [length]    data
 *       mp_int :
 *                    short            num of bits (nob)
 *                    byte [(nob+7)/8] data
  *
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
 */
public class      SshPrivateKeyInputStream
       extends    FilterInputStream
       implements EncryptionKeyInputStream {

   private DataInputStream _in ;

   private final static int    SSH_CIPHER_NONE    = 0 ;
   private final static String AUTHFILE_ID_STRING =
     "SSH PRIVATE_KEY_FILE_FORMAT 1.1\n" ;

   BigInteger _n;
   BigInteger _e;
   BigInteger _d;
   String     _comment;
   String  [] _domainList;
   int        _readCounter;

   public SshPrivateKeyInputStream( InputStream in ) {
      super( in ) ;
      _in = new DataInputStream( in ) ;

   }
   @Override
   public EncryptionKey readEncryptionKey()
          throws IOException {

     _readCounter ++ ;
     if( _readCounter == 1 ){
         _readAll() ;
         return new RsaEncryptionKey( _domainList , "private" , _d , _n ) ;
     }else if( _readCounter == 2 ){
         return new RsaEncryptionKey( _domainList , "public"  , _e , _n ) ;
     }
     return null ;
   }
   private void _readAll() throws IOException{

        byte [] check = new byte[4] ;
        byte [] id    = new byte[AUTHFILE_ID_STRING.length()+1] ;
        _in.readFully( id ) ;
        // now we could check ....
        int        cipherType = _in.readByte() ;
        int        waste      = _in.readInt() ;
        int        bits       = _in.readInt() ;
                   _n         = readBigInteger( _in ) ;
                   _e         = readBigInteger( _in ) ;
        String     _comment   = readString( _in ) ;
        _in.readFully( check ) ;
        if( ( check[0] != check[2] ) ||
            ( check[1] != check[3] )    ) {
            throw new IOException("check failed");
        }

                   _d = readBigInteger( _in ) ;
        BigInteger  u = readBigInteger( _in ) ;
        BigInteger  p = readBigInteger( _in ) ;
        BigInteger  q = readBigInteger( _in ) ;

        _domainList     = new String[1] ;
        _domainList[0]  = _comment ;

//        System.out.println( " bits : "+bits ) ;
//        System.out.println( " com  : "+_comment ) ;
//        System.out.println( " n    : "+_n.toString(16) ) ;
//        System.out.println( " e    : "+_e.toString(16) ) ;
//        System.out.println( " d    : "+_d.toString(16) ) ;

   }
   private String readString( DataInputStream in )
           throws IOException {

      int len = in.readInt() ;
      if( len > 2048 ) {
          throw new IOException("Comment String too long " + len);
      }
      byte [] data = new byte [ len ] ;
      in.readFully( data ) ;
      return new String( data ) ;
   }
   private BigInteger readBigInteger( DataInputStream in )
           throws IOException {

       int bits = in.readUnsignedShort() ;
       int len  = ( bits + 7 ) / 8 ;
       byte [] data = new byte[ len ] ;
       in.readFully( data ) ;
       BigInteger n = new BigInteger( data ) ;
       return n ;
   }
   public static void main( String [] args ){
      if( args.length != 1 ){
         System.out.println( "USAGE : ... <keyfilename> " ) ;
         System.exit(4) ;
      }
      try{
         SshPrivateKeyInputStream in = new SshPrivateKeyInputStream(
                                  new FileInputStream( args[0] ) ) ;
         EncryptionKey key ;
         while( ( key = in.readEncryptionKey() ) != null ){

            System.out.println( ""+key ) ;
         }

      }catch(IOException e ){
        System.err.println( " Exception : " + e ) ;
        System.exit(4);
      }

   }


}
