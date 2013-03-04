package dmg.security.cipher.pgp ;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;

public class PGPInputStream  extends FilterInputStream {

   private DataInputStream _input ;

   public PGPInputStream( InputStream in ){
      super( in ) ;
      _input = new DataInputStream(
               new BufferedInputStream( this ) ) ;
   }
   public PGPPacket readPGPPacket()
          throws IOException          {
       PGPPacket pgp ;

       while( ( pgp = _readPGPPacket() ) == null ) {
       }
       return pgp ;
   }
   private PGPPacket _readPGPPacket()
          throws IOException          {

    int length , type , lengthLength ;

    int ctb = _input.readUnsignedByte() ;
//    System.out.println( " ctb : "+ctb ) ;
    if( ( ctb & 0x80 ) == 0 ) {
        throw new IOException("NOT a Cipher Type Byte");
    }

    try{

      type = ( ctb >>> 2 ) & 0xF ;

      lengthLength = ctb & 0x3 ;
      if( lengthLength == 0 ){
         length = _input.readUnsignedByte() ;
      }else if( lengthLength == 1 ){
         length = _input.readUnsignedShort() ;
      }else if( lengthLength == 2 ){
         length = _input.readInt() ;
      }else{
         throw new IOException( "can't lengthLength = "+lengthLength) ;
      }
    }catch( EOFException eof ){
      throw new IOException( "Premature EOF Encountered" ) ;
    }
    PGPPacket pgpPacket;
    switch( type ){
      case PGPPacket.SECRET_KEY_CERTIFICATE :
         pgpPacket = _readSecretKeyCertificate( ctb , length ) ;
      break ;
      case PGPPacket.PUBLIC_KEY_CERTIFICATE :
         pgpPacket = _readPublicKeyCertificate( ctb , length ) ;
      break ;
      case PGPPacket.USER_ID_PACKET :
         pgpPacket = _readUserIdPacket( ctb , length ) ;
      break ;
      default :
         try{
            _input.skipBytes( length ) ;
            return null ;
         }catch( IOException ioe ){
            throw new IOException( "Premature EOF Encountered" ) ;
         }

    }
    return pgpPacket ;

   }
   private PGPPacket _readUserIdPacket( int ctb , int length )
           throws IOException {
       byte [] data = new byte [ length ] ;

       _input.readFully( data ) ;

       return new PGPUserIdPacket( ctb , new String( data ) ) ;
   }
   private PGPPacket _readPublicKeyCertificate( int ctb , int length )
           throws IOException {

      int version , timestamp , validity , publicAlgorithm ;

      version         = _input.readUnsignedByte() ;
      if( version != 3 ) {
          throw new IOException("Unsupported format version" + version);
      }

      timestamp       = _input.readInt() ;
      validity        = _input.readUnsignedShort() ;
      publicAlgorithm = _input.readUnsignedByte() ;

      if( publicAlgorithm != 1 ) {
          throw new IOException("Can't read Non RSA " + publicAlgorithm);
      }

      PGPKeyCertificate k =
                    new PGPKeyCertificate( ctb ,
                                           version ,
                                           timestamp ,
                                           validity  ,
                                           publicAlgorithm  ) ;

      BigInteger n  =   readMPI() ;
      BigInteger e  =   readMPI() ;

      k.setPublic( n , e ) ;

      return k ;
   }
   private PGPPacket _readSecretKeyCertificate( int ctb , int length )
           throws IOException {

      int version , timestamp , validity , publicAlgorithm ;

      version         = _input.readUnsignedByte() ;
      if( version != 3 ) {
          throw new IOException("Unsupported format version" + version);
      }

      timestamp       = _input.readInt() ;
      validity        = _input.readUnsignedShort() ;
      publicAlgorithm = _input.readUnsignedByte() ;

      if( publicAlgorithm != 1 ) {
          throw new IOException("Can't read Non RSA " + publicAlgorithm);
      }

      PGPSecretKeyCertificate k =
                    new PGPSecretKeyCertificate( ctb ,
                                                 version ,
                                                 timestamp ,
                                                 validity  ,
                                                 publicAlgorithm  ) ;

      BigInteger n  =   readMPI() ;
      BigInteger e  =   readMPI() ;

      k.setPublic( n , e ) ;

      int secretAlgorithm = _input.readUnsignedByte() ;
      if( secretAlgorithm != 0 ) {
          throw new IOException("Can't decrypt sa : " + secretAlgorithm);
      }

      BigInteger d  =   readMPI() ;
      BigInteger p  =   readMPI() ;
      BigInteger q  =   readMPI() ;
      BigInteger u  =   readMPI() ;

      int checkSum = _input.readUnsignedShort() ;

      k.setPrivate( d , p , q , u ) ;

      return k ;
   }
   private BigInteger readMPI() throws IOException {
      int rc ;
      int len  = _input.readUnsignedShort() ;
//      System.out.println( " trying to read "+len ) ;
      len = ( len / 8 ) + ((( len % 8 ) != 0 ) ? 1 : 0 );
      byte [] data = new byte[len] ;
      if( ( rc = _input.read( data ) ) < len ) {
          throw new
                  IOException("Premature EOF encountered(2) " + rc + "<" + len);
      }

      try{
        return new BigInteger( 1 , data ) ;
      }catch( NumberFormatException nfe ){
        throw new IOException( "Not a BigInteger" ) ;
      }
   }

   public static void main( String [] args ){
     if( args.length < 1 ){
       System.err.println( " USAGE : ... <secretKeyRing> " ) ;
       System.exit(4) ;
     }
     String filename = args[0] ;
     try{
         PGPInputStream  pgpInput =
             new PGPInputStream(  new FileInputStream( filename ) ) ;
         PGPPacket pgp ;
         while( ( pgp = pgpInput.readPGPPacket() ) != null ){
         if( pgp instanceof PGPSecretKeyCertificate ){
           PGPSecretKeyCertificate s = (PGPSecretKeyCertificate)pgp ;
           System.out.println( s.toString() ) ;
         }else if( pgp instanceof PGPKeyCertificate ){
           PGPKeyCertificate s = (PGPKeyCertificate)pgp ;
           System.out.println( s.toString() ) ;
         }else if( pgp instanceof PGPUserIdPacket ){
            System.out.println( pgp.toString() );
         }else{
           System.out.println( " Don't know : "+pgpInput.getClass().toString() );
         }
         }
     }catch( IOException ioe ){
       System.out.println( " Exception : "+ioe );
     }

   }
}
