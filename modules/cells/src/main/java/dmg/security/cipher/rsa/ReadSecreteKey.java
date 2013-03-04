package dmg.security.cipher.rsa ;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class ReadSecreteKey {

   public final static int PUBLIC_KEY_ENCRYPTED    = 0x01 ;
   public final static int SECRET_KEY_ENCRYPTED    = 0x02 ;
   public final static int SECRET_KEY_CERTIFICATE  = 0x05 ;
   public final static int PUBLIC_KEY_CERTIFICATE  = 0x06 ;

   DataInputStream _input ;

   int _ctb ;
   int _version ;
   int _timestamp ;
   int _validity ;
   int _checkSum ;
   int _secreteAlgorithm ;
   int _publicAlgorithm ;

   BigInteger _d , _p , _q , _u , _e , _n ;

   public ReadSecreteKey( String filename )
          throws FileNotFoundException, IOException       {

        _input =
         new DataInputStream(
         new BufferedInputStream(
         new FileInputStream( filename ) ) ) ;

         _read() ;
   }
   public String toString(){
      StringBuilder sb = new StringBuilder() ;

      sb.append(" Validity : ").append(_validity).append(" Days\n");
      sb.append(" n = ").append(_n.toString(16)).append("\n");
      sb.append(" p = ").append(_p.toString(16)).append("\n");
      sb.append(" q = ").append(_q.toString(16)).append("\n");
      sb.append(" e = ").append(_e.toString(16)).append("\n");
      sb.append(" d = ").append(_d.toString(16)).append("\n");
      sb.append(" u = ").append(_u.toString(16)).append("\n");

      BigInteger p = _e.multiply( _d ) ;
      sb.append(" (e*d)       = ").append(p.toString(16)).append("\n");
      BigInteger pmodn = p.mod( p ) ;
      sb.append(" (e*d) mod n = ").append(pmodn.toString(16)).append("\n");
      return sb.toString() ;

   }
   private BigInteger readMPI() throws IOException {
      int rc ;
      int len  = _input.readUnsignedShort() ;
      System.out.println( " trying to read "+len ) ;
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
   private int _read() throws IOException {
    int length ;
    try{
      _ctb = _input.readUnsignedByte() ;
    }catch( EOFException x ){
      return 0 ;
    }
      if( ( _ctb & 0x80 ) == 0 ) {
          throw new IOException("NOT a Cipher Type Byte");
      }
    try{

      int type = ( _ctb >>> 2 ) & 0xF ;
      if( type != SECRET_KEY_CERTIFICATE ) {
          throw new IOException("NOT a SECRET_KEY_ENCRYPTED");
      }

      int lengthLength = _ctb & 0x3 ;
      if( lengthLength == 0 ){
         length = _input.readUnsignedByte() ;
      }else if( lengthLength == 1 ){
         length = _input.readUnsignedShort() ;
      }else if( lengthLength == 2 ){
         length = _input.readInt() ;
      }else{
         throw new IOException( "can't lengthLength = "+lengthLength) ;
      }
      _version   = _input.readUnsignedByte() ;
      _timestamp = _input.readInt() ;
      _validity  = _input.readUnsignedShort() ;
      _publicAlgorithm = _input.readUnsignedByte() ;
      if( _publicAlgorithm != 1 ) {
          throw new IOException("Can't read Non RSA " + _publicAlgorithm);
      }
      _n  =   readMPI() ;
      _e  =   readMPI() ;
      _secreteAlgorithm = _input.readUnsignedByte() ;
      if( _secreteAlgorithm != 0 ) {
          throw new IOException("Can't decrypt sa : " + _secreteAlgorithm);
      }
      _d  =   readMPI() ;
      _p  =   readMPI() ;
      _q  =   readMPI() ;
      _u  =   readMPI() ;
      _checkSum = _input.readUnsignedShort() ;

    }catch( EOFException eof ){
      throw new IOException( "Premature EOF encountered" ) ;
    }
    return 1 ;

   }
   public static void main( String[] args ){
     if( args.length < 1 ){
       System.err.println( " USAGE : ... <secretKeyRing> " ) ;
       System.exit(4) ;
     }
     try {
       ReadSecreteKey rsa = new ReadSecreteKey( args[0] ) ;
       System.out.println( rsa ) ;
     }catch( IOException e ){
       System.err.println( " Sorry : "+e ) ;
     }
   }
}
