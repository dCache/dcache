package dmg.security.cipher.rsa ;

import java.math.BigInteger;
import java.util.Date;
import java.util.Random;

import dmg.security.cipher.IllegalEncryptionException;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class RsaEncryption {

  private RsaEncryptionKey [] _key = new RsaEncryptionKey[2] ;
  private BigInteger [] _e = new BigInteger[2] ;
  private BigInteger [] _n = new BigInteger[2] ;
  private int           _blockLength ;
  private int           _cipherLength ;

  public RsaEncryption( RsaEncryptionKey key1 ,
                        RsaEncryptionKey key2  )
         throws IllegalArgumentException          {

     BigInteger n1 = key1.getModulus() ;
     BigInteger n2 = key2.getModulus() ;
     //
     //  adjust n1 to be smaller or equal to n2
     //
     if( n1.compareTo( n2 ) > 0 ){
        _key[0] = key2 ;
        _key[1] = key1 ;
     }else{
        _key[0] = key1 ;
        _key[1] = key2 ;
     }
     for( int i = 0 ; i < 2 ; i++ ){
        _e[i] = _key[i].getExponent() ;
        _n[i] = _key[i].getModulus() ;
     }
     _cipherLength = ( _n[0].bitLength() + 7 ) / 8 ;
     _blockLength  = _cipherLength - 3 - 8 ;
  }
  public int getMaxBlockSize(){ return _blockLength ; }
  protected int getCipherBlockLength() { return _cipherLength ; }
  public byte [] encrypt( byte [] data , int off , int size )
     throws IllegalEncryptionException {

     if( size > _blockLength ) {
         throw new IllegalEncryptionException("Max Blocksize exceeded");
     }
     byte [] in = new byte [ _cipherLength ] ;

     int    randomCount = in.length - 3 - size ;
     Random r           = new Random( new Date().getTime() ) ;
     byte [] randoms    = new byte [ randomCount ] ;

     r.nextBytes( randoms ) ;
     for( int i = 0 ; i < randoms.length ; i++ ) {
         while (randoms[i] == 0) {
             randoms[i] = (byte) r.nextInt();
         }
     }

     //
     //   [ 0 ] [ 4 ] [ randoms ... != 0 ] [0] [ data ... ]
     //
     in[0] = (byte) 0 ;
     in[1] = (byte) 4 ;
     System.arraycopy( randoms , 0 , in , 2 , randoms.length ) ;
     in[randoms.length+2] = (byte) 0 ;
     System.arraycopy( data , off , in , randoms.length+3 , size ) ;
     BigInteger x   = new BigInteger( 1 , in ) ;
     for( int i = 0  ; i < 2 ; i++ ) {
         x = x.modPow(_e[i], _n[i]);
     }

     return x.toByteArray() ;
  }
  public byte [] decrypt( byte [] data , int off , int size )
     throws IllegalEncryptionException {

     byte [] in ;
     if( ( off != 0 ) || ( data.length != size ) ){
       in = new byte[ size ] ;
       System.arraycopy( data , off , in , 0 , size ) ;
     }else{
       in = data ;
     }
     BigInteger x = new BigInteger( in ) ;
     for( int i = 1 ; i >= 0 ; i -- ){
       if( x.compareTo( _n[i] ) > 0 ) {
           throw new IllegalEncryptionException("Cipher larger then modulus " + i);
       }
       x = x.modPow( _e[i] , _n[i] ) ;
     }
     in = x.toByteArray() ;
     if( in.length < _cipherLength-1 ) {
         throw new IllegalEncryptionException("Cipher length < " + (_cipherLength - 1));
     }

     if( in[0] != 4 ) {
         throw new IllegalEncryptionException("initial protocol violation " + in[0]);
     }

     int i ;
     for( i = 1 ;
          ( i < in.length ) && ( in[i] != 0 ) ; i++ ) {
     }
     if( i == in.length ) {
         throw new IllegalEncryptionException("random delimiter missing");
     }
     i++ ; //skip the delimiter zero byte

     byte [] out = new byte[ in.length - i ] ;
     System.arraycopy( in , i , out , 0 , out.length ) ;

     return out ;
  }

}
