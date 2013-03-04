package dmg.protocols.ssh ;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Date;
import java.util.Random;
import java.util.StringTokenizer;

import dmg.security.digest.Md5;

public class SshRsaKey  {

   private boolean    _fullIdentity;
   private int        _bits;
   private int        _cipherLength;
   private int        _blockLength;
   private BigInteger _n;
   private BigInteger _e;
   private BigInteger _d;
   private String     _comment;

   private final static int    SSH_CIPHER_NONE    = 0 ;
   private final static String AUTHFILE_ID_STRING =
           "SSH PRIVATE_KEY_FILE_FORMAT 1.1\n" ;


   public SshRsaKey( InputStream in ) throws IOException {
      _readSshIdentity( new DataInputStream( in ) ) ;
      _cipherLength = ( _n.bitLength() + 7 ) / 8 ;
      _blockLength  = _cipherLength - 3 - 8 ;
      _fullIdentity = true ;

   }
   public SshRsaKey( int bits , byte [] exponent , byte [] modulus ){
      _bits         = bits ;
      _n            = new BigInteger( 1 , modulus ) ;
      _e            = new BigInteger( 1 , exponent ) ;
      _comment      = "None" ;
      _cipherLength = ( _n.bitLength() + 7 ) / 8 ;
      _blockLength  = _cipherLength - 3 - 8 ;
      _fullIdentity = false ;
   }
   public SshRsaKey( int bits , BigInteger exponent ,
                                BigInteger modulus , String user ){
      _bits         = bits ;
      _n            = modulus ;
      _e            = exponent ;
      _comment      = user ;
      _cipherLength = ( _n.bitLength() + 7 ) / 8 ;
      _blockLength  = _cipherLength - 3 - 8 ;
      _fullIdentity = false ;
   }
   public SshRsaKey( int bits , byte [] modulus ){
      _bits         = bits ;
      _n            = new BigInteger( 1 , modulus ) ;
      _comment      = "None" ;
      _cipherLength = ( _n.bitLength() + 7 ) / 8 ;
      _blockLength  = _cipherLength - 3 - 8 ;
      _fullIdentity = false ;
   }
   public SshRsaKey( String str ){
      StringTokenizer st = new StringTokenizer( str ) ;
      try{
         String com = st.nextToken() ;
         try{
            _bits = new Integer(com);
         }catch( NumberFormatException nfe ){
            _bits    = new Integer(st.nextToken());
            _comment = com ;
         }
         _e    = new BigInteger( st.nextToken() ) ;
         _n    = new BigInteger( st.nextToken() ) ;
      }catch( Exception e ){
         throw new
         IllegalArgumentException( "Rsa : Public Key Format Problem : "+e ) ;
      }
      _cipherLength = ( _n.bitLength() + 7 ) / 8 ;
      _blockLength  = _cipherLength - 3 - 8 ;
      if( st.hasMoreTokens() ) {
          _comment = _comment == null ?
                  st.nextToken() :
                  _comment + " " + st.nextToken();
      } else if( _comment == null ) {
          _comment = "NoComment";
      }

      _fullIdentity = false ;

   }
   public byte [] encrypt( byte [] data ){
      return encrypt( data , 0 , data.length ) ;
   }
   public byte []  encrypt( byte [] data , int off , int size ) {
      return encryptBigInteger( data , off , size ).toByteArray() ;
   }
   public BigInteger encryptBigInteger( byte [] data , int off , int size ) {

     if( size > _blockLength ) {
         throw new
                 IllegalArgumentException(" Rsa : Max Blocksize exceeded");
     }

     byte [] in          = new byte [ _cipherLength ] ;
     int     randomCount = in.length - 3 - size ;
     Random  r           = new Random( new Date().getTime() ) ;
     byte [] randoms     = new byte [ randomCount ] ;

     r.nextBytes( randoms ) ;
     for( int i = 0 ; i < randoms.length ; i++ ) {
         while (randoms[i] == 0) {
             randoms[i] = (byte) r.nextInt();
         }
     }

     //
     //   [ 0 ] [ 2 ] [ randoms ... != 0 ] [0] [ data ... ]
     //
     in[0] = (byte) 0 ;
     in[1] = (byte) 2 ;
     System.arraycopy( randoms , 0 , in , 2 , randoms.length ) ;
     in[randoms.length+2] = (byte) 0 ;
     System.arraycopy( data , off , in , randoms.length+3 , size ) ;
     BigInteger x   = new BigInteger( 1 , in ) ;
     x = x.modPow( _e , _n ) ;

     return x ;
   }
   public byte [] decrypt( byte [] in ){

     BigInteger x = new BigInteger( 1 ,  in ) ;
     if( x.compareTo( _n ) > 0 ) {
         throw new IllegalArgumentException("Rsa : Cipher larger then modulus ");
     }

     x  = x.modPow( _d , _n ) ;
     in = x.toByteArray() ;

     if( in[0] != 2 ) {
         throw new
                 IllegalArgumentException("Rsa : protocol violation 2 != " + in[0]);
     }

     int i ;
     for( i = 1 ;
          ( i < in.length ) && ( in[i] != 0 ) ; i++ ) {
     }
     if( i == in.length ) {
         throw new
                 IllegalArgumentException("Rsa : Random delimiter missing");
     }

     i++ ; //skip the delimiter zero byte

     byte [] out = new byte[ in.length - i ] ;
     System.arraycopy( in , i , out , 0 , out.length ) ;

     return out ;

   }
   public byte [] toByteArray(){
      byte [] nArray = _n.toByteArray() ;
      byte [] eArray = _e.toByteArray() ;
      byte [] out    = new byte[nArray.length+eArray.length+8] ;
      int pos = 0 ;
      punchInt( out , pos , _bits ) ;
      pos += 4 ;
      punchShort( out , pos , eArray.length*8 ) ;
      pos += 2 ;
      System.arraycopy( eArray ,  0 ,out , pos ,  eArray.length ) ;
      pos += eArray.length ;
      punchShort( out , pos , nArray.length*8 ) ;
      pos += 2 ;
      System.arraycopy(  nArray ,  0 , out , pos , nArray.length ) ;
      return out ;
   }
   public boolean    equals( Object n ){
       if (this==n){
           return true;
       }
       if (n instanceof SshRsaKey){
           SshRsaKey other = (SshRsaKey) n;
           return _n.equals( other._n ) ;
       }
       return false;
   }
   public int        hashCode(){         return _n.hashCode() ; }
   public BigInteger getModulus(){       return _n ; }
   public int        getKeySize(){       return _bits ; }
   public String     getComment(){       return _comment ; }
   public byte []    getModulusBytes(){  return _n.toByteArray() ;  }
   public int        getModulusBits(){   return _bits ; }
   public boolean    isFullIdentity(){   return _fullIdentity ; }
   public String     getFingerPrint(){
      try{
         Md5 md5 = new Md5() ;
         md5.update( _n.toByteArray() ) ;
         return byteToHexString(  md5.digest() ) ;
      }catch( Exception e ){
        return "" ;
      }

   }
   public String toString(){
      StringBuilder sb = new StringBuilder() ;
      sb.append(" SshRsaKey : ").append(_fullIdentity ? "Full Identity" :
              _e == null ? "Modulus Only" : "Public Part").append("\n");
      sb.append(" Comment   : ").append(_comment).append("\n");
      sb.append(" KeySize   : ").append(_bits).append("\n");
      if( _e != null ) {
          sb.append(" Exp (e)   : ").append(_e.toString(10)).append("\n");
      }
      if( _fullIdentity ){
          sb.append(" Exp (d)   : ").append(_d.toString(10)).append("\n");
      }
      sb.append(" Modulus   : ").append(_n.toString(10)).append("\n");

      return sb.toString() ;
   }
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
    */
   private void _readSshIdentity( DataInputStream dataIn ) throws IOException{

        byte [] check = new byte[4] ;
        byte [] id    = new byte[AUTHFILE_ID_STRING.length()+1] ;
        dataIn.readFully( id ) ;
        //
        // now we could check .... AUTHFILE_ID_STRING
        //
        int cipherType = dataIn.readByte() ;

        if( cipherType != SSH_CIPHER_NONE ) {
            throw new IOException("Rsa : Sorry, Identity file encrypted");
        }

        int        waste      = dataIn.readInt() ;
                  _bits       = dataIn.readInt() ;
                   _n         = readBigInteger( dataIn ) ;
                   _e         = readBigInteger( dataIn ) ;
        String     _comment   = readString( dataIn ) ;
        dataIn.readFully( check ) ;
        if( ( check[0] != check[2] ) ||
            ( check[1] != check[3] )    ) {
            throw new IOException("Rsa : Check failed");
        }

                   _d = readBigInteger( dataIn ) ;
        BigInteger  u = readBigInteger( dataIn ) ;
        BigInteger  p = readBigInteger( dataIn ) ;
        BigInteger  q = readBigInteger( dataIn ) ;


//        System.out.println( " bits : "+_bits ) ;
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
       BigInteger n = new BigInteger( 1 , data ) ;
       return n ;
   }
   private void punchInt( byte [] a , int off , int value ){
      a[off+0] = (byte)( (value>>>24) & 0xff ) ;
      a[off+1] = (byte)( (value>>>16) & 0xff ) ;
      a[off+2] = (byte)( (value>>> 8) & 0xff ) ;
      a[off+3] = (byte)( (value>>> 0) & 0xff ) ;
   }
   private void punchShort( byte [] a , int off , int value ){
      a[off+0] = (byte)( (value>>> 8) & 0xff ) ;
      a[off+1] = (byte)( (value>>> 0) & 0xff ) ;
   }
  static public String byteToHexString( byte b ) {
       String s = Integer.toHexString( ( b < 0 ) ? ( 256 + (int)b ) : (int)b  ) ;
       if( s.length() == 1 ) {
           return "0" + s;
       } else {
           return s;
       }
  }
  static public String byteToHexString( byte [] b ) {
       StringBuilder sb = new StringBuilder() ;
       for( int i = 0 ; i < b.length ; i ++ ) {
           sb.append(byteToHexString(b[i]))
                   .append(i == (b.length - 1) ? "" : ":");
       }
       return sb.toString() ;

  }
  public static void main( String [] args ){
     if( args.length != 1 ){
        System.err.println( " USAGE : ... <identityFile>" ) ;
        System.exit(4) ;
     }
     SshRsaKey key = null ;
     try{
        key = new SshRsaKey( new FileInputStream( args[0] ) ) ;
     }catch( Exception e ){
        System.err.println( "Problem reading : "+args[0] +" : "+e ) ;
        System.exit(4) ;
     }
     Random r  = new Random( new Date().getTime() ) ;
     byte [] challenge = new byte[32] ;

     for( int i = 0 ; i < 1000 ; i++ ){
        byte [] de = null ;
        byte [] re = null ;
        try{
           r.nextBytes( challenge ) ;
           de = key.encrypt( challenge ) ;
           BigInteger bi = new BigInteger(1,de)  ;
           System.out.println( " encrypted "+
                                bi.bitCount()+" "+
                                bi.bitLength() +" :\n"+byteToHexString( de ) ) ;
           re = key.decrypt( de ) ;
           if( challenge.length != re.length ){
              System.out.println( "Problem in decryption length : " ) ;
              System.out.println( " or : "+byteToHexString( challenge ) ) ;
              System.out.println( " en : "+byteToHexString( de ) ) ;
              System.out.println( " de : "+byteToHexString( re ) ) ;
              System.exit(5) ;
           }
           int l ;
           for( l = 0 ;
                ( l < challenge.length ) && ( challenge[l] == re[l] ) ; l++ ) {
           }
           if( l < challenge.length ){
              System.out.println( "Problem in decryption content : " ) ;
              System.out.println( " or : "+byteToHexString( challenge ) ) ;
              System.out.println( " en : "+byteToHexString( de ) ) ;
              System.out.println( " de : "+byteToHexString( re ) ) ;
              System.exit(5) ;
           }
           System.out.println( "O.K. " + i ) ;
        }catch( IllegalArgumentException iae ){
            System.out.println( "Problem  : "+iae ) ;
            System.out.println( " or : "+byteToHexString( challenge ) ) ;
            if( de != null ) {
                System.out.println(" en : " + byteToHexString(de));
            }
            if( re != null ) {
                System.out.println(" de : " + byteToHexString(re));
            }
            System.exit(5) ;

        }


     }
  }


}
