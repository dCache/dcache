package dmg.security.cipher.idea;

import java.util.Date;
import java.util.Random;

/**
  *  <strong>This module is an java implementation of the
  *  IDEA encryption algorithm. We are a registered Idea Developer.
  *  This license is only valid from 1.Mar.98 to Feb 2000 and as long
  *  as this code is used together with the EuroStore Project in the
  *  development phase. This does not include problems arising
  *  by domestic restrictions of using strong encryption code
  *  (France etc.).
  *  </strong><hr>
  *  The Jdea Class is an implementation of the Idea cipher
  *  algorithm. An instance of the class is always bound to
  *  the key, this instance was created with. The cbf64 vector
  *  can be set or obtained at any time. Currently only the
  *  ECB (electronic codebook) and the CFB (cipher feedback) are
  *  implemented.
  *
  *
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
  *  @version  0.1 (cell)
  */
public class Jdea {

    private long [] _k   = new long [6*9] ;
    private long [] _dk  = new long [6*9] ;
    private byte [] _vec = new byte[8] ;
    private int     _num;
    private byte [] _key = new byte [16]  ;

    public Jdea(){
       byte [] key  = new byte [_key.length ] ;
       Random  r    = new Random( new Date().getTime() ) ;
       r.nextBytes( key ) ;
       _Jdea( key ) ;
    }
    /*
     *
     *  Jdea creates an Idea cipher object and binds this object
     *  to the 128 bit Idea key. 'key' must be at least of
     *  16 bytes length. All residual bytes are ignored. The initial
     *  cfb vector is calculated as a function of 'key'. The
     *  vector can be obtained by 'getStartValue'.
     *  Another startvector may be set by 'setStartValue'.
     *  Setting the startvector or obtaining its value must be
     *  done before any encryption has been performed.
     *  get and set of the start vector doesn't fully restore
     *  the encryption state in the cfb mode. The start vector
     *  has no meaning in ECB mode.
     *
     */
    private void printSchedule( long [] x ){
       for( int j = 0 ; j < 6 ; j++ ){
         for( int i = 0 ; i < 9 ; i++ ) {
             System.out.print(Long.toHexString(x[6 * i + j]) + " ");
         }
         System.out.println("");
       }
    }
    public Jdea( byte [] key ){
       _Jdea( key ) ;
    }
    private void  _Jdea( byte [] key ){
       System.arraycopy( key , 0 , _key , 0 , _key.length ) ;
       for( int i = 0 ; i < _key.length /2 ; i ++ ) {
           _vec[i] = (byte) (_key[i] ^ _key[i + _vec.length]);
       }
       createEnKey( _key ) ;
       printSchedule( _k );
       createDeKey() ;
    }
    /**
      * 'getKeyBytes' returns a copy of the  128 bit idea key.
      */
    public byte [] getKeyBytes(){
       byte [] out = new byte[_key.length] ;
       System.arraycopy( _key , 0 , out , 0 , _key.length ) ;
       return out ;
    }
    /**
      * 'setStartValue' sets a new cfb vector but does not fully
      * restore the state when the start vector was taken with getStartValue.
      */
    public void setStartValue( byte [] vec ){
       System.arraycopy( vec , 0 , _vec , 0 ,
                         vec.length > _vec.length ? _vec.length : vec.length ) ;
       _num = 0 ;
    }
    /**
      * 'getStartValue' returns a copy of the  cfb vector. Getting the
      * startvector only makes sense before any encryption or decryption
      * has been performed. It is not able to fully restore the en/decryption
      * state;
      */
    public byte [] getStartValue(){
       byte [] vec = new byte[_vec.length] ;
       System.arraycopy( _vec , 0 , vec , 0 , _vec.length ) ;
       return vec ;
    }
    /**
      *  'encryptECB' encrypts 8 bytes of data in ECB mode starting at
      *  in[inOff]. The encrypted 8 bytes are written to 'out' starting
      *  at out[outOff]. The input bytes remain unchanged. The transformation
      *  is stateless. The same input
      *
      *  @param in Input byte array containing the data to be encrypted. The
      *            arraylength must be at least 'inOff+8' bytes.
      *  @param inOff Start position of the 'in' array.
      *  @param out Output array. The arraylength must be at least 'outOff+8' bytes.
      *  @param outOff Start position of the 'out' array.
      */
    public void encryptECB( byte [] in , int inOff , byte [] out , int outOff ){
      xcrypt( in , inOff , out , outOff , _k ) ;
    }
    /**
      *  'decryptECB' decrypts 8 bytes of data in ECB mode starting at
      *  in[inOff]. The decrypted 8 bytes are written to out starting
      *  at out[outOff]. The input bytes remain unchanged.
      *
      *  @param in Input byte array containing the data to be decrypted. The
      *            arraylength must be at least 'inOff+8' bytes.
      *  @param inOff Start position of the 'in' array.
      *  @param out Output array. The arraylength must be at least 'outOff+8' bytes.
      *  @param outOff Start position of the 'out' array.
      */
    public void decryptECB( byte [] in , int inOff , byte [] out , int outOff ){
      xcrypt( in , inOff , out , outOff , _dk ) ;
    }
    public void encryptCFB64( byte [] in  , int oIn  , int len ){
        encryptCFB64( in , oIn , in , oIn , len ) ;
    }
    public void decryptCFB64( byte [] in  , int oIn  , int len ){
        decryptCFB64( in , oIn , in , oIn , len ) ;
    }
    public void encryptCFB64( byte [] in  , int oIn  ,
                              byte [] out , int oOut , int len ){

        _num = cbf64_xcrypt( in , oIn , out , oOut , len ,
                             _k , _vec , _num , ENCRYPT   ) ;
    }
    public void decryptCFB64( byte [] in  , int oIn  ,
                              byte [] out , int oOut , int len ){
        //
        // NOTE : SAME Key as encrypt but different flag
        //
        _num = cbf64_xcrypt( in , oIn , out , oOut , len ,
                             _k , _vec , _num , DECRYPT   ) ;
    }
    private void createEnKey( byte [] key ){
      long r0 , r1 , r2 ;
      long [] k = new long[16] ;
      for( int i = 0 ; i < 16 ; i++ ) {
          k[i] = (key[i] < 0) ? (256 + (long) key[i]) : (long) key[i];
      }

      for( int i = 0 ; i < 8 ; i++ ) {
          _k[i] = (k[2 * i] << 8) | (k[2 * i + 1]);
      }

      int kt = 0 ;
      int kf = 0 ;
      kt += 8 ;
      for( int i = 0 ; i < 6 ; i ++ ){
         r2 = _k[kf+1] ;
         r1 = _k[kf+2] ;
         _k[kt++] = ( ( r2 << 9 ) | ( r1 >>> 7 ) ) & 0xffff ;
         r0 = _k[kf+3] ;
         _k[kt++] = ( ( r1 << 9 ) | ( r0 >>> 7 ) ) & 0xffff ;
         r1 = _k[kf+4] ;
         _k[kt++] = ( ( r0 << 9 ) | ( r1 >>> 7 ) ) & 0xffff ;
         r0 = _k[kf+5] ;
         _k[kt++] = ( ( r1 << 9 ) | ( r0 >>> 7 ) ) & 0xffff ;
         r1 = _k[kf+6] ;
         _k[kt++] = ( ( r0 << 9 ) | ( r1 >>> 7 ) ) & 0xffff ;
         r0 = _k[kf+7] ;
         _k[kt++] = ( ( r1 << 9 ) | ( r0 >>> 7 ) ) & 0xffff ;
         r1 = _k[kf+0] ;
         if( i >= 5 ) {
             break;
         }
         _k[kt++] = ( ( r0 << 9 ) | ( r1 >>> 7 ) ) & 0xffff ;
         _k[kt++] = ( ( r1 << 9 ) | ( r2 >>> 7 ) ) & 0xffff ;
         kf += 8 ;
      }

    }
    private void createDeKey(){
       int tp , fp  ;

       tp = 0 ;
       fp = 6 * 8 ;

       for( int r = 0 ; r < 9 ; r++ ){

          _dk[tp++] = inverse( _k[fp+0] ) ;
          _dk[tp++] = ((0x10000L - _k[fp + 2]) & 0xffff);
          _dk[tp++] = ((0x10000L - _k[fp + 1]) & 0xffff);
          _dk[tp++] = inverse( _k[fp+3] ) ;
          if( r == 8 ) {
              break;
          }
          fp -= 6 ;
          _dk[tp++] = _k[fp+4] ;
          _dk[tp++] = _k[fp+5] ;
       }

       long t ;

       t      = _dk[1] ;
       _dk[1] = _dk[2] ;
       _dk[2] = t ;

       t       = _dk[49] ;
       _dk[49] = _dk[50] ;
       _dk[50] = t ;
    }
    private long inverse( long xin ){
       long n1 , n2 , q , r , b1 , b2 , t ;

       if( xin == 0 ){
           b2=0;
       }else{
           n1=0x10001;
           n2=xin;
           b2=1;
           b1=0;
           while(true){
              r=(n1%n2);
              q=(n1-r)/n2;
              if (r == 0){
                  if( b2 < 0 ) {
                      b2 = 0x10001 + b2;
                  }
              }else{

                  n1=n2;
                  n2=r;
                  t=b2;
                  b2=b1-q*b2;
                  b1=t;
              }
              if( r == 0 ) {
                  break;
              }
           }
        }
        return( b2 );
    }
    private long mul( long a, long b ){
          long ul = a * b;
          long r ;
          if( ul != 0 ){
            r=(ul&0xffff)-(ul>>16);
	    r-=((r)>>16);
//	    if (r&0xffff0000L) r=(r+0x10001); */
          }else{
	    r=(-(int)a-b+1); /* assuming a or b is 0 and in range */
	  }
	  return r ;
    }
    private final int DECRYPT = 0 ;
    private final int ENCRYPT = 1 ;

    private int cbf64_xcrypt( byte [] in  , int oIn  ,
                              byte [] out , int oOut , int len ,
                              long [] key ,
                              byte [] vec , int num , int mode ){

       if( mode == ENCRYPT ){
          for( int i = 0 ; i < len ; i++ ){
             if( num == 0 ) {
                 xcrypt(vec, 0, vec, 0, key);
             }
             vec[num] = out[i+oOut] = (byte)(vec[num] ^ in[i+oIn]) ;
             num = ( num + 1 ) & 0x7 ;
          }
       }else{
          for( int i = 0 ; i < len ; i++ ){
             if( num == 0 ) {
                 xcrypt(vec, 0, vec, 0, key);
             }
             out[i+oOut] = (byte) ( vec[num] ^ ( vec[num] = in[i+oIn] ) );
             num = ( num + 1 ) & 0x7 ;
          }
       }
       return num ;
    }
    private void xcrypt( byte [] in  , int inOff ,
                         byte [] out , int outOff , long [] key ){
        long [] d = new long[2] ;

        d[0] = ((((long)in[inOff+0])&0xff) << 24 ) |
               ((((long)in[inOff+1])&0xff) << 16 ) |
               ((((long)in[inOff+2])&0xff) <<  8 ) |
               ((((long)in[inOff+3])&0xff) <<  0 ) ;
        d[1] = ((((long)in[inOff+4])&0xff) << 24 ) |
               ((((long)in[inOff+5])&0xff) << 16 ) |
               ((((long)in[inOff+6])&0xff) <<  8 ) |
               ((((long)in[inOff+7])&0xff) <<  0 ) ;

        xcrypt( d , key ) ;

        out[outOff+0] = (byte) ( ( d[0] >>> 24  ) & 0xff );
        out[outOff+1] = (byte) ( ( d[0] >>> 16  ) & 0xff );
        out[outOff+2] = (byte) ( ( d[0] >>>  8  ) & 0xff );
        out[outOff+3] = (byte) ( ( d[0] >>>  0  ) & 0xff );
        out[outOff+4] = (byte) ( ( d[1] >>> 24  ) & 0xff );
        out[outOff+5] = (byte) ( ( d[1] >>> 16  ) & 0xff );
        out[outOff+6] = (byte) ( ( d[1] >>>  8  ) & 0xff );
        out[outOff+7] = (byte) ( ( d[1] >>>  0  ) & 0xff );

    }
    public long bytesToLong( byte [] b , int off , int l ){
        l = l > 8 ? 8 : l ;
        l = l > b.length ? b.length : l ;
        long r = 0 ;
        int  s = 8 * ( l - 1 ) ;
        for( int i = 0 ; i < l ; i++ , s-=8 ) {
            r |= (((long) b[i]) & 0xff) << s;
        }
        return r ;

    }
    public long bytesToLong( byte [] b , int l ){
        l = l > 8 ? 8 : l ;
        l = l > b.length ? b.length : l ;
        long r = 0 ;
        int  s = 8 * ( l - 1 ) ;
        for( int i = 0 ; i < l ; i++ , s-=8 ) {
            r |= (((long) b[i]) & 0xff) << s;
        }
        return r ;

    }
    public void longToBytes( long lg , byte [] b , int l ){
        l = l > b.length ? b.length : l ;
        long r = 0 , n ;
        int  s = 8 * ( l - 1 ) ;
        for( int i = 0 ; i < l ; i++ , s-=8 ){
//          n = ( lg >>> s ) &0xff ;
//          b[i] = (byte)( (n & 0x80) != 0 ? ( 0x100 - n ) : n ) ;
            b[i] = (byte)( ( lg >>> s ) & 0xff ) ;
        }
    }
    static public String byteToHexString( byte b ) {
       String str = Integer.toHexString( ( b < 0 ) ? ( 256 + (int)b ) : (int)b  ) ;
       return str.length() == 1 ? "0"+str : str ;
    }
    static public String byteToHexString( byte [] bytes ) {

  	  StringBuilder sb = new StringBuilder(bytes.length +1);

        for (byte aByte : bytes) {
            sb.append(byteToHexString(aByte)).append(" ");
        }
         return sb.toString() ;
    }
    private void xcrypt( long [] d, long [] key ){
	int  i , p;
	long x1,x2,x3,x4,t0,t1,ul ;


	x2 = d[0];
	x1 = (x2>>16);
	x4 = d[1] ;
	x3 = (x4>>16);

	p = 0 ;
	for ( i = 0 ; i < 8; i++ ){

            x1 &= 0xffff;
            x1 =  mul( x1 , key[p++] ) ;

            x2 += key[p++] ;
            x3 += key[p++] ;

            x4 &= 0xffff;
            x4 = mul( x4 , key[p++] );

            t0 = ( x1 ^ x3 ) & 0xffff ;
            t0 = mul( t0 , key[p++] );

            t1 = ( t0 + ( x2^x4 ) ) & 0xffff;
            t1 = mul( t1 , key[p++] );

            t0 += t1;

            x1 ^= t1;
            x4 ^= t0;
            ul =x2 ^ t0;
            x2 =x3 ^ t1;
            x3 = ul;
        }

        x1 &= 0xffff;
        x1 = mul( x1, key[p++] );

        t0 = x3 + key[p++];
        t1 = x2 + key[p++];

        x4 &= 0xffff;
        x4 = mul( x4, key[p] );

        d[0] = ( t0 & 0xffff ) | ( ( x1 & 0xffff ) << 16 );
        d[1] = ( x4 & 0xffff ) | ( ( t1 & 0xffff ) << 16 );
    }
    public static void main( String [] args ){

       if( args.length < 16 ){
         System.err.println( " Jdea <key0> <key15> [<data0> ... <data15>]" ) ;
         System.exit(3) ;
       }

       byte [] key = new byte[16] ;
       for( int i = 0 ; i < key.length ; i ++ ) {
           key[i] = (byte) i;
       }
       for( int i = 0 ; i < 16 ; i++ ){
             key[i] = (byte)Long.parseLong( args[i] , 16 ) ;
       }

       Random r = new Random () ;
       byte [] vector = new byte [8] ;
       r.nextBytes( vector ) ;

       Jdea encrypt = new Jdea( key ) ;
       Jdea decrypt = new Jdea( key ) ;
       encrypt.setStartValue( vector ) ;
       decrypt.setStartValue( vector ) ;

       int restArgs = args.length - 16 ;

       byte [] b = new byte[restArgs] ;
       for( int i = 0 ; i < restArgs ; i++ ){
             b[i] = (byte)Long.parseLong( args[16+i] , 16 ) ;
       }
//        System.out.println( " Plain :  "+encrypt.byteToHexString(b) )  ;
//        encrypt.encryptCFB64( b , 0 , b.length ) ;
//        System.out.println( " Cypher : "+encrypt.byteToHexString(b) )  ;
//        decrypt.decryptCFB64( b , 0 , b.length  ) ;
//        System.out.println( " Plain :  "+encrypt.byteToHexString(b) )  ;
        System.out.println( " Plain :  "+encrypt.byteToHexString(b) )  ;
        encrypt.encryptECB( b , 0 , b , 0  ) ;
        System.out.println( " Cypher : "+encrypt.byteToHexString(b) )  ;
        decrypt.decryptECB( b , 0 , b , 0   ) ;
        System.out.println( " Plain :  "+encrypt.byteToHexString(b) )  ;


    }

}
