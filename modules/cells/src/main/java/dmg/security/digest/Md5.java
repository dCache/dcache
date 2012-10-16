package dmg.security.digest ;

public class Md5 implements MsgDigest {
   private static final long [] _f1_a = {
      0xd76aa478L,
      0xe8c7b756L, 
      0x242070dbL,
      0xc1bdceeeL,
      0xf57c0fafL, 
      0x4787c62aL,
      0xa8304613L, 
      0xfd469501L, 
      0x698098d8L,
      0x8b44f7afL,
      0xffff5bb1L,
      0x895cd7beL,
      0x6b901122L, 
      0xfd987193L,
      0xa679438eL,
      0x49b40821L,
   } ;
   private static final int [] _f1_b = { 7 , 12 , 17 , 22 } ;

   private static final long [] _f2_a = {
      0xf61e2562L,
      0xc040b340L,
      0x265e5a51L,
      0xe9b6c7aaL,
      0xd62f105dL,
      0x02441453L,
      0xd8a1e681L,
      0xe7d3fbc8L,
      0x21e1cde6L,
      0xc33707d6L,
      0xf4d50d87L,
      0x455a14edL,
      0xa9e3e905L,
      0xfcefa3f8L,
      0x676f02d9L,
      0x8d2a4c8aL,
   };
   private static final int [] _f2_b = { 5 , 9 , 14 , 20 } ;

   private static final long [] _f3_a = {
      0xfffa3942L,
      0x8771f681L,
      0x6d9d6122L,
      0xfde5380cL,
      0xa4beea44L,
      0x4bdecfa9L,
      0xf6bb4b60L,
      0xbebfbc70L,
      0x289b7ec6L,
      0xeaa127faL,
      0xd4ef3085L,
      0x04881d05L,
      0xd9d4d039L,
      0xe6db99e5L,
      0x1fa27cf8L,
      0xc4ac5665L,
   };
   private static final int [] _f3_b = { 4 , 11 , 16 , 23 } ;
   
   private static final long [] _f4_a = {

      0xf4292244L,
      0x432aff97L,
      0xab9423a7L,
      0xfc93a039L,
      0x655b59c3L,
      0x8f0ccc92L,
      0xffeff47dL,
      0x85845dd1L,
      0x6fa87e4fL,
      0xfe2ce6e0L,
      0xa3014314L,
      0x4e0811a1L,
      0xf7537e82L,
      0xbd3af235L,
      0x2ad7d2bbL,
      0xeb86d391L,
   };
   private static final int [] _f4_b = { 6 , 10 , 15 , 21 } ;
   private long [] _buf  = new long[4] ;
   private long [] _bits = new long[2] ;
   private byte [] _in   = new byte[64] ;
   public Md5(){
      _init() ;
   } 
   @Override
   public void reset(){ _init() ; }
   @Override
   public void update( byte [] data ){ update( data , 0 ,data.length ) ; }
   @Override
   public void update( byte [] data , int off , int size ){
      int t = (int)_bits[0] ;
      if( (_bits[0] = ( t + ((long)size << 3 ) ) & 0xffffffff ) < t ) {
          _bits[1]++;
      }
      _bits[1] += size >> 29 ;
      
            
      t = ( t >> 3 ) & 0x3f ;
      
//      l2hex( "update bits= " , _bits ) ;
//      l2hex( "update t   = " , t ) ;

      if( t != 0 ){
         int p = t ;
         t = 64 - t ;
         if( size < t ){
             System.arraycopy( data , off , _in , p , size ) ;
             return ;
         }
         System.arraycopy( data , off , _in , p , t ) ;
         _transform() ;
         off  += t ;
         size -= t ;
      }
      while( size >= 64 ){
          System.arraycopy( data , off , _in , 0 , 64 ) ;
          _transform() ;
          off  += 64 ;
          size -= 64 ;
      }
      
      System.arraycopy( data , off , _in , 0 , size ) ;
   }
   @Override
   public byte []  digest(){
      byte [] result = new byte[16] ;
      
      int count = (int)( _bits[0] >> 3 ) & 0x3F ;
      
      int p = count ;
      
      _in[p++] = -128 ; /* 0x80 */
      
      count = 64 - 1 - count ;
      
      if( count < 8 ){
         for(int i = 0 ; i < count ; i++ ) {
             _in[p + i] = 0;
         }
         _transform() ;
         for(int i = 0 ; i < 56 ; i++ ) {
             _in[i] = 0;
         }
      }else{
         for(int i = 0 ; i < (count-8) ; i++ ) {
             _in[p + i] = 0;
         }
      }
      _in[56] = (byte) ( ( _bits[0] >>>  0  ) & 0xff );
      _in[57] = (byte) ( ( _bits[0] >>>  8  ) & 0xff );
      _in[58] = (byte) ( ( _bits[0] >>> 16  ) & 0xff );
      _in[59] = (byte) ( ( _bits[0] >>> 24  ) & 0xff );
      _in[60] = (byte) ( ( _bits[1] >>>  0  ) & 0xff );
      _in[61] = (byte) ( ( _bits[1] >>>  8  ) & 0xff );
      _in[62] = (byte) ( ( _bits[1] >>> 16  ) & 0xff );
      _in[63] = (byte) ( ( _bits[1] >>> 24  ) & 0xff );
//      b2hex( "Final in  = ",_in ) ;
//      l2hex( "Final buf = ",_buf ) ;
      
      _transform() ;

//      l2hex( "Final buf = ",_buf ) ;
      
      result[0]  = (byte) ( ( _buf[0] >>>  0  ) & 0xff );
      result[1]  = (byte) ( ( _buf[0] >>>  8  ) & 0xff );
      result[2]  = (byte) ( ( _buf[0] >>> 16  ) & 0xff );
      result[3]  = (byte) ( ( _buf[0] >>> 24  ) & 0xff );
      result[4]  = (byte) ( ( _buf[1] >>>  0  ) & 0xff );
      result[5]  = (byte) ( ( _buf[1] >>>  8  ) & 0xff );
      result[6]  = (byte) ( ( _buf[1] >>> 16  ) & 0xff );
      result[7]  = (byte) ( ( _buf[1] >>> 24  ) & 0xff );
      result[8]  = (byte) ( ( _buf[2] >>>  0  ) & 0xff );
      result[9]  = (byte) ( ( _buf[2] >>>  8  ) & 0xff );
      result[10] = (byte) ( ( _buf[2] >>> 16  ) & 0xff );
      result[11] = (byte) ( ( _buf[2] >>> 24  ) & 0xff );
      result[12] = (byte) ( ( _buf[3] >>>  0  ) & 0xff );
      result[13] = (byte) ( ( _buf[3] >>>  8  ) & 0xff );
      result[14] = (byte) ( ( _buf[3] >>> 16  ) & 0xff );
      result[15] = (byte) ( ( _buf[3] >>> 24  ) & 0xff );
      
      return result ;
   }
   
   private void _init(){
  
       _buf[0] = 0x67452301;
       _buf[1] = 0xefcdab89L ;
       _buf[2] = 0x98badcfeL  ;
       _buf[3] = 0x10325476;

       _bits[0] = 0;
       _bits[1] = 0;
       
//       l2hex( "init buf =" , _buf ) ;
//       l2hex( "init bits=" , _bits ) ;
          
   }
   private void  _transform(){
      long [] in = new long[16] ;
      for( int i = 0 ; i < 16 ; i++ ){
        in[i] = ((((long)_in[4*i+0])&0xff) <<  0 ) |
                ((((long)_in[4*i+1])&0xff) <<  8 ) |
                ((((long)_in[4*i+2])&0xff) << 16 ) |
                ((((long)_in[4*i+3])&0xff) << 24 ) ;
      }
//      b2hex( "transform in(bytes)=",_in) ;
//      l2hex( "transform in(long)=",in) ;
//      l2hex( "transform buf(long)=",_buf) ;
//      for( int i = 0 ; i < 16 ; i++ )
//        System.out.println( Long.toHexString( in[i] ) ) ; 
      long a = _buf[0] ,
           b = _buf[1] , 
           c = _buf[2] ,
           d = _buf[3] ; 
           
      a+=(d^(b&(c^d)))+in[0] +_f1_a[0] ;a&=0xffffffffL;a=(a<<_f1_b[0])|(a>>>(32-_f1_b[0]));a+=b;
      d+=(c^(a&(b^c)))+in[1] +_f1_a[1] ;d&=0xffffffffL;d =(d<<_f1_b[1])|(d>>>(32-_f1_b[1]));d+=a;
      c+=(b^(d&(a^b)))+in[2] +_f1_a[2] ;c&=0xffffffffL;c=(c<<_f1_b[2])|(c>>>(32-_f1_b[2]));c+=d;
      b+=(a^(c&(d^a)))+in[3] +_f1_a[3] ;b&=0xffffffffL;b =(b<<_f1_b[3])|(b>>>(32-_f1_b[3]));b+=c;
      a+=(d^(b&(c^d)))+in[4] +_f1_a[4] ;a&=0xffffffffL;a =(a<<_f1_b[0])|(a>>>(32-_f1_b[0]));a+=b;
      d+=(c^(a&(b^c)))+in[5] +_f1_a[5] ;d&=0xffffffffL;d =(d<<_f1_b[1])|(d>>>(32-_f1_b[1]));d+=a;
      c+=(b^(d&(a^b)))+in[6] +_f1_a[6] ;c&=0xffffffffL;c =(c<<_f1_b[2])|(c>>>(32-_f1_b[2]));c+=d;
      b+=(a^(c&(d^a)))+in[7] +_f1_a[7] ;b&=0xffffffffL;b =(b<<_f1_b[3])|(b>>>(32-_f1_b[3]));b+=c;
      a+=(d^(b&(c^d)))+in[8] +_f1_a[8] ;a&=0xffffffffL;a =(a<<_f1_b[0])|(a>>>(32-_f1_b[0]));a+=b;
      d+=(c^(a&(b^c)))+in[9] +_f1_a[9] ;d&=0xffffffffL;d =(d<<_f1_b[1])|(d>>>(32-_f1_b[1]));d+=a;
      c+=(b^(d&(a^b)))+in[10]+_f1_a[10];c&=0xffffffffL;c =(c<<_f1_b[2])|(c>>>(32-_f1_b[2]));c+=d;
      b+=(a^(c&(d^a)))+in[11]+_f1_a[11];b&=0xffffffffL;b =(b<<_f1_b[3])|(b>>>(32-_f1_b[3]));b+=c;
      a+=(d^(b&(c^d)))+in[12]+_f1_a[12];a&=0xffffffffL;a =(a<<_f1_b[0])|(a>>>(32-_f1_b[0]));a+=b;
      d+=(c^(a&(b^c)))+in[13]+_f1_a[13];d&=0xffffffffL;d =(d<<_f1_b[1])|(d>>>(32-_f1_b[1]));d+=a;
      c+=(b^(d&(a^b)))+in[14]+_f1_a[14];c&=0xffffffffL;c =(c<<_f1_b[2])|(c>>>(32-_f1_b[2]));c+=d;
      b+=(a^(c&(d^a)))+in[15]+_f1_a[15];b&=0xffffffffL;b =(b<<_f1_b[3])|(b>>>(32-_f1_b[3]));b+=c;
//      l2hex( "transform a1 = ",a ) ;
//      l2hex( "transform b1 = ",b ) ;
//      l2hex( "transform c1 = ",c ) ;
//      l2hex( "transform d1 = ",d ) ;
      
      a+=(c^(d&(b^c)))+in[1] +_f2_a[0]; a&=0xffffffffL;a =(a<<_f2_b[0])|(a>>>(32-_f2_b[0]));a+=b;
      d+=(b^(c&(a^b)))+in[6] +_f2_a[1]; d&=0xffffffffL;d =(d<<_f2_b[1])|(d>>>(32-_f2_b[1]));d+=a;
      c+=(a^(b&(d^a)))+in[11]+_f2_a[2]; c&=0xffffffffL;c =(c<<_f2_b[2])|(c>>>(32-_f2_b[2]));c+=d;
      b+=(d^(a&(c^d)))+in[0] +_f2_a[3]; b&=0xffffffffL;b =(b<<_f2_b[3])|(b>>>(32-_f2_b[3]));b+=c;
      a+=(c^(d&(b^c)))+in[5] +_f2_a[4]; a&=0xffffffffL;a =(a<<_f2_b[0])|(a>>>(32-_f2_b[0]));a+=b;
      d+=(b^(c&(a^b)))+in[10]+_f2_a[5]; d&=0xffffffffL;d =(d<<_f2_b[1])|(d>>>(32-_f2_b[1]));d+=a;
      c+=(a^(b&(d^a)))+in[15]+_f2_a[6]; c&=0xffffffffL;c =(c<<_f2_b[2])|(c>>>(32-_f2_b[2]));c+=d;
      b+=(d^(a&(c^d)))+in[4] +_f2_a[7]; b&=0xffffffffL;b =(b<<_f2_b[3])|(b>>>(32-_f2_b[3]));b+=c;
      a+=(c^(d&(b^c)))+in[9] +_f2_a[8]; a&=0xffffffffL;a =(a<<_f2_b[0])|(a>>>(32-_f2_b[0]));a+=b;
      d+=(b^(c&(a^b)))+in[14]+_f2_a[9]; d&=0xffffffffL;d =(d<<_f2_b[1])|(d>>>(32-_f2_b[1]));d+=a;
      c+=(a^(b&(d^a)))+in[3] +_f2_a[10];c&=0xffffffffL;c =(c<<_f2_b[2])|(c>>>(32-_f2_b[2]));c+=d;
      b+=(d^(a&(c^d)))+in[8] +_f2_a[11];b&=0xffffffffL;b =(b<<_f2_b[3])|(b>>>(32-_f2_b[3]));b+=c;
      a+=(c^(d&(b^c)))+in[13]+_f2_a[12];a&=0xffffffffL;a =(a<<_f2_b[0])|(a>>>(32-_f2_b[0]));a+=b;
      d+=(b^(c&(a^b)))+in[2] +_f2_a[13];d&=0xffffffffL;d =(d<<_f2_b[1])|(d>>>(32-_f2_b[1]));d+=a;
      c+=(a^(b&(d^a)))+in[7] +_f2_a[14];c&=0xffffffffL;c =(c<<_f2_b[2])|(c>>>(32-_f2_b[2]));c+=d;
      b+=(d^(a&(c^d)))+in[12]+_f2_a[15];b&=0xffffffffL;b =(b<<_f2_b[3])|(b>>>(32-_f2_b[3]));b+=c;

      a+=(b^c^d)+in[5] +_f3_a[0]; a&=0xffffffffL;a =(a<<_f3_b[0])|(a>>>(32-_f3_b[0]));a+=b;
      d+=(a^b^c)+in[8] +_f3_a[1]; d&=0xffffffffL;d =(d<<_f3_b[1])|(d>>>(32-_f3_b[1]));d+=a;
      c+=(d^a^b)+in[11]+_f3_a[2]; c&=0xffffffffL;c =(c<<_f3_b[2])|(c>>>(32-_f3_b[2]));c+=d;
      b+=(c^d^a)+in[14]+_f3_a[3]; b&=0xffffffffL;b =(b<<_f3_b[3])|(b>>>(32-_f3_b[3]));b+=c;
      a+=(b^c^d)+in[1] +_f3_a[4]; a&=0xffffffffL;a =(a<<_f3_b[0])|(a>>>(32-_f3_b[0]));a+=b;
      d+=(a^b^c)+in[4] +_f3_a[5]; d&=0xffffffffL;d =(d<<_f3_b[1])|(d>>>(32-_f3_b[1]));d+=a;
      c+=(d^a^b)+in[7] +_f3_a[6]; c&=0xffffffffL;c =(c<<_f3_b[2])|(c>>>(32-_f3_b[2]));c+=d;
      b+=(c^d^a)+in[10]+_f3_a[7]; b&=0xffffffffL;b =(b<<_f3_b[3])|(b>>>(32-_f3_b[3]));b+=c;
      a+=(b^c^d)+in[13]+_f3_a[8]; a&=0xffffffffL;a =(a<<_f3_b[0])|(a>>>(32-_f3_b[0]));a+=b;
      d+=(a^b^c)+in[0] +_f3_a[9]; d&=0xffffffffL;d =(d<<_f3_b[1])|(d>>>(32-_f3_b[1]));d+=a;
      c+=(d^a^b)+in[3] +_f3_a[10];c&=0xffffffffL;c =(c<<_f3_b[2])|(c>>>(32-_f3_b[2]));c+=d;
      b+=(c^d^a)+in[6] +_f3_a[11];b&=0xffffffffL;b =(b<<_f3_b[3])|(b>>>(32-_f3_b[3]));b+=c;
      a+=(b^c^d)+in[9] +_f3_a[12];a&=0xffffffffL;a =(a<<_f3_b[0])|(a>>>(32-_f3_b[0]));a+=b;
      d+=(a^b^c)+in[12]+_f3_a[13];d&=0xffffffffL;d =(d<<_f3_b[1])|(d>>>(32-_f3_b[1]));d+=a;
      c+=(d^a^b)+in[15]+_f3_a[14];c&=0xffffffffL;c =(c<<_f3_b[2])|(c>>>(32-_f3_b[2]));c+=d;
      b+=(c^d^a)+in[2] +_f3_a[15];b&=0xffffffffL;b =(b<<_f3_b[3])|(b>>>(32-_f3_b[3]));b+=c;
//      l2hex( "transform a3 = ",a ) ;
//      l2hex( "transform b3 = ",b ) ;
//      l2hex( "transform c3 = ",c ) ;
//      l2hex( "transform d3 = ",d ) ;
       
      a+=(c^(b|~d))+in[0] +_f4_a[0]; a&=0xffffffffL;a =(a<<_f4_b[0])|(a>>>(32-_f4_b[0]));a+=b;
      d+=(b^(a|~c))+in[7] +_f4_a[1]; d&=0xffffffffL;d =(d<<_f4_b[1])|(d>>>(32-_f4_b[1]));d+=a;
      c+=(a^(d|~b))+in[14]+_f4_a[2]; c&=0xffffffffL;c =(c<<_f4_b[2])|(c>>>(32-_f4_b[2]));c+=d;
      b+=(d^(c|~a))+in[5] +_f4_a[3]; b&=0xffffffffL;b =(b<<_f4_b[3])|(b>>>(32-_f4_b[3]));b+=c;
      a+=(c^(b|~d))+in[12]+_f4_a[4]; a&=0xffffffffL;a =(a<<_f4_b[0])|(a>>>(32-_f4_b[0]));a+=b;
      d+=(b^(a|~c))+in[3] +_f4_a[5]; d&=0xffffffffL;d =(d<<_f4_b[1])|(d>>>(32-_f4_b[1]));d+=a;
      c+=(a^(d|~b))+in[10]+_f4_a[6]; c&=0xffffffffL;c =(c<<_f4_b[2])|(c>>>(32-_f4_b[2]));c+=d;
      b+=(d^(c|~a))+in[1] +_f4_a[7]; b&=0xffffffffL;b =(b<<_f4_b[3])|(b>>>(32-_f4_b[3]));b+=c;
      a+=(c^(b|~d))+in[8] +_f4_a[8]; a&=0xffffffffL;a =(a<<_f4_b[0])|(a>>>(32-_f4_b[0]));a+=b;
      d+=(b^(a|~c))+in[15]+_f4_a[9]; d&=0xffffffffL;d =(d<<_f4_b[1])|(d>>>(32-_f4_b[1]));d+=a;
      c+=(a^(d|~b))+in[6] +_f4_a[10];c&=0xffffffffL;c =(c<<_f4_b[2])|(c>>>(32-_f4_b[2]));c+=d;
      b+=(d^(c|~a))+in[13]+_f4_a[11];b&=0xffffffffL;b =(b<<_f4_b[3])|(b>>>(32-_f4_b[3]));b+=c;
      a+=(c^(b|~d))+in[4] +_f4_a[12];a&=0xffffffffL;a =(a<<_f4_b[0])|(a>>>(32-_f4_b[0]));a+=b;
      d+=(b^(a|~c))+in[11]+_f4_a[13];d&=0xffffffffL;d =(d<<_f4_b[1])|(d>>>(32-_f4_b[1]));d+=a;
      c+=(a^(d|~b))+in[2] +_f4_a[14];c&=0xffffffffL;c =(c<<_f4_b[2])|(c>>>(32-_f4_b[2]));c+=d;
      b+=(d^(c|~a))+in[9] +_f4_a[15];b&=0xffffffffL;b =(b<<_f4_b[3])|(b>>>(32-_f4_b[3]));b+=c;
//      l2hex( "transform a4 = ",a ) ;
//      l2hex( "transform b4 = ",b ) ;
//      l2hex( "transform c4 = ",c ) ;
//      l2hex( "transform d4 = ",d ) ;

      _buf[0] += a ;
      _buf[1] += b ;
      _buf[2] += c ;
      _buf[3] += d ;

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
   static public void b2hex( String head , byte [] bytes ) {
	   
		  StringBuilder sb = new StringBuilder(head);

       for (byte aByte : bytes) {
           sb.append(byteToHexString(aByte)).append(" ");
       }
	       
      System.out.println( sb.toString() ) ;

   }
   static public String longToHexString( long [] longs ){
	  StringBuilder sb = new StringBuilder(longs.length +1);

       for (long aLong : longs) {
           sb.append(Long.toHexString(aLong)).append(" ");
       }
	  return sb.toString() ;   	   
   }
   static public void l2hex( String head , long [] longs ){

	  StringBuilder sb = new StringBuilder(head);

       for (long aLong : longs) {
           sb.append(Long.toHexString(aLong)).append(" ");
       }
      System.out.println( sb.toString() ) ;
   }
   static public void l2hex( String head , long l ){
      String out = head ;
      out += ( Long.toHexString( l ) ) ;
      System.out.println( out ) ;
   }
   public static void main( String [] args ) throws Exception {
       byte [] x;
       Md5ext md5ext = new Md5ext() ;
       for (String arg : args) {
           x = arg.getBytes();
           md5ext.update(x, 0, x.length);
       }
       x = md5ext.digest() ;
       System.out.println( "md5p : "+byteToHexString( x ) ) ;
       Md5 md5 = new Md5() ;
       for (String arg : args) {
           x = arg.getBytes();
           md5.update(x, 0, x.length);
       }
       x = md5.digest() ;
       System.out.println( "md5  : "+byteToHexString( x ) ) ;
   }
}
 
